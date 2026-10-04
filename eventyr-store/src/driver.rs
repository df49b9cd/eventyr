//! The drivers: perform a [`WriteMachine`]'s actions against an
//! [`EventStore`] (and, when the machine asks, a [`SnapshotStore`]),
//! feeding results back until the machine is done.
//!
//! A driver is a boring loop: interpret the action, do the I/O, report
//! the input. All policy — retries, version expectations, snapshot
//! cadence, conflict handling — lives in the machine, so the same loop
//! serves every aggregate and every store, once per runtime: async
//! ([`drive_write`], [`drive_write_with_snapshots`]) for service code,
//! blocking ([`drive_write_blocking`],
//! [`drive_write_with_snapshots_blocking`]) for CLI and embedded use.

use futures::TryStreamExt;
use futures::executor::block_on;

use eventyr_core::aggregate::Aggregate;
use eventyr_core::batch::{BatchAction, BatchInput, BatchMachine, BatchOutcome, Decide};
use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::snapshot::HasSnapshotState;
use eventyr_core::write::{WriteAction, WriteInput, WriteMachine, WriteOutcome};

use crate::metrics::{Metrics, names};
use crate::snapshot_store::SnapshotStore;
use crate::store::EventStore;

/// Drives `machine` against `store` until it finishes, returning its
/// terminal outcome.
///
/// The loop performs each action the machine emits:
///
/// - `LoadStream` → read the stream from the requested bound, collect
///   the envelopes, report [`Loaded`](WriteInput::Loaded) — or
///   [`Failed`](WriteInput::Failed) when the read errors;
/// - `Append` → call [`append`](EventStore::append), reporting
///   [`Appended`](WriteInput::Appended), or mapping a
///   [`StoreError::Conflict`] to [`Conflict`](WriteInput::Conflict)
///   and anything else to [`Failed`](WriteInput::Failed);
/// - `Done` → stop and return the outcome.
///
/// Every store error — load or append — travels through the machine as
/// [`Failed`](WriteInput::Failed), so
/// [`Failed`](WriteOutcome::Failed) is the one failure shape callers
/// see: the machine owns the protocol, not the driver.
///
/// This is the snapshot-off driver: a snapshots-on machine would emit
/// [`LoadSnapshot`](WriteAction::LoadSnapshot), which this driver cannot
/// serve (it has no [`SnapshotStore`]); use
/// [`drive_write_with_snapshots`] for those machines. On a snapshot-off
/// machine the two are the same loop.
pub async fn drive_write<A, S>(
    machine: &mut WriteMachine<A>,
    store: &S,
) -> WriteOutcome<A::Event, A::Error>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    drive_write_with_metrics(machine, store, &crate::metrics::NoopMetrics).await
}

/// [`drive_write`] with metrics: every append reports a latency, every
/// conflict a counter tick, every snapshot load a read. The metrics are
/// the driver's view of its own I/O; the machine never sees them.
pub async fn drive_write_with_metrics<A, S>(
    machine: &mut WriteMachine<A>,
    store: &S,
    metrics: &(dyn Metrics + Send + Sync),
) -> WriteOutcome<A::Event, A::Error>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    let mut action = machine.start();
    loop {
        action = match action {
            WriteAction::LoadStream { stream_id, from } => {
                let loaded: Result<Vec<EventEnvelope<A::Event>>, StoreError> =
                    store.stream(&stream_id, from).try_collect().await;
                match loaded {
                    Ok(events) => machine.handle(WriteInput::Loaded { events }),
                    Err(error) => machine.handle(WriteInput::Failed(error)),
                }
            }
            WriteAction::LoadSnapshot { .. } => {
                machine.handle(WriteInput::SnapshotLoaded { snapshot: None })
            }
            WriteAction::Append {
                stream_id,
                expected,
                events,
            } => {
                let start = std::time::Instant::now();
                match store.append(&stream_id, expected, events).await {
                    Ok(committed) => {
                        metrics.histogram(names::APPEND_LATENCY, start.elapsed());
                        metrics.counter(names::APPENDS, committed.len() as u64);
                        machine.handle(WriteInput::Appended { committed })
                    }
                    Err(error) => {
                        if matches!(error, StoreError::Conflict { .. }) {
                            metrics.counter(names::CONFLICTS, 1);
                        }
                        machine.handle(error.into())
                    }
                }
            }
            WriteAction::Done(outcome) => return outcome,
        };
    }
}

/// Drives a snapshots-on `machine` against `store` and `snapshots`,
/// returning its terminal outcome.
///
/// Same loop as [`drive_write`], plus the one extra action:
///
/// - `LoadSnapshot` → [`SnapshotStore::load`];
///   [`SnapshotLoaded`](WriteInput::SnapshotLoaded) back to the machine,
///   or [`Failed`](WriteInput::Failed) when the read errors (a snapshot
///   read failing fails the interaction: the store just told the machine
///   its reads are broken);
/// - after a [`Committed`](WriteOutcome::Committed) outcome carrying a
///   snapshot offer, the driver persists it through
///   [`SnapshotStore::save`] **fire-and-forget**: the committed outcome
///   the caller sees is not affected by whether the save succeeded.
pub async fn drive_write_with_snapshots<A, S, SS>(
    machine: &mut WriteMachine<A, A::State>,
    store: &S,
    snapshots: &SS,
) -> WriteOutcome<A::Event, A::Error, A::State>
where
    A: HasSnapshotState,
    A::State: Clone + Send,
    S: EventStore<Event = A::Event>,
    SS: SnapshotStore<State = A::State>,
{
    drive_write_with_snapshots_and_metrics(machine, store, snapshots, &crate::metrics::NoopMetrics)
        .await
}

/// [`drive_write_with_snapshots`] with metrics (0.5.3): appends,
/// conflicts, and the snapshot save all report through `metrics`.
pub async fn drive_write_with_snapshots_and_metrics<A, S, SS>(
    machine: &mut WriteMachine<A, A::State>,
    store: &S,
    snapshots: &SS,
    metrics: &(dyn Metrics + Send + Sync),
) -> WriteOutcome<A::Event, A::Error, A::State>
where
    A: HasSnapshotState,
    A::State: Clone + Send,
    S: EventStore<Event = A::Event>,
    SS: SnapshotStore<State = A::State>,
{
    let mut action = machine.start();
    loop {
        action = match action {
            WriteAction::LoadStream { stream_id, from } => {
                let loaded: Result<Vec<EventEnvelope<A::Event>>, StoreError> =
                    store.stream(&stream_id, from).try_collect().await;
                match loaded {
                    Ok(events) => machine.handle(WriteInput::Loaded { events }),
                    Err(error) => machine.handle(WriteInput::Failed(error)),
                }
            }
            WriteAction::LoadSnapshot { stream_id } => match snapshots.load(&stream_id).await {
                Ok(snapshot) => {
                    metrics.counter(names::SNAPSHOTS, 1);
                    machine.handle(WriteInput::SnapshotLoaded { snapshot })
                }
                Err(error) => machine.handle(WriteInput::Failed(error)),
            },
            WriteAction::Append {
                stream_id,
                expected,
                events,
            } => {
                let start = std::time::Instant::now();
                match store.append(&stream_id, expected, events).await {
                    Ok(committed) => {
                        metrics.histogram(names::APPEND_LATENCY, start.elapsed());
                        metrics.counter(names::APPENDS, committed.len() as u64);
                        machine.handle(WriteInput::Appended { committed })
                    }
                    Err(error) => {
                        if matches!(error, StoreError::Conflict { .. }) {
                            metrics.counter(names::CONFLICTS, 1);
                        }
                        machine.handle(error.into())
                    }
                }
            }
            WriteAction::Done(outcome) => {
                // Fire-and-forget: persist the offer if the policy fired;
                // the caller's outcome is already fixed either way.
                if let WriteOutcome::Committed {
                    snapshot: Some(offer),
                    ..
                } = &outcome
                {
                    // A failed save is dropped on purpose: the snapshot is
                    // a cache, the commit already landed, and the next
                    // load's delta fold self-corrects a stale or missing
                    // snapshot.
                    let _ = snapshots.save(offer.clone().into_inner()).await;
                }
                return outcome;
            }
        };
    }
}

/// The blocking [`drive_write`]: drives `machine` to its outcome
/// without an async runtime, parking the thread through the store's
/// futures (a synchronous store's futures resolve immediately).
///
/// The machine's protocol is untouched — one loop per runtime, the
/// machine never knows which is driving it.
pub fn drive_write_blocking<A, S>(
    machine: &mut WriteMachine<A>,
    store: &S,
) -> WriteOutcome<A::Event, A::Error>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    block_on(drive_write(machine, store))
}

/// The blocking [`drive_write_with_snapshots`]: drives a snapshots-on
/// `machine` without an async runtime.
pub fn drive_write_with_snapshots_blocking<A, S, SS>(
    machine: &mut WriteMachine<A, A::State>,
    store: &S,
    snapshots: &SS,
) -> WriteOutcome<A::Event, A::Error, A::State>
where
    A: HasSnapshotState,
    A::State: Clone + Send,
    S: EventStore<Event = A::Event>,
    SS: SnapshotStore<State = A::State>,
{
    block_on(drive_write_with_snapshots(machine, store, snapshots))
}

/// Drives a [`BatchMachine`] against `store` until it finishes,
/// returning its terminal outcome.
///
/// The loop performs each action the machine emits:
///
/// - `LoadStreams` → read each requested stream from its requested
///   bound, collect the envelopes, and answer with one
///   [`Loaded`](BatchInput::Loaded) per stream — the machine re-emits
///   `LoadStreams` for whatever is still outstanding, and the loop
///   keeps answering until it transitions. The reads are sequential:
///   the machine needs every one before it decides, so concurrency here
///   would only complicate the driver;
/// - `AppendBatch` → [`append_batch`](EventStore::append_batch),
///   reporting [`Appended`](BatchInput::Appended), or mapping a
///   [`StoreError::Conflict`](StoreError::Conflict) to
///   [`Conflict`](BatchInput::Conflict) and anything else to
///   [`Failed`](BatchInput::Failed);
/// - `Done` → stop and return the outcome.
///
/// Every store error — load or append — travels through the machine as
/// [`Failed`](BatchInput::Failed), so
/// [`Failed`](BatchOutcome::Failed) is the one failure shape callers
/// see: the machine owns the protocol, not the driver.
pub async fn drive_write_batch<E, Err, D, S>(
    machine: &mut BatchMachine<E, Err, D>,
    store: &S,
) -> BatchOutcome<E, Err>
where
    D: Decide<E, Err>,
    S: EventStore<Event = E>,
{
    drive_write_batch_with_metrics(machine, store, &crate::metrics::NoopMetrics).await
}

/// [`drive_write_batch`] with metrics (0.5.3): the atomic batch append
/// reports a latency, a conflict-tick on conflict, and the per-stream
/// count of committed events on commit.
pub async fn drive_write_batch_with_metrics<E, Err, D, S>(
    machine: &mut BatchMachine<E, Err, D>,
    store: &S,
    metrics: &(dyn Metrics + Send + Sync),
) -> BatchOutcome<E, Err>
where
    D: Decide<E, Err>,
    S: EventStore<Event = E>,
{
    let mut action = machine.start();
    loop {
        action = match action {
            BatchAction::LoadStreams { streams, from } => {
                // Answer one stream at a time; the machine waits for
                // every outstanding stream before deciding, re-emitting
                // `LoadStreams` for the remainder after each — so the
                // loop keeps answering until it transitions. On an
                // error, report it and let the machine end.
                let mut next = None;
                for stream_id in streams {
                    let from = from.get(&stream_id).copied().unwrap_or_default();
                    let loaded: Result<Vec<EventEnvelope<E>>, StoreError> =
                        store.stream(&stream_id, from).try_collect().await;
                    let input = match loaded {
                        Ok(events) => BatchInput::Loaded { stream_id: stream_id.clone(), events },
                        Err(error) => BatchInput::Failed(error),
                    };
                    match machine.handle(input) {
                        BatchAction::LoadStreams { .. } => {} // still loading
                        action => {
                            next = Some(action);
                            break;
                        }
                    }
                }
                match next {
                    Some(action) => action,
                    // The machine kept re-emitting `LoadStreams` past the
                    // last outstanding stream — a protocol violation,
                    // surfaced the machine's way.
                    None => machine.handle(BatchInput::Failed(StoreError::other(
                        "the batch machine asked for more loads than it requested",
                    ))),
                }
            }
            BatchAction::AppendBatch { appends } => {
                let start = std::time::Instant::now();
                match store.append_batch(appends).await {
                    Ok(committed) => {
                        metrics.histogram(names::APPEND_LATENCY, start.elapsed());
                        let count: u64 = committed.iter().map(|c| c.events.len() as u64).sum();
                        metrics.counter(names::APPENDS, count);
                        machine.handle(BatchInput::Appended { committed })
                    }
                    Err(error) => {
                        if matches!(error, StoreError::Conflict { .. }) {
                            metrics.counter(names::CONFLICTS, 1);
                        }
                        match error {
                            StoreError::Conflict { stream_id, current } => {
                                // A batch conflict should name its stream;
                                // one that doesn't is attributed to the
                                // boundary's first stream — a wrong guess
                                // surfaces as a violation, never as a fold
                                // against the wrong stream's state.
                                let stream = eventyr_core::error::named_conflict_stream(
                                    stream_id,
                                    machine
                                        .streams()
                                        .first()
                                        .expect("a conflict follows an append to a non-empty boundary"),
                                );
                                machine.handle(BatchInput::Conflict { stream, current })
                            }
                            other => machine.handle(BatchInput::Failed(other)),
                        }
                    }
                }
            }
            BatchAction::Done(outcome) => return outcome,
        };
    }
}

/// The blocking [`drive_write_batch`]: drives `machine` to its outcome
/// without an async runtime, parking the thread through the store's
/// futures.
pub fn drive_write_batch_blocking<E, Err, D, S>(
    machine: &mut BatchMachine<E, Err, D>,
    store: &S,
) -> BatchOutcome<E, Err>
where
    D: Decide<E, Err>,
    S: EventStore<Event = E>,
{
    block_on(drive_write_batch(machine, store))
}

