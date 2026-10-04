//! The async driver: performs a [`WriteMachine`]'s actions against an
//! [`EventStore`] (and, when the machine asks, a [`SnapshotStore`]),
//! feeding results back until the machine is done.
//!
//! A driver is a boring loop: interpret the action, do the I/O, report
//! the input. All policy — retries, version expectations, snapshot
//! cadence, conflict handling — lives in the machine, so the same loop
//! serves every aggregate and every store.

use futures::TryStreamExt;

use eventyr_core::aggregate::Aggregate;
use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::snapshot::HasSnapshotState;
use eventyr_core::write::{WriteAction, WriteInput, WriteMachine, WriteOutcome};

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
                // A snapshots-off machine never emits this; a snapshots-on
                // one driven here is a driver/protocol bug. Answer `None`
                // (no snapshot — always honest), letting the machine fall
                // back to a full stream read rather than violating the
                // protocol.
                machine.handle(WriteInput::SnapshotLoaded { snapshot: None })
            }
            WriteAction::Append {
                stream_id,
                expected,
                events,
            } => match store.append(&stream_id, expected, events).await {
                Ok(committed) => machine.handle(WriteInput::Appended { committed }),
                Err(StoreError::Conflict { current }) => {
                    machine.handle(WriteInput::Conflict { current })
                }
                Err(error) => machine.handle(WriteInput::Failed(error)),
            },
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
            WriteAction::LoadSnapshot { stream_id } => {
                match snapshots.load(&stream_id).await {
                    Ok(snapshot) => machine.handle(WriteInput::SnapshotLoaded { snapshot }),
                    Err(error) => machine.handle(WriteInput::Failed(error)),
                }
            }
            WriteAction::Append {
                stream_id,
                expected,
                events,
            } => match store.append(&stream_id, expected, events).await {
                Ok(committed) => machine.handle(WriteInput::Appended { committed }),
                Err(StoreError::Conflict { current }) => {
                    machine.handle(WriteInput::Conflict { current })
                }
                Err(error) => machine.handle(WriteInput::Failed(error)),
            },
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
