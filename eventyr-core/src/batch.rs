//! The batch machine: the sans-IO state machine behind multi-stream
//! commands.
//!
//! A single [`WriteMachine`](crate::write::WriteMachine) commits to one
//! stream — the aggregate is the consistency boundary. Some commands
//! genuinely span two or more aggregates (the canonical transfer
//! between accounts); for those the boundary is a *fixed set* of
//! streams, decided per interaction by the caller, all read, guarded,
//! and written atomically. This is what [`BatchMachine`] runs: load
//! every stream of the boundary → fold each in isolation → batch-decide
//! against all folds → append all streams in one atomic, per-stream-
//! guarded commit.
//!
//! The protocol mirrors the write machine's. `start()` emits one
//! [`LoadStreams`](BatchAction::LoadStreams) for the whole (sorted,
//! deduplicated) boundary; the driver answers once per stream; the
//! machine folds each stream's events through that stream's own
//! [`Fold`] — there is no shared state, so cross-stream invariants live
//! in the [`Decide`] implementation, which reads *every* folded state —
//! and decides (validation) while routing each event to a stream
//! ([`BatchDecision::of`]). The result is one atomic
//! [`AppendBatch`](BatchAction::AppendBatch): every stream or none, each
//! guarded by its own expectation. On a conflict the machine re-reads
//! only the delta of the stream that moved and re-decides — a conflict
//! names exactly one stream.
//!
//! The atomic, per-stream-guarded commit the protocol ends in is what a
//! store must provide: `EventStore::append_batch` in the store crate.
//!
//! The dynamic half of eventcore's multi-stream commands — computing
//! the boundary from the command — lives at the call site: resolve the
//! command to its stream set, then call [`BatchMachine::new`]. The
//! machine takes a fixed set, which keeps the protocol (and its scripted
//! tests) the small thing here.

use alloc::collections::BTreeMap;
use alloc::collections::btree_map::Entry;
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use crate::envelope::{EventEnvelope, NewEvent};
use crate::error::{ProtocolError, StoreError};
use crate::vocabulary::{ExpectedVersion, StreamId, Version};
use crate::write::RetryPolicy;

/// The per-stream fold: the batch analogue of computing an
/// [`Aggregate::State`](crate::aggregate::Aggregate::State).
///
/// Object-safe and stateless — the machine holds each stream's state
/// (type-erased, created by [`initial`](Fold::initial)) and threads it
/// back through [`apply`](Fold::apply); [`Decide::decide`] receives the
/// folded states and downcasts each to the type its stream's `Fold`
/// produces. A blanket impl covers every
/// [`Aggregate`](crate::aggregate::Aggregate) via [`AggregateFold`].
pub trait Fold<E>: Send {
    /// The state before any event of this stream.
    fn initial(&self) -> Box<dyn Any + Send>;
    /// Fold one event into `state` (which `initial` made). Pure and
    /// total, like [`Aggregate::apply`](crate::aggregate::Aggregate::apply).
    fn apply(&self, state: &mut dyn Any, event: &E);
}

/// The [`Fold`] an [`Aggregate`](crate::aggregate::Aggregate) provides:
/// state is `A::State`, folded by
/// [`Aggregate::apply`](crate::aggregate::Aggregate::apply).
///
/// Construct with the stream's aggregate id —
/// [`Aggregate::initial`](crate::aggregate::Aggregate::initial) may
/// embed it in the state.
#[derive(Clone, Debug)]
pub struct AggregateFold<A: crate::aggregate::Aggregate>(pub A::Id);

impl<E, A> Fold<E> for AggregateFold<A>
where
    A: crate::aggregate::Aggregate<Event = E>,
    A::Id: Send,
    A::State: Send + 'static,
{
    fn initial(&self) -> Box<dyn Any + Send> {
        Box::new(A::initial(&self.0))
    }

    fn apply(&self, state: &mut dyn Any, event: &E) {
        let state = state
            .downcast_mut::<A::State>()
            .expect("the machine typed this state at `initial`");
        A::apply(state, event);
    }
}

/// A write-only [`Fold`]: for boundary streams that are guarded and
/// appended to but never folded or decided on, the state is `()`.
#[derive(Clone, Copy, Debug)]
pub struct NoFold;

impl<E> Fold<E> for NoFold {
    fn initial(&self) -> Box<dyn Any + Send> {
        Box::new(())
    }

    fn apply(&self, _: &mut dyn Any, _: &E) {}
}

/// The batch decision: pure, against every folded state of the
/// boundary.
///
/// Stateless like [`Aggregate`](crate::aggregate::Aggregate) — the
/// implementing type is a namespace. Cross-stream invariants live here:
/// `decide` reads *every* folded state of the boundary (`folded`,
/// keyed by stream id; downcast each to the state type that stream's
/// [`Fold`] produces — the machine guarantees the types line up with
/// the folds given at construction) and routes each accepted event to
/// its stream.
pub trait Decide<E, Err>: Send {
    /// The command type this decision consumes.
    type Command: Send;

    /// Decide the batch: the events and their target streams
    /// ([`BatchDecision::of`]), a rejection
    /// ([`BatchDecision::reject`]), or no change
    /// ([`BatchDecision::noop`]).
    fn decide(
        &self,
        folded: &BTreeMap<StreamId, Box<dyn Any + Send>>,
        command: &Self::Command,
    ) -> BatchDecision<E, Err>;
}

/// The decision [`Decide::decide`] returns: accept with events routed
/// to streams, reject, or no change.
///
/// Construct with [`of`](BatchDecision::of), [`reject`](BatchDecision::reject),
/// or [`noop`](BatchDecision::noop); the machine destructures it.
pub struct BatchDecision<E, Err> {
    outcome: Result<Vec<(NewEvent<E>, StreamId)>, Err>,
}

impl<E, Err> BatchDecision<E, Err> {
    /// Accept: append `events`, each routed to the stream at the same
    /// index of `targets` (the two vecs are the same length — every
    /// event needs a target). A target outside the machine's boundary
    /// is a protocol violation at emit time.
    ///
    /// An empty `events` is a noop that still round-trips the store;
    /// prefer [`noop`](BatchDecision::noop).
    pub fn of(events: Vec<E>, targets: Vec<StreamId>) -> Self {
        assert_eq!(
            events.len(),
            targets.len(),
            "every event needs a target stream",
        );
        Self {
            outcome: Ok(events
                .into_iter()
                .map(NewEvent::new)
                .zip(targets)
                .collect()),
        }
    }

    /// Like [`of`](BatchDecision::of), but each event keeps the
    /// [`Metadata`] the caller set on it.
    pub fn of_new_events(events: Vec<NewEvent<E>>, targets: Vec<StreamId>) -> Self {
        assert_eq!(
            events.len(),
            targets.len(),
            "every event needs a target stream",
        );
        Self {
            outcome: Ok(events.into_iter().zip(targets).collect()),
        }
    }

    /// Reject the command: a domain outcome (e.g. "insufficient
    /// funds"), not a failure. Nothing is appended.
    pub fn reject(error: Err) -> Self {
        Self {
            outcome: Err(error),
        }
    }

    /// The command decides nothing; nothing is appended. Use when the
    /// decision types `Err` but a no-change outcome carries no error.
    pub fn noop() -> Self {
        Self {
            outcome: Ok(Vec::new()),
        }
    }
}

/// One stream's events inside an atomic batch append.
#[derive(Clone, Debug)]
pub struct StreamAppend<E> {
    /// The stream to append to.
    pub stream_id: StreamId,
    /// This stream's own optimistic-concurrency expectation.
    pub expected: ExpectedVersion,
    /// The events to append, in stream order.
    pub events: Vec<NewEvent<E>>,
}

/// One stream's committed events inside a committed batch: one per
/// requested append, including the empty ones — the batch is a
/// transaction over the whole boundary, so the answer covers it whole.
#[derive(Clone, Debug)]
pub struct CommittedStream<E> {
    /// The stream the events were appended to.
    pub stream_id: StreamId,
    /// The committed events as the store recorded it (possibly none).
    pub events: Vec<EventEnvelope<E>>,
}

/// What the machine wants the driver to do.
///
/// Actions are data, not calls: the driver interprets each variant,
/// performs the I/O, and reports back with a [`BatchInput`].
#[derive(Clone, Debug)]
pub enum BatchAction<E, Err> {
    /// Read the given streams, each from `from` (exclusive) onward, one
    /// [`Loaded`](BatchInput::Loaded) answer per stream.
    ///
    /// Sorted and deduplicated at `start`; on a conflict retry exactly
    /// one stream — the one that moved.
    LoadStreams {
        /// The streams to read.
        streams: Vec<StreamId>,
        /// The exclusive lower bound.
        from: BTreeMap<StreamId, Version>,
    },
    /// Append all stream batches atomically: every append or none, each
    /// guarded by its own expectation.
    AppendBatch {
        /// The per-stream appends, in boundary order (sorted by stream).
        appends: Vec<StreamAppend<E>>,
    },
    /// Terminal: the interaction's outcome.
    Done(BatchOutcome<E, Err>),
}

/// What the driver reports back to the machine.
#[derive(Clone, Debug)]
pub enum BatchInput<E> {
    /// One requested stream's read completed. Events must belong to
    /// that stream and continue its sequence contiguously from the
    /// requested bound.
    Loaded {
        /// The stream these events belong to.
        stream_id: StreamId,
        /// The events read, in stream order.
        events: Vec<EventEnvelope<E>>,
    },
    /// The atomic batch append committed.
    Appended {
        /// One entry per requested append, as the store recorded it.
        committed: Vec<CommittedStream<E>>,
    },
    /// The append conflicted: `stream` is at `current`, not at the
    /// expected version.
    Conflict {
        /// The stream whose expectation failed.
        stream: StreamId,
        /// That stream's actual version at append time.
        current: Version,
    },
    /// A store operation failed — a stream read or the append.
    Failed(StoreError),
}

/// The terminal outcome of a driven batch machine.
#[derive(Clone, Debug)]
pub enum BatchOutcome<E, Err> {
    /// The batch committed atomically; one entry per requested append,
    /// as the store recorded it.
    Committed {
        /// One entry per requested append, as the store recorded it.
        committed: Vec<CommittedStream<E>>,
    },
    /// The decision produced no events; nothing was appended.
    Noop,
    /// The domain rejected the command.
    Rejected(Err),
    /// The store failed — a conflict that exhausted the retry budget, a
    /// transient error, or a fatal one. Protocol violations by the
    /// driver land here too (see [`ProtocolError`]).
    Failed(StoreError),
}

/// Which input the machine is waiting for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for one `Loaded` per outstanding stream.
    Loading,
    /// Waiting for `Appended`/`Conflict`/`Failed`.
    Appending,
    /// Terminal.
    Done,
}

/// The sans-IO machine behind a multi-stream command: load the whole
/// boundary → fold each stream → decide against all folds → append
/// atomically, with conflict retry.
///
/// The boundary is fixed at construction: `streams` is the set the
/// command touches (deduplicated, sorted), `folds` the per-stream
/// [`Fold`]s (a stream absent from `folds` is write-only: guarded,
/// never folded). The machine owns the folded states, the per-stream
/// versions, and the retry budget; it never does I/O — the driver
/// performs each [`BatchAction`] and reports back a [`BatchInput`].
///
/// Like the write machine, it never panics on bad input: a driver that
/// feeds the wrong input for the current phase, or drives a finished
/// machine, gets [`Done`](BatchAction::Done) with
/// [`Failed(StoreError::Other(ProtocolError))`](BatchOutcome::Failed).
pub struct BatchMachine<E, Err, D: Decide<E, Err>> {
    decide: D,
    command: D::Command,
    streams: Vec<StreamId>,
    folds: BTreeMap<StreamId, Box<dyn Fold<E>>>,
    retry_policy: RetryPolicy,
    retries_used: u32,
    phase: Phase,
    /// Folded state per boundary stream.
    folded: BTreeMap<StreamId, Box<dyn Any + Send>>,
    /// The version each stream's fold has reached. Monotone per stream.
    versions: BTreeMap<StreamId, Version>,
    /// The streams still expected to answer the current `LoadStreams`.
    pending: alloc::collections::BTreeSet<StreamId>,
    /// The metadata stamped onto every event this interaction emits
    /// (0.5.2): set by [`with_metadata`](Self::with_metadata), applied
    /// to each `StreamAppend`'s events at emit time.
    metadata: crate::envelope::Metadata,
}

impl<E, Err, D: Decide<E, Err>> BatchMachine<E, Err, D> {
    /// Begin an interaction over the fixed boundary `streams`.
    ///
    /// `streams` is the set the command touches — deduplicated and
    /// sorted here; the protocol and the atomic append speak the sorted
    /// set. `folds` gives a [`Fold`] per stream to rebuild state from; a
    /// stream absent from `folds` is write-only (guarded, appended,
    /// never folded or decided on — it gets a `()` state).
    pub fn new(
        streams: Vec<StreamId>,
        folds: BTreeMap<StreamId, Box<dyn Fold<E>>>,
        decide: D,
        command: D::Command,
        retry_policy: RetryPolicy,
    ) -> Self {
        let mut streams = streams;
        streams.sort();
        streams.dedup();
        let mut folded = BTreeMap::new();
        let mut versions = BTreeMap::new();
        for stream in &streams {
            let state = match folds.get(stream) {
                Some(fold) => fold.initial(),
                None => Box::new(()),
            };
            folded.insert(stream.clone(), state);
            versions.insert(stream.clone(), Version::EMPTY);
        }
        let pending = streams.iter().cloned().collect();
        Self {
            decide,
            command,
            streams,
            folds,
            retry_policy,
            retries_used: 0,
            phase: Phase::Loading,
            folded,
            versions,
            pending,
            metadata: crate::envelope::Metadata::default(),
        }
    }

    /// Builder-style: the metadata stamped on every event this batch
    /// emits (0.5.2), applied to each `StreamAppend` at emit time. Set
    /// before [`start`](Self::start); causation/correlation are boundary
    /// concerns, never the domain's.
    pub fn with_metadata(mut self, metadata: crate::envelope::Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// The first action: read every stream of the boundary from the
    /// beginning. Idempotent until the first [`handle`](Self::handle).
    pub fn start(&mut self) -> BatchAction<E, Err> {
        if self.phase == Phase::Loading {
            self.load_action(self.pending.iter().cloned().collect())
        } else {
            self.violation("start() on a machine that already progressed")
        }
    }

    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: BatchInput<E>) -> BatchAction<E, Err> {
        match input {
            BatchInput::Loaded { stream_id, events } => self.on_loaded(stream_id, events),
            BatchInput::Appended { committed } => self.on_appended(committed),
            BatchInput::Conflict { stream, current } => self.on_conflict(stream, current),
            BatchInput::Failed(error) => self.on_failed(error),
        }
    }

    /// The boundary: the streams this machine reads, in sorted order.
    pub fn streams(&self) -> &[StreamId] {
        &self.streams
    }

    /// Whether the machine reached its terminal outcome.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    fn load_action(&self, streams: Vec<StreamId>) -> BatchAction<E, Err> {
        let from = streams
            .iter()
            .map(|stream| {
                (
                    stream.clone(),
                    self.versions.get(stream).copied().unwrap_or_default(),
                )
            })
            .collect();
        BatchAction::LoadStreams { streams, from }
    }

    fn on_loaded(
        &mut self,
        stream_id: StreamId,
        events: Vec<EventEnvelope<E>>,
    ) -> BatchAction<E, Err> {
        if self.phase != Phase::Loading {
            return self.violation("`Loaded` outside the loading phase");
        }
        if !self.pending.remove(&stream_id) {
            return if self.streams.contains(&stream_id) {
                self.violation("`Loaded` answered a stream that was not requested")
            } else {
                self.violation("`Loaded` delivered a stream outside the boundary")
            };
        }
        // Validate while folding: events must belong to this stream and
        // continue its sequence contiguously from the folded version —
        // the same guard the write machine states.
        let mut expected = self
            .versions
            .get(&stream_id)
            .copied()
            .unwrap_or_default()
            .as_u64()
            .saturating_add(1);
        let fold = self.folds.get(&stream_id);
        let state = self
            .folded
            .get_mut(&stream_id)
            .expect("every boundary stream has a fold state");
        for envelope in events {
            if envelope.stream_id != stream_id {
                return self.violation("`Loaded` delivered events from another stream");
            }
            if envelope.version.as_u64() != expected {
                return self.violation("`Loaded` delivered a non-contiguous sequence");
            }
            expected = expected.saturating_add(1);
            if let Some(fold) = fold {
                fold.apply(&mut **state, &envelope.event);
            }
            self.versions.insert(stream_id.clone(), envelope.version);
        }
        if !self.pending.is_empty() {
            // The rest of the boundary is still out: stay in Loading,
            // non-actionable — the driver is already answering the
            // outstanding streams; re-emit the read for the remainder
            // so the action stream stays a faithful request log.
            return self.load_action(self.pending.iter().cloned().collect());
        }
        self.decide_and_emit()
    }

    fn on_appended(&mut self, committed: Vec<CommittedStream<E>>) -> BatchAction<E, Err> {
        if self.phase != Phase::Appending {
            return self.violation("`Appended` outside the appending phase");
        }
        self.phase = Phase::Done;
        BatchAction::Done(BatchOutcome::Committed { committed })
    }

    fn on_conflict(&mut self, stream: StreamId, current: Version) -> BatchAction<E, Err> {
        if self.phase != Phase::Appending {
            return self.violation("`Conflict` outside the appending phase");
        }
        let folded_version = self.versions.get(&stream).copied().unwrap_or_default();
        if current <= folded_version {
            return self.violation("conflict reported a version at or before the folded one");
        }
        if self.retries_used < self.retry_policy.max_retries {
            self.retries_used += 1;
            self.phase = Phase::Loading;
            // Reload only the conflicting stream's delta: the conflict
            // names the one stream that moved; the batch was atomic, so
            // nothing else committed, and re-reading unchanged streams
            // would only churn.
            self.pending.clear();
            self.pending.insert(stream.clone());
            self.load_action(alloc::vec![stream])
        } else {
            self.phase = Phase::Done;
            BatchAction::Done(BatchOutcome::Failed(StoreError::Conflict {
                stream_id: Some(stream),
                current,
            }))
        }
    }

    fn on_failed(&mut self, error: StoreError) -> BatchAction<E, Err> {
        if self.phase == Phase::Done {
            return self.violation("`Failed` on a finished machine");
        }
        self.phase = Phase::Done;
        BatchAction::Done(BatchOutcome::Failed(error))
    }

    fn decide_and_emit(&mut self) -> BatchAction<E, Err> {
        match self.decide.decide(&self.folded, &self.command).outcome {
            Ok(routed) if routed.is_empty() => {
                self.phase = Phase::Done;
                BatchAction::Done(BatchOutcome::Noop)
            }
            Ok(routed) => {
                // Group the routed events per stream, preserving the
                // decision's order within each stream.
                let mut by_stream: BTreeMap<StreamId, Vec<NewEvent<E>>> = BTreeMap::new();
                for (event, stream) in routed {
                    if !self.streams.contains(&stream) {
                        return self.violation(
                            "the decision routed an event outside the boundary",
                        );
                    }
                    match by_stream.entry(stream) {
                        Entry::Occupied(mut entry) => entry.get_mut().push(event),
                        Entry::Vacant(entry) => {
                            entry.insert(alloc::vec![event]);
                        }
                    }
                }
                self.phase = Phase::Appending;
                // Stamp the interaction's metadata on every event the
                // decision routed (0.5.2): the boundary decides the
                // causation/correlation, not the domain.
                let metadata = self.metadata.clone();
                BatchAction::AppendBatch {
                    appends: by_stream
                        .into_iter()
                        .map(|(stream_id, events)| StreamAppend {
                            expected: match self
                                .versions
                                .get(&stream_id)
                                .copied()
                                .unwrap_or_default()
                            {
                                Version::EMPTY => ExpectedVersion::Empty,
                                version => ExpectedVersion::Exact(version),
                            },
                            events: events
                                .into_iter()
                                .map(|event| NewEvent {
                                    event: event.event,
                                    metadata: metadata.clone(),
                                })
                                .collect(),
                            stream_id,
                        })
                        .collect(),
                }
            }
            Err(error) => {
                self.phase = Phase::Done;
                BatchAction::Done(BatchOutcome::Rejected(error))
            }
        }
    }

    fn violation(&mut self, message: &'static str) -> BatchAction<E, Err> {
        self.phase = Phase::Done;
        BatchAction::Done(BatchOutcome::Failed(StoreError::Other(Arc::new(
            ProtocolError::new(message),
        ))))
    }
}

/// Reusable test fixture: a transfer between two accounts — the
/// canonical multi-stream command. The `Transfer` struct is both the
/// command and the [`Decide`] implementation, resolved stream-by-stream
/// through [`AggregateFold`]s over the account fixture.
#[cfg(test)]
pub(crate) mod transfer {
    use alloc::boxed::Box;
    use alloc::vec::Vec;

    use super::{AggregateFold, BatchDecision, Decide, Fold};
    use crate::aggregate::Aggregate;
    use crate::testing::account::{Account, AccountCommand, AccountEvent, AccountId, AccountState};
    use crate::vocabulary::StreamId;
    use core::any::Any;
    use std::collections::BTreeMap;

    /// The stream id of the account with this id.
    pub fn stream_of(id: u64) -> StreamId {
        StreamId::for_aggregate::<Account>(&AccountId(id))
    }

    /// An account fold per stream, keyed by stream id.
    pub fn folds(ids: &[u64]) -> BTreeMap<StreamId, Box<dyn Fold<AccountEvent>>> {
        ids.iter()
            .map(|&id| {
                (
                    stream_of(id),
                    Box::new(AggregateFold::<Account>(AccountId(id))) as Box<dyn Fold<_>>,
                )
            })
            .collect()
    }

    /// The transfer: withdraw `amount` from `from`, deposit it on `to`.
    pub struct Transfer {
        /// The account to debit.
        pub from: u64,
        /// The account to credit.
        pub to: u64,
        /// The amount to move.
        pub amount: u64,
    }

    /// A transfer rejection, in the account's vocabulary.
    pub type TransferError = crate::testing::account::AccountError;

    fn state(folded: &BTreeMap<StreamId, Box<dyn Any + Send>>, id: u64) -> &AccountState {
        folded
            .get(&stream_of(id))
            .and_then(|s| s.downcast_ref::<AccountState>())
            .expect("the account fold typed this state")
    }

    impl Decide<AccountEvent, TransferError> for Transfer {
        type Command = Self;

        fn decide(
            &self,
            folded: &BTreeMap<StreamId, Box<dyn Any + Send>>,
            command: &Self,
        ) -> BatchDecision<AccountEvent, TransferError> {
            if let Err(error) = Account::decide(
                state(folded, command.from),
                &AccountCommand::Withdraw {
                    amount: command.amount,
                },
            ) {
                return BatchDecision::reject(error);
            }
            if let Err(error) = Account::decide(
                state(folded, command.to),
                &AccountCommand::Deposit {
                    amount: command.amount,
                },
            ) {
                return BatchDecision::reject(error);
            }
            BatchDecision::of(
                Vec::from([
                    AccountEvent::Withdrawn {
                        amount: command.amount,
                    },
                    AccountEvent::Deposited {
                        amount: command.amount,
                    },
                ]),
                Vec::from([stream_of(command.from), stream_of(command.to)]),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::transfer::{Transfer, TransferError, folds, stream_of};
    use super::*;
    use crate::envelope::Metadata;
    use crate::testing::account::{AccountError, AccountEvent};
    use crate::vocabulary::Sequence;
    use alloc::string::String;
    use alloc::vec;

    fn transfer_machine(
        from: u64,
        to: u64,
        amount: u64,
        ids: &[u64],
    ) -> BatchMachine<AccountEvent, TransferError, Transfer> {
        BatchMachine::new(
            ids.iter().map(|&id| stream_of(id)).collect(),
            folds(ids),
            Transfer { from, to, amount },
            Transfer { from, to, amount },
            RetryPolicy::default(),
        )
    }

    fn env(stream_id: &StreamId, version: u64, event: AccountEvent) -> EventEnvelope<AccountEvent> {
        EventEnvelope {
            sequence: Sequence::new(version), // fake global position; only version matters
            stream_id: stream_id.clone(),
            version: Version::new(version),
            event,
            metadata: Metadata::default(),
        }
    }

    fn opened(owner: &str) -> AccountEvent {
        AccountEvent::Opened {
            owner: String::from(owner),
        }
    }

    fn is_protocol_violation<E, Err>(action: &BatchAction<E, Err>) -> bool {
        let BatchAction::Done(BatchOutcome::Failed(StoreError::Other(source))) = action else {
            return false;
        };
        source.downcast_ref::<ProtocolError>().is_some()
    }

    /// Drive the load phase with empty streams for the whole boundary,
    /// returning the action emitted once the boundary has answered.
    fn load_all_empty(
        m: &mut BatchMachine<AccountEvent, TransferError, Transfer>,
    ) -> BatchAction<AccountEvent, TransferError> {
        let streams: Vec<StreamId> = m.streams().to_vec();
        m.start();
        let mut last = None;
        for stream in streams {
            last = Some(m.handle(BatchInput::Loaded {
                stream_id: stream,
                events: vec![],
            }));
        }
        last.expect("a non-empty boundary")
    }

    /// Load stream 1 opened with balance 10 and stream 2 opened, then
    /// return the action emitted once the boundary has answered — the
    /// setup a successful transfer needs (both accounts open, `from`
    /// funded).
    fn load_funded(
        m: &mut BatchMachine<AccountEvent, TransferError, Transfer>,
    ) -> BatchAction<AccountEvent, TransferError> {
        m.start();
        m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![
                env(&stream_of(1), 1, opened("a")),
                env(&stream_of(1), 2, AccountEvent::Deposited { amount: 10 }),
            ],
        });
        m.handle(BatchInput::Loaded {
            stream_id: stream_of(2),
            events: vec![env(&stream_of(2), 1, opened("b"))],
        })
    }

    // -- transitions ----------------------------------------------------

    #[test]
    fn start_loads_every_stream_from_the_beginning() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        let BatchAction::LoadStreams { streams, from } = m.start() else {
            panic!("expected a load")
        };
        assert_eq!(streams, vec![stream_of(1), stream_of(2)]);
        assert_eq!(
            from.values().copied().collect::<Vec<_>>(),
            vec![Version::EMPTY, Version::EMPTY]
        );
    }

    #[test]
    fn the_boundary_is_deduplicated_and_sorted() {
        let mut m = BatchMachine::new(
            vec![stream_of(2), stream_of(1), stream_of(2)],
            folds(&[1, 2]),
            Transfer {
                from: 1,
                to: 2,
                amount: 5,
            },
            Transfer {
                from: 1,
                to: 2,
                amount: 5,
            },
            RetryPolicy::default(),
        );
        assert_eq!(m.streams(), &[stream_of(1), stream_of(2)]);
        let BatchAction::LoadStreams { streams, .. } = m.start() else {
            panic!("expected a load")
        };
        assert_eq!(streams, vec![stream_of(1), stream_of(2)]);
    }

    #[test]
    fn a_transfer_between_fresh_streams_appends_to_both() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        // Empty streams: `from` isn't open, so the transfer is rejected
        // — the pure decision sees the fold, not a guess. (The
        // appending path is `a_transfer_folds_both_histories_before_deciding`.)
        let action = load_all_empty(&mut m);
        assert!(matches!(
            action,
            BatchAction::Done(BatchOutcome::Rejected(AccountError::NotOpen))
        ));
    }

    #[test]
    fn a_funded_transfer_appends_to_both_streams() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        let BatchAction::AppendBatch { appends } = load_funded(&mut m) else {
            panic!("expected an append")
        };
        assert_eq!(appends.len(), 2);
        assert_eq!(appends[0].stream_id, stream_of(1));
        assert_eq!(appends[0].expected, ExpectedVersion::Exact(Version::new(2)));
        assert_eq!(appends[1].stream_id, stream_of(2));
        assert_eq!(appends[1].expected, ExpectedVersion::Exact(Version::new(1)));
        assert!(matches!(
            appends[0].events[0].event,
            AccountEvent::Withdrawn { amount: 5 }
        ));
        assert!(matches!(
            appends[1].events[0].event,
            AccountEvent::Deposited { amount: 5 }
        ));
    }

    #[test]
    fn a_transfer_folds_both_histories_before_deciding() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        m.start();
        // Stream 1: opened, deposited 10 (balance 10, version 2).
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![
                env(&stream_of(1), 1, opened("a")),
                env(&stream_of(1), 2, AccountEvent::Deposited { amount: 10 }),
            ],
        });
        // Stream 2 still outstanding: the machine waits.
        assert!(matches!(action, BatchAction::LoadStreams { .. }));
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(2),
            events: vec![env(&stream_of(2), 1, opened("b"))],
        });
        let BatchAction::AppendBatch { appends } = action else {
            panic!("expected an append")
        };
        assert_eq!(appends[0].expected, ExpectedVersion::Exact(Version::new(2)));
        assert_eq!(appends[1].expected, ExpectedVersion::Exact(Version::new(1)));
    }

    #[test]
    fn a_transfer_against_insufficient_funds_is_rejected() {
        let mut m = transfer_machine(1, 2, 50, &[1, 2]);
        m.start();
        m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![
                env(&stream_of(1), 1, opened("a")),
                env(&stream_of(1), 2, AccountEvent::Deposited { amount: 10 }),
            ],
        });
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(2),
            events: vec![env(&stream_of(2), 1, opened("b"))],
        });
        // 50 > the 10 in `from`: the account rejects the withdrawal.
        assert!(matches!(
            action,
            BatchAction::Done(BatchOutcome::Rejected(AccountError::InsufficientFunds))
        ));
    }

    #[test]
    fn appended_commits_one_entry_per_append() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        load_funded(&mut m);
        let committed = vec![
            CommittedStream {
                stream_id: stream_of(1),
                events: vec![env(&stream_of(1), 3, AccountEvent::Withdrawn { amount: 5 })],
            },
            CommittedStream {
                stream_id: stream_of(2),
                events: vec![env(&stream_of(2), 2, AccountEvent::Deposited { amount: 5 })],
            },
        ];
        let action = m.handle(BatchInput::Appended { committed });
        let BatchAction::Done(BatchOutcome::Committed { committed }) = action else {
            panic!("expected a committed outcome")
        };
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].stream_id, stream_of(1));
        assert_eq!(committed[1].stream_id, stream_of(2));
        assert!(m.is_done());
    }

    #[test]
    fn a_decision_with_no_events_noops() {
        let mut m = BatchMachine::new(
            vec![stream_of(1)],
            folds(&[1]),
            NoopDecision,
            NoopDecision,
            RetryPolicy::default(),
        );
        m.start();
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![],
        });
        assert!(matches!(action, BatchAction::Done(BatchOutcome::Noop)));
    }

    /// A decision that always noops.
    struct NoopDecision;

    impl Decide<AccountEvent, TransferError> for NoopDecision {
        type Command = Self;

        fn decide(
            &self,
            _: &BTreeMap<StreamId, Box<dyn Any + Send>>,
            _: &Self,
        ) -> BatchDecision<AccountEvent, TransferError> {
            BatchDecision::noop()
        }
    }

    // -- conflict & retry -------------------------------------------------

    #[test]
    fn a_conflict_reloads_only_the_moved_stream_delta() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        m.start();
        m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![
                env(&stream_of(1), 1, opened("a")),
                env(&stream_of(1), 2, AccountEvent::Deposited { amount: 10 }),
            ],
        });
        m.handle(BatchInput::Loaded {
            stream_id: stream_of(2),
            events: vec![env(&stream_of(2), 1, opened("b"))],
        });
        // Another writer appended v3 on stream 1 while we decided.
        let action = m.handle(BatchInput::Conflict {
            stream: stream_of(1),
            current: Version::new(3),
        });
        // Retry: re-read only stream 1, from its folded version 2.
        let BatchAction::LoadStreams { streams, from } = action else {
            panic!("expected a reload")
        };
        assert_eq!(streams, vec![stream_of(1)]);
        assert_eq!(from.get(&stream_of(1)), Some(&Version::new(2)));
        // Fold the delta and re-decide: stream 1 is now at version 3.
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![env(&stream_of(1), 3, AccountEvent::Deposited { amount: 1 })],
        });
        let BatchAction::AppendBatch { appends } = action else {
            panic!("expected an append")
        };
        assert_eq!(appends[0].expected, ExpectedVersion::Exact(Version::new(3)));
    }

    #[test]
    fn conflict_exhausts_the_retry_budget() {
        let mut m = BatchMachine::new(
            vec![stream_of(1), stream_of(2)],
            folds(&[1, 2]),
            Transfer {
                from: 1,
                to: 2,
                amount: 5,
            },
            Transfer {
                from: 1,
                to: 2,
                amount: 5,
            },
            RetryPolicy::NEVER,
        );
        load_funded(&mut m);
        let action = m.handle(BatchInput::Conflict {
            stream: stream_of(1),
            current: Version::new(3),
        });
        assert!(matches!(
            action,
            BatchAction::Done(BatchOutcome::Failed(StoreError::Conflict { current, .. }))
                if current == Version::new(3)
        ));
    }

    // -- protocol violations --------------------------------------------

    #[test]
    fn driving_a_finished_machine_is_a_protocol_violation() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        load_all_empty(&mut m);
        m.handle(BatchInput::Appended { committed: vec![] });
        assert!(is_protocol_violation(
            &m.handle(BatchInput::Appended { committed: vec![] })
        ));
        assert!(is_protocol_violation(&m.start()));
    }

    #[test]
    fn loaded_outside_loading_is_a_protocol_violation() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        load_all_empty(&mut m); // → Appending
        assert!(is_protocol_violation(&m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![],
        })));
    }

    #[test]
    fn loaded_from_a_stream_outside_the_boundary_is_a_protocol_violation() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        m.start();
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(99),
            events: vec![],
        });
        assert!(is_protocol_violation(&action));
    }

    #[test]
    fn non_contiguous_events_are_a_protocol_violation() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        m.start();
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![env(&stream_of(1), 2, AccountEvent::Deposited { amount: 1 })], // gap at v1
        });
        assert!(is_protocol_violation(&action));
    }

    #[test]
    fn conflict_at_or_before_the_folded_version_is_a_protocol_violation() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        m.start();
        m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![env(&stream_of(1), 1, opened("a"))],
        });
        m.handle(BatchInput::Loaded {
            stream_id: stream_of(2),
            events: vec![env(&stream_of(2), 1, opened("b"))],
        });
        // Folded stream 1 to version 1; a conflict reporting current == 1
        // is a contradiction.
        let action = m.handle(BatchInput::Conflict {
            stream: stream_of(1),
            current: Version::new(1),
        });
        assert!(is_protocol_violation(&action));
    }

    #[test]
    fn a_store_failure_during_load_ends_the_interaction() {
        let mut m = transfer_machine(1, 2, 5, &[1, 2]);
        m.start();
        let action = m.handle(BatchInput::Failed(StoreError::Unavailable));
        assert!(matches!(
            action,
            BatchAction::Done(BatchOutcome::Failed(StoreError::Unavailable))
        ));
    }

    #[test]
    fn a_decision_routing_outside_the_boundary_is_a_protocol_violation() {
        let mut m = BatchMachine::new(
            vec![stream_of(1)],
            folds(&[1]),
            Rogue,
            Rogue,
            RetryPolicy::default(),
        );
        m.start();
        let action = m.handle(BatchInput::Loaded {
            stream_id: stream_of(1),
            events: vec![env(&stream_of(1), 1, opened("a"))],
        });
        // Rogue routes its event to stream 99 — outside the boundary.
        assert!(is_protocol_violation(&action));
    }

    /// Routes its event to a stream outside the boundary.
    struct Rogue;

    impl Decide<AccountEvent, TransferError> for Rogue {
        type Command = Self;

        fn decide(
            &self,
            _: &BTreeMap<StreamId, Box<dyn Any + Send>>,
            _: &Self,
        ) -> BatchDecision<AccountEvent, TransferError> {
            BatchDecision::of(
                vec![AccountEvent::Deposited { amount: 1 }],
                vec![stream_of(99)],
            )
        }
    }

    // -- properties -------------------------------------------------------

    use proptest::prelude::*;

    fn arb_batch_input() -> BoxedStrategy<BatchInput<AccountEvent>> {
        prop_oneof![
            (1u64..4, 0u64..4).prop_map(|(stream, len)| {
                let stream_id = stream_of(stream);
                let events = (0..len)
                    .map(|i| env(&stream_id, 1 + i, AccountEvent::Deposited { amount: 1 }))
                    .collect();
                BatchInput::Loaded { stream_id, events }
            }),
            (1u64..3, 0u64..3).prop_map(|(stream, len)| {
                let stream_id = stream_of(stream);
                let events = (0..len)
                    .map(|i| env(&stream_id, 1 + i, AccountEvent::Deposited { amount: 1 }))
                    .collect();
                BatchInput::Appended {
                    committed: vec![CommittedStream {
                        stream_id,
                        events,
                    }],
                }
            }),
            (1u64..3, 0u64..10).prop_map(|(stream, current)| BatchInput::Conflict {
                stream: stream_of(stream),
                current: Version::new(current),
            }),
            Just(BatchInput::Failed(StoreError::Unavailable)),
        ]
        .boxed()
    }

    proptest! {
        /// The machine never panics on any input sequence, and once it
        /// is done it stays done: every action after the first `Done` is
        /// a protocol-violation `Done`.
        #[test]
        fn never_panics_and_stays_done(script in prop::collection::vec(arb_batch_input(), 0..16)) {
            let mut m = transfer_machine(1, 2, 5, &[1, 2]);
            let actions = crate::testing::batch_scripted(&mut m, script);

            prop_assert!(!actions.is_empty()); // start always emits
            if let Some(i) = actions.iter().position(|a| matches!(a, BatchAction::Done(_))) {
                for a in &actions[i + 1..] {
                    prop_assert!(is_protocol_violation(a));
                }
            }
        }
    }
}
