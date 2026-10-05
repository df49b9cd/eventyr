//! The write machine: the sans-IO state machine behind every command
//! execution.
//!
//! The write path is not a pure function — load → fold → decide → append
//! can conflict and retry. So it is modeled as a machine:
//! [`WriteMachine`] consumes driver results ([`WriteInput`]) and emits
//! I/O requests ([`WriteAction`]); the driver performs them. The machine
//! owns the protocol — retry budget, conflict handling, version
//! expectations — and never does I/O itself, so a retry bug reproduces
//! in a unit test with a `Vec` of inputs (see
//! [`scripted`](crate::testing::scripted)).
//!
//! Snapshots are opt-in via [`WriteMachine::with_snapshots`]. When off
//! (the default snapshot channel `S = ()`), the protocol is exactly the
//! pre-snapshot one: `start()` emits [`LoadStream`](WriteAction::LoadStream)
//! and the outcome carries no snapshot. When on, `start()` emits
//! [`LoadSnapshot`](WriteAction::LoadSnapshot), the driver answers with
//! the newest stored snapshot (if any), the machine adopts its state,
//! folds only the post-snapshot delta it then loads — the contiguity
//! check is the snapshot-version monotonicity guard — and on commit,
//! when the [`SnapshotPolicy`] fires,
//! offers the driver a [`Snapshot`] on the
//! [`Committed`](WriteOutcome::Committed) outcome. Fire-and-forget: the
//! driver may persist it; persistence is not part of the protocol, and
//! a skipped save never turns into a store failure.

use alloc::vec::Vec;

use crate::aggregate::Aggregate;
use crate::envelope::{EventEnvelope, Metadata, NewEvent};
use crate::error::StoreError;
use crate::snapshot::{HasSnapshotState, OfferSnapshot, Snapshot, SnapshotPolicy};
use crate::vocabulary::{ExpectedVersion, StreamId, Version};

/// What the machine wants the driver to do.
///
/// Actions are data, not calls: the driver interprets each variant,
/// performs the I/O, and reports back with a [`WriteInput`].
///
/// `S` is the snapshot channel: `()` on a plain machine, `A::State` on
/// one built by [`with_snapshots`](WriteMachine::with_snapshots).
/// Splitting it off the aggregate keeps the snapshot-off path — types
/// *and* bounds — exactly what it was before snapshots existed.
#[derive(Clone, Debug)]
pub enum WriteAction<E, Err, S = ()> {
    /// Read the stream from `from` (exclusive) to rebuild state.
    LoadStream {
        /// The stream to read.
        stream_id: StreamId,
        /// Exclusive lower bound on the event version.
        from: Version,
    },
    /// Read the newest persisted snapshot for the stream.
    ///
    /// The very first action of a snapshots-on machine. Answered by
    /// [`SnapshotLoaded`](WriteInput::SnapshotLoaded): `None` when the
    /// store has none, `Some` otherwise. A snapshot whose `stream_id`
    /// does not match the machine's is a protocol violation.
    LoadSnapshot {
        /// The stream to read the snapshot for.
        stream_id: StreamId,
    },
    /// Append events, guarded by the expected version.
    ///
    /// Drivers may enrich each event's metadata (correlation,
    /// causation) before persisting.
    Append {
        /// The stream to append to.
        stream_id: StreamId,
        /// The optimistic-concurrency expectation.
        expected: ExpectedVersion,
        /// The events to append.
        events: Vec<NewEvent<E>>,
    },
    /// Terminal: the interaction's outcome.
    Done(WriteOutcome<E, Err, S>),
}

/// What the driver reports back to the machine.
#[derive(Clone, Debug)]
pub enum WriteInput<E, S = ()> {
    /// The stream read completed. Events must belong to the requested
    /// stream and continue the sequence contiguously from where the
    /// machine last folded — after a snapshot load that is the
    /// snapshot's version, which makes the contiguity check double as
    /// the snapshot-version monotonicity guard.
    Loaded {
        /// The events read, in stream order.
        events: Vec<EventEnvelope<E>>,
    },
    /// The snapshot read completed: the newest persisted snapshot for
    /// the stream, or `None` when the store has none.
    ///
    /// Accepted only as the answer to
    /// [`LoadSnapshot`](WriteAction::LoadSnapshot); anywhere else it is
    /// a protocol violation.
    SnapshotLoaded {
        /// The snapshot the store has, if any.
        snapshot: Option<Snapshot<S>>,
    },
    /// The append committed.
    Appended {
        /// The committed events, as the store recorded them.
        committed: Vec<EventEnvelope<E>>,
    },
    /// The append conflicted: the stream is at `current`, not at the
    /// expected version.
    Conflict {
        /// The stream's actual version at append time.
        current: Version,
    },
    /// A store operation failed — a stream read, an append, or a
    /// snapshot read. (A snapshot read failing fails the interaction:
    /// the store just told the machine its reads are broken.)
    Failed(StoreError),
}

impl<E, S> From<StoreError> for WriteInput<E, S> {
    /// Every store failure travels through the machine as
    /// [`Failed`](WriteInput::Failed) — except a [`Conflict`](StoreError::Conflict), which the
    /// write protocol owns a retry path for
    /// ([`WriteInput::Conflict`]). This is the one place that mapping
    /// exists; a `StoreError` variant that is not retry-shaped lands in
    /// `Failed` without a driver edit.
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Conflict { current, .. } => WriteInput::Conflict { current },
            other => WriteInput::Failed(other),
        }
    }
}

/// The terminal outcome of a driven write machine.
#[derive(Clone, Debug)]
pub enum WriteOutcome<E, Err, S = ()> {
    /// The events were committed; the envelopes are as the store
    /// recorded them.
    ///
    /// `snapshot` is the fire-and-forget post-commit offer: `Some` when
    /// the machine ran with snapshots on and the [`SnapshotPolicy`]
    /// fired for this commit, `None` otherwise. Its state is the
    /// folded state *including* the committed events, at the committed
    /// version. Persisting it — or declining to — never changes the
    /// committed outcome; snapshots are read-side shortcuts, not the
    /// source of truth.
    Committed {
        /// The committed events as the store recorded them.
        committed: Vec<EventEnvelope<E>>,
        /// The post-commit snapshot offer, when the policy fired.
        snapshot: Option<OfferSnapshot<S>>,
    },
    /// The command carried an idempotency key (0.7.5) and the stream
    /// already holds the events of an earlier commit with that key:
    /// nothing was decided or appended. `committed` is that earlier
    /// commit, as stored.
    AlreadyCommitted {
        /// The events the earlier commit with this key appended.
        committed: Vec<EventEnvelope<E>>,
    },
    /// The command decided no events; nothing was appended.
    Noop,
    /// The domain rejected the command.
    Rejected(Err),
    /// The store failed — a conflict that exhausted the retry budget, a
    /// transient error, or a fatal one. Protocol violations by the
    /// driver land here too (see [`ProtocolError`](crate::error::ProtocolError)).
    Failed(StoreError),
}

/// How many times the machine may reload and re-decide after a conflict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of conflict retries.
    pub max_retries: u32,
}

impl RetryPolicy {
    /// A policy allowing `max_retries` conflict retries.
    pub const fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }

    /// A policy that never retries: the first conflict fails the
    /// interaction.
    pub const NEVER: Self = Self { max_retries: 0 };
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_retries: 3 }
    }
}

/// One interaction's conflict-retry allowance: a [`RetryPolicy`] and
/// the retries spent against it. Shared by the write, batch, and
/// boundary machines.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryBudget {
    policy: RetryPolicy,
    used: u32,
}

impl RetryBudget {
    /// A fresh budget: nothing spent.
    pub(crate) const fn new(policy: RetryPolicy) -> Self {
        Self { policy, used: 0 }
    }

    /// Spend one retry: `true` when the policy still allowed it, `false`
    /// once the budget is exhausted (nothing is spent then).
    pub(crate) fn try_consume(&mut self) -> bool {
        if self.used < self.policy.max_retries {
            self.used += 1;
            true
        } else {
            false
        }
    }
}

/// Which input the machine is waiting for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for `SnapshotLoaded` — only on a snapshots-on machine,
    /// only as its first phase.
    LoadingSnapshot,
    /// Waiting for `Loaded`.
    Loading,
    /// Waiting for `Appended`/`Conflict`/`Failed`.
    Appending,
    /// Terminal.
    Done,
}

/// Snapshot bookkeeping, generic in the state channel `S`.
///
/// The loaded snapshot's *state* is adopted into `folded` the moment
/// the answer arrives; what the machine needs afterwards is only the
/// baseline version the fold started from, for the cadence, and the
/// hooks that cross from `A::State` to `S`. Snapshots are on exactly
/// when the hooks are present — the variant carries them.
enum Snapshots<A: Aggregate, S> {
    /// Snapshots are off: the pre-snapshot machine, byte-for-byte.
    Off,
    /// Snapshots are on. One `LoadSnapshot` per interaction: issued at
    /// `start`, answered exactly once; conflict retries afterwards
    /// reload only the event delta — never a second snapshot.
    On {
        /// The version the fold started from: the loaded snapshot's
        /// version, or [`Version::EMPTY`] when the store had none.
        base_version: Version,
        /// The cadence policy the commit path consults.
        policy: SnapshotPolicy,
        /// Crossing between the fold and the snapshot channel.
        hooks: SnapshotHooks<A, S>,
    },
}

/// Fold committed events into a snapshot-channel state. Stored on the
/// machine only when snapshots are on (where `S = A::State`).
type FoldCommitted<A, S> = fn(&mut S, &[EventEnvelope<<A as Aggregate>::Event>]);

/// The three hooks a snapshots-on machine needs to cross from
/// `A::State` to the snapshot channel `S`: `adopt`, `fold_committed`,
/// and `materialize`.
///
/// Kept as one unit inside [`Snapshots::On`] — a snapshots-off machine
/// has `S = ()` and no hooks, a snapshots-on one has `S = A::State` and
/// the three function pointers
/// [`with_snapshots`](WriteMachine::with_snapshots) installs. Bundling
/// them makes "all present or all absent" structural; three separate
/// `Option<fn>` fields would leave it documented.
struct SnapshotHooks<A: Aggregate, S> {
    /// Adopt the loaded snapshot's state as the fold's starting point.
    adopt: fn(&mut A::State, S),
    /// Fold committed events into a snapshot-channel state, so the
    /// post-commit offer carries the state *at* the committed version.
    fold_committed: FoldCommitted<A, S>,
    /// Clone the folded state into the snapshot channel, for the
    /// post-commit offer.
    materialize: fn(&A::State) -> S,
}

/// The sans-IO machine behind command execution: load → fold → decide →
/// append, with conflict retry.
///
/// The machine owns the folded state, the current version, and the retry
/// budget. It never does I/O: the driver performs each [`WriteAction`]
/// and reports back with a [`WriteInput`]. On a conflict the machine
/// reloads only the delta since the version it folded, re-folds, and
/// re-decides against fresh state.
///
/// `S` is the snapshot channel: `()` (the default) on a plain machine,
/// `A::State` on one built by
/// [`with_snapshots`](WriteMachine::with_snapshots). The machine stays
/// fully generic over `S`: crossing from `A::State` to `S` is what the
/// three hooks [`with_snapshots`](WriteMachine::with_snapshots) installs
/// (adopt, fold-committed, materialize) are for, so no `Clone` bound
/// reaches the snapshot-off protocol.
///
/// Machines never panic on bad input: a driver that feeds the wrong
/// input for the current phase, or drives a finished machine, gets
/// [`Done`](WriteAction::Done) with
/// [`Failed(StoreError::Other(ProtocolError))`](WriteOutcome::Failed).
pub struct WriteMachine<A: Aggregate, S = ()> {
    stream_id: StreamId,
    command: A::Command,
    /// The metadata stamped onto every event this interaction emits.
    /// Set once at construction (or via [`with_metadata`](Self::with_metadata));
    /// the domain's `decide` never sees it — causation/correlation are
    /// boundary concerns, not domain ones (0.5.2).
    metadata: Metadata,
    retries: RetryBudget,
    /// Off, or on with its baseline, cadence, and hooks.
    snapshots: Snapshots<A, S>,
    phase: Phase,
    folded: A::State,
    /// The stream version the machine has folded up to — the loaded
    /// snapshot's version first, then each folded event's. Monotone
    /// across an interaction.
    version: Version,
    /// Loaded events carrying this interaction's idempotency key: the
    /// earlier commit, when the command has run before (0.7.5).
    earlier: Vec<EventEnvelope<A::Event>>,
}

impl<A: Aggregate> WriteMachine<A, ()> {
    /// Begin an interaction: `id` identifies the aggregate instance,
    /// `command` is what to decide, `retry_policy` bounds conflict
    /// retries. Snapshots are off — the protocol is exactly the
    /// pre-snapshot one.
    pub fn new(id: A::Id, command: A::Command, retry_policy: RetryPolicy) -> Self {
        Self {
            stream_id: StreamId::for_aggregate::<A>(&id),
            command,
            metadata: Metadata::default(),
            retries: RetryBudget::new(retry_policy),
            snapshots: Snapshots::Off,
            phase: Phase::Loading,
            folded: A::initial(&id),
            version: Version::EMPTY,
            earlier: Vec::new(),
        }
    }
}

impl<A> WriteMachine<A, A::State>
where
    A: HasSnapshotState,
    A::State: Clone,
{
    /// Begin an interaction whose load may start from a stored snapshot
    /// and whose commit may offer one.
    ///
    /// Same call shape as [`new`](WriteMachine::new), plus a
    /// [`SnapshotPolicy`]. Bound on [`HasSnapshotState`]: an aggregate
    /// opts into snapshots by having a `Clone`-able state — the write
    /// protocol and its drivers stay free of snapshot concern
    /// everywhere else.
    pub fn with_snapshots(
        id: A::Id,
        command: A::Command,
        retry_policy: RetryPolicy,
        policy: SnapshotPolicy,
    ) -> Self {
        Self {
            stream_id: StreamId::for_aggregate::<A>(&id),
            command,
            metadata: Metadata::default(),
            retries: RetryBudget::new(retry_policy),
            snapshots: Snapshots::On {
                base_version: Version::EMPTY,
                policy,
                hooks: SnapshotHooks {
                    adopt: |folded, state| *folded = state,
                    fold_committed: |state, committed| {
                        for envelope in committed {
                            A::apply(state, &envelope.event);
                        }
                    },
                    materialize: Clone::clone,
                },
            },
            phase: Phase::LoadingSnapshot,
            folded: A::initial(&id),
            version: Version::EMPTY,
            earlier: Vec::new(),
        }
    }
}

impl<A: Aggregate, S> WriteMachine<A, S> {
    /// The first action: a snapshot read on a snapshots-on machine (the
    /// driver answers with the newest stored snapshot, if any), else a
    /// full stream read. Idempotent until the first
    /// [`handle`](Self::handle).
    ///
    /// A keyed interaction (0.7.5) skips the snapshot and loads the whole
    /// stream: an earlier commit with the key may lie before the
    /// snapshot, where a delta load would never see it. The commit may
    /// still offer a snapshot.
    pub fn start(&mut self) -> WriteAction<A::Event, A::Error, S> {
        if self.phase == Phase::LoadingSnapshot && self.metadata.idempotency_key.is_some() {
            self.phase = Phase::Loading;
        }
        match self.phase {
            Phase::LoadingSnapshot => WriteAction::LoadSnapshot {
                stream_id: self.stream_id.clone(),
            },
            Phase::Loading => WriteAction::LoadStream {
                stream_id: self.stream_id.clone(),
                from: self.version,
            },
            _ => self.violation("start() on a machine that already progressed"),
        }
    }

    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: WriteInput<A::Event, S>) -> WriteAction<A::Event, A::Error, S> {
        match input {
            WriteInput::Loaded { events } => self.on_loaded(events),
            WriteInput::SnapshotLoaded { snapshot } => self.on_snapshot_loaded(snapshot),
            WriteInput::Appended { committed } => self.on_appended(committed),
            WriteInput::Conflict { current } => self.on_conflict(current),
            WriteInput::Failed(error) => self.on_failed(error),
        }
    }

    /// The stream this machine writes to.
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    /// The metadata this machine stamps onto every event it emits.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Builder-style: set the metadata stamped on every emitted event
    /// (0.5.2). Construct with [`new`](Self::new) /
    /// [`with_snapshots`](Self::with_snapshots), then call this before
    /// [`start`](Self::start). Causation/correlation are boundary
    /// concerns — set here, never inside `decide`.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// The version folded so far (the loaded snapshot's version, plus
    /// the events folded on top of it).
    pub fn version(&self) -> Version {
        self.version
    }

    /// Whether the machine reached its terminal outcome.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    fn on_snapshot_loaded(
        &mut self,
        snapshot: Option<Snapshot<S>>,
    ) -> WriteAction<A::Event, A::Error, S> {
        if self.phase != Phase::LoadingSnapshot {
            return self.violation("`SnapshotLoaded` outside the snapshot-loading phase");
        }
        let Snapshots::On {
            base_version,
            hooks,
            ..
        } = &mut self.snapshots
        else {
            // Unreachable by construction (a snapshots-off machine never
            // enters `LoadingSnapshot`); defended per machine rule 4.
            return self.violation("`SnapshotLoaded` on a snapshots-off machine");
        };
        if let Some(snapshot) = snapshot {
            if snapshot.stream_id != self.stream_id {
                return self.violation("`SnapshotLoaded` delivered a snapshot for another stream");
            }
            // The monotonicity guard the protocol states: a snapshot
            // claims to cover the stream up to `version`, so adopting
            // it must never move the fold backwards. At this phase the
            // fold is at `Version::EMPTY`, but the check is the stated
            // invariant, not an optimization.
            if snapshot.version < self.version {
                return self.violation("`SnapshotLoaded` is older than the state already folded");
            }
            (hooks.adopt)(&mut self.folded, snapshot.state);
            self.version = snapshot.version;
            *base_version = snapshot.version;
        }
        self.phase = Phase::Loading;
        // Fold only the post-snapshot delta: from the snapshot's version
        // when one loaded, from the beginning otherwise.
        WriteAction::LoadStream {
            stream_id: self.stream_id.clone(),
            from: self.version,
        }
    }

    fn on_loaded(
        &mut self,
        events: Vec<EventEnvelope<A::Event>>,
    ) -> WriteAction<A::Event, A::Error, S> {
        if self.phase != Phase::Loading {
            return self.violation("`Loaded` outside the loading phase");
        }
        // Validate while folding: events must belong to this stream and
        // continue the sequence contiguously from the folded version —
        // which, after a snapshot load, *is* the snapshot's version.
        // This is where the snapshot-version monotonicity guard bites:
        // a snapshot ahead of the stream's tip makes the first delta
        // event's version discontinuous (violation), and a store that
        // simply has fewer events than the snapshot claims fails later
        // at the append expectation — never as silently folded-over
        // state: the machine only decides on folds it can defend.
        let mut expected = self.version.as_u64().saturating_add(1);
        for envelope in events {
            if envelope.stream_id != self.stream_id {
                return self.violation("`Loaded` delivered events from another stream");
            }
            if envelope.version.as_u64() != expected {
                return self.violation("`Loaded` delivered a non-contiguous sequence");
            }
            expected = expected.saturating_add(1);
            A::apply(&mut self.folded, &envelope.event);
            self.version = envelope.version;
            if self.metadata.idempotency_key.is_some()
                && envelope.metadata.idempotency_key == self.metadata.idempotency_key
            {
                self.earlier.push(envelope);
            }
        }
        if !self.earlier.is_empty() {
            // The command has committed before: return that commit
            // instead of deciding it again. On a conflict retry this is
            // how a concurrent duplicate is caught — the reloaded delta
            // carries its events.
            self.phase = Phase::Done;
            return WriteAction::Done(WriteOutcome::AlreadyCommitted {
                committed: core::mem::take(&mut self.earlier),
            });
        }
        self.decide_and_emit()
    }

    fn on_appended(
        &mut self,
        committed: Vec<EventEnvelope<A::Event>>,
    ) -> WriteAction<A::Event, A::Error, S> {
        if self.phase != Phase::Appending {
            return self.violation("`Appended` outside the appending phase");
        }
        self.phase = Phase::Done;
        let snapshot = self.snapshot_offer(&committed);
        WriteAction::Done(WriteOutcome::Committed {
            committed,
            snapshot,
        })
    }

    /// Build the fire-and-forget post-commit snapshot offer.
    ///
    /// The cadence speaks in *progress since the base*: the policy
    /// fires when the committed version moved `every` past the
    /// version the fold started from (the loaded snapshot's version,
    /// or `EMPTY`). The offered state is the folded state with the
    /// committed events applied, at the committed version — exactly
    /// what a later `LoadSnapshot` answer wants to carry.
    fn snapshot_offer(&self, committed: &[EventEnvelope<A::Event>]) -> Option<OfferSnapshot<S>> {
        let Snapshots::On {
            base_version,
            policy,
            hooks,
        } = &self.snapshots
        else {
            return None;
        };
        let committed_version = committed.last().map_or(self.version, |e| e.version);
        if !policy.is_due(*base_version, committed_version) {
            return None;
        }
        // Materialize the post-commit state without disturbing
        // `self.folded`: clone the pre-commit fold (via `materialize`)
        // and apply the batch on top (via `fold_committed`). `A::apply`
        // is pure and total, so this is exactly the state the commit
        // produced.
        let mut state = (hooks.materialize)(&self.folded);
        (hooks.fold_committed)(&mut state, committed);
        Some(OfferSnapshot(Snapshot {
            stream_id: self.stream_id.clone(),
            version: committed_version,
            state,
        }))
    }

    fn on_conflict(&mut self, current: Version) -> WriteAction<A::Event, A::Error, S> {
        if self.phase != Phase::Appending {
            return self.violation("`Conflict` outside the appending phase");
        }
        if current <= self.version {
            return self.violation("conflict reported a version at or before the folded one");
        }
        if self.retries.try_consume() {
            self.phase = Phase::Loading;
            // Reload only the delta since the version we folded — the
            // same shape with or without snapshots: the base (if any)
            // is already part of the fold; a second snapshot load would
            // duplicate it and waste a round-trip.
            WriteAction::LoadStream {
                stream_id: self.stream_id.clone(),
                from: self.version,
            }
        } else {
            self.phase = Phase::Done;
            WriteAction::Done(WriteOutcome::Failed(StoreError::Conflict {
                stream_id: Some(self.stream_id.clone()),
                current,
            }))
        }
    }

    fn on_failed(&mut self, error: StoreError) -> WriteAction<A::Event, A::Error, S> {
        if self.phase == Phase::Done {
            return self.violation("`Failed` on a finished machine");
        }
        self.phase = Phase::Done;
        WriteAction::Done(WriteOutcome::Failed(error))
    }

    fn decide_and_emit(&mut self) -> WriteAction<A::Event, A::Error, S> {
        let decision = A::decide(&self.folded, &self.command);
        match decision {
            Ok(events) if events.is_empty() => {
                self.phase = Phase::Done;
                WriteAction::Done(WriteOutcome::Noop)
            }
            Ok(events) => {
                self.phase = Phase::Appending;
                let expected = ExpectedVersion::after(self.version);
                // Stamp the interaction's metadata on every event the
                // decision produced (0.5.2). The domain decided which
                // events; the boundary decided *why* (correlation,
                // causation).
                let metadata = self.metadata.clone();
                WriteAction::Append {
                    stream_id: self.stream_id.clone(),
                    expected,
                    events: events
                        .into_iter()
                        .map(|event| NewEvent {
                            event,
                            metadata: metadata.clone(),
                        })
                        .collect(),
                }
            }
            Err(error) => {
                self.phase = Phase::Done;
                WriteAction::Done(WriteOutcome::Rejected(error))
            }
        }
    }

    fn violation(&mut self, message: &'static str) -> WriteAction<A::Event, A::Error, S> {
        self.phase = Phase::Done;
        WriteAction::Done(WriteOutcome::Failed(StoreError::protocol(message)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{EventEnvelope, Metadata};
    use crate::snapshot::{Snapshot, SnapshotPolicy};
    use crate::testing::account::{
        Account, AccountCommand, AccountError, AccountEvent, AccountId, AccountState,
    };
    use crate::testing::drive_scripted;
    use crate::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
    use alloc::string::String;
    use core::num::NonZeroU64;
    use proptest::prelude::*;

    // -- helpers ---------------------------------------------------------

    fn stream() -> StreamId {
        StreamId::for_aggregate::<Account>(&AccountId(7))
    }

    fn env(version: u64, event: AccountEvent) -> EventEnvelope<AccountEvent> {
        EventEnvelope {
            sequence: Sequence::new(version), // fake global position; only version matters
            stream_id: stream(),
            version: Version::new(version),
            event,
            metadata: Metadata::default(),
        }
    }

    fn machine(command: AccountCommand) -> WriteMachine<Account> {
        WriteMachine::new(AccountId(7), command, RetryPolicy::default())
    }

    fn snap_machine(command: AccountCommand, every: u64) -> WriteMachine<Account, AccountState> {
        WriteMachine::with_snapshots(
            AccountId(7),
            command,
            RetryPolicy::default(),
            SnapshotPolicy::new(NonZeroU64::new(every).unwrap()),
        )
    }

    fn snapshot(version: u64, balance: u64) -> Snapshot<AccountState> {
        Snapshot {
            stream_id: stream(),
            version: Version::new(version),
            state: AccountState {
                open: true,
                balance,
            },
        }
    }

    /// Whether `action` is the protocol-violation outcome.
    fn is_protocol_violation<E, Err, S>(action: &WriteAction<E, Err, S>) -> bool {
        matches!(
            action,
            WriteAction::Done(WriteOutcome::Failed(error)) if error.is_protocol_violation()
        )
    }

    /// Assert `action` is the protocol-violation outcome, naming the
    /// scenario step on failure.
    macro_rules! assert_protocol_violation {
        ($action:expr) => {
            assert!(
                is_protocol_violation(&$action),
                "expected a protocol-violation outcome"
            );
        };
        ($action:expr, $step:expr) => {
            assert!(
                is_protocol_violation(&$action),
                "[{}] expected a protocol-violation outcome",
                $step
            );
        };
    }

    // -- transitions (snapshots off: the pre-snapshot protocol) ----------

    #[test]
    fn start_loads_the_whole_stream() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        assert!(matches!(
            m.start(),
            WriteAction::LoadStream { ref stream_id, from }
                if stream_id.as_str() == "account-7" && from == Version::EMPTY
        ));
    }

    #[test]
    fn new_stream_append_expects_empty() {
        let mut m = machine(AccountCommand::Open {
            owner: String::from("me"),
        });
        m.start();
        let action = m.handle(WriteInput::Loaded { events: vec![] });
        let WriteAction::Append {
            stream_id,
            expected,
            events,
        } = action
        else {
            panic!("expected an append action")
        };
        assert_eq!(stream_id.as_str(), "account-7");
        assert_eq!(expected, ExpectedVersion::Empty);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn existing_stream_append_expects_exact_version() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        let WriteAction::Append { expected, .. } = action else {
            panic!("expected an append action")
        };
        assert_eq!(expected, ExpectedVersion::Exact(Version::new(1)));
    }

    #[test]
    fn appended_ends_in_committed_without_a_snapshot_offer() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        let committed = vec![env(2, AccountEvent::Deposited { amount: 5 })];
        let action = m.handle(WriteInput::Appended {
            committed: committed.clone(),
        });
        let WriteAction::Done(WriteOutcome::Committed {
            committed: events,
            snapshot,
        }) = action
        else {
            panic!("expected a committed outcome")
        };
        assert_eq!(events, committed);
        assert!(snapshot.is_none(), "snapshots off means no offer");
        assert!(m.is_done());
    }

    #[test]
    fn rejected_command_skips_the_store() {
        let mut m = machine(AccountCommand::Withdraw { amount: 10 }); // not open
        m.start();
        let action = m.handle(WriteInput::Loaded { events: vec![] });
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::Rejected(AccountError::NotOpen))
        ));
    }

    #[test]
    fn noop_command_ends_without_append() {
        let mut m = machine(AccountCommand::CheckBalance);
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        assert!(matches!(action, WriteAction::Done(WriteOutcome::Noop)));
    }

    #[test]
    fn conflict_reloads_only_the_delta() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        m.handle(WriteInput::Loaded {
            events: vec![
                env(
                    1,
                    AccountEvent::Opened {
                        owner: String::from("me"),
                    },
                ),
                env(2, AccountEvent::Deposited { amount: 10 }),
            ],
        });
        // Another writer appended v3..v4 while we were deciding.
        let action = m.handle(WriteInput::Conflict {
            current: Version::new(4),
        });
        assert!(matches!(
            action,
            WriteAction::LoadStream { from, .. } if from == Version::new(2)
        ));
        let action = m.handle(WriteInput::Loaded {
            events: vec![
                env(3, AccountEvent::Deposited { amount: 7 }),
                env(4, AccountEvent::Deposited { amount: 8 }),
            ],
        });
        let WriteAction::Append { expected, .. } = action else {
            panic!("expected an append action")
        };
        assert_eq!(expected, ExpectedVersion::Exact(Version::new(4)));
    }

    #[test]
    fn conflict_exhausts_the_retry_budget() {
        let mut m = WriteMachine::<Account>::new(
            AccountId(7),
            AccountCommand::Deposit { amount: 5 },
            RetryPolicy::new(1),
        );
        m.start();
        m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        m.handle(WriteInput::Conflict {
            current: Version::new(3),
        }); // retry 1 of 1
        m.handle(WriteInput::Loaded {
            events: vec![env(2, AccountEvent::Deposited { amount: 1 })],
        });
        let action = m.handle(WriteInput::Conflict {
            current: Version::new(5),
        });
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::Failed(StoreError::Conflict { current, .. }))
                if current == Version::new(5)
        ));
    }

    #[test]
    fn retry_policy_never_fails_on_first_conflict() {
        let mut m = WriteMachine::<Account>::new(
            AccountId(7),
            AccountCommand::Deposit { amount: 5 },
            RetryPolicy::NEVER,
        );
        m.start();
        m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        let action = m.handle(WriteInput::Conflict {
            current: Version::new(2),
        });
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::Failed(StoreError::Conflict { .. }))
        ));
    }

    #[test]
    fn store_failure_during_load_ends_the_interaction() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Failed(StoreError::Unavailable));
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::Failed(StoreError::Unavailable))
        ));
    }

    #[test]
    fn store_failure_during_append_ends_the_interaction() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        let action = m.handle(WriteInput::Failed(StoreError::Unavailable));
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::Failed(StoreError::Unavailable))
        ));
    }

    // -- protocol violations (snapshots off) ------------------------------

    #[test]
    fn appended_after_done_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::CheckBalance);
        m.start();
        m.handle(WriteInput::Loaded { events: vec![] }); // Done(Noop)
        assert_protocol_violation!(m.handle(WriteInput::Appended { committed: vec![] }));
    }

    #[test]
    fn start_after_done_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::CheckBalance);
        m.start();
        m.handle(WriteInput::Loaded { events: vec![] }); // Done(Noop)
        assert_protocol_violation!(m.start());
    }

    #[test]
    fn loaded_outside_loading_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        m.handle(WriteInput::Loaded { events: vec![] }); // → Appending
        assert!(is_protocol_violation(
            &m.handle(WriteInput::Loaded { events: vec![] })
        ));
    }

    #[test]
    fn appended_outside_appending_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        assert!(is_protocol_violation(
            &m.handle(WriteInput::Appended { committed: vec![] })
        ));
    }

    #[test]
    fn non_contiguous_events_are_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![env(2, AccountEvent::Deposited { amount: 1 })], // gap at v1
        });
        assert!(
            is_protocol_violation(&action),
            "expected a protocol-violation outcome"
        );
    }

    #[test]
    fn events_from_another_stream_are_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        let mut envelope = env(
            1,
            AccountEvent::Opened {
                owner: String::from("me"),
            },
        );
        envelope.stream_id = StreamId::from("other-stream");
        let action = m.handle(WriteInput::Loaded {
            events: vec![envelope],
        });
        assert!(
            is_protocol_violation(&action),
            "expected a protocol-violation outcome"
        );
    }

    #[test]
    fn conflict_at_or_before_the_folded_version_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        // Folded version is 1; a conflict claiming current == 1 means
        // the store reported a conflict at the expected version.
        let action = m.handle(WriteInput::Conflict {
            current: Version::new(1),
        });
        assert!(
            is_protocol_violation(&action),
            "expected a protocol-violation outcome"
        );
    }

    // -- the scripted driver ----------------------------------------------

    #[test]
    fn scripted_driver_records_every_action() {
        let mut m = machine(AccountCommand::Open {
            owner: String::from("me"),
        });
        let actions = drive_scripted(
            &mut m,
            vec![
                WriteInput::Loaded { events: vec![] },
                WriteInput::Appended {
                    committed: vec![env(
                        1,
                        AccountEvent::Opened {
                            owner: String::from("me"),
                        },
                    )],
                },
            ],
        );
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], WriteAction::LoadStream { .. }));
        assert!(matches!(actions[1], WriteAction::Append { .. }));
        assert!(matches!(
            actions[2],
            WriteAction::Done(WriteOutcome::Committed { .. })
        ));
    }

    // -- snapshots on -----------------------------------------------------

    #[test]
    fn with_snapshots_starts_with_a_snapshot_read() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        assert!(matches!(
            m.start(),
            WriteAction::LoadSnapshot { ref stream_id } if stream_id.as_str() == "account-7"
        ));
    }

    #[test]
    fn no_snapshot_loads_the_full_stream_and_appends() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        let action = m.handle(WriteInput::SnapshotLoaded { snapshot: None });
        assert!(matches!(
            action,
            WriteAction::LoadStream { from, .. } if from == Version::EMPTY
        ));
        let action = m.handle(WriteInput::Loaded {
            events: vec![env(
                1,
                AccountEvent::Opened {
                    owner: String::from("me"),
                },
            )],
        });
        assert!(matches!(action, WriteAction::Append { .. }));
    }

    #[test]
    fn a_snapshot_shortens_the_stream_read_to_the_delta() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        let action = m.handle(WriteInput::SnapshotLoaded {
            snapshot: Some(snapshot(40, 900)),
        });
        assert!(matches!(
            action,
            WriteAction::LoadStream { from, .. } if from == Version::new(40)
        ));
        assert_eq!(m.version(), Version::new(40));

        // Fold the delta onto the snapshot's state: 900 + 7 = 907, and
        // the decided deposit of 5 lands on top of it at append time.
        let action = m.handle(WriteInput::Loaded {
            events: vec![env(41, AccountEvent::Deposited { amount: 7 })],
        });
        let WriteAction::Append {
            expected, events, ..
        } = action
        else {
            panic!("expected an append action")
        };
        assert_eq!(expected, ExpectedVersion::Exact(Version::new(41)));
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn commit_offers_a_snapshot_when_the_policy_fires() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 2);
        m.start();
        m.handle(WriteInput::SnapshotLoaded {
            snapshot: Some(snapshot(10, 100)),
        });
        m.handle(WriteInput::Loaded {
            events: vec![env(11, AccountEvent::Deposited { amount: 50 })],
        });
        // Progress since base 10: folded one event (v11) + one committing
        // (v12) = 2 ≥ every(2) → offer.
        let action = m.handle(WriteInput::Appended {
            committed: vec![env(12, AccountEvent::Deposited { amount: 5 })],
        });
        let WriteAction::Done(WriteOutcome::Committed {
            snapshot: Some(offer),
            ..
        }) = action
        else {
            panic!("expected a committed outcome carrying a snapshot offer")
        };
        let offered = offer.into_inner();
        assert_eq!(offered.version, Version::new(12));
        assert_eq!(offered.stream_id, stream());
        // The offered state is the snapshot's, with the folded delta and
        // the committed event on top: 100 + 50 + 5.
        assert_eq!(offered.state.balance, 155);
        assert!(offered.state.open);
    }

    #[test]
    fn commit_offers_no_snapshot_below_the_cadence() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        m.handle(WriteInput::SnapshotLoaded {
            snapshot: Some(snapshot(10, 100)),
        });
        m.handle(WriteInput::Loaded {
            events: vec![env(11, AccountEvent::Deposited { amount: 50 })],
        });
        let action = m.handle(WriteInput::Appended {
            committed: vec![env(12, AccountEvent::Deposited { amount: 5 })],
        });
        let WriteAction::Done(WriteOutcome::Committed { snapshot, .. }) = action else {
            panic!("expected a committed outcome")
        };
        assert!(snapshot.is_none(), "two versions of progress < every(10)");
    }

    #[test]
    fn snapshot_policy_cadence_counts_from_the_base() {
        let every3 = SnapshotPolicy::new(NonZeroU64::new(3).unwrap());
        assert!(!every3.is_due(Version::new(10), Version::new(12)));
        assert!(every3.is_due(Version::new(10), Version::new(13)));
        // No base snapshot: base is EMPTY.
        assert!(!every3.is_due(Version::EMPTY, Version::new(2)));
        assert!(every3.is_due(Version::EMPTY, Version::new(3)));
    }

    #[test]
    fn snapshot_for_another_stream_is_a_protocol_violation() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        let mut alien = snapshot(4, 100);
        alien.stream_id = StreamId::from("account-999");
        let action = m.handle(WriteInput::SnapshotLoaded {
            snapshot: Some(alien),
        });
        assert!(
            is_protocol_violation(&action),
            "expected a protocol-violation outcome"
        );
    }

    #[test]
    fn snapshot_delta_must_continue_from_the_snapshot_version() {
        // The monotonicity guard: a snapshot at v40 against a delta that
        // starts at v41 is fine; a stale/replayed delta starting at v12,
        // or one past the tip the snapshot claims, is a violation.
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        m.handle(WriteInput::SnapshotLoaded {
            snapshot: Some(snapshot(40, 900)),
        });
        let action = m.handle(WriteInput::Loaded {
            events: vec![env(12, AccountEvent::Deposited { amount: 1 })],
        });
        assert!(
            is_protocol_violation(&action),
            "expected a protocol-violation outcome"
        );
    }

    #[test]
    fn snapshot_loaded_on_a_snapshots_off_machine_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::<AccountEvent>::SnapshotLoaded { snapshot: None });
        assert!(
            is_protocol_violation(&action),
            "expected a protocol-violation outcome"
        );
    }

    #[test]
    fn snapshot_loaded_after_the_load_phase_is_a_protocol_violation() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        m.handle(WriteInput::SnapshotLoaded { snapshot: None });
        assert!(is_protocol_violation(
            &m.handle(WriteInput::SnapshotLoaded { snapshot: None })
        ));
    }

    #[test]
    fn snapshot_read_failure_fails_the_interaction() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        let action = m.handle(WriteInput::Failed(StoreError::Unavailable));
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::Failed(StoreError::Unavailable))
        ));
    }

    #[test]
    fn conflict_after_a_snapshot_reloads_the_delta_not_the_snapshot() {
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 10);
        m.start();
        m.handle(WriteInput::SnapshotLoaded {
            snapshot: Some(snapshot(40, 900)),
        });
        m.handle(WriteInput::Loaded {
            events: vec![env(41, AccountEvent::Deposited { amount: 7 })],
        });
        let action = m.handle(WriteInput::Conflict {
            current: Version::new(43),
        });
        // The retry reloads the event delta from the folded version —
        // never a second LoadSnapshot.
        assert!(matches!(
            action,
            WriteAction::LoadStream { from, .. } if from == Version::new(41)
        ));
        // And the retry's fold lands on the snapshot's state: 900 + 7 +
        // v42 + v43's deposits, then decides on top.
        let action = m.handle(WriteInput::Loaded {
            events: vec![
                env(42, AccountEvent::Deposited { amount: 1 }),
                env(43, AccountEvent::Deposited { amount: 2 }),
            ],
        });
        assert!(matches!(action, WriteAction::Append { .. }));
    }

    // -- properties -------------------------------------------------------

    fn arb_write_input() -> BoxedStrategy<WriteInput<AccountEvent>> {
        prop_oneof![
            (1u64..8, 0u64..8).prop_map(|(start, len)| {
                let events = (0..len)
                    .map(|i| env(start + i, AccountEvent::Deposited { amount: 1 }))
                    .collect();
                WriteInput::Loaded { events }
            }),
            (0u64..8).prop_map(|len| {
                let committed = (1..=len)
                    .map(|v| env(v, AccountEvent::Deposited { amount: 1 }))
                    .collect();
                WriteInput::Appended { committed }
            }),
            (0u64..10).prop_map(|current| WriteInput::Conflict {
                current: Version::new(current)
            }),
            Just(WriteInput::Failed(StoreError::Unavailable)),
        ]
        .boxed()
    }

    fn arb_snapshot_input() -> BoxedStrategy<WriteInput<AccountEvent, AccountState>> {
        prop_oneof![
            (1u64..8, 0u64..8).prop_map(|(start, len)| {
                let events = (0..len)
                    .map(|i| env(start + i, AccountEvent::Deposited { amount: 1 }))
                    .collect();
                WriteInput::Loaded { events }
            }),
            (0u64..8).prop_map(|len| {
                let committed = (1..=len)
                    .map(|v| env(v, AccountEvent::Deposited { amount: 1 }))
                    .collect();
                WriteInput::Appended { committed }
            }),
            (0u64..10).prop_map(|current| WriteInput::Conflict {
                current: Version::new(current)
            }),
            Just(WriteInput::Failed(StoreError::Unavailable)),
            (0u64..10, any::<bool>()).prop_map(|(version, some)| {
                WriteInput::SnapshotLoaded {
                    snapshot: some.then(|| snapshot(version, 0)),
                }
            }),
        ]
        .boxed()
    }

    // -- idempotency keys (0.7.5) -----------------------------------------

    fn keyed(key: &str, version: u64, event: AccountEvent) -> EventEnvelope<AccountEvent> {
        EventEnvelope {
            metadata: Metadata::default().with_idempotency_key(key),
            ..env(version, event)
        }
    }

    fn opened() -> AccountEvent {
        AccountEvent::Opened {
            owner: String::from("me"),
        }
    }

    fn keyed_machine(key: &str, command: AccountCommand) -> WriteMachine<Account> {
        machine(command).with_metadata(Metadata::default().with_idempotency_key(key))
    }

    #[test]
    fn a_keyed_command_stamps_its_key_on_every_event() {
        let mut m = keyed_machine("cmd-1", AccountCommand::Deposit { amount: 5 });
        m.start();
        let WriteAction::Append { events, .. } = m.handle(WriteInput::Loaded {
            events: vec![env(1, opened())],
        }) else {
            panic!("append");
        };
        assert_eq!(events[0].metadata.idempotency_key.as_deref(), Some("cmd-1"));
    }

    #[test]
    fn a_replayed_key_returns_the_earlier_commit_without_deciding() {
        // The command already ran: its deposit is in the stream. Run
        // again it would deposit twice; with the key it decides nothing.
        let mut m = keyed_machine("cmd-1", AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![
                env(1, opened()),
                keyed("cmd-1", 2, AccountEvent::Deposited { amount: 5 }),
                env(3, AccountEvent::Deposited { amount: 1 }),
            ],
        });
        let WriteAction::Done(WriteOutcome::AlreadyCommitted { committed }) = action else {
            panic!("expected AlreadyCommitted, got {action:?}");
        };
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].version, Version::new(2));
    }

    #[test]
    fn a_key_rejected_before_is_decided_again() {
        // Rejections append nothing, so they leave no trace to match:
        // the command is decided against the current state, which may
        // now accept it.
        let mut m = keyed_machine("cmd-1", AccountCommand::Withdraw { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![
                env(1, opened()),
                env(2, AccountEvent::Deposited { amount: 9 }),
            ],
        });
        assert!(matches!(action, WriteAction::Append { .. }));
    }

    #[test]
    fn a_concurrent_duplicate_is_caught_on_the_conflict_retry() {
        let mut m = keyed_machine("cmd-1", AccountCommand::Deposit { amount: 5 });
        m.start();
        m.handle(WriteInput::Loaded {
            events: vec![env(1, opened())],
        });
        // A twin of this command committed first.
        let reload = m.handle(WriteInput::Conflict {
            current: Version::new(2),
        });
        assert!(matches!(reload, WriteAction::LoadStream { from, .. } if from == Version::new(1)));
        let action = m.handle(WriteInput::Loaded {
            events: vec![keyed("cmd-1", 2, AccountEvent::Deposited { amount: 5 })],
        });
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::AlreadyCommitted { .. })
        ));
    }

    #[test]
    fn other_keys_and_unkeyed_events_do_not_match() {
        let mut m = keyed_machine("cmd-1", AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![
                env(1, opened()),
                keyed("cmd-2", 2, AccountEvent::Deposited { amount: 5 }),
            ],
        });
        assert!(matches!(action, WriteAction::Append { .. }));

        // An unkeyed command never matches anything.
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![
                env(1, opened()),
                keyed("cmd-1", 2, AccountEvent::Deposited { amount: 5 }),
            ],
        });
        assert!(matches!(action, WriteAction::Append { .. }));
    }

    #[test]
    fn a_keyed_snapshots_on_machine_loads_the_whole_stream() {
        // The earlier commit may predate the newest snapshot, where a
        // delta load would never see it.
        let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 1)
            .with_metadata(Metadata::default().with_idempotency_key("cmd-1"));
        assert!(matches!(
            m.start(),
            WriteAction::LoadStream { from, .. } if from == Version::EMPTY
        ));
        let action = m.handle(WriteInput::Loaded {
            events: vec![
                env(1, opened()),
                keyed("cmd-1", 2, AccountEvent::Deposited { amount: 5 }),
            ],
        });
        assert!(matches!(
            action,
            WriteAction::Done(WriteOutcome::AlreadyCommitted { .. })
        ));
    }

    proptest! {
        /// The machine never panics on any input sequence, and once it
        /// is done it stays done: every action after the first `Done` is
        /// a protocol-violation `Done`. Snapshots off.
        #[test]
        fn never_panics_and_stays_done(script in prop::collection::vec(arb_write_input(), 0..16)) {
            let mut m = machine(AccountCommand::Deposit { amount: 5 });
            let actions = drive_scripted(&mut m, script);

            prop_assert!(!actions.is_empty()); // start always emits
            if let Some(i) = actions.iter().position(|a| matches!(a, WriteAction::Done(_))) {
                for a in &actions[i + 1..] {
                    prop_assert!(is_protocol_violation(a));
                }
            }
        }

        /// Same invariant with snapshots on: arbitrary input soup —
        /// including snapshot answers at arbitrary points — never panics
        /// and never un-dones the machine.
        #[test]
        fn snapshots_on_never_panics_and_stays_done(
            script in prop::collection::vec(arb_snapshot_input(), 0..16)
        ) {
            let mut m = snap_machine(AccountCommand::Deposit { amount: 5 }, 3);
            let mut actions: Vec<WriteAction<AccountEvent, AccountError, AccountState>> = Vec::new();
            actions.push(m.start());
            for input in script {
                actions.push(m.handle(input));
            }

            prop_assert!(!actions.is_empty());
            if let Some(i) = actions.iter().position(|a| matches!(a, WriteAction::Done(_))) {
                for a in &actions[i + 1..] {
                    prop_assert!(is_protocol_violation(a));
                }
            }
        }
    }
}
