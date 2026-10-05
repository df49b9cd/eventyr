//! The subscription machine and its vocabulary: the sans-IO core of the
//! catch-up projector (DESIGN.md §6, §7).
//!
//! [`SubscriptionMachine`] consumes driver results ([`SubscriptionInput`])
//! and emits I/O requests ([`SubscriptionAction`]); the driver performs
//! them. It is the machine the §7 table calls `ProjectorMachine`; it is
//! named for its §6-facing role. Its protocol is the §7 table verbatim —
//! inputs `Fetched`/`ApplyFailed`/`AckFailed`, actions
//! `Apply`/`Ack`/`Sleep`/`Done` — plus two augmentation steps:
//!
//! - **`Fetch`**: the table's `Batch` input must arrive, and §6's
//!   `Subscription::poll` plus the [`StreamsAll`](https://docs.rs/eventyr-store)
//!   pull model make *requesting* it the faithful completion. The machine
//!   therefore emits [`Fetch`](SubscriptionAction::Fetch) and waits for
//!   [`Fetched`](SubscriptionInput::Fetched) — "poll twice" in the table's
//!   terms. The delta between the table and the implemented protocol is
//!   this one implicit step, made explicit so it stays testable.
//! - **`Slept`**: the machine has no clock, so [`Sleep`](SubscriptionAction::Sleep)
//!   carries a machine-computed [`Duration`] as
//!   data and the driver reports [`Slept`](SubscriptionInput::Slept) after
//!   it elapses.
//! - **`Shutdown`**: graceful stop — drains the in-flight batch, acks,
//!   and ends at [`Stopped`](SubscriptionOutcome::Stopped).
//!
//! Delivery is at-least-once: the checkpoint moves only after the whole
//! batch applied, and any apply/ack failure falls back to the last ack,
//! so the same envelope may be delivered twice — projections must apply
//! idempotently.
//!
//! ## Testing
//!
//! The machine is pure; [`projector_scripted`](crate::testing::projector_scripted)
//! drives it against a [`Vec`] of scripted inputs — no runtime. Machine
//! tests feed [`Slept`](SubscriptionInput::Slept) instantly (the duration
//! is data); only crate-side driver tests touch a wall clock.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;

use crate::envelope::EventEnvelope;
use crate::error::{ProtocolError, StoreError};
use crate::vocabulary::Sequence;

/// A subscription's resume position: the last globally-acked sequence.
///
/// The newtype twin of [`Version::EMPTY`](crate::vocabulary::Version::EMPTY): as an exclusive lower bound,
/// [`ORIGIN`](Checkpoint::ORIGIN) means "from the very first event".
/// New — not an alias for [`Sequence`] — so a checkpoint is never
/// confused with raw envelopes' sequences at call sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Checkpoint(Sequence);

impl Checkpoint {
    /// Before any event: "from the origin". Wraps [`Sequence::START`].
    pub const ORIGIN: Self = Self(Sequence::START);

    /// Wrap a sequence as a checkpoint.
    pub const fn new(sequence: Sequence) -> Self {
        Self(sequence)
    }

    /// The underlying sequence (the exclusive lower bound for polls).
    pub const fn as_sequence(self) -> Sequence {
        self.0
    }
}

impl fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One poll's worth of events, delivered in global sequence order.
///
/// An empty `events` with `upper: None` is a caught-up poll: nothing left
/// after the fetch bound. An empty batch never moves the checkpoint, so
/// an idle subscription provably never stalls mid-batch or regresses.
///
/// Sequences must be strictly increasing and strictly after the acked
/// checkpoint — but they need not be contiguous. Stores backing the
/// global sequence with a database identity column burn values on
/// rolled-back appends, and a snapshot read can observe a later commit
/// before an earlier one: a gap is treated as permanently absent and
/// skipped, never stalled on. The at-least-once invariant is kept
/// because the checkpoint moves only to the last *delivered* sequence:
/// every sequence below the ack bound was either delivered or skipped
/// as a gap it will never fill. (Stores therefore must not emit a later
/// sequence before an earlier one is durably visible; see
/// `StreamsAll`.)
///
/// Delivery is at-least-once: after a crash between apply and ack, the
/// next poll after the last acked checkpoint re-delivers these events.
#[derive(Clone, Debug)]
pub struct Batch<E> {
    /// The events, strictly increasing in global sequence after the poll
    /// bound (gaps tolerated and skipped).
    pub events: Vec<EventEnvelope<E>>,
    /// Highest sequence in `events`: the ack target. `None` exactly when
    /// `events` is empty (a caught-up poll acknowledges nothing).
    pub upper: Option<Checkpoint>,
    /// How far a *filtered* source scanned (0.7.4): every sequence up to
    /// here was delivered, filtered out, or is a permanent gap. `None`
    /// for unfiltered sources, where the scan ends at `upper`.
    ///
    /// When it lies past `upper` the machine acks it instead, so a
    /// projection over a sparse filter checkpoints past long unmatched
    /// runs rather than re-scanning them after every restart. A batch
    /// with no events but a scan past the checkpoint is acked without
    /// applying anything. It must never lie below `upper`, and a source
    /// may only report it under `StreamsAll`'s visibility rule — no
    /// later sequence visible before an earlier one that will commit.
    pub scanned: Option<Checkpoint>,
}

impl<E> Batch<E> {
    /// An empty (caught-up) batch.
    pub const fn empty() -> Self {
        Self {
            events: Vec::new(),
            upper: None,
            scanned: None,
        }
    }

    /// Wrap deliveries: `upper` must be the highest sequence in `events`,
    /// or `None` exactly when `events` is empty. The machine validates
    /// both invariants on arrival regardless.
    pub fn new(events: Vec<EventEnvelope<E>>, upper: Option<Checkpoint>) -> Self {
        Self {
            events,
            upper,
            scanned: None,
        }
    }

    /// Builder-style: the source scanned through `scanned` (see
    /// [`scanned`](Self::scanned)).
    pub fn scanned_to(mut self, scanned: Checkpoint) -> Self {
        self.scanned = Some(scanned);
        self
    }
}

/// Tuning for the subscription machine and its driver. Mirrors
/// [`RetryPolicy`](crate::write::RetryPolicy): a config newtype with
/// `const fn` construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionPolicy {
    /// Maximum events per [`Fetch`](SubscriptionAction::Fetch).
    pub batch_size: usize,
    /// How long the driver waits after a caught-up (empty) batch before
    /// polling again. Data — the machine has no clock; the driver waits.
    pub idle_sleep: Duration,
    /// How long the driver waits after
    /// [`ApplyFailed`](SubscriptionInput::ApplyFailed) /
    /// [`AckFailed`](SubscriptionInput::AckFailed) before re-fetching
    /// from the last ack.
    pub retry_sleep: Duration,
    /// `true` = stop after catching up with the store's position:
    /// [`Done`](SubscriptionAction::Done)`(`[`CaughtUp`](SubscriptionOutcome::CaughtUp)`)`
    /// on the first empty poll after the final ack. `false` (the
    /// default) = perennial: the subscription idles and keeps polling
    /// forever. Termination on success exists only on request — a
    /// subscription that stopped merely because it caught up would make
    /// `is_done` mean the wrong thing for the perennial case.
    pub stop_at_catch_up: bool,
    /// What to do with an event the projection keeps rejecting (0.7.7).
    /// [`Halt`](FailurePolicy::Halt), the default, retries it forever.
    pub on_failure: FailurePolicy,
}

/// What a subscription does with an event its projection keeps
/// rejecting (0.7.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FailurePolicy {
    /// Back off and redeliver forever: the projection stalls on the
    /// event until it applies. The default — skipping an event is a
    /// correctness decision, never one the library makes for you.
    #[default]
    Halt,
    /// Redeliver up to `retries` more times; then
    /// [`Park`](SubscriptionAction::Park) the event — record it for
    /// inspection and replay — and carry on past it as if it had
    /// applied. Only rejections by the projection count: a failed ack
    /// or fetch is the infrastructure's fault, not the event's.
    Park {
        /// Redeliveries before parking. `0` parks on the first
        /// rejection.
        retries: u32,
    },
}

impl SubscriptionPolicy {
    /// A policy: `batch_size` events per fetch, `idle_sleep` between
    /// caught-up polls, `retry_sleep` after a failed apply or ack.
    pub const fn new(batch_size: usize, idle_sleep: Duration, retry_sleep: Duration) -> Self {
        Self {
            batch_size,
            idle_sleep,
            retry_sleep,
            stop_at_catch_up: false,
            on_failure: FailurePolicy::Halt,
        }
    }

    /// `self`, with `policy` for events the projection keeps rejecting.
    pub const fn on_failure(mut self, policy: FailurePolicy) -> Self {
        self.on_failure = policy;
        self
    }

    /// `self`, stopping after the first caught-up poll once caught up.
    pub const fn stop_at_catch_up(mut self) -> Self {
        self.stop_at_catch_up = true;
        self
    }
}

impl Default for SubscriptionPolicy {
    fn default() -> Self {
        Self::new(128, Duration::from_millis(100), Duration::from_secs(1))
    }
}

/// What the machine wants the driver to do.
///
/// Actions are data, not calls: the driver interprets each variant,
/// performs the I/O, and reports back with a [`SubscriptionInput`].
#[derive(Clone, Debug)]
pub enum SubscriptionAction<E> {
    /// Read up to `policy.batch_size` events after `from` (exclusive),
    /// in global sequence order. §7's implied poll step — the table's
    /// `Batch` input is this action's answer.
    Fetch {
        /// Resume from here — always the last acked checkpoint.
        from: Checkpoint,
        /// At most this many events.
        limit: usize,
    },
    /// Left-fold one event into the projection. At-least-once — the
    /// projection may see the same envelope again after a failure, so
    /// it must apply idempotently.
    Apply {
        /// The event to fold.
        envelope: EventEnvelope<E>,
    },
    /// Record `envelope` as parked (0.7.7): the projection rejected it
    /// `attempts` times and the [`FailurePolicy`] gave up on it. Answer
    /// [`Parked`](SubscriptionInput::Parked) once it is durably
    /// recorded — the machine then carries on past it — or
    /// [`ParkFailed`](SubscriptionInput::ParkFailed).
    Park {
        /// The event given up on.
        envelope: EventEnvelope<E>,
        /// How many times it was rejected.
        attempts: u32,
        /// The last rejection, rendered.
        error: StoreError,
    },
    /// Persist the checkpoint: every event up to `checkpoint` is applied.
    Ack {
        /// The batch's upper bound.
        checkpoint: Checkpoint,
    },
    /// Caught up or backing off; the driver waits `for_`, then reports
    /// [`Slept`](SubscriptionInput::Slept). The duration is the machine's
    /// decision; only the wait performs I/O (a clock).
    ///
    /// `reason` tells the driver whether the wait may end early: an
    /// [`Idle`](SleepReason::Idle) wait exists only because nothing new
    /// was visible, so a driver that learns of a commit (0.7.2's commit
    /// signal) may report `Slept` at once; a
    /// [`Backoff`](SleepReason::Backoff) is the retry delay after a
    /// failure and must run its course — new events are no reason to
    /// hammer a failing projection.
    Sleep {
        /// How long to wait.
        for_: Duration,
        /// Why the machine is waiting.
        reason: SleepReason,
    },
    /// Terminal. Normal operation never reaches this — subscriptions are
    /// perennial; only a fatal store error, a protocol violation, a
    /// [`Shutdown`](SubscriptionInput::Shutdown), or a
    /// [`stop_at_catch_up`](SubscriptionPolicy::stop_at_catch_up)
    /// policy ends a subscription.
    Done(SubscriptionOutcome),
}

/// Why a subscription [`Sleep`](SubscriptionAction::Sleep)s.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SleepReason {
    /// Caught up: the last poll was empty. A wake-up on commit may end
    /// the wait early.
    Idle,
    /// Backing off after a failed apply or ack. The wait runs its
    /// course.
    Backoff,
}

/// What the driver reports back to the machine.
#[derive(Clone, Debug)]
pub enum SubscriptionInput<E> {
    /// The answer to [`Fetch`](SubscriptionAction::Fetch): events
    /// strictly increasing in global sequence after the requested
    /// checkpoint (gaps tolerated and skipped — see [`Batch`]).
    Fetched {
        /// The fetched events and their upper bound.
        batch: Batch<E>,
    },
    /// The projection accepted the event.
    Applied,
    /// The projection rejected the event, carrying a description of the
    /// rejection so the driver can report it. The machine sleeps and
    /// re-fetches from the last ack, so the offending (and any later)
    /// event redelivers — the at-least-once contract. The machine does
    /// not branch on `error`: the payload exists so a permanently
    /// unprocessable (poison) event is diagnosable instead of looping
    /// invisibly; the checkpointer/observer may count repeats and skip
    /// or dead-letter them.
    ApplyFailed {
        /// Why the projection rejected the event, rendered for logging.
        error: StoreError,
    },
    /// The parked event was recorded (0.7.7).
    Parked,
    /// Recording the parked event failed. The event is not skipped:
    /// the machine backs off and redelivers, as for a rejection.
    ParkFailed,
    /// The checkpoint write persisted.
    Acked,
    /// The checkpoint write failed. Same as an apply failure: sleep and
    /// re-fetch from the last ack; the un-acked events re-apply.
    AckFailed,
    /// The sleep elapsed.
    Slept,
    /// A store operation failed fatally (e.g. [`StoreError::Unavailable`]),
    /// outside the apply/ack paths.
    Failed(StoreError),
    /// Request to stop: finish any in-flight batch, ack it, and end at
    /// [`Stopped`](SubscriptionOutcome::Stopped). Sleeping and fetching
    /// phases stop immediately; the checkpoint never regresses and no
    /// fetched-but-unapplied event is lost — the ack boundary is always
    /// a whole batch.
    Shutdown,
}

/// The terminal outcome of a driven subscription machine.
#[derive(Clone, Debug)]
pub enum SubscriptionOutcome {
    /// A requested graceful stop completed; `checkpoint` is where a
    /// restart resumes.
    Stopped {
        /// Last acked position.
        checkpoint: Checkpoint,
    },
    /// [`stop_at_catch_up`](SubscriptionPolicy::stop_at_catch_up) policy
    /// met: the first empty poll after the last in-store event.
    /// `checkpoint` is where a restart resumes.
    CaughtUp {
        /// Last acked position.
        checkpoint: Checkpoint,
    },
    /// A fatal store failure or a protocol violation by the driver
    /// (as [`StoreError::Other`] carrying a [`ProtocolError`]).
    Failed(StoreError),
}

/// Which input the phase guards accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for `Fetched`.
    Fetching,
    /// Waiting for `Applied`/`ApplyFailed` while draining the batch.
    Applying,
    /// Waiting for `Parked`/`ParkFailed`.
    Parking,
    /// Waiting for `Acked`/`AckFailed`.
    Acking,
    /// Waiting for `Slept`; trips back to fetch or stop at catch-up.
    Sleeping,
    /// Optional inter-phase when a shutdown was requested while
    /// sleeping: waiting for the *catch-up* fetch's `Fetched`.
    DrainingForStop,
    /// Terminal.
    Done,
}

/// The sans-IO machine behind a catch-up subscription: poll → apply each
/// event → ack → repeat, sleeping on empty or failure.
///
/// The machine owns the acked checkpoint, the pending batch, and the
/// cursor into it. It never does I/O: the driver performs each
/// [`SubscriptionAction`] and reports back with a [`SubscriptionInput`].
///
/// At-least-once is structural: the checkpoint moves only after the
/// whole batch applied, and `ApplyFailed`/`AckFailed` drop everything
/// not yet acked and re-fetch from the last ack — so the projection may
/// re-see events and must apply idempotently.
///
/// Machines never panic on bad input: a driver that feeds the wrong
/// input for the current phase, or drives a finished machine, gets
/// [`Done`](SubscriptionAction::Done) with
/// [`Failed(StoreError::Other(ProtocolError))`](SubscriptionOutcome::Failed).
pub struct SubscriptionMachine<E> {
    policy: SubscriptionPolicy,
    phase: Phase,
    /// The persisted position; also the exclusive fetch bound.
    acked: Checkpoint,
    /// The fetched batch: being applied, or awaiting ack.
    pending: Vec<EventEnvelope<E>>,
    /// How much of `pending` is applied (the next `Apply` is
    /// `pending[cursor]`).
    cursor: usize,
    /// A shutdown was requested for after the current batch's ack.
    stop_after_ack: bool,
    /// The upper bound of `pending`, captured at `Fetched`.
    upper: Option<Checkpoint>,
    /// The event the projection last rejected, and how many times in a
    /// row (0.7.7). Survives redelivery; cleared once the event applies
    /// or is parked.
    failing: Option<(crate::vocabulary::Sequence, u32)>,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Clone> SubscriptionMachine<E> {
    /// `resume_from` is the runner-loaded checkpoint —
    /// [`ORIGIN`](Checkpoint::ORIGIN) for a fresh projection.
    pub fn new(policy: SubscriptionPolicy, resume_from: Checkpoint) -> Self {
        Self {
            policy,
            phase: Phase::Fetching,
            acked: resume_from,
            pending: Vec::new(),
            cursor: 0,
            stop_after_ack: false,
            upper: None,
            failing: None,
            _marker: core::marker::PhantomData,
        }
    }

    /// The first action: read after the resume checkpoint. Idempotent
    /// until the first [`handle`](Self::handle).
    pub fn start(&mut self) -> SubscriptionAction<E> {
        if self.phase != Phase::Fetching {
            return self.violation("start() on a machine that already progressed");
        }
        self.fetch()
    }

    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: SubscriptionInput<E>) -> SubscriptionAction<E> {
        match input {
            SubscriptionInput::Fetched { batch } => self.on_fetched(batch),
            SubscriptionInput::Applied => self.on_applied(),
            SubscriptionInput::ApplyFailed { error } => self.on_apply_failed(error),
            SubscriptionInput::Acked => self.on_acked(),
            SubscriptionInput::AckFailed => self.on_ack_failed(),
            SubscriptionInput::Slept => self.on_slept(),
            SubscriptionInput::Parked => self.on_parked(),
            SubscriptionInput::ParkFailed => self.on_park_failed(),
            SubscriptionInput::Failed(error) => self.on_failed(error),
            SubscriptionInput::Shutdown => self.on_shutdown(),
        }
    }

    /// The last acked position — where a restart would resume.
    pub fn checkpoint(&self) -> Checkpoint {
        self.acked
    }

    /// Whether the machine reached its terminal outcome.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    fn fetch(&self) -> SubscriptionAction<E> {
        SubscriptionAction::Fetch {
            from: self.acked,
            limit: self.policy.batch_size,
        }
    }

    fn on_fetched(&mut self, batch: Batch<E>) -> SubscriptionAction<E> {
        if !matches!(self.phase, Phase::Fetching | Phase::DrainingForStop) {
            return self.violation("`Fetched` outside the fetching phase");
        }
        // Validate while applying bounds: sequences must be strictly
        // increasing and strictly after the acked checkpoint, so a
        // re-delivery or mis-ordering bug in the store lands as a
        // protocol violation, not corruption. Gaps are *not* a
        // violation: stores backing the global sequence with an identity
        // column burn values on rolled-back appends, and a gap is never
        // delivered later and out of order, so skipping it keeps the
        // at-least-once invariant — the checkpoint only advances past
        // sequences that were delivered or will never arrive.
        let floor = self.acked.as_sequence().as_u64();
        let mut last = floor;
        for envelope in &batch.events {
            let sequence = envelope.sequence.as_u64();
            if sequence <= last {
                return self.violation(
                    "`Fetched` delivered a sequence not strictly increasing past the ack bound",
                );
            }
            last = sequence;
        }
        // A filtered source may have scanned past its last delivery
        // (0.7.4). The scan bound may not trail the deliveries, and a
        // scan that went nowhere is no progress.
        let scanned = match (batch.scanned, batch.upper) {
            (Some(scanned), Some(upper)) if scanned < upper => {
                return self.violation("`Fetched` reported a scan bound below its last event");
            }
            (Some(scanned), _) if scanned < self.acked => {
                return self.violation("`Fetched` reported a scan bound behind the checkpoint");
            }
            (Some(scanned), _) if scanned > self.acked => Some(scanned),
            _ => None,
        };
        match (batch.events.is_empty(), batch.upper) {
            (true, None) if scanned.is_some() => {
                // Nothing matched, but the scan moved: ack it without
                // applying anything.
                self.upper = scanned;
                self.stop_after_ack = self.phase == Phase::DrainingForStop;
                self.phase = Phase::Acking;
                SubscriptionAction::Ack {
                    checkpoint: scanned.expect("matched on is_some"),
                }
            }
            (true, None) => {
                if self.phase == Phase::DrainingForStop {
                    let checkpoint = self.acked;
                    self.phase = Phase::Done;
                    SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint })
                } else if self.policy.stop_at_catch_up {
                    let checkpoint = self.acked;
                    self.phase = Phase::Done;
                    SubscriptionAction::Done(SubscriptionOutcome::CaughtUp { checkpoint })
                } else {
                    self.phase = Phase::Sleeping;
                    SubscriptionAction::Sleep {
                        for_: self.policy.idle_sleep,
                        reason: SleepReason::Idle,
                    }
                }
            }
            (false, Some(upper)) if upper.as_sequence().as_u64() == last => {
                let draining_for_stop = self.phase == Phase::DrainingForStop;
                // The ack target: the scan bound when it lies past the
                // last event.
                self.upper = Some(scanned.map_or(upper, |scanned| scanned.max(upper)));
                self.pending = batch.events;
                self.cursor = 0;
                self.stop_after_ack = draining_for_stop;
                self.phase = Phase::Applying;
                SubscriptionAction::Apply {
                    envelope: self.pending[0].clone(),
                }
            }
            _ => self.violation("batch upper bound does not match its last event"),
        }
    }

    fn on_applied(&mut self) -> SubscriptionAction<E> {
        if self.phase != Phase::Applying {
            return self.violation("`Applied` outside the applying phase");
        }
        // Whatever was failing has now applied (or was parked).
        if let Some((sequence, _)) = self.failing
            && self.pending.get(self.cursor).map(|e| e.sequence) == Some(sequence)
        {
            self.failing = None;
        }
        self.cursor = self.cursor.saturating_add(1);
        if let Some(envelope) = self.pending.get(self.cursor) {
            SubscriptionAction::Apply {
                envelope: envelope.clone(),
            }
        } else {
            // Batch drained: ack its upper bound.
            self.phase = Phase::Acking;
            let upper = match self.upper {
                Some(upper) => upper,
                None => return self.violation("batch drained with no upper bound"),
            };
            SubscriptionAction::Ack { checkpoint: upper }
        }
    }

    fn on_acked(&mut self) -> SubscriptionAction<E> {
        if self.phase != Phase::Acking {
            return self.violation("`Acked` outside the acking phase");
        }
        if let Some(upper) = self.upper {
            self.acked = upper;
        }
        self.pending.clear();
        self.cursor = 0;
        self.upper = None;
        if self.stop_after_ack {
            self.phase = Phase::DrainingForStop;
        } else {
            self.phase = Phase::Fetching;
        }
        self.fetch()
    }

    fn on_apply_failed(&mut self, error: StoreError) -> SubscriptionAction<E> {
        if self.phase != Phase::Applying {
            return self.violation("`ApplyFailed` outside the applying phase");
        }
        let Some(envelope) = self.pending.get(self.cursor) else {
            return self.violation("`ApplyFailed` with no event in flight");
        };
        let attempts = match self.failing {
            Some((sequence, n)) if sequence == envelope.sequence => n.saturating_add(1),
            _ => 1,
        };
        self.failing = Some((envelope.sequence, attempts));
        match self.policy.on_failure {
            FailurePolicy::Park { retries } if attempts > retries => {
                self.phase = Phase::Parking;
                SubscriptionAction::Park {
                    envelope: envelope.clone(),
                    attempts,
                    error,
                }
            }
            _ => self.drop_pending_and_sleep(),
        }
    }

    fn on_parked(&mut self) -> SubscriptionAction<E> {
        if self.phase != Phase::Parking {
            return self.violation("`Parked` outside the parking phase");
        }
        // The event is recorded: carry on past it exactly as if it had
        // applied.
        self.phase = Phase::Applying;
        self.on_applied()
    }

    fn on_park_failed(&mut self) -> SubscriptionAction<E> {
        if self.phase != Phase::Parking {
            return self.violation("`ParkFailed` outside the parking phase");
        }
        // Not recorded, so not skipped. The attempt count stands: the
        // next rejection tries to park again.
        self.drop_pending_and_sleep()
    }

    fn on_ack_failed(&mut self) -> SubscriptionAction<E> {
        if self.phase != Phase::Acking {
            return self.violation("`AckFailed` outside the acking phase");
        }
        self.drop_pending_and_sleep()
    }

    /// At-least-once fallback: everything past the acked checkpoint is
    /// re-delivered after the sleep — the checkpoint never moved, so the
    /// next fetch re-reads it all.
    fn drop_pending_and_sleep(&mut self) -> SubscriptionAction<E> {
        self.pending.clear();
        self.cursor = 0;
        self.upper = None;
        // A pending shutdown survives the failure: it re-reads at close.
        self.stop_after_ack = false;
        self.phase = Phase::Sleeping;
        SubscriptionAction::Sleep {
            for_: self.policy.retry_sleep,
            reason: SleepReason::Backoff,
        }
    }

    fn on_slept(&mut self) -> SubscriptionAction<E> {
        if self.phase != Phase::Sleeping {
            return self.violation("`Slept` outside the sleeping phase");
        }
        self.phase = Phase::Fetching;
        self.fetch()
    }

    fn on_shutdown(&mut self) -> SubscriptionAction<E> {
        match self.phase {
            // Nothing in flight: stop at the acked checkpoint.
            Phase::Fetching | Phase::DrainingForStop => {
                let checkpoint = self.acked;
                self.phase = Phase::Done;
                SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint })
            }
            Phase::Sleeping => {
                // Re-read once at close: events since the last poll
                // may have arrived during the idle wait.
                self.phase = Phase::DrainingForStop;
                self.fetch()
            }
            // A step is in flight: the shutdown does not disturb it. The
            // machine re-issues the pending action — the driver performs
            // it as usual — and stops right after the batch's ack.
            Phase::Applying => {
                self.stop_after_ack = true;
                match self.pending.get(self.cursor) {
                    Some(envelope) => SubscriptionAction::Apply {
                        envelope: envelope.clone(),
                    },
                    None => self.violation("`Shutdown` while applying a drained batch"),
                }
            }
            Phase::Parking => {
                self.stop_after_ack = true;
                // Re-issue the park: the in-flight event was given up on
                // and the driver is recording it.
                match (self.pending.get(self.cursor), self.failing) {
                    (Some(envelope), Some((_, attempts))) => SubscriptionAction::Park {
                        envelope: envelope.clone(),
                        attempts,
                        error: StoreError::other("parked during shutdown"),
                    },
                    _ => self.violation("`Shutdown` while parking with nothing in flight"),
                }
            }
            Phase::Acking => {
                self.stop_after_ack = true;
                match self.upper {
                    Some(checkpoint) => SubscriptionAction::Ack { checkpoint },
                    None => self.violation("`Shutdown` while acking with no upper bound"),
                }
            }
            Phase::Done => self.violation("`Shutdown` on a finished machine"),
        }
    }

    fn on_failed(&mut self, error: StoreError) -> SubscriptionAction<E> {
        if self.phase == Phase::Done {
            return self.violation("`Failed` on a finished machine");
        }
        self.phase = Phase::Done;
        SubscriptionAction::Done(SubscriptionOutcome::Failed(error))
    }

    fn violation(&mut self, message: &'static str) -> SubscriptionAction<E> {
        self.phase = Phase::Done;
        SubscriptionAction::Done(SubscriptionOutcome::Failed(StoreError::Other(Arc::new(
            ProtocolError::new(message),
        ))))
    }
}

#[cfg(test)]
#[allow(clippy::missing_const_for_fn)]
mod tests {
    use super::*;
    use crate::envelope::{EventEnvelope, Metadata};
    use crate::testing::{account::AccountEvent, projector_scripted};
    use crate::vocabulary::{StreamId, Version};
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;
    use proptest::prelude::*;

    fn envelope(sequence: u64) -> EventEnvelope<AccountEvent> {
        EventEnvelope {
            sequence: Sequence::new(sequence),
            stream_id: StreamId::from("account-1"),
            version: Version::new(sequence),
            event: AccountEvent::Opened {
                owner: String::new(),
            },
            metadata: Metadata::default(),
        }
    }

    fn batch(events: Vec<EventEnvelope<AccountEvent>>) -> Batch<AccountEvent> {
        let upper = events.last().map(|e| Checkpoint::new(e.sequence));
        Batch::new(events, upper)
    }

    fn machine() -> SubscriptionMachine<AccountEvent> {
        SubscriptionMachine::new(SubscriptionPolicy::default(), Checkpoint::ORIGIN)
    }

    fn is_protocol_violation(action: &SubscriptionAction<AccountEvent>) -> bool {
        let SubscriptionAction::Done(SubscriptionOutcome::Failed(StoreError::Other(source))) =
            action
        else {
            return false;
        };
        source.downcast_ref::<ProtocolError>().is_some()
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

    // -- transitions -----------------------------------------------------

    #[test]
    fn start_fetches_from_the_resume_checkpoint() {
        let mut m = machine();
        assert!(matches!(
            m.start(),
            SubscriptionAction::Fetch {
                from: Checkpoint::ORIGIN,
                limit: 128
            }
        ));
    }

    #[test]
    fn resume_loads_from_the_last_acked_checkpoint() {
        let checkpoint = Checkpoint::new(Sequence::new(41));
        let mut m =
            SubscriptionMachine::<AccountEvent>::new(SubscriptionPolicy::default(), checkpoint);
        assert!(matches!(
            m.start(),
            SubscriptionAction::Fetch { from, .. } if from == checkpoint
        ));
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(42)]),
        });
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Acked);
        assert_eq!(m.checkpoint(), Checkpoint::new(Sequence::new(42)));
    }

    #[test]
    fn fetched_batch_applies_each_event_then_acks() {
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1), envelope(2)]),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Apply { ref envelope }
                if envelope.sequence == Sequence::new(1)
        ));
        let action = m.handle(SubscriptionInput::Applied);
        assert!(matches!(
            action,
            SubscriptionAction::Apply { ref envelope }
                if envelope.sequence == Sequence::new(2)
        ));
        let action = m.handle(SubscriptionInput::Applied);
        assert!(matches!(
            action,
            SubscriptionAction::Ack { checkpoint }
                if checkpoint == Checkpoint::new(Sequence::new(2))
        ));
    }

    #[test]
    fn acked_advances_the_checkpoint_and_re_fetches() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1), envelope(2)]),
        });
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Applied);
        let action = m.handle(SubscriptionInput::Acked);
        assert!(matches!(
            action,
            SubscriptionAction::Fetch { from, .. }
                if from == Checkpoint::new(Sequence::new(2))
        ));
        assert_eq!(m.checkpoint(), Checkpoint::new(Sequence::new(2)));
    }

    #[test]
    fn an_empty_batch_sleeps_the_idle_time() {
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Sleep { for_, reason: SleepReason::Idle } if for_ == Duration::from_millis(100)
        ));
        assert_eq!(m.checkpoint(), Checkpoint::ORIGIN);
    }

    #[test]
    fn slept_re_fetches_from_the_acked_checkpoint() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        let action = m.handle(SubscriptionInput::Slept);
        assert!(matches!(
            action,
            SubscriptionAction::Fetch {
                from: Checkpoint::ORIGIN,
                ..
            }
        ));
    }

    #[test]
    fn apply_failed_sleeps_and_redelivers_from_the_last_ack() {
        let mut m = SubscriptionMachine::<AccountEvent>::new(
            SubscriptionPolicy::default(),
            Checkpoint::new(Sequence::new(1)),
        );
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(2), envelope(3)]),
        });
        m.handle(SubscriptionInput::Applied);
        let action = m.handle(SubscriptionInput::ApplyFailed {
            error: StoreError::other("boom"),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Sleep { for_, reason: SleepReason::Backoff } if for_ == Duration::from_secs(1)
        ));
        // The checkpoint never moved: the offending event redelivers.
        assert_eq!(m.checkpoint(), Checkpoint::new(Sequence::new(1)));
        let action = m.handle(SubscriptionInput::Slept);
        assert!(matches!(
            action,
            SubscriptionAction::Fetch { from, .. }
                if from == Checkpoint::new(Sequence::new(1))
        ));
        let action = m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(2), envelope(3)]),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Apply { ref envelope }
                if envelope.sequence == Sequence::new(2)
        ));
    }

    #[test]
    fn ack_failed_sleeps_and_redelivers_from_the_last_ack() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1)]),
        });
        m.handle(SubscriptionInput::Applied);
        let action = m.handle(SubscriptionInput::AckFailed);
        assert!(matches!(
            action,
            SubscriptionAction::Sleep { for_, reason: SleepReason::Backoff } if for_ == Duration::from_secs(1)
        ));
        assert_eq!(m.checkpoint(), Checkpoint::ORIGIN);
        m.handle(SubscriptionInput::Slept);
        let action = m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1)]),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Apply { ref envelope }
                if envelope.sequence == Sequence::new(1)
        ));
    }

    #[test]
    fn stop_at_catch_up_stops_after_an_empty_poll() {
        let mut m = SubscriptionMachine::<AccountEvent>::new(
            SubscriptionPolicy::default().stop_at_catch_up(),
            Checkpoint::new(Sequence::new(3)),
        );
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Done(SubscriptionOutcome::CaughtUp { checkpoint })
                if checkpoint == Checkpoint::new(Sequence::new(3))
        ));
        assert!(m.is_done());
    }

    #[test]
    fn shutdown_while_idle_stops_at_the_acked_checkpoint() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        let action = m.handle(SubscriptionInput::Shutdown);
        // A final guarded fetch: events may have arrived during the idle.
        assert!(matches!(action, SubscriptionAction::Fetch { .. }));
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint })
                if checkpoint == Checkpoint::ORIGIN
        ));
    }

    #[test]
    fn shutdown_mid_batch_finishes_and_acks_it_first() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1), envelope(2)]),
        });
        m.handle(SubscriptionInput::Applied); // applied 1; 2 in flight
        let action = m.handle(SubscriptionInput::Shutdown);
        // Normal life resumes with the in-flight apply; the shutdown
        // takes effect right after the batch's ack.
        assert!(matches!(
            action,
            SubscriptionAction::Apply { ref envelope }
                if envelope.sequence == Sequence::new(2)
        ));
        let action = m.handle(SubscriptionInput::Applied);
        assert!(matches!(
            action,
            SubscriptionAction::Ack { checkpoint }
                if checkpoint == Checkpoint::new(Sequence::new(2))
        ));
        let action = m.handle(SubscriptionInput::Acked);
        assert!(matches!(action, SubscriptionAction::Fetch { .. }));
        // Nothing since came in: stop, having acked the whole batch.
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint })
                if checkpoint == Checkpoint::new(Sequence::new(2))
        ));
    }

    #[test]
    fn fatal_failure_ends_the_machine() {
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Failed(StoreError::Unavailable));
        assert!(matches!(
            action,
            SubscriptionAction::Done(SubscriptionOutcome::Failed(StoreError::Unavailable))
        ));
        assert!(m.is_done());
    }

    // -- protocol violations ----------------------------------------------

    #[test]
    fn applied_after_done_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Failed(StoreError::Unavailable)); // Done
        assert_protocol_violation!(m.handle(SubscriptionInput::Applied));
    }

    #[test]
    fn start_after_done_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Failed(StoreError::Unavailable)); // Done
        assert_protocol_violation!(m.start());
    }

    #[test]
    fn fetched_outside_fetching_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1)]),
        });
        assert!(is_protocol_violation(&m.handle(
            SubscriptionInput::Fetched {
                batch: batch(vec![envelope(2)]),
            }
        )));
    }

    #[test]
    fn applied_outside_applying_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        assert!(is_protocol_violation(&m.handle(SubscriptionInput::Applied)));
    }

    #[test]
    fn acked_outside_acking_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        assert!(is_protocol_violation(&m.handle(SubscriptionInput::Acked)));
    }

    #[test]
    fn slept_outside_sleeping_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        assert!(is_protocol_violation(&m.handle(SubscriptionInput::Slept)));
    }

    #[test]
    fn a_batch_skipping_sequences_is_tolerated_and_acked_at_its_upper() {
        // Stores backing the global sequence with an identity column
        // burn values on rolled-back appends: a gap is permanently
        // absent, and skipping it keeps the at-least-once invariant.
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(2)]), // gap at 1: skipped
        });
        assert!(matches!(action, SubscriptionAction::Apply { .. }));
        let action = m.handle(SubscriptionInput::Applied);
        assert!(matches!(
            action,
            SubscriptionAction::Ack { checkpoint }
                if checkpoint == Checkpoint::new(Sequence::new(2))
        ));
    }

    #[test]
    fn a_batch_at_or_below_the_ack_bound_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1)]),
        });
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Acked); // acked at 1
        // A store that re-delivers the acked sequence inside a later
        // batch breaks ordering, not contiguity: still a violation.
        let action = m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(3), envelope(1)]),
        });
        assert!(is_protocol_violation(&action));
    }

    #[test]
    fn an_empty_batch_with_an_upper_bound_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::new(Vec::new(), Some(Checkpoint::new(Sequence::new(1)))),
        });
        assert!(is_protocol_violation(&action));
    }

    #[test]
    fn a_batch_whose_upper_does_not_match_its_last_event_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::new(vec![envelope(1)], Some(Checkpoint::new(Sequence::new(9)))),
        });
        assert!(is_protocol_violation(&action));
    }

    // -- the scripted driver ----------------------------------------------

    #[test]
    fn projector_scripted_records_every_action() {
        let mut m = machine();
        let actions = projector_scripted(
            &mut m,
            vec![
                SubscriptionInput::Fetched {
                    batch: batch(vec![envelope(1)]),
                },
                SubscriptionInput::Applied,
                SubscriptionInput::Acked,
            ],
        );
        assert_eq!(actions.len(), 4);
        assert!(matches!(actions[0], SubscriptionAction::Fetch { .. }));
        assert!(matches!(actions[1], SubscriptionAction::Apply { .. }));
        assert!(matches!(actions[2], SubscriptionAction::Ack { .. }));
        assert!(matches!(actions[3], SubscriptionAction::Fetch { .. }));
        assert!(!m.is_done());
    }

    #[test]
    fn a_full_batch_stop_at_catch_up_script_runs_start_to_done() {
        let mut m = SubscriptionMachine::<AccountEvent>::new(
            SubscriptionPolicy::default().stop_at_catch_up(),
            Checkpoint::ORIGIN,
        );
        let actions = projector_scripted(
            &mut m,
            vec![
                SubscriptionInput::Fetched {
                    batch: batch(vec![envelope(1), envelope(2)]),
                },
                SubscriptionInput::Applied,
                SubscriptionInput::Applied,
                SubscriptionInput::Acked,
                SubscriptionInput::Fetched {
                    batch: Batch::empty(),
                },
            ],
        );
        assert!(matches!(
            actions.last(),
            Some(SubscriptionAction::Done(SubscriptionOutcome::CaughtUp { checkpoint }))
                if *checkpoint == Checkpoint::new(Sequence::new(2))
        ));
        assert!(m.is_done());
    }

    // -- properties -------------------------------------------------------

    fn arb_sequence() -> impl Strategy<Value = u64> {
        0u64..8
    }

    fn arb_subscription_input() -> BoxedStrategy<SubscriptionInput<AccountEvent>> {
        prop_oneof![
            (arb_sequence(), 0usize..3).prop_map(|(start, len)| {
                let events: Vec<_> = (0..len as u64).map(|i| envelope(start + i + 1)).collect();
                SubscriptionInput::Fetched {
                    batch: batch(events),
                }
            }),
            // Filtered polls (0.7.4): a scan bound anywhere, valid or not.
            (arb_sequence(), 0usize..3, arb_sequence()).prop_map(|(start, len, scanned)| {
                let events: Vec<_> = (0..len as u64).map(|i| envelope(start + i + 1)).collect();
                SubscriptionInput::Fetched {
                    batch: batch(events).scanned_to(Checkpoint::new(Sequence::new(scanned))),
                }
            }),
            Just(SubscriptionInput::Applied),
            Just(SubscriptionInput::ApplyFailed {
                error: StoreError::other("boom"),
            }),
            Just(SubscriptionInput::Acked),
            Just(SubscriptionInput::AckFailed),
            Just(SubscriptionInput::Slept),
            Just(SubscriptionInput::Parked),
            Just(SubscriptionInput::ParkFailed),
            Just(SubscriptionInput::Failed(StoreError::Unavailable)),
            Just(SubscriptionInput::Shutdown),
        ]
        .boxed()
    }

    // -- filtered sources (0.7.4) ---------------------------------------

    fn at(sequence: u64) -> Checkpoint {
        Checkpoint::new(Sequence::new(sequence))
    }

    #[test]
    fn a_scan_past_the_last_event_is_acked() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(3)]).scanned_to(at(40)),
        });
        let action = m.handle(SubscriptionInput::Applied);
        assert!(matches!(
            action,
            SubscriptionAction::Ack { checkpoint } if checkpoint == at(40)
        ));
        m.handle(SubscriptionInput::Acked);
        assert_eq!(m.checkpoint(), at(40));
    }

    #[test]
    fn a_scan_with_no_matches_is_acked_without_applying() {
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty().scanned_to(at(500)),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Ack { checkpoint } if checkpoint == at(500)
        ));
        let action = m.handle(SubscriptionInput::Acked);
        assert!(matches!(
            action,
            SubscriptionAction::Fetch { from, .. } if from == at(500)
        ));
    }

    #[test]
    fn a_scan_that_went_nowhere_is_a_caught_up_poll() {
        let mut m = SubscriptionMachine::<AccountEvent>::new(SubscriptionPolicy::default(), at(9));
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty().scanned_to(at(9)),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Sleep {
                reason: SleepReason::Idle,
                ..
            }
        ));
    }

    #[test]
    fn a_failed_scan_ack_redelivers_from_the_old_checkpoint() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty().scanned_to(at(500)),
        });
        m.handle(SubscriptionInput::AckFailed);
        assert_eq!(m.checkpoint(), Checkpoint::ORIGIN);
        let action = m.handle(SubscriptionInput::Slept);
        assert!(matches!(
            action,
            SubscriptionAction::Fetch { from, .. } if from == Checkpoint::ORIGIN
        ));
    }

    #[test]
    fn a_shutdown_while_draining_acks_the_scan_then_stops() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        // Sleeping: the shutdown re-reads once.
        m.handle(SubscriptionInput::Shutdown);
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty().scanned_to(at(70)),
        });
        assert!(matches!(action, SubscriptionAction::Ack { .. }));
        let action = m.handle(SubscriptionInput::Acked);
        assert!(matches!(
            action,
            SubscriptionAction::Fetch { .. } | SubscriptionAction::Done(_)
        ));
        if let SubscriptionAction::Fetch { .. } = action {
            let action = m.handle(SubscriptionInput::Fetched {
                batch: Batch::empty(),
            });
            assert!(matches!(
                action,
                SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint }) if checkpoint == at(70)
            ));
        }
    }

    #[test]
    fn scan_bounds_that_break_the_rules_are_violations() {
        // Below the batch's last event.
        let mut m = machine();
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(5)]).scanned_to(at(4)),
        });
        assert!(is_protocol_violation(&action));

        // Behind the checkpoint.
        let mut m = SubscriptionMachine::<AccountEvent>::new(SubscriptionPolicy::default(), at(10));
        m.start();
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty().scanned_to(at(3)),
        });
        assert!(is_protocol_violation(&action));
    }

    // -- poison events (0.7.7) ----------------------------------------

    fn parking(retries: u32) -> SubscriptionMachine<AccountEvent> {
        SubscriptionMachine::new(
            SubscriptionPolicy::default().on_failure(FailurePolicy::Park { retries }),
            Checkpoint::ORIGIN,
        )
    }

    fn reject() -> SubscriptionInput<AccountEvent> {
        SubscriptionInput::ApplyFailed {
            error: StoreError::other("poison"),
        }
    }

    /// Fetch `[1, 2, 3]`, apply 1, and reject 2.
    fn reject_the_second(
        m: &mut SubscriptionMachine<AccountEvent>,
    ) -> SubscriptionAction<AccountEvent> {
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1), envelope(2), envelope(3)]),
        });
        m.handle(SubscriptionInput::Applied);
        m.handle(reject())
    }

    #[test]
    fn halt_is_the_default_and_never_parks() {
        let mut m = machine();
        m.start();
        for _ in 0..50 {
            let action = reject_the_second(&mut m);
            assert!(matches!(
                action,
                SubscriptionAction::Sleep {
                    reason: SleepReason::Backoff,
                    ..
                }
            ));
            m.handle(SubscriptionInput::Slept);
        }
        assert_eq!(m.checkpoint(), Checkpoint::ORIGIN, "stalled, never skipped");
    }

    #[test]
    fn an_event_is_parked_after_its_retry_budget_and_the_rest_applies() {
        let mut m = parking(2);
        m.start();
        // Two retries: the first two rejections back off.
        for _ in 0..2 {
            assert!(matches!(
                reject_the_second(&mut m),
                SubscriptionAction::Sleep { .. }
            ));
            m.handle(SubscriptionInput::Slept);
        }
        // The third rejection parks it.
        let action = reject_the_second(&mut m);
        let SubscriptionAction::Park {
            envelope, attempts, ..
        } = action
        else {
            panic!("expected Park, got {action:?}");
        };
        assert_eq!(envelope.sequence, Sequence::new(2));
        assert_eq!(attempts, 3);
        // Recorded: carry on with event 3, then ack the whole batch.
        let action = m.handle(SubscriptionInput::Parked);
        assert!(
            matches!(action, SubscriptionAction::Apply { envelope } if envelope.sequence == Sequence::new(3))
        );
        let action = m.handle(SubscriptionInput::Applied);
        assert!(
            matches!(action, SubscriptionAction::Ack { checkpoint } if checkpoint == Checkpoint::new(Sequence::new(3)))
        );
        m.handle(SubscriptionInput::Acked);
        assert_eq!(m.checkpoint(), Checkpoint::new(Sequence::new(3)));
    }

    #[test]
    fn zero_retries_parks_on_the_first_rejection() {
        let mut m = parking(0);
        m.start();
        assert!(matches!(
            reject_the_second(&mut m),
            SubscriptionAction::Park { attempts: 1, .. }
        ));
    }

    #[test]
    fn a_failed_park_never_skips_the_event() {
        let mut m = parking(0);
        m.start();
        reject_the_second(&mut m);
        let action = m.handle(SubscriptionInput::ParkFailed);
        assert!(matches!(
            action,
            SubscriptionAction::Sleep {
                reason: SleepReason::Backoff,
                ..
            }
        ));
        assert_eq!(m.checkpoint(), Checkpoint::ORIGIN);
        // Redelivered; the next rejection tries to park again.
        m.handle(SubscriptionInput::Slept);
        assert!(matches!(
            reject_the_second(&mut m),
            SubscriptionAction::Park { .. }
        ));
    }

    #[test]
    fn an_event_that_recovers_resets_its_count() {
        let mut m = parking(1);
        m.start();
        assert!(matches!(
            reject_the_second(&mut m),
            SubscriptionAction::Sleep { .. }
        ));
        m.handle(SubscriptionInput::Slept);
        // This time event 2 applies, and later event 4 is rejected: its
        // count starts from one, not from event 2's.
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1), envelope(2), envelope(3)]),
        });
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Acked);
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(4)]),
        });
        assert!(matches!(
            m.handle(reject()),
            SubscriptionAction::Sleep { .. }
        ));
    }

    #[test]
    fn failed_acks_do_not_count_against_an_event() {
        let mut m = parking(0);
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1)]),
        });
        m.handle(SubscriptionInput::Applied);
        let action = m.handle(SubscriptionInput::AckFailed);
        assert!(
            matches!(action, SubscriptionAction::Sleep { .. }),
            "an ack failure backs off, never parks"
        );
    }

    #[test]
    fn park_answers_outside_the_parking_phase_are_violations() {
        let mut m = parking(0);
        m.start();
        assert!(is_protocol_violation(&m.handle(SubscriptionInput::Parked)));
        let mut m = parking(0);
        m.start();
        assert!(is_protocol_violation(
            &m.handle(SubscriptionInput::ParkFailed)
        ));
    }

    proptest! {
        /// The machine never panics on any input sequence, and once it
        /// is done it stays done: every action after the first `Done` is
        /// a protocol-violation `Done`.
        #[test]
        fn never_panics_and_stays_done(
            script in prop::collection::vec(arb_subscription_input(), 0..16),
            park in proptest::option::of(0u32..3),
        ) {
            let mut m = match park {
                None => machine(),
                Some(retries) => parking(retries),
            };
            let actions = projector_scripted(&mut m, script);

            prop_assert!(!actions.is_empty()); // start always emits
            if let Some(i) = actions.iter().position(|a| matches!(a, SubscriptionAction::Done(_))) {
                for a in &actions[i + 1..] {
                    prop_assert!(is_protocol_violation(a));
                }
            }
        }

        /// The checkpoint never regresses and never moves before an ack:
        /// `Ack`/`Acked` is the only path by which `checkpoint()` changes.
        #[test]
        fn checkpoint_never_regresses_and_moves_only_on_ack(
            script in prop::collection::vec(arb_subscription_input(), 0..16),
            park in proptest::option::of(0u32..3),
        ) {
            // Halting and parking machines alike.
            let mut m = match park {
                None => machine(),
                Some(retries) => parking(retries),
            };
            let mut last = m.checkpoint();
            let _ = m.start();
            for (i, input) in script.into_iter().enumerate() {
                let was_acked = matches!(input, SubscriptionInput::Acked);
                let _ = m.handle(input);
                let now = m.checkpoint();
                prop_assert!(last <= now, "checkpoint must not regress (step {i})");
                if now != last {
                    prop_assert!(was_acked, "checkpoint may move only after an ack (step {i})");
                }
                last = now;
            }
        }
    }
}
