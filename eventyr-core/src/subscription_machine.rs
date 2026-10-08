//! The subscription machine and its vocabulary: the sans-IO core of the
//! catch-up projector (DESIGN.md §6, §7).
//!
//! [`SubscriptionMachine`] consumes driver results ([`SubscriptionInput`])
//! and emits I/O requests ([`SubscriptionAction`]); the driver performs
//! them. It is the machine the §7 table calls `ProjectorMachine`; it is
//! named for its §6-facing role. Its core is the poll → apply → ack
//! loop — inputs `Fetched`/`Applied`/`ApplyFailed`/`Acked`/`AckFailed`,
//! actions `Apply`/`Ack`/`Sleep`/`Done` — plus four augmentation
//! steps:
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
//! - **`Park`** (roadmap 0.7.7): under [`FailurePolicy::Park`], an event the
//!   projection keeps rejecting is handed to the driver as
//!   [`Park`](SubscriptionAction::Park); the driver records it and
//!   answers [`Parked`](SubscriptionInput::Parked) — the machine carries
//!   on past it — or [`ParkFailed`](SubscriptionInput::ParkFailed).
//!
//! Delivery is at-least-once: the checkpoint moves only after the whole
//! batch applied, and any apply/ack failure falls back to the last ack,
//! so the same envelope may be delivered twice — projections must apply
//! idempotently.
//!
//! ## Testing
//!
//! The machine is pure; [`scripted`](crate::testing::scripted)
//! (or its named wrapper [`projector_scripted`](crate::testing::projector_scripted))
//! drives it against a [`Vec`] of scripted inputs — no runtime. Machine
//! tests feed [`Slept`](SubscriptionInput::Slept) instantly (the duration
//! is data); only crate-side driver tests touch a wall clock.

use alloc::vec;
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;

use crate::envelope::EventEnvelope;
use crate::error::StoreError;
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
    /// How far a *filtered* source scanned (roadmap 0.7.4): every sequence up to
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

    /// Wrap deliveries, deriving `upper` from the last event — the
    /// common case for unfiltered sources, where the scan ends at
    /// `upper`. An empty batch is caught up.
    pub fn of(events: Vec<EventEnvelope<E>>) -> Self {
        let upper = events.last().map(|event| Checkpoint::new(event.sequence));
        Self::new(events, upper)
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
    /// [`ParkFailed`](SubscriptionInput::ParkFailed) /
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
    /// What to do with an event the projection keeps rejecting (roadmap 0.7.7).
    /// [`Halt`](FailurePolicy::Halt), the default, retries it forever.
    pub on_failure: FailurePolicy,
}

/// What a subscription does with an event its projection keeps
/// rejecting (roadmap 0.7.7).
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
    /// Record `envelope` as parked (roadmap 0.7.7): the projection rejected it
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
    /// was visible, so a driver that learns of a commit (roadmap 0.7.2's
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
    /// Backing off after a failed apply, park, or ack. The wait runs
    /// its course.
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
    /// rejection so the driver can report it. Under
    /// [`FailurePolicy::Halt`] the machine sleeps and re-fetches from the
    /// last ack, so the offending (and any later) event redelivers — the
    /// at-least-once contract. Under [`FailurePolicy::Park`] the machine
    /// counts consecutive rejections of the same event (roadmap 0.7.7) and, once
    /// the budget is spent, emits [`Park`](SubscriptionAction::Park)
    /// carrying this `error` instead of backing off.
    ApplyFailed {
        /// Why the projection rejected the event, rendered for logging.
        error: StoreError,
    },
    /// The parked event was recorded (roadmap 0.7.7).
    Parked,
    /// Recording the parked event failed. The event is not skipped:
    /// the machine backs off and redelivers, as for a rejection. The
    /// machine does not branch on `error`; it is the driver's to report.
    ParkFailed {
        /// Why the parked store refused the record.
        error: StoreError,
    },
    /// The checkpoint write persisted.
    Acked,
    /// The checkpoint write failed. Same as an apply failure: sleep and
    /// re-fetch from the last ack; the un-acked events re-apply. The
    /// machine does not branch on `error`; it is the driver's to report.
    AckFailed {
        /// Why the checkpoint write failed.
        error: StoreError,
    },
    /// The sleep elapsed.
    Slept,
    /// A store operation failed fatally (e.g. [`StoreError::Unavailable`]),
    /// outside the apply/ack paths.
    Failed(StoreError),
    /// Request to stop: finish any in-flight batch, ack it, and end at
    /// [`Stopped`](SubscriptionOutcome::Stopped). A fetching phase stops
    /// immediately; a sleeping one re-reads once at close. The checkpoint
    /// never regresses and no fetched-but-unapplied event is lost — the
    /// ack boundary is always a whole batch. A shutdown survives a
    /// failure: if the in-flight batch fails to apply, park, or ack, the
    /// machine backs off and re-reads once at close, as when shut down
    /// while sleeping; should that closing attempt fail too, it stops at
    /// the last ack (the batch redelivers after a restart) rather than
    /// retrying forever.
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
    /// (as [`StoreError::Other`] carrying a
    /// [`ProtocolError`](crate::error::ProtocolError)).
    Failed(StoreError),
}

/// The batch being applied: the event in flight, the events after it,
/// and the position the batch acks once drained.
struct InFlight<E> {
    /// The event the driver is applying (or parking).
    current: EventEnvelope<E>,
    /// The batch's events after `current`, in order.
    rest: vec::IntoIter<EventEnvelope<E>>,
    /// The ack target: the batch's last sequence, or the source's scan
    /// bound when that lies past it.
    upper: Checkpoint,
}

/// Which input the machine is waiting for, carrying what that phase
/// needs.
enum Phase<E> {
    /// Waiting for `Fetched`. With a shutdown requested this is the
    /// closing re-read: an empty answer stops.
    Fetching,
    /// Waiting for `Applied`/`ApplyFailed` while draining the batch.
    Applying(InFlight<E>),
    /// Waiting for `Parked`/`ParkFailed` for the batch's current event.
    Parking {
        /// The batch, its current event the one being parked.
        batch: InFlight<E>,
        /// The rejections the park records.
        attempts: u32,
        /// The last rejection, which the park records.
        error: StoreError,
    },
    /// Waiting for `Acked`/`AckFailed`.
    Acking {
        /// The position being persisted.
        checkpoint: Checkpoint,
    },
    /// Waiting for `Slept`; trips back to fetch.
    Sleeping,
    /// Terminal.
    Done,
}

/// How far a requested shutdown has got.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stopping {
    /// Running normally.
    No,
    /// Stop at the next point with nothing in flight: an empty closing
    /// re-read.
    Requested,
    /// A batch failed after the request; the machine backed off and is
    /// making its one closing attempt. Another failure stops.
    Retried,
}

/// The event the projection last rejected, and how many times in a row
/// (roadmap 0.7.7).
#[derive(Clone, Copy)]
struct Failing {
    /// The rejected event's global position.
    sequence: Sequence,
    /// Consecutive rejections of it.
    attempts: u32,
}

/// What a valid [`Batch`] asks of the machine.
enum Delivery<E> {
    /// Nothing new: a caught-up poll.
    CaughtUp,
    /// No events, but a filtered source scanned past the checkpoint:
    /// ack the scan bound.
    ScanOnly(Checkpoint),
    /// Events to apply.
    Events(InFlight<E>),
}

/// The sans-IO machine behind a catch-up subscription: poll → apply each
/// event → ack → repeat, sleeping on empty or failure.
///
/// The machine owns the acked checkpoint and, per phase, the batch in
/// flight. It never does I/O: the driver performs each
/// [`SubscriptionAction`] and reports back with a [`SubscriptionInput`].
///
/// At-least-once is structural: the checkpoint moves only after the
/// whole batch applied, and `ApplyFailed`/`ParkFailed`/`AckFailed` drop
/// everything not yet acked and re-fetch from the last ack — so the
/// projection may re-see events and must apply idempotently.
///
/// Machines never panic on bad input: a driver that feeds the wrong
/// input for the current phase, or drives a finished machine, gets
/// [`Done`](SubscriptionAction::Done) with
/// [`Failed(StoreError::Other(ProtocolError))`](SubscriptionOutcome::Failed).
pub struct SubscriptionMachine<E> {
    policy: SubscriptionPolicy,
    phase: Phase<E>,
    /// The persisted position; also the exclusive fetch bound.
    acked: Checkpoint,
    /// Whether a shutdown was requested, and whether its closing
    /// attempt already failed once.
    stopping: Stopping,
    /// The event the projection last rejected (roadmap 0.7.7). Survives
    /// redelivery; cleared once the event applies or is parked.
    failing: Option<Failing>,
}

impl<E: Clone> SubscriptionMachine<E> {
    /// `resume_from` is the runner-loaded checkpoint —
    /// [`ORIGIN`](Checkpoint::ORIGIN) for a fresh projection.
    pub fn new(policy: SubscriptionPolicy, resume_from: Checkpoint) -> Self {
        Self {
            policy,
            phase: Phase::Fetching,
            acked: resume_from,
            stopping: Stopping::No,
            failing: None,
        }
    }

    /// The first action: read after the resume checkpoint. Idempotent
    /// until the first [`handle`](Self::handle).
    pub fn start(&mut self) -> SubscriptionAction<E> {
        if !matches!(self.phase, Phase::Fetching) || self.stopping != Stopping::No {
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
            SubscriptionInput::AckFailed { .. } => self.on_ack_failed(),
            SubscriptionInput::Slept => self.on_slept(),
            SubscriptionInput::Parked => self.on_parked(),
            SubscriptionInput::ParkFailed { .. } => self.on_park_failed(),
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
        matches!(self.phase, Phase::Done)
    }

    fn fetch(&self) -> SubscriptionAction<E> {
        SubscriptionAction::Fetch {
            from: self.acked,
            limit: self.policy.batch_size,
        }
    }

    /// Take the current phase, leaving `Done` behind: every transition
    /// sets the next phase explicitly, and a violation leaves it done.
    fn take_phase(&mut self) -> Phase<E> {
        core::mem::replace(&mut self.phase, Phase::Done)
    }

    fn on_fetched(&mut self, batch: Batch<E>) -> SubscriptionAction<E> {
        if !matches!(self.phase, Phase::Fetching) {
            return self.violation("`Fetched` outside the fetching phase");
        }
        match self.validate(batch) {
            Ok(Delivery::CaughtUp) => self.on_caught_up(),
            Ok(Delivery::ScanOnly(checkpoint)) => {
                // Nothing matched, but the scan moved: ack it without
                // applying anything.
                self.ack(checkpoint)
            }
            Ok(Delivery::Events(batch)) => {
                let envelope = batch.current.clone();
                self.phase = Phase::Applying(batch);
                SubscriptionAction::Apply { envelope }
            }
            Err(message) => self.violation(message),
        }
    }

    /// Check a fetched batch against the protocol and say what it asks
    /// for.
    ///
    /// Sequences must be strictly increasing and strictly after the
    /// acked checkpoint, so a re-delivery or mis-ordering bug in the
    /// store lands as a protocol violation, not corruption. Gaps are
    /// *not* a violation: stores backing the global sequence with an
    /// identity column burn values on rolled-back appends, and a gap is
    /// never delivered later and out of order, so skipping it keeps the
    /// at-least-once invariant — the checkpoint only advances past
    /// sequences that were delivered or will never arrive.
    fn validate(&self, batch: Batch<E>) -> Result<Delivery<E>, &'static str> {
        let mut last = self.acked.as_sequence().as_u64();
        for envelope in &batch.events {
            let sequence = envelope.sequence.as_u64();
            if sequence <= last {
                return Err(
                    "`Fetched` delivered a sequence not strictly increasing past the ack bound",
                );
            }
            last = sequence;
        }
        // A filtered source may have scanned past its last delivery
        // (roadmap 0.7.4). The scan bound may not trail the deliveries, and a
        // scan that went nowhere is no progress.
        let scanned = match (batch.scanned, batch.upper) {
            (Some(scanned), Some(upper)) if scanned < upper => {
                return Err("`Fetched` reported a scan bound below its last event");
            }
            (Some(scanned), _) if scanned < self.acked => {
                return Err("`Fetched` reported a scan bound behind the checkpoint");
            }
            (Some(scanned), _) if scanned > self.acked => Some(scanned),
            _ => None,
        };
        let mut events = batch.events.into_iter();
        match (events.next(), batch.upper) {
            (None, None) => Ok(scanned.map_or(Delivery::CaughtUp, Delivery::ScanOnly)),
            (Some(current), Some(upper)) if upper.as_sequence().as_u64() == last => {
                Ok(Delivery::Events(InFlight {
                    current,
                    rest: events,
                    // The ack target: the scan bound when it lies past
                    // the last event.
                    upper: scanned.map_or(upper, |scanned| scanned.max(upper)),
                }))
            }
            _ => Err("batch upper bound does not match its last event"),
        }
    }

    /// Nothing new after the checkpoint: stop if asked to, else idle.
    fn on_caught_up(&mut self) -> SubscriptionAction<E> {
        if self.stopping != Stopping::No {
            self.stop()
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

    fn on_applied(&mut self) -> SubscriptionAction<E> {
        match self.take_phase() {
            Phase::Applying(batch) => self.advance(batch),
            _ => self.violation("`Applied` outside the applying phase"),
        }
    }

    fn on_parked(&mut self) -> SubscriptionAction<E> {
        match self.take_phase() {
            // The event is recorded: carry on past it exactly as if it
            // had applied.
            Phase::Parking { batch, .. } => self.advance(batch),
            _ => self.violation("`Parked` outside the parking phase"),
        }
    }

    /// The current event is done with (applied or parked): apply the
    /// next one, or ack the drained batch.
    fn advance(&mut self, mut batch: InFlight<E>) -> SubscriptionAction<E> {
        // Whatever was failing has now applied (or was parked).
        if self
            .failing
            .is_some_and(|failing| failing.sequence == batch.current.sequence)
        {
            self.failing = None;
        }
        match batch.rest.next() {
            Some(next) => {
                batch.current = next;
                let envelope = batch.current.clone();
                self.phase = Phase::Applying(batch);
                SubscriptionAction::Apply { envelope }
            }
            None => self.ack(batch.upper),
        }
    }

    fn ack(&mut self, checkpoint: Checkpoint) -> SubscriptionAction<E> {
        self.phase = Phase::Acking { checkpoint };
        SubscriptionAction::Ack { checkpoint }
    }

    fn on_acked(&mut self) -> SubscriptionAction<E> {
        match self.take_phase() {
            Phase::Acking { checkpoint } => {
                self.acked = checkpoint;
                // With a shutdown requested, this is the closing re-read.
                self.phase = Phase::Fetching;
                self.fetch()
            }
            _ => self.violation("`Acked` outside the acking phase"),
        }
    }

    fn on_apply_failed(&mut self, error: StoreError) -> SubscriptionAction<E> {
        let Phase::Applying(batch) = self.take_phase() else {
            return self.violation("`ApplyFailed` outside the applying phase");
        };
        let sequence = batch.current.sequence;
        let attempts = match self.failing {
            Some(failing) if failing.sequence == sequence => failing.attempts.saturating_add(1),
            _ => 1,
        };
        self.failing = Some(Failing { sequence, attempts });
        match self.policy.on_failure {
            FailurePolicy::Park { retries } if attempts > retries => {
                let envelope = batch.current.clone();
                self.phase = Phase::Parking {
                    batch,
                    attempts,
                    error: error.clone(),
                };
                SubscriptionAction::Park {
                    envelope,
                    attempts,
                    error,
                }
            }
            _ => self.drop_batch(),
        }
    }

    fn on_park_failed(&mut self) -> SubscriptionAction<E> {
        if !matches!(self.phase, Phase::Parking { .. }) {
            return self.violation("`ParkFailed` outside the parking phase");
        }
        // Not recorded, so not skipped. The attempt count stands: the
        // next rejection tries to park again.
        self.drop_batch()
    }

    fn on_ack_failed(&mut self) -> SubscriptionAction<E> {
        if !matches!(self.phase, Phase::Acking { .. }) {
            return self.violation("`AckFailed` outside the acking phase");
        }
        self.drop_batch()
    }

    /// At-least-once fallback after a failed apply, park, or ack: back
    /// off, then re-fetch — everything past the acked checkpoint is
    /// re-delivered, because the checkpoint never moved.
    ///
    /// A requested shutdown survives the failure: the re-fetch after the
    /// backoff is its closing re-read. If that closing attempt fails as
    /// well, the machine stops at the acked checkpoint — retrying would
    /// only redeliver the batch that just failed, forever for an event
    /// the projection always rejects.
    fn drop_batch(&mut self) -> SubscriptionAction<E> {
        match self.stopping {
            Stopping::No => {}
            Stopping::Requested => self.stopping = Stopping::Retried,
            Stopping::Retried => return self.stop(),
        }
        self.phase = Phase::Sleeping;
        SubscriptionAction::Sleep {
            for_: self.policy.retry_sleep,
            reason: SleepReason::Backoff,
        }
    }

    fn on_slept(&mut self) -> SubscriptionAction<E> {
        if !matches!(self.phase, Phase::Sleeping) {
            return self.violation("`Slept` outside the sleeping phase");
        }
        self.phase = Phase::Fetching;
        self.fetch()
    }

    fn on_shutdown(&mut self) -> SubscriptionAction<E> {
        let action = match &self.phase {
            // Nothing in flight: stop at the acked checkpoint.
            Phase::Fetching => return self.stop(),
            Phase::Sleeping => {
                // Re-read once at close: events since the last poll
                // may have arrived during the wait.
                self.phase = Phase::Fetching;
                self.fetch()
            }
            // A step is in flight: the shutdown does not disturb it. The
            // machine re-issues the pending action — the driver performs
            // it as usual — and stops right after the batch's ack.
            Phase::Applying(batch) => SubscriptionAction::Apply {
                envelope: batch.current.clone(),
            },
            Phase::Parking {
                batch,
                attempts,
                error,
            } => SubscriptionAction::Park {
                envelope: batch.current.clone(),
                attempts: *attempts,
                error: error.clone(),
            },
            Phase::Acking { checkpoint } => SubscriptionAction::Ack {
                checkpoint: *checkpoint,
            },
            Phase::Done => return self.violation("`Shutdown` on a finished machine"),
        };
        if self.stopping == Stopping::No {
            self.stopping = Stopping::Requested;
        }
        action
    }

    /// End a requested shutdown at the acked checkpoint.
    fn stop(&mut self) -> SubscriptionAction<E> {
        let checkpoint = self.acked;
        self.phase = Phase::Done;
        SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint })
    }

    fn on_failed(&mut self, error: StoreError) -> SubscriptionAction<E> {
        if self.is_done() {
            return self.violation("`Failed` on a finished machine");
        }
        self.phase = Phase::Done;
        SubscriptionAction::Done(SubscriptionOutcome::Failed(error))
    }

    fn violation(&mut self, message: &'static str) -> SubscriptionAction<E> {
        self.phase = Phase::Done;
        SubscriptionAction::Done(SubscriptionOutcome::Failed(StoreError::protocol(message)))
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
        matches!(
            action,
            SubscriptionAction::Done(SubscriptionOutcome::Failed(error)) if error.is_protocol_violation()
        )
    }

    fn ack_failed() -> SubscriptionInput<AccountEvent> {
        SubscriptionInput::AckFailed {
            error: StoreError::Unavailable,
        }
    }

    fn park_failed() -> SubscriptionInput<AccountEvent> {
        SubscriptionInput::ParkFailed {
            error: StoreError::Unavailable,
        }
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
        let action = m.handle(ack_failed());
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

    /// Fetch `[1, 2]`, ack it, fetch `[3, 4]`, apply 3, and shut down
    /// with 4 in flight: the acked checkpoint is 2.
    fn shut_down_mid_second_batch(m: &mut SubscriptionMachine<AccountEvent>) {
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1), envelope(2)]),
        });
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Applied);
        m.handle(SubscriptionInput::Acked);
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(3), envelope(4)]),
        });
        m.handle(SubscriptionInput::Applied);
        assert!(matches!(
            m.handle(SubscriptionInput::Shutdown),
            SubscriptionAction::Apply { ref envelope } if envelope.sequence == Sequence::new(4)
        ));
    }

    /// After a failure that follows a shutdown: back off, re-read once
    /// at close, and — nothing new arriving — stop at the last ack.
    fn assert_backs_off_then_stops_at(
        m: &mut SubscriptionMachine<AccountEvent>,
        failure: SubscriptionAction<AccountEvent>,
        checkpoint: Checkpoint,
    ) {
        assert!(
            matches!(
                failure,
                SubscriptionAction::Sleep {
                    reason: SleepReason::Backoff,
                    ..
                }
            ),
            "expected a backoff, got {failure:?}"
        );
        assert!(matches!(
            m.handle(SubscriptionInput::Slept),
            SubscriptionAction::Fetch { from, .. } if from == checkpoint
        ));
        let action = m.handle(SubscriptionInput::Fetched {
            batch: Batch::empty(),
        });
        assert!(
            matches!(
                action,
                SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint: at })
                    if at == checkpoint
            ),
            "expected Stopped at {checkpoint}, got {action:?}"
        );
    }

    #[test]
    fn a_shutdown_survives_a_failed_apply() {
        let mut m = machine();
        shut_down_mid_second_batch(&mut m);
        let failure = m.handle(SubscriptionInput::ApplyFailed {
            error: StoreError::other("boom"),
        });
        assert_backs_off_then_stops_at(&mut m, failure, at(2));
    }

    #[test]
    fn a_shutdown_survives_a_failed_ack() {
        let mut m = machine();
        shut_down_mid_second_batch(&mut m);
        m.handle(SubscriptionInput::Applied);
        let failure = m.handle(ack_failed());
        assert_backs_off_then_stops_at(&mut m, failure, at(2));
    }

    #[test]
    fn a_shutdown_survives_a_failed_park() {
        let mut m = parking(0);
        shut_down_mid_second_batch(&mut m);
        assert!(matches!(
            m.handle(reject()),
            SubscriptionAction::Park { .. }
        ));
        let failure = m.handle(park_failed());
        assert_backs_off_then_stops_at(&mut m, failure, at(2));
    }

    #[test]
    fn a_shutdown_whose_closing_attempt_fails_again_stops_at_the_last_ack() {
        let mut m = machine();
        shut_down_mid_second_batch(&mut m);
        m.handle(SubscriptionInput::ApplyFailed {
            error: StoreError::other("poison"),
        });
        m.handle(SubscriptionInput::Slept);
        // The closing re-read redelivers the poison event; it fails
        // again, and the machine stops instead of retrying forever.
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(3), envelope(4)]),
        });
        let action = m.handle(SubscriptionInput::ApplyFailed {
            error: StoreError::other("poison"),
        });
        assert!(matches!(
            action,
            SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint }) if checkpoint == at(2)
        ));
    }

    #[test]
    fn a_shutdown_while_parking_re_issues_the_real_rejection() {
        let mut m = parking(0);
        m.start();
        m.handle(SubscriptionInput::Fetched {
            batch: batch(vec![envelope(1)]),
        });
        m.handle(SubscriptionInput::ApplyFailed {
            error: StoreError::other("bad payload"),
        });
        let SubscriptionAction::Park {
            envelope,
            attempts,
            error,
        } = m.handle(SubscriptionInput::Shutdown)
        else {
            panic!("expected the park re-issued");
        };
        assert_eq!(envelope.sequence, Sequence::new(1));
        assert_eq!(attempts, 1);
        assert_eq!(error.to_string(), "store failure: bad payload");
        // Recorded: ack the batch, re-read at close, and stop.
        assert!(matches!(
            m.handle(SubscriptionInput::Parked),
            SubscriptionAction::Ack { checkpoint } if checkpoint == at(1)
        ));
        m.handle(SubscriptionInput::Acked);
        assert!(matches!(
            m.handle(SubscriptionInput::Fetched { batch: Batch::empty() }),
            SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint }) if checkpoint == at(1)
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
        assert!(is_protocol_violation(&m.handle(SubscriptionInput::Applied)));
    }

    #[test]
    fn start_after_done_is_a_protocol_violation() {
        let mut m = machine();
        m.start();
        m.handle(SubscriptionInput::Failed(StoreError::Unavailable)); // Done
        assert!(is_protocol_violation(&m.start()));
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
            // Filtered polls (roadmap 0.7.4): a scan bound anywhere, valid or not.
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
            Just(SubscriptionInput::AckFailed {
                error: StoreError::Unavailable,
            }),
            Just(SubscriptionInput::Slept),
            Just(SubscriptionInput::Parked),
            Just(SubscriptionInput::ParkFailed {
                error: StoreError::Unavailable,
            }),
            Just(SubscriptionInput::Failed(StoreError::Unavailable)),
            Just(SubscriptionInput::Shutdown),
        ]
        .boxed()
    }

    // -- filtered sources (roadmap 0.7.4) ---------------------------------------

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
        m.handle(ack_failed());
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

    // -- poison events (roadmap 0.7.7) ----------------------------------------

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
        let action = m.handle(park_failed());
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
        let action = m.handle(ack_failed());
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
        assert!(is_protocol_violation(&m.handle(park_failed())));
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

        /// A shutdown is never lost: a driver that answers whatever the
        /// machine asks — failing at random, delivering new events until
        /// the shutdown and none after — always reaches `Stopped`, at a
        /// checkpoint that never regressed.
        #[test]
        fn a_shutdown_always_ends_in_stopped(
            failures in prop::collection::vec(any::<bool>(), 0..48),
            shutdown_at in 0usize..24,
            park in proptest::option::of(0u32..3),
        ) {
            let mut m = match park {
                None => machine(),
                Some(retries) => parking(retries),
            };
            let mut action = m.start();
            let mut failures = failures.into_iter();
            let mut shut_down = false;
            for step in 0..96 {
                let fail = failures.next().unwrap_or(false);
                let input = match &action {
                    SubscriptionAction::Done(outcome) => {
                        prop_assert!(shut_down, "done before the shutdown: {outcome:?}");
                        prop_assert!(
                            matches!(outcome, SubscriptionOutcome::Stopped { checkpoint } if *checkpoint == m.checkpoint()),
                            "expected Stopped at the last ack, got {outcome:?}"
                        );
                        return Ok(());
                    }
                    _ if step == shutdown_at => {
                        shut_down = true;
                        SubscriptionInput::Shutdown
                    }
                    SubscriptionAction::Fetch { from, .. } if !shut_down => {
                        let next = from.as_sequence().as_u64();
                        SubscriptionInput::Fetched {
                            batch: batch(vec![envelope(next + 1), envelope(next + 2)]),
                        }
                    }
                    SubscriptionAction::Fetch { .. } => SubscriptionInput::Fetched {
                        batch: Batch::empty(),
                    },
                    SubscriptionAction::Apply { .. } if fail => reject(),
                    SubscriptionAction::Apply { .. } => SubscriptionInput::Applied,
                    SubscriptionAction::Park { .. } if fail => park_failed(),
                    SubscriptionAction::Park { .. } => SubscriptionInput::Parked,
                    SubscriptionAction::Ack { .. } if fail => ack_failed(),
                    SubscriptionAction::Ack { .. } => SubscriptionInput::Acked,
                    SubscriptionAction::Sleep { .. } => SubscriptionInput::Slept,
                };
                action = m.handle(input);
            }
            prop_assert!(false, "no Stopped within 96 steps");
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
