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
}

impl<E> Batch<E> {
    /// An empty (caught-up) batch.
    pub const fn empty() -> Self {
        Self {
            events: Vec::new(),
            upper: None,
        }
    }

    /// Wrap deliveries: `upper` must be the highest sequence in `events`,
    /// or `None` exactly when `events` is empty. The machine validates
    /// both invariants on arrival regardless.
    pub fn new(events: Vec<EventEnvelope<E>>, upper: Option<Checkpoint>) -> Self {
        Self { events, upper }
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
        }
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
        match (batch.events.is_empty(), batch.upper) {
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
                self.upper = Some(upper);
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

    fn on_apply_failed(&mut self, _error: StoreError) -> SubscriptionAction<E> {
        if self.phase != Phase::Applying {
            return self.violation("`ApplyFailed` outside the applying phase");
        }
        // The error is not the machine's to act on — the policy is
        // always backoff-and-redeliver — but it has been carried this
        // far so the driver reports the cause before the retry.
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
            Just(SubscriptionInput::Applied),
            Just(SubscriptionInput::ApplyFailed {
                error: StoreError::other("boom"),
            }),
            Just(SubscriptionInput::Acked),
            Just(SubscriptionInput::AckFailed),
            Just(SubscriptionInput::Slept),
            Just(SubscriptionInput::Failed(StoreError::Unavailable)),
            Just(SubscriptionInput::Shutdown),
        ]
        .boxed()
    }

    proptest! {
        /// The machine never panics on any input sequence, and once it
        /// is done it stays done: every action after the first `Done` is
        /// a protocol-violation `Done`.
        #[test]
        fn never_panics_and_stays_done(
            script in prop::collection::vec(arb_subscription_input(), 0..16)
        ) {
            let mut m = machine();
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
            script in prop::collection::vec(arb_subscription_input(), 0..16)
        ) {
            let mut m = machine();
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
