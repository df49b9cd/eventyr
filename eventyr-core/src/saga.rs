//! The saga machine: the sans-IO core of a process manager.
//!
//! A **saga** — a process manager, `esrs`'s *policy* noun — is the read
//! side's counterpart to [`Aggregate::decide`](crate::aggregate::Aggregate::decide):
//! where an aggregate decides events from a command against its folded
//! state, a saga decides *commands* from an *event* — and keeps no state
//! of its own. It is the §7 machine table's saga row:
//!
//! - [`SagaMachine::start`] runs the saga's pure [`react`](Saga::react)
//!   against the event and emits the first [`SagaAction::Dispatch`] (or
//!   [`Done`](SagaAction::Done) when the reaction is empty);
//! - the driver performs each dispatch against the command's target
//!   stream and answers [`SagaInput::Dispatched`] or
//!   [`SagaInput::DispatchFailed`];
//! - the last dispatch answered ends at [`SagaOutcome::Done`].
//!
//! Checkpointing and redelivery — what makes a saga *reliable* — stay
//! with the [`SubscriptionMachine`](crate::subscription::SubscriptionMachine)
//! a saga runner composes into (see `eventyr-subscription`'s `saga`
//! module): the subscription owns at-least-once per event, the saga owns
//! the per-event reaction. The reaction is a pure function of the event,
//! so a crash between dispatch *n* and the ack re-delivers the event and
//! re-issues every command.
//!
//! Each command therefore carries a deterministic idempotency key
//! (0.7.5): the triggering event's global sequence and the command's
//! index in the reaction, under the saga's [`name`](Saga::name). A
//! re-issued command carries the key it carried the first time, so a
//! dispatcher that executes it through a keyed write (the repository's
//! `execute_with_metadata`) commits it once — at-least-once delivery
//! becomes effectively-once at the command boundary.
//!
//! Like every machine here, it never panics on bad input: a driver that
//! feeds the wrong input for the current phase, or drives a finished
//! machine, gets [`Done`](SagaAction::Done) with
//! [`Failed(StoreError::Other(ProtocolError))`](SagaOutcome::Failed).

use alloc::collections::VecDeque;
use alloc::vec::Vec;

use crate::envelope::{EventEnvelope, Metadata};
use crate::error::StoreError;
use crate::vocabulary::StreamId;

/// The saga: pure, stateless — commands from events.
///
/// The implementing type is a namespace or configuration, like
/// [`Aggregate`](crate::aggregate::Aggregate): the reaction reads the
/// event's payload, never a held-over fold. A saga that needs state
/// reads it back through a projection first, or the event carries what
/// it needs.
pub trait Saga: Send {
    /// The events this saga reacts to — the same type the source
    /// delivers.
    type Event: Send;
    /// The commands this saga emits.
    type Command: Send;

    /// The saga's stable name, scoping the idempotency keys of the
    /// commands it issues (0.7.5). Two sagas reacting to the same event
    /// must have different names, or their commands' keys collide.
    /// Like a stored event name, it is part of the storage schema: a
    /// renamed saga re-issues commands for events it already handled.
    fn name(&self) -> &str;

    /// React to one event: the commands to issue, in order, each paired
    /// with the stream it targets. An empty list is a no-op.
    ///
    /// Pure: no I/O, no clocks, no randomness — anything the reaction
    /// needs from the environment belongs on the event, as in
    /// [`Aggregate::decide`](crate::aggregate::Aggregate::decide).
    fn react(&self, event: &EventEnvelope<Self::Event>) -> Vec<(StreamId, Self::Command)>;
}

/// One command a saga issues: the command, the stream it targets, and
/// the metadata it carries.
#[derive(Clone, Debug)]
pub struct SagaCommand<C> {
    /// The command to execute.
    pub command: C,
    /// The stream it targets, as [`Saga::react`] named it.
    pub target: StreamId,
    /// The interaction's metadata ([`with_metadata`](SagaMachine::with_metadata))
    /// layered over the triggering event's own ids — see
    /// [`Metadata::overlay`].
    pub metadata: Metadata,
}

/// The idempotency key of the `index`-th command `saga` issues for
/// `event`: `"{saga}:{sequence}:{index}"`.
///
/// The global sequence identifies the event for good — redelivery
/// carries the same envelope — and the reaction is pure, so a re-issued
/// command lands on the same index.
pub fn idempotency_key<E>(
    saga: &str,
    event: &EventEnvelope<E>,
    index: usize,
) -> alloc::string::String {
    alloc::format!("{saga}:{}:{index}", event.sequence)
}

/// What the machine wants the driver to do.
#[derive(Clone, Debug)]
pub enum SagaAction<C> {
    /// Execute this command against its target stream, then answer
    /// [`Dispatched`](SagaInput::Dispatched) or
    /// [`DispatchFailed`](SagaInput::DispatchFailed). Commands dispatch
    /// one at a time, in the order the saga emitted them.
    Dispatch {
        /// The command and where it goes.
        command: SagaCommand<C>,
    },
    /// Terminal: the interaction's outcome.
    Done(SagaOutcome),
}

/// What the driver reports back to the machine.
#[derive(Clone, Debug)]
pub enum SagaInput {
    /// The last-issued command committed on its target stream.
    Dispatched,
    /// The last-issued command failed — the remaining commands are not
    /// issued, and the subscription runner's policy decides whether the
    /// event is redelivered.
    DispatchFailed(StoreError),
}

/// The terminal outcome of a driven saga machine.
#[derive(Clone, Debug)]
pub enum SagaOutcome {
    /// Every command the saga emitted was dispatched (or the reaction
    /// was empty).
    Done,
    /// A dispatch failed, or the driver broke the protocol (as
    /// [`StoreError::Other`] carrying a [`ProtocolError`](crate::error::ProtocolError)).
    Failed(StoreError),
}

/// Which input the phase guards accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Before `start`.
    Ready,
    /// Waiting for `Dispatched` / `DispatchFailed`.
    Dispatching,
    /// Terminal.
    Done,
}

/// The sans-IO machine behind one saga interaction: react to the event,
/// then dispatch each command in order.
pub struct SagaMachine<S: Saga> {
    saga: S,
    /// The interaction's metadata, layered onto every command.
    metadata: Metadata,
    phase: Phase,
    /// The commands left to dispatch, in order.
    pending: VecDeque<SagaCommand<S::Command>>,
}

impl<S: Saga> SagaMachine<S> {
    /// A saga interaction over `saga`. Metadata starts empty; set it
    /// with [`with_metadata`](Self::with_metadata) before
    /// [`start`](Self::start).
    pub fn new(saga: S) -> Self {
        Self {
            saga,
            metadata: Metadata::default(),
            phase: Phase::Ready,
            pending: VecDeque::new(),
        }
    }

    /// Builder-style: the interaction's metadata. Its set fields win over
    /// the triggering event's ids on every command (see
    /// [`Metadata::overlay`]).
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// React to `event` and emit the first dispatch, or `Done` when the
    /// reaction is empty.
    pub fn start(&mut self, event: &EventEnvelope<S::Event>) -> SagaAction<S::Command> {
        if self.phase != Phase::Ready {
            return self.violation("start() on a machine that already progressed");
        }
        let metadata = Metadata::overlay(&self.metadata, &event.metadata);
        let name = self.saga.name();
        self.pending = self
            .saga
            .react(event)
            .into_iter()
            .enumerate()
            .map(|(index, (target, command))| SagaCommand {
                command,
                target,
                metadata: metadata
                    .clone()
                    .with_idempotency_key(idempotency_key(name, event, index)),
            })
            .collect();
        self.phase = Phase::Dispatching;
        self.next()
    }

    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: SagaInput) -> SagaAction<S::Command> {
        if self.phase != Phase::Dispatching {
            return self.violation("input outside the dispatching phase");
        }
        match input {
            SagaInput::Dispatched => self.next(),
            SagaInput::DispatchFailed(error) => {
                self.phase = Phase::Done;
                SagaAction::Done(SagaOutcome::Failed(error))
            }
        }
    }

    /// Whether the machine reached its terminal outcome.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    fn next(&mut self) -> SagaAction<S::Command> {
        match self.pending.pop_front() {
            Some(command) => SagaAction::Dispatch { command },
            None => {
                self.phase = Phase::Done;
                SagaAction::Done(SagaOutcome::Done)
            }
        }
    }

    fn violation(&mut self, message: &'static str) -> SagaAction<S::Command> {
        self.phase = Phase::Done;
        SagaAction::Done(SagaOutcome::Failed(StoreError::protocol(message)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::account::{Account, AccountCommand, AccountEvent, AccountId};
    use crate::vocabulary::{Sequence, Version};
    use alloc::vec;

    /// A standing order: a deposit of at least `amount` on any account
    /// withdraws `amount` from `from`.
    struct StandingOrder {
        from: AccountId,
        amount: u64,
    }

    impl Saga for StandingOrder {
        type Event = AccountEvent;
        type Command = AccountCommand;

        fn name(&self) -> &str {
            "standing-order"
        }

        fn react(&self, event: &EventEnvelope<AccountEvent>) -> Vec<(StreamId, AccountCommand)> {
            match &event.event {
                AccountEvent::Deposited { amount } if *amount >= self.amount => vec![(
                    target_of(self.from.clone()),
                    AccountCommand::Withdraw {
                        amount: self.amount,
                    },
                )],
                _ => vec![],
            }
        }
    }

    /// Emits two withdrawals per deposit, to check dispatch order.
    struct Twice;

    impl Saga for Twice {
        type Event = AccountEvent;
        type Command = AccountCommand;

        fn name(&self) -> &str {
            "twice"
        }

        fn react(&self, _: &EventEnvelope<AccountEvent>) -> Vec<(StreamId, AccountCommand)> {
            vec![
                (
                    target_of(AccountId(1)),
                    AccountCommand::Withdraw { amount: 10 },
                ),
                (
                    target_of(AccountId(2)),
                    AccountCommand::Withdraw { amount: 5 },
                ),
            ]
        }
    }

    fn target_of(id: AccountId) -> StreamId {
        StreamId::for_aggregate::<Account>(&id)
    }

    fn deposit(sequence: u64, amount: u64, metadata: Metadata) -> EventEnvelope<AccountEvent> {
        EventEnvelope {
            sequence: Sequence::new(sequence),
            stream_id: target_of(AccountId(2)),
            version: Version::new(sequence),
            event: AccountEvent::Deposited { amount },
            metadata,
        }
    }

    fn machine() -> SagaMachine<StandingOrder> {
        SagaMachine::new(StandingOrder {
            from: AccountId(1),
            amount: 10,
        })
    }

    #[test]
    fn each_command_carries_a_key_from_the_event_and_its_index() {
        let mut m = SagaMachine::new(Twice);
        let SagaAction::Dispatch { command: first } =
            m.start(&deposit(42, 10, Metadata::default()))
        else {
            panic!("dispatch");
        };
        let SagaAction::Dispatch { command: second } = m.handle(SagaInput::Dispatched) else {
            panic!("dispatch");
        };
        assert_eq!(
            first.metadata.idempotency_key.as_deref(),
            Some("twice:42:0")
        );
        assert_eq!(
            second.metadata.idempotency_key.as_deref(),
            Some("twice:42:1")
        );
    }

    #[test]
    fn a_redelivered_event_reissues_the_same_keys() {
        let keys = |sequence| {
            let mut m = SagaMachine::new(Twice);
            let mut keys = Vec::new();
            let mut action = m.start(&deposit(sequence, 10, Metadata::default()));
            while let SagaAction::Dispatch { command } = action {
                keys.push(command.metadata.idempotency_key.clone());
                action = m.handle(SagaInput::Dispatched);
            }
            keys
        };
        assert_eq!(keys(7), keys(7), "same event, same keys");
        assert_ne!(keys(7), keys(8), "another event, other keys");
    }

    #[test]
    fn the_triggering_events_own_key_is_not_inherited() {
        // The event was itself produced by a keyed command; the saga's
        // command is a different command and gets its own key.
        let event = deposit(
            3,
            10,
            Metadata::default().with_idempotency_key("upstream:1:0"),
        );
        let mut m = machine();
        let SagaAction::Dispatch { command } = m.start(&event) else {
            panic!("dispatch");
        };
        assert_eq!(
            command.metadata.idempotency_key.as_deref(),
            Some("standing-order:3:0")
        );
    }

    fn is_protocol_violation(action: &SagaAction<AccountCommand>) -> bool {
        matches!(
            action,
            SagaAction::Done(SagaOutcome::Failed(error)) if error.is_protocol_violation()
        )
    }

    #[test]
    fn an_empty_reaction_ends_done_at_start() {
        let mut m = machine();
        let action = m.start(&deposit(1, 4, Metadata::default()));
        assert!(matches!(action, SagaAction::Done(SagaOutcome::Done)));
        assert!(m.is_done());
    }

    #[test]
    fn a_command_dispatches_to_the_stream_the_saga_named() {
        let mut m = machine();
        let SagaAction::Dispatch { command } = m.start(&deposit(7, 42, Metadata::default())) else {
            panic!("expected a dispatch")
        };
        assert_eq!(command.target, target_of(AccountId(1)));
        assert!(matches!(
            command.command,
            AccountCommand::Withdraw { amount: 10 }
        ));
        assert!(matches!(
            m.handle(SagaInput::Dispatched),
            SagaAction::Done(SagaOutcome::Done)
        ));
    }

    #[test]
    fn commands_dispatch_one_at_a_time_in_order() {
        let mut m = SagaMachine::new(Twice);
        let SagaAction::Dispatch { command: first } = m.start(&deposit(1, 1, Metadata::default()))
        else {
            panic!("expected the first dispatch")
        };
        assert_eq!(first.target, target_of(AccountId(1)));
        let SagaAction::Dispatch { command: second } = m.handle(SagaInput::Dispatched) else {
            panic!("expected the second dispatch")
        };
        assert_eq!(second.target, target_of(AccountId(2)));
        assert!(matches!(
            m.handle(SagaInput::Dispatched),
            SagaAction::Done(SagaOutcome::Done)
        ));
    }

    #[test]
    fn a_dispatch_failure_stops_the_remaining_commands() {
        let mut m = SagaMachine::new(Twice);
        m.start(&deposit(1, 1, Metadata::default()));
        assert!(matches!(
            m.handle(SagaInput::DispatchFailed(StoreError::Unavailable)),
            SagaAction::Done(SagaOutcome::Failed(StoreError::Unavailable))
        ));
        assert!(m.is_done());
    }

    #[test]
    fn interaction_metadata_layers_over_the_event_ids() {
        // The interaction sets only a correlation id; the event's own
        // causation id survives underneath it.
        let mut m = machine().with_metadata(Metadata::of_ids(None, Some("corr-request".into())));
        let event = deposit(
            3,
            100,
            Metadata::of_ids(Some("cause-event".into()), Some("corr-event".into())),
        );
        let SagaAction::Dispatch { command } = m.start(&event) else {
            panic!("expected a dispatch")
        };
        assert_eq!(
            command.metadata.causation_id.as_deref(),
            Some("cause-event")
        );
        assert_eq!(
            command.metadata.correlation_id.as_deref(),
            Some("corr-request")
        );
    }

    #[test]
    fn driving_a_finished_machine_is_a_protocol_violation() {
        let mut m = machine();
        m.start(&deposit(1, 4, Metadata::default()));
        assert!(is_protocol_violation(&m.handle(SagaInput::Dispatched)));
        assert!(is_protocol_violation(&m.start(&deposit(
            2,
            4,
            Metadata::default()
        ))));
    }

    #[test]
    fn input_before_start_is_a_protocol_violation() {
        let mut m = machine();
        assert!(is_protocol_violation(&m.handle(SagaInput::Dispatched)));
    }

    use proptest::prelude::*;

    fn arb_input() -> BoxedStrategy<SagaInput> {
        prop_oneof![
            Just(SagaInput::Dispatched),
            Just(SagaInput::DispatchFailed(StoreError::Unavailable)),
        ]
        .boxed()
    }

    proptest! {
        /// The machine never panics on any input sequence, and once it
        /// is done it stays done: every action after the first `Done` is
        /// a protocol-violation `Done`.
        #[test]
        fn never_panics_and_stays_done(
            amount in 0u64..20,
            script in prop::collection::vec(arb_input(), 0..16)
        ) {
            let mut m = SagaMachine::new(Twice);
            let mut actions = vec![m.start(&deposit(1, amount, Metadata::default()))];
            for input in script {
                actions.push(m.handle(input));
            }
            if let Some(i) = actions.iter().position(|a| matches!(a, SagaAction::Done(_))) {
                for a in &actions[i + 1..] {
                    prop_assert!(is_protocol_violation(a));
                }
            }
        }
    }
}
