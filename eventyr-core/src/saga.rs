//! The saga machine: the sans-IO core of a process manager.
//!
//! A **saga** — a process manager, `esrs`'s *policy* noun — is the read
//! side's counterpart to [`Aggregate::decide`](crate::aggregate::Aggregate::decide):
//! where an aggregate decides events from a command against its folded
//! state, a saga decides *commands* from an *event* — and keeps no state
//! of its own. This module is the trailhead for roadmap §13's 0.6.1
//! ("sagas as a machine"), the §7 table's third row:
//!
//! - [`SagaMachine::start`] emits [`SagaAction::React`]: "fold the event
//!   in" — the saga sees the envelope, pure;
//! - each rejected command from [`Saga::react`] (or none) becomes a
//!   [`SagaAction::Dispatch`] the driver performs, answering with
//!   [`SagaInput::Dispatched`] / [`SagaInput::DispatchFailed`];
//! - an empty reaction, or the last dispatch answered, ends at
//!   [`SagaOutcome::Done`].
//!
//! Checkpointing and redelivery — what makes a saga *reliable* — stay
//! with the [`SubscriptionMachine`](crate::subscription::SubscriptionMachine)
//! a saga runner composes into (0.6.1's deliberate line; see
//! `eventyr-subscription::saga`): the subscription owns at-least-once
//! per event, the saga owns per-event reaction, and within one event
//! `Dispatched` must answer before the next command issues. The saga
//! itself is always replayable: its reaction is a pure function of the
//! event, so a crash between dispatch *n* and its ack re-delivers the
//! event and re-issues every command — idempotency of each command is
//! the caller's to guarantee, exactly as §6 puts it for projections.
//!
//! Like every machine here, it never panics on bad input: a driver that
//! feeds the wrong input for the current phase, or drives a finished
//! machine, gets [`Done`](SagaAction::Done) with
//! [`Failed(StoreError::Other(ProtocolError))`](SagaOutcome::Failed).

use alloc::vec::Vec;
use alloc::sync::Arc;

use crate::envelope::{EventEnvelope, Metadata};
use crate::error::{ProtocolError, StoreError};
use crate::vocabulary::StreamId;

/// The saga: pure, stateless — commands from events.
///
/// The implementing type is a namespace, like
/// [`Aggregate`](crate::aggregate::Aggregate): state is folded per interaction by
/// the subscription the saga runner drives, and the reaction reads the
/// event's payload — never a held-over fold. If a saga needs its own
/// state, it reads it back through a projection first (a
/// [`Projector`](https://docs.rs/eventyr-subscription) run before the
/// saga), or the event carries whatever it needs.
pub trait Saga: Send {
    /// The events this saga reacts to — the same type the source
    /// delivers.
    type Event: Send;
    /// The commands this saga emits — routed per event.
    type Command: Send;

    /// React to one event: the commands to issue, in order. An empty
    /// list is a no-op; the saga's second actor (a validator, a
    /// compensating command) is the same event read twice — never
    /// folded state.
    ///
    /// Pure: no I/O, no clocks, no randomness — anything the reaction
    /// needs from the environment belongs on the event or a context
    /// type, as in [`Aggregate::decide`](crate::aggregate::Aggregate::decide).
    fn react(&self, event: &EventEnvelope<Self::Event>) -> Vec<Self::Command>;
}

/// One command a saga issues: its payload, the stream it targets, and
/// the metadata the interaction stamps on it.
///
/// A command is not a store payload — the driver resolves the target
/// stream's aggregate and runs the command through the write machine.
/// `target` names the stream the command belongs to (the saga's own
/// domain of interest: "the account `To` side of a transfer-failed
/// event").
#[derive(Clone, Debug)]
pub struct SagaCommand<C> {
    /// The command to execute.
    pub command: C,
    /// The stream it belongs to.
    pub target: StreamId,
    /// Causation/correlation the interaction stamps: set by
    /// [`with_metadata`](SagaMachine::with_metadata), never by `react`
    /// itself — a saga decides *what*, the boundary decides *why*.
    pub metadata: Metadata,
}

impl<C> SagaCommand<C> {
    /// Overlay metadata onto this command: fields set on `interaction`
    /// (the 0.5.2 boundary stamp) win over the event's own ids, which
    /// win over whatever the reaction set. Called by the driver at
    /// dispatch time; the saga's own `metadata` is the least-authoritative
    /// layer.
    pub fn overlay_metadata(&mut self, interaction: Metadata, event: Metadata) {
        self.metadata = Metadata {
            causation_id: interaction
                .causation_id
                .or(event.causation_id)
                .or(self.metadata.causation_id.clone()),
            correlation_id: interaction
                .correlation_id
                .or(event.correlation_id)
                .or(self.metadata.correlation_id.clone()),
            #[cfg(feature = "time")]
            timestamp: interaction.timestamp.or(event.timestamp),
        };
    }
}

/// What the machine wants the driver to do.
///
/// Actions are data, not calls: the driver interprets each variant,
/// performs the I/O, and reports back with a [`SagaInput`].
#[derive(Clone, Debug)]
pub enum SagaAction<E, C> {
    /// Fold the event in: the driver's answer is the event's own
    /// command list, as computed by [`Saga::react`]. The machine does
    /// not fold — the saga is stateless.
    React {
        /// The event to react to.
        event: EventEnvelope<E>,
    },
    /// Execute this command against its target stream. One
    /// [`Dispatched`](SagaInput::Dispatched) per command, in the order
    /// the saga emitted them; [`SagaInput::DispatchFailed`] short-circuits
    /// the rest of the batch.
    Dispatch {
        /// The command and where it goes.
        command: SagaCommand<C>,
    },
    /// Terminal: the interaction's outcome.
    Done(SagaOutcome),
}

/// What the driver reports back to the machine.
#[derive(Clone, Debug)]
pub enum SagaInput<C> {
    /// The reaction against the event, computed by the driver's call to
    /// [`Saga::react`].
    Reacted {
        /// The commands this event emits, in order.
        commands: Vec<SagaCommand<C>>,
    },
    /// The last-issued command committed on its target stream.
    Dispatched,
    /// The last-issued command failed at the store — a conflict the
    /// write machine's retry budget could not cover, or a fatal one.
    /// The saga is done: the subscription runner's own policy decides
    /// whether the event is retried.
    DispatchFailed(StoreError),
}

/// The terminal outcome of a driven saga machine.
#[derive(Clone, Debug)]
pub enum SagaOutcome {
    /// Every command the saga emitted was dispatched (or the reaction
    /// was empty).
    Done,
    /// A dispatch failed, or the driver broke the protocol (as
    /// [`StoreError::Other`] carrying a [`ProtocolError`]).
    Failed(StoreError),
}

/// Which input the phase guards accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for `Reacted`.
    Reacting,
    /// Waiting for `Dispatched` / `DispatchFailed`.
    Dispatching,
    /// Terminal.
    Done,
}

/// The sans-IO machine behind one saga interaction: react to the event,
/// then dispatch each command in order until the batch is done. Never
/// does I/O itself; never panics on bad input.
pub struct SagaMachine<S: Saga> {
    /// The reaction — kept on the machine so the driver's one call shape
    /// (`start` then `react`) is the whole interaction.
    saga: S,
    /// The metadata stamped onto every command this interaction emits.
    metadata: Metadata,
    phase: Phase,
    /// The commands left to dispatch, in order.
    pending: Vec<SagaCommand<S::Command>>,
}

impl<S: Saga> SagaMachine<S> {
    /// Compute the saga's reaction to `event`, freshly: the driver's
    /// `React` step. Each command targets the stream the saga names and
    /// carries its own metadata; the interaction's `with_metadata`
    /// overlays both at dispatch time.
    pub fn react(&self, event: &EventEnvelope<S::Event>) -> Vec<SagaCommand<S::Command>> {
        self.saga
            .react(event)
            .into_iter()
            .map(|command| SagaCommand {
                command,
                target: StreamId::default(),
                metadata: Metadata::default(),
            })
            .collect()
    }

    /// A saga interaction over `saga`. Metadata starts empty; set it
    /// with [`with_metadata`](Self::with_metadata) before
    /// [`start`](Self::start).
    pub fn new(saga: S) -> Self {
        Self {
            saga,
            metadata: Metadata::default(),
            phase: Phase::Reacting,
            pending: Vec::new(),
        }
    }

    /// The metadata this machine stamps onto every command it dispatches.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Builder-style: the metadata stamped on every command this
    /// interaction emits (0.5.2's boundary concern, applied to saga
    /// emissions the same way).
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// The first action: react to the event. Idempotent until the first
    /// [`handle`](Self::handle). The event passes through untouched —
    /// the machine does not fold it; the caller's
    /// [`Saga::react`] is the fold.
    pub fn start(&mut self, event: EventEnvelope<S::Event>) -> SagaAction<S::Event, S::Command> {
        if self.phase != Phase::Reacting {
            return self.violation("start() on a machine that already progressed");
        }
        SagaAction::React { event }
    }

    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: SagaInput<S::Command>) -> SagaAction<S::Event, S::Command> {
        match input {
            SagaInput::Reacted { commands } => self.on_reacted(commands),
            SagaInput::Dispatched => self.on_dispatched(),
            SagaInput::DispatchFailed(error) => self.on_dispatch_failed(error),
        }
    }

    /// Whether the machine reached its terminal outcome.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    fn on_reacted(
        &mut self,
        commands: Vec<SagaCommand<S::Command>>,
    ) -> SagaAction<S::Event, S::Command> {
        if self.phase != Phase::Reacting {
            return self.violation("`Reacted` outside the reacting phase");
        }
        let mut commands = commands;
        // Stamp the interaction's metadata on every command before any
        // of them is emitted (0.5.2's seam, applied to sagas the same
        // way the repository stamps its events).
        let metadata = self.metadata.clone();
        for command in &mut commands {
            command.metadata = metadata.clone();
        }
        match commands.is_empty() {
            true => {
                self.phase = Phase::Done;
                SagaAction::Done(SagaOutcome::Done)
            }
            false => {
                // Dispatch in the saga's order: the first one now, the
                // rest behind `Dispatched` per step.
                self.pending = commands.split_off(1);
                self.phase = Phase::Dispatching;
                let first = commands.into_iter().next().expect("checked non-empty above");
                SagaAction::Dispatch { command: first }
            }
        }
    }

    fn on_dispatched(&mut self) -> SagaAction<S::Event, S::Command> {
        if self.phase != Phase::Dispatching {
            return self.violation("`Dispatched` outside the dispatching phase");
        }
        if self.pending.is_empty() {
            self.phase = Phase::Done;
            SagaAction::Done(SagaOutcome::Done)
        } else {
            let next = self.pending.remove(0);
            SagaAction::Dispatch { command: next }
        }
    }

    fn on_dispatch_failed(&mut self, error: StoreError) -> SagaAction<S::Event, S::Command> {
        if self.phase == Phase::Done {
            return self.violation("`DispatchFailed` on a finished machine");
        }
        self.phase = Phase::Done;
        SagaAction::Done(SagaOutcome::Failed(error))
    }

    fn violation(&mut self, message: &'static str) -> SagaAction<S::Event, S::Command> {
        self.phase = Phase::Done;
        SagaAction::Done(SagaOutcome::Failed(StoreError::Other(Arc::new(
            ProtocolError::new(message),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::account::{AccountCommand, AccountEvent, AccountId};
    use crate::vocabulary::{Sequence, StreamId, Version};
    use alloc::vec;

    /// The canonical saga: a `Deposited` on one account debits
    /// `amount` from another (a standing order). Its `react` ignores
    /// every other event.
    struct StandingOrder {
        from: AccountId,
        amount: u64,
    }

    impl Saga for StandingOrder {
        type Event = AccountEvent;
        type Command = AccountCommand;

        fn react(&self, event: &EventEnvelope<AccountEvent>) -> Vec<AccountCommand> {
            match &event.event {
                AccountEvent::Deposited { amount } if *amount >= self.amount => {
                    vec![AccountCommand::Withdraw {
                        amount: self.amount,
                    }]
                }
                _ => vec![],
            }
        }
    }

    fn target_of(id: AccountId) -> StreamId {
        StreamId::for_aggregate::<crate::testing::account::Account>(&id)
    }

    fn envelope(sequence: u64, event: AccountEvent) -> EventEnvelope<AccountEvent> {
        EventEnvelope {
            sequence: Sequence::new(sequence),
            stream_id: target_of(AccountId(2)),
            version: Version::new(sequence),
            event,
            metadata: Metadata::default(),
        }
    }

    fn deposit_commands(saga: &StandingOrder, event: &EventEnvelope<AccountEvent>) -> Vec<SagaCommand<AccountCommand>> {
        saga.react(event)
            .into_iter()
            .map(|command| SagaCommand {
                command,
                target: target_of(saga.from.clone()),
                metadata: Metadata::default(),
            })
            .collect()
    }

    fn machine() -> SagaMachine<StandingOrder> {
        SagaMachine::new(StandingOrder {
            from: AccountId(1),
            amount: 10,
        })
    }

    fn is_protocol_violation(
        action: &SagaAction<AccountEvent, AccountCommand>,
    ) -> bool {
        matches!(
            action,
            SagaAction::Done(SagaOutcome::Failed(StoreError::Other(source)))
                if source.downcast_ref::<ProtocolError>().is_some()
        )
    }

    #[test]
    fn a_reaction_with_no_commands_ends_done() {
        let mut m = machine();
        let event = envelope(1, AccountEvent::Deposited { amount: 4 });
        let action = m.start(event);
        assert!(matches!(action, SagaAction::React { .. }));
        let action = m.handle(SagaInput::Reacted { commands: vec![] });
        assert!(matches!(action, SagaAction::Done(SagaOutcome::Done)));
        assert!(m.is_done());
    }

    #[test]
    fn a_reaction_with_commands_dispatches_each_in_order() {
        let saga = StandingOrder {
            from: AccountId(1),
            amount: 10,
        };
        let mut m = SagaMachine::new(saga);
        let event = envelope(7, AccountEvent::Deposited { amount: 42 });
        let action = m.start(event.clone());
        assert!(matches!(action, SagaAction::React { .. }));

        // The saga's command, dispatched to the `from` stream.
        let action = m.handle(SagaInput::Reacted {
            commands: deposit_commands(&StandingOrder { from: AccountId(1), amount: 10 }, &event),
        });
        let SagaAction::Dispatch { command } = action else {
            panic!("expected a dispatch")
        };
        assert_eq!(command.target, target_of(AccountId(1)));
        assert!(matches!(
            command.command,
            AccountCommand::Withdraw { amount: 10 }
        ));

        let action = m.handle(SagaInput::Dispatched);
        assert!(matches!(action, SagaAction::Done(SagaOutcome::Done)));
    }

    #[test]
    fn a_dispatch_failure_ends_the_interaction() {
        let mut m = machine();
        let action = m.start(envelope(1, AccountEvent::Deposited { amount: 42 }));
        assert!(matches!(action, SagaAction::React { .. }));
        let action = m.handle(SagaInput::Reacted {
            commands: vec![SagaCommand {
                command: AccountCommand::Withdraw { amount: 10 },
                target: target_of(AccountId(1)),
                metadata: Metadata::default(),
            }],
        });
        assert!(matches!(action, SagaAction::Dispatch { .. }));
        let action = m.handle(SagaInput::DispatchFailed(StoreError::Unavailable));
        assert!(matches!(
            action,
            SagaAction::Done(SagaOutcome::Failed(StoreError::Unavailable))
        ));
    }

    #[test]
    fn driving_a_finished_machine_is_a_protocol_violation() {
        let mut m = machine();
        m.start(envelope(1, AccountEvent::Deposited { amount: 4 }));
        m.handle(SagaInput::Reacted { commands: vec![] });
        assert!(is_protocol_violation(&m.handle(SagaInput::Dispatched)));
        assert!(is_protocol_violation(
            &m.start(envelope(2, AccountEvent::Deposited { amount: 4 })),
        ));
    }

    #[test]
    fn dispatched_before_reacted_is_a_protocol_violation() {
        let mut m = machine();
        m.start(envelope(1, AccountEvent::Deposited { amount: 4 }));
        assert!(is_protocol_violation(&m.handle(SagaInput::Dispatched)));
    }

    #[test]
    fn with_metadata_carries_through_every_dispatch() {
        let mut m = machine().with_metadata(Metadata {
            causation_id: Some("request-9".into()),
            correlation_id: Some("corr-1".into()),
            ..Default::default()
        });
        let action = m.start(envelope(3, AccountEvent::Deposited { amount: 100 }));
        assert!(matches!(action, SagaAction::React { .. }));

        // Two commands: both stamped with the interaction's metadata at
        // dispatch time.
        let action = m.handle(SagaInput::Reacted {
            commands: vec![
                SagaCommand {
                    command: AccountCommand::Withdraw { amount: 10 },
                    target: target_of(AccountId(1)),
                    metadata: Metadata::default(),
                },
                SagaCommand {
                    command: AccountCommand::Withdraw { amount: 5 },
                    target: target_of(AccountId(1)),
                    metadata: Metadata::default(),
                },
            ],
        });
        let SagaAction::Dispatch { command } = action else {
            panic!("expected the first dispatch");
        };
        assert_eq!(command.metadata.causation_id.as_deref(), Some("request-9"));

        let action = m.handle(SagaInput::Dispatched);
        let SagaAction::Dispatch { command } = action else {
            panic!("expected the second dispatch");
        };
        assert_eq!(command.metadata.correlation_id.as_deref(), Some("corr-1"));

        let action = m.handle(SagaInput::Dispatched);
        assert!(matches!(action, SagaAction::Done(SagaOutcome::Done)));
    }

    // -- the scripted driver + invariants -----------------------------------

    /// Drive `machine` through `start` and every input in `script`,
    /// recording each action in order — the saga sibling of
    /// `crate::testing::drive_scripted`. The event `start` reacts to is
    /// fixed by construction here (the test's `envelope` helper above).
    fn drive_scripted_saga(
        machine: &mut SagaMachine<StandingOrder>,
        script: impl IntoIterator<Item = SagaInput<AccountCommand>>,
    ) -> Vec<SagaAction<AccountEvent, AccountCommand>> {
        let mut actions = Vec::new();
        actions.push(machine.start(envelope(
            1,
            AccountEvent::Deposited { amount: 100 },
        )));
        for input in script {
            actions.push(machine.handle(input));
        }
        actions
    }

    #[test]
    fn a_script_runs_start_to_done_and_records_every_action() {
        let mut m = machine();
        let actions = drive_scripted_saga(
            &mut m,
            vec![
                SagaInput::Reacted {
                    commands: vec![
                        SagaCommand {
                            command: AccountCommand::Withdraw { amount: 10 },
                            target: target_of(AccountId(1)),
                            metadata: Metadata::default(),
                        },
                        SagaCommand {
                            command: AccountCommand::Withdraw { amount: 5 },
                            target: target_of(AccountId(1)),
                            metadata: Metadata::default(),
                        },
                    ],
                },
                SagaInput::Dispatched,
                SagaInput::Dispatched,
            ],
        );
        assert_eq!(actions.len(), 4);
        assert!(matches!(actions[0], SagaAction::React { .. }));
        assert!(matches!(actions[1], SagaAction::Dispatch { .. }));
        assert!(matches!(actions[2], SagaAction::Dispatch { .. }));
        assert!(matches!(actions[3], SagaAction::Done(SagaOutcome::Done)));
    }

    use proptest::prelude::*;

    fn arb_saga_input() -> BoxedStrategy<SagaInput<AccountCommand>> {
        prop_oneof![
            (0usize..4).prop_map(|len| {
                let commands = (0..len)
                    .map(|_| SagaCommand {
                        command: AccountCommand::Withdraw { amount: 1 },
                        target: target_of(AccountId(1)),
                        metadata: Metadata::default(),
                    })
                    .collect();
                SagaInput::Reacted { commands }
            }),
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
            script in prop::collection::vec(arb_saga_input(), 0..16)
        ) {
            let mut m = machine();
            let actions = drive_scripted_saga(&mut m, script);

            prop_assert!(!actions.is_empty()); // start always emits
            if let Some(i) = actions.iter().position(|a| matches!(a, SagaAction::Done(_))) {
                for a in &actions[i + 1..] {
                    prop_assert!(is_protocol_violation(a));
                }
            }
        }
    }
}
