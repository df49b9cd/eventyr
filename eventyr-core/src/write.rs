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
//! [`drive_scripted`](crate::testing::drive_scripted)).

use alloc::vec::Vec;
use alloc::sync::Arc;

use crate::aggregate::Aggregate;
use crate::envelope::{EventEnvelope, NewEvent};
use crate::error::{ProtocolError, StoreError};
use crate::vocabulary::{ExpectedVersion, StreamId, Version};

/// What the machine wants the driver to do.
///
/// Actions are data, not calls: the driver interprets each variant,
/// performs the I/O, and reports back with a [`WriteInput`].
#[derive(Clone, Debug)]
pub enum WriteAction<E, Err> {
    /// Read the stream from `from` (exclusive) to rebuild state.
    LoadStream {
        /// The stream to read.
        stream_id: StreamId,
        /// Exclusive lower bound on the event version.
        from: Version,
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
    Done(WriteOutcome<E, Err>),
}

/// What the driver reports back to the machine.
#[derive(Clone, Debug)]
pub enum WriteInput<E> {
    /// The stream read completed. Events must belong to the requested
    /// stream and continue the sequence contiguously from where the
    /// machine last folded.
    Loaded {
        /// The events read, in stream order.
        events: Vec<EventEnvelope<E>>,
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
    /// A store operation failed.
    Failed(StoreError),
}

/// The terminal outcome of a driven write machine.
#[derive(Clone, Debug)]
pub enum WriteOutcome<E, Err> {
    /// The events were committed; the envelopes are as the store
    /// recorded them.
    Committed(Vec<EventEnvelope<E>>),
    /// The command decided no events; nothing was appended.
    Noop,
    /// The domain rejected the command.
    Rejected(Err),
    /// The store failed — a conflict that exhausted the retry budget, a
    /// transient error, or a fatal one. Protocol violations by the
    /// driver land here too (see [`ProtocolError`]).
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

/// Which input the machine is waiting for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for `Loaded`.
    Loading,
    /// Waiting for `Appended`/`Conflict`/`Failed`.
    Appending,
    /// Terminal.
    Done,
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
/// Machines never panic on bad input: a driver that feeds the wrong
/// input for the current phase, or drives a finished machine, gets
/// [`Done`](WriteAction::Done) with
/// [`Failed(StoreError::Other(ProtocolError))`](WriteOutcome::Failed).
pub struct WriteMachine<A: Aggregate> {
    stream_id: StreamId,
    command: A::Command,
    retry_policy: RetryPolicy,
    retries_used: u32,
    phase: Phase,
    folded: A::State,
    version: Version,
}

impl<A: Aggregate> WriteMachine<A> {
    /// Begin an interaction: `id` identifies the aggregate instance,
    /// `command` is what to decide, `retry_policy` bounds conflict
    /// retries.
    pub fn new(id: A::Id, command: A::Command, retry_policy: RetryPolicy) -> Self {
        Self {
            stream_id: StreamId::for_aggregate::<A>(&id),
            command,
            retry_policy,
            retries_used: 0,
            phase: Phase::Loading,
            folded: A::initial(&id),
            version: Version::EMPTY,
        }
    }

    /// The first action: read the stream. Idempotent until the first
    /// [`handle`](Self::handle).
    pub fn start(&mut self) -> WriteAction<A::Event, A::Error> {
        if self.phase != Phase::Loading {
            return self.violation("start() on a machine that already progressed");
        }
        WriteAction::LoadStream {
            stream_id: self.stream_id.clone(),
            from: self.version,
        }
    }

    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: WriteInput<A::Event>) -> WriteAction<A::Event, A::Error> {
        match input {
            WriteInput::Loaded { events } => self.on_loaded(events),
            WriteInput::Appended { committed } => self.on_appended(committed),
            WriteInput::Conflict { current } => self.on_conflict(current),
            WriteInput::Failed(error) => self.on_failed(error),
        }
    }

    /// The stream this machine writes to.
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    /// The version folded so far.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Whether the machine reached its terminal outcome.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    fn on_loaded(
        &mut self,
        events: Vec<EventEnvelope<A::Event>>,
    ) -> WriteAction<A::Event, A::Error> {
        if self.phase != Phase::Loading {
            return self.violation("`Loaded` outside the loading phase");
        }
        // Validate before folding: the events must belong to this stream
        // and continue the sequence contiguously from the folded version.
        let mut expected = self.version.as_u64().saturating_add(1);
        for envelope in &events {
            if envelope.stream_id != self.stream_id {
                return self.violation("`Loaded` delivered events from another stream");
            }
            if envelope.version.as_u64() != expected {
                return self.violation("`Loaded` delivered a non-contiguous sequence");
            }
            expected = expected.saturating_add(1);
        }
        for envelope in events {
            A::apply(&mut self.folded, &envelope.event);
            self.version = envelope.version;
        }
        self.decide_and_emit()
    }

    fn on_appended(
        &mut self,
        committed: Vec<EventEnvelope<A::Event>>,
    ) -> WriteAction<A::Event, A::Error> {
        if self.phase != Phase::Appending {
            return self.violation("`Appended` outside the appending phase");
        }
        self.phase = Phase::Done;
        WriteAction::Done(WriteOutcome::Committed(committed))
    }

    fn on_conflict(&mut self, current: Version) -> WriteAction<A::Event, A::Error> {
        if self.phase != Phase::Appending {
            return self.violation("`Conflict` outside the appending phase");
        }
        if current <= self.version {
            return self.violation("conflict reported a version at or before the folded one");
        }
        if self.retries_used < self.retry_policy.max_retries {
            self.retries_used += 1;
            self.phase = Phase::Loading;
            // Reload only the delta since the version we folded.
            WriteAction::LoadStream {
                stream_id: self.stream_id.clone(),
                from: self.version,
            }
        } else {
            self.phase = Phase::Done;
            WriteAction::Done(WriteOutcome::Failed(StoreError::Conflict { current }))
        }
    }

    fn on_failed(&mut self, error: StoreError) -> WriteAction<A::Event, A::Error> {
        if self.phase == Phase::Done {
            return self.violation("`Failed` on a finished machine");
        }
        self.phase = Phase::Done;
        WriteAction::Done(WriteOutcome::Failed(error))
    }

    fn decide_and_emit(&mut self) -> WriteAction<A::Event, A::Error> {
        let decision = A::decide(&self.folded, &self.command);
        match decision {
            Ok(events) if events.is_empty() => {
                self.phase = Phase::Done;
                WriteAction::Done(WriteOutcome::Noop)
            }
            Ok(events) => {
                self.phase = Phase::Appending;
                let expected = if self.version == Version::EMPTY {
                    ExpectedVersion::Empty
                } else {
                    ExpectedVersion::Exact(self.version)
                };
                WriteAction::Append {
                    stream_id: self.stream_id.clone(),
                    expected,
                    events: events.into_iter().map(NewEvent::new).collect(),
                }
            }
            Err(error) => {
                self.phase = Phase::Done;
                WriteAction::Done(WriteOutcome::Rejected(error))
            }
        }
    }

    fn violation(&mut self, message: &'static str) -> WriteAction<A::Event, A::Error> {
        self.phase = Phase::Done;
        WriteAction::Done(WriteOutcome::Failed(StoreError::Other(Arc::new(
            ProtocolError::new(message),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::Aggregate;
    use crate::envelope::{EventEnvelope, Metadata};
    use crate::testing::drive_scripted;
    use crate::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
    use alloc::string::String;
    use core::fmt;
    use proptest::prelude::*;

    // -- test aggregate: a small bank account ---------------------------

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct AccountId(u64);

    impl fmt::Display for AccountId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Clone, PartialEq, Debug)]
    enum AccountEvent {
        Opened { owner: String },
        Deposited { amount: u64 },
        Withdrawn { amount: u64 },
    }

    #[derive(Clone, Debug)]
    enum AccountCommand {
        Open { owner: String },
        Deposit { amount: u64 },
        Withdraw { amount: u64 },
        CheckBalance,
    }

    #[derive(Debug, PartialEq)]
    enum AccountError {
        AlreadyOpen,
        NotOpen,
        InsufficientFunds,
    }

    impl fmt::Display for AccountError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                AccountError::AlreadyOpen => "account is already open",
                AccountError::NotOpen => "account is not open",
                AccountError::InsufficientFunds => "insufficient funds",
            })
        }
    }

    #[derive(Debug)]
    struct AccountState {
        open: bool,
        balance: u64,
    }

    struct Account;

    impl Aggregate for Account {
        const NAME: &'static str = "account";
        type Id = AccountId;
        type State = AccountState;
        type Event = AccountEvent;
        type Command = AccountCommand;
        type Error = AccountError;

        fn initial(_id: &Self::Id) -> Self::State {
            AccountState {
                open: false,
                balance: 0,
            }
        }

        fn apply(state: &mut Self::State, event: &Self::Event) {
            match event {
                AccountEvent::Opened { .. } => {
                    state.open = true;
                    state.balance = 0;
                }
                AccountEvent::Deposited { amount } => state.balance += amount,
                AccountEvent::Withdrawn { amount } => state.balance -= amount,
            }
        }

        fn decide(
            state: &Self::State,
            command: &Self::Command,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            match command {
                AccountCommand::Open { .. } if state.open => Err(AccountError::AlreadyOpen),
                AccountCommand::Open { owner } => {
                    Ok(vec![AccountEvent::Opened { owner: owner.clone() }])
                }
                AccountCommand::Deposit { .. }
                | AccountCommand::Withdraw { .. }
                | AccountCommand::CheckBalance
                    if !state.open =>
                {
                    Err(AccountError::NotOpen)
                }
                AccountCommand::Deposit { amount } => {
                    Ok(vec![AccountEvent::Deposited { amount: *amount }])
                }
                AccountCommand::Withdraw { amount } if *amount > state.balance => {
                    Err(AccountError::InsufficientFunds)
                }
                AccountCommand::Withdraw { amount } => {
                    Ok(vec![AccountEvent::Withdrawn { amount: *amount }])
                }
                AccountCommand::CheckBalance => Ok(vec![]),
            }
        }
    }

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
    }    fn is_protocol_violation(action: &WriteAction<AccountEvent, AccountError>) -> bool {
        matches!(
            action,
            WriteAction::Done(WriteOutcome::Failed(StoreError::Other(_)))
        )
    }

    // -- transitions -----------------------------------------------------

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
    fn appended_ends_in_committed() {
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
        let WriteAction::Done(WriteOutcome::Committed(events)) = action else {
            panic!("expected a committed outcome")
        };
        assert_eq!(events, committed);
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
            WriteAction::Done(WriteOutcome::Failed(StoreError::Conflict { current }))
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

    // -- protocol violations ----------------------------------------------

    #[test]
    fn driving_a_finished_machine_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::CheckBalance);
        m.start();
        m.handle(WriteInput::Loaded { events: vec![] }); // Done(Noop)
        assert!(is_protocol_violation(&m.handle(WriteInput::Appended {
            committed: vec![]
        })));
        assert!(is_protocol_violation(&m.start()));
    }

    #[test]
    fn loaded_outside_loading_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        m.handle(WriteInput::Loaded { events: vec![] }); // → Appending
        assert!(is_protocol_violation(&m.handle(WriteInput::Loaded {
            events: vec![]
        })));
    }

    #[test]
    fn appended_outside_appending_is_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        assert!(is_protocol_violation(&m.handle(WriteInput::Appended {
            committed: vec![]
        })));
    }

    #[test]
    fn non_contiguous_events_are_a_protocol_violation() {
        let mut m = machine(AccountCommand::Deposit { amount: 5 });
        m.start();
        let action = m.handle(WriteInput::Loaded {
            events: vec![env(2, AccountEvent::Deposited { amount: 1 })], // gap at v1
        });
        assert!(is_protocol_violation(&action));
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
        assert!(is_protocol_violation(&action));
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
        assert!(is_protocol_violation(&action));
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
            WriteAction::Done(WriteOutcome::Committed(_))
        ));
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

    proptest! {
        /// The machine never panics on any input sequence, and once it
        /// is done it stays done: every action after the first `Done` is
        /// a protocol-violation `Done`.
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
    }
}
