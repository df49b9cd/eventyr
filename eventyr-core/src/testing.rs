//! Test support: the scripted drivers, the [`Scenario`] domain-test
//! builder, and a canonical test aggregate.
//!
//! Everything here is pure — no store, no async, no I/O — so it lives in
//! the core and serves every crate's test suite.

use alloc::string::String;
use alloc::vec::Vec;

use crate::aggregate::Aggregate;
use crate::batch::{BatchAction, BatchInput, BatchMachine, Decide};
use crate::boundary::{BoundaryAction, BoundaryInput, BoundaryMachine, Decision};
use crate::subscription::{SubscriptionAction, SubscriptionInput, SubscriptionMachine};
use crate::write::{WriteAction, WriteInput, WriteMachine};

mod sealed {
    pub trait Sealed {}
}

/// A driver-facing machine: a first action from [`start`](Machine::start),
/// then one action per [`handle`](Machine::handle)d input — the shape
/// [`scripted`] drives.
///
/// Implemented by [`WriteMachine`] (snapshots on or off),
/// [`BatchMachine`], [`BoundaryMachine`], and [`SubscriptionMachine`];
/// sealed, since the scripted driver's guarantees are about these
/// machines' protocols. (The [`SagaMachine`](crate::saga::SagaMachine)
/// starts from an event, not from nothing, so it drives itself.)
pub trait Machine: sealed::Sealed {
    /// What the driver reports back.
    type Input;
    /// What the machine asks the driver to do.
    type Action;

    /// The first action.
    fn start(&mut self) -> Self::Action;

    /// Consume a driver result and emit the next action.
    fn handle(&mut self, input: Self::Input) -> Self::Action;
}

impl<A: Aggregate, S> sealed::Sealed for WriteMachine<A, S> {}

impl<A: Aggregate, S> Machine for WriteMachine<A, S> {
    type Input = WriteInput<A::Event, S>;
    type Action = WriteAction<A::Event, A::Error, S>;

    fn start(&mut self) -> Self::Action {
        WriteMachine::start(self)
    }

    fn handle(&mut self, input: Self::Input) -> Self::Action {
        WriteMachine::handle(self, input)
    }
}

impl<E, Err, D: Decide<E, Err>> sealed::Sealed for BatchMachine<E, Err, D> {}

impl<E, Err, D: Decide<E, Err>> Machine for BatchMachine<E, Err, D> {
    type Input = BatchInput<E>;
    type Action = BatchAction<E, Err>;

    fn start(&mut self) -> Self::Action {
        BatchMachine::start(self)
    }

    fn handle(&mut self, input: Self::Input) -> Self::Action {
        BatchMachine::handle(self, input)
    }
}

impl<D: Decision> sealed::Sealed for BoundaryMachine<D> {}

impl<D: Decision> Machine for BoundaryMachine<D> {
    type Input = BoundaryInput<D::Event>;
    type Action = BoundaryAction<D::Event, D::Error>;

    fn start(&mut self) -> Self::Action {
        BoundaryMachine::start(self)
    }

    fn handle(&mut self, input: Self::Input) -> Self::Action {
        BoundaryMachine::handle(self, input)
    }
}

impl<E: Clone> sealed::Sealed for SubscriptionMachine<E> {}

impl<E: Clone> Machine for SubscriptionMachine<E> {
    type Input = SubscriptionInput<E>;
    type Action = SubscriptionAction<E>;

    fn start(&mut self) -> Self::Action {
        SubscriptionMachine::start(self)
    }

    fn handle(&mut self, input: Self::Input) -> Self::Action {
        SubscriptionMachine::handle(self, input)
    }
}

/// Drives `machine` through `start()` and every input in `script`,
/// recording each action in order.
///
/// The script is fed verbatim: inputs after the machine finished are
/// answered with protocol-violation outcomes, exactly as they would be
/// at runtime — which is itself worth asserting on. A subscription's
/// [`Slept`](SubscriptionInput::Slept) is fed instantly — the machine
/// decides durations as data, so the scripted driver needs no clock.
pub fn scripted<M: Machine>(
    machine: &mut M,
    script: impl IntoIterator<Item = M::Input>,
) -> Vec<M::Action> {
    let mut actions = Vec::new();
    actions.push(machine.start());
    for input in script {
        actions.push(machine.handle(input));
    }
    actions
}

/// [`scripted`] for a [`WriteMachine`], snapshots on or off.
pub fn drive_scripted<A: Aggregate, S>(
    machine: &mut WriteMachine<A, S>,
    script: impl IntoIterator<Item = WriteInput<A::Event, S>>,
) -> Vec<WriteAction<A::Event, A::Error, S>> {
    scripted(machine, script)
}

/// [`scripted`] for a [`SubscriptionMachine`]. A perennial machine
/// stops only at a scripted `Done`-reaching input (`Failed`,
/// `Shutdown`, or a catch-up policy).
pub fn projector_scripted<E: Clone>(
    machine: &mut SubscriptionMachine<E>,
    script: impl IntoIterator<Item = SubscriptionInput<E>>,
) -> Vec<SubscriptionAction<E>> {
    scripted(machine, script)
}

/// [`scripted`] for a [`BatchMachine`].
pub fn batch_scripted<E, Err, D: Decide<E, Err>>(
    machine: &mut BatchMachine<E, Err, D>,
    script: impl IntoIterator<Item = BatchInput<E>>,
) -> Vec<BatchAction<E, Err>> {
    scripted(machine, script)
}

/// [`scripted`] for a [`BoundaryMachine`].
pub fn boundary_scripted<D: Decision>(
    machine: &mut BoundaryMachine<D>,
    script: impl IntoIterator<Item = BoundaryInput<D::Event>>,
) -> Vec<BoundaryAction<D::Event, D::Error>> {
    scripted(machine, script)
}

/// A domain test: `given` a history, `when` a command, `then` an
/// expectation.
///
/// `Scenario` is a tower of two phases with one aggregate type parameter
/// throughout — the same discipline as [`drive_scripted`]: one generic
/// (`A: Aggregate`), no mocks, no async, no store. Outcomes are data;
/// assertions are plain `assert_eq!`/panics inside the `then_*` methods of
/// the second phase ([`Outcome`]), each naming the failing scenario.
///
/// ```
/// use eventyr_core::testing::{Scenario, account::*};
///
/// // Given an opened account with 50 on it, withdrawing 30 emits Withdrawn.
/// Scenario::<Account>::given(
///         &AccountId(1),
///         [AccountEvent::Opened { owner: "me".into() },
///          AccountEvent::Deposited { amount: 50 }],
///     )
///     .when(&AccountCommand::Withdraw { amount: 30 })
///     .then_events(&[AccountEvent::Withdrawn { amount: 30 }])
///     .with_state(|s| assert_eq!(s.balance, 50)); // state is pre-command
///
/// // Over-withdrawing is a domain rejection.
/// Scenario::<Account>::given(
///         &AccountId(1),
///         [AccountEvent::Opened { owner: "me".into() }],
///     )
///     .when(&AccountCommand::Withdraw { amount: 100 })
///     .then_error(&AccountError::InsufficientFunds);
/// ```
pub struct Scenario<A: Aggregate> {
    state: A::State,
    label: String,
}

impl<A: Aggregate> Scenario<A> {
    /// Entry point. `id` seeds [`Aggregate::initial`]; every event in
    /// `history` is folded with [`Aggregate::apply`], in order, before the
    /// command is decided.
    ///
    /// Starting from the true initial state (`given(&id, [])`) is the
    /// common case; an empty-history convenience is deliberately absent —
    /// writing `[]` costs nothing and keeps construction to one method.
    pub fn given(id: &A::Id, history: impl IntoIterator<Item = A::Event>) -> Self {
        let mut state = A::initial(id);
        for event in history {
            A::apply(&mut state, &event);
        }
        Scenario {
            state,
            label: alloc::format!("{}/{id}", A::NAME),
        }
    }

    /// Decide `command` against the folded history. Pure; the pre-command
    /// state survives into the [`Outcome`] for
    /// [`with_state`](Outcome::with_state).
    pub fn when(self, command: &A::Command) -> Outcome<A> {
        let result = A::decide(&self.state, command);
        Outcome {
            label: self.label,
            state: self.state,
            result,
        }
    }
}

/// The second phase of a [`Scenario`]: the decided outcome of the command,
/// plus the pre-command state it was decided against.
///
/// Assertion helpers consume the outcome, panic with a message naming the
/// scenario on mismatch, and return `self` so several `then_*` calls can
/// chain.
pub struct Outcome<A: Aggregate> {
    label: String,
    /// Pre-command folded state. Naming: this is the state the command was
    /// decided *against*, never a post-decide fold.
    state: A::State,
    result: Result<Vec<A::Event>, A::Error>,
}

impl<A: Aggregate> Outcome<A> {
    /// Expect success with exactly these events, in order.
    pub fn then_events(self, expected: &[A::Event]) -> Self
    where
        A::Event: PartialEq + core::fmt::Debug,
    {
        match &self.result {
            Ok(events) => assert_eq!(
                events.as_slice(),
                expected,
                "[{}] expected events {expected:?}, got {events:?}",
                self.label,
            ),
            Err(err) => panic!(
                "[{}] expected events {expected:?}, got rejection {err}",
                self.label,
            ),
        }
        self
    }

    /// Expect success with no events (the `Noop` outcome,
    /// e.g. `AccountCommand::CheckBalance`).
    pub fn then_none(self) -> Self
    where
        A::Event: PartialEq + core::fmt::Debug,
    {
        self.then_events(&[])
    }

    /// Expect a domain rejection with exactly this reason.
    ///
    /// `A::Error: PartialEq` first appears here — aggregates whose error
    /// type lacks `PartialEq` can still assert rejections with
    /// [`then_error_matching`](Outcome::then_error_matching).
    pub fn then_error(self, expected: &A::Error) -> Self
    where
        A::Event: core::fmt::Debug,
        A::Error: PartialEq + core::fmt::Debug,
    {
        match &self.result {
            Err(err) => assert_eq!(
                err, expected,
                "[{}] expected rejection {expected:?}, got {err:?}",
                self.label,
            ),
            Ok(events) => panic!(
                "[{}] expected rejection {expected}, got events {events:?}",
                self.label,
            ),
        }
        self
    }

    /// Expect a domain rejection matching `pred` — for aggregates whose
    /// error type carries no `PartialEq`.
    pub fn then_error_matching(self, pred: impl FnOnce(&A::Error) -> bool) -> Self
    where
        A::Event: core::fmt::Debug,
    {
        match &self.result {
            Err(err) => assert!(
                pred(err),
                "[{}] rejection {err:?} did not match the predicate",
                self.label,
            ),
            Ok(events) => panic!(
                "[{}] expected a rejection, got events {events:?}",
                self.label,
            ),
        }
        self
    }

    /// Expect the exact `Result`, when the shape itself is the point.
    pub fn then_result(self, expected: &Result<Vec<A::Event>, A::Error>) -> Self
    where
        A::Event: PartialEq + core::fmt::Debug,
        A::Error: PartialEq + core::fmt::Debug,
    {
        assert_eq!(
            &self.result, expected,
            "[{}] expected result {expected:?}, got {:?}",
            self.label, self.result,
        );
        self
    }

    /// Pull the pre-command folded state for a bespoke assertion. Stays an
    /// `Outcome` so more `then_*` calls can follow.
    pub fn with_state(self, inspect: impl FnOnce(&A::State)) -> Self {
        inspect(&self.state);
        self
    }
}

/// A canonical bank-account aggregate for tests, doctests, and examples.
///
/// One shared definition so every crate's test suite exercises the same
/// domain: open, deposit, withdraw, and a no-op balance check (which
/// exercises the `Noop` outcome). Doctests stay self-contained — they
/// must read standalone.
pub mod account {
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::fmt;

    use crate::aggregate::Aggregate;
    use crate::event_name::EventName;

    /// The account instance's identifier.
    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct AccountId(
        /// The raw id value.
        pub u64,
    );

    impl fmt::Display for AccountId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// The account's domain events.
    #[derive(Clone, PartialEq, Debug)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub enum AccountEvent {
        /// The account was opened by `owner`.
        Opened {
            /// The owner's name.
            owner: String,
        },
        /// Funds arrived.
        Deposited {
            /// How much.
            amount: u64,
        },
        /// Funds left.
        Withdrawn {
            /// How much.
            amount: u64,
        },
    }

    /// The account's commands.
    #[derive(Clone, Debug)]
    pub enum AccountCommand {
        /// Open the account.
        Open {
            /// The owner's name.
            owner: String,
        },
        /// Deposit funds.
        Deposit {
            /// How much.
            amount: u64,
        },
        /// Withdraw funds.
        Withdraw {
            /// How much.
            amount: u64,
        },
        /// Check the balance: decides no events (the `Noop` outcome).
        CheckBalance,
    }

    /// The account's domain rejections.
    #[derive(Debug, PartialEq)]
    pub enum AccountError {
        /// The account is already open.
        AlreadyOpen,
        /// The account is not open yet.
        NotOpen,
        /// The balance is too low.
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

    /// The account's folded state.
    #[derive(Clone, Debug)]
    pub struct AccountState {
        /// Whether the account exists.
        pub open: bool,
        /// The current balance.
        pub balance: u64,
    }

    /// The account aggregate: a unit struct, as aggregates usually are.
    pub struct Account;

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
                AccountCommand::Open { owner } => Ok(vec![AccountEvent::Opened {
                    owner: owner.clone(),
                }]),
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

    impl EventName for AccountEvent {
        fn event_name(&self) -> &'static str {
            match self {
                Self::Opened { .. } => "Opened",
                Self::Deposited { .. } => "Deposited",
                Self::Withdrawn { .. } => "Withdrawn",
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::account::*;
    use super::*;

    #[test]
    fn happy_path_emits_the_expected_events() {
        Scenario::<Account>::given(&AccountId(1), [AccountEvent::Opened { owner: "me".into() }])
            .when(&AccountCommand::Deposit { amount: 50 })
            .then_events(&[AccountEvent::Deposited { amount: 50 }])
            .with_state(|s| {
                assert!(s.open);
                assert_eq!(s.balance, 0); // state is pre-command
            });
    }

    #[test]
    fn domain_rejection_matches_the_reason() {
        Scenario::<Account>::given(
            &AccountId(1),
            [
                AccountEvent::Opened { owner: "me".into() },
                AccountEvent::Deposited { amount: 50 },
            ],
        )
        .when(&AccountCommand::Withdraw { amount: 100 })
        .then_error(&AccountError::InsufficientFunds)
        .then_error_matching(|e| matches!(e, AccountError::InsufficientFunds));
    }

    #[test]
    fn empty_given_starts_from_initial_state() {
        Scenario::<Account>::given(&AccountId(7), [])
            .when(&AccountCommand::Open {
                owner: "ada".into(),
            })
            .then_events(&[AccountEvent::Opened {
                owner: "ada".into(),
            }])
            .with_state(|s| assert!(!s.open));

        // The noop outcome: decide produces an empty event list.
        Scenario::<Account>::given(
            &AccountId(7),
            [AccountEvent::Opened {
                owner: "ada".into(),
            }],
        )
        .when(&AccountCommand::CheckBalance)
        .then_none()
        .then_result(&Ok(vec![]));
    }

    #[test]
    #[should_panic(expected = "expected rejection NotOpen, got InsufficientFunds")]
    fn wrong_expectation_names_the_scenario_and_the_diff() {
        Scenario::<Account>::given(
            &AccountId(1),
            [
                AccountEvent::Opened { owner: "me".into() },
                AccountEvent::Deposited { amount: 50 },
            ],
        )
        .when(&AccountCommand::Withdraw { amount: 100 })
        .then_error(&AccountError::NotOpen);
    }

    #[test]
    #[should_panic(
        expected = "expected events [Withdrawn { amount: 100 }], got rejection insufficient funds"
    )]
    fn events_expectation_surfaces_a_rejection_readably() {
        Scenario::<Account>::given(
            &AccountId(1),
            [
                AccountEvent::Opened { owner: "me".into() },
                AccountEvent::Deposited { amount: 50 },
            ],
        )
        .when(&AccountCommand::Withdraw { amount: 100 })
        .then_events(&[AccountEvent::Withdrawn { amount: 100 }]);
    }
}
