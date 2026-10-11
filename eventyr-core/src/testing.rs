//! Test support: the scripted drivers, the [`Scenario`] domain-test
//! builder, and the reusable fixtures — the [`account`] aggregate and
//! the [`enrollment`] DCB example.
//!
//! Everything here is pure — no store, no async, no I/O — so it lives in
//! the core and serves every crate's test suite.

use alloc::string::String;
use alloc::vec::Vec;

use crate::aggregate::Aggregate;
use crate::batch::{BatchAction, BatchInput, BatchMachine, Decide};
use crate::boundary::{BoundaryAction, BoundaryInput, BoundaryMachine, Decision};
use crate::subscription_machine::{SubscriptionAction, SubscriptionInput, SubscriptionMachine};
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
    ///
    /// # Panics
    ///
    /// When the outcome mismatched: the decision was a rejection while
    /// events were expected, or the committed events differ from
    /// `expected`. The message names the scenario, the expected events,
    /// and what was actually decided.
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
    ///
    /// # Panics
    ///
    /// When the outcome mismatched: the decision was a success while a
    /// rejection was expected, or the rejection differs from `expected`.
    /// The message names the scenario, the expected rejection, and what
    /// was actually decided.
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
    ///
    /// # Panics
    ///
    /// When the outcome mismatched: the decision was a success while a
    /// rejection was expected, or the rejection did not satisfy `pred`.
    /// The message names the scenario and the rejection that failed it.
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
    ///
    /// # Panics
    ///
    /// When the decided `Result` differs from `expected` — wrong events,
    /// or a rejection where events were expected (or the reverse). The
    /// message names the scenario, the expected result, and the actual.
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

/// Reusable fixture: course enrollment — the canonical DCB example.
///
/// A student may enroll in a course while the course has seats and the
/// student holds fewer than [`MAX_COURSES`](enrollment::MAX_COURSES)
/// courses: one invariant per tag, two tags per decision, and no
/// aggregate owning both.
///
/// Also re-exported as
/// [`boundary::enrollment`](crate::boundary::enrollment) for its
/// documented path.
pub mod enrollment {
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::fmt;

    use crate::boundary::{BoundaryDecision, Decision, Query, QueryItem, Tag, Tagged};
    use crate::event_name::EventName;
    use crate::vocabulary::StreamId;

    /// How many courses one student may hold.
    pub const MAX_COURSES: usize = 2;

    /// The enrollment events.
    #[derive(Clone, PartialEq, Eq, Debug)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub enum EnrollmentEvent {
        /// A course opened with `seats` places.
        CourseDefined {
            /// The course.
            course: String,
            /// Its capacity.
            seats: u32,
        },
        /// A student enrolled in a course.
        Enrolled {
            /// The course.
            course: String,
            /// The student.
            student: String,
        },
        /// A student's address changed — selected by nothing an
        /// enrollment decision reads.
        AddressChanged {
            /// The student.
            student: String,
        },
    }

    impl EventName for EnrollmentEvent {
        fn event_name(&self) -> &'static str {
            match self {
                Self::CourseDefined { .. } => "CourseDefined",
                Self::Enrolled { .. } => "Enrolled",
                Self::AddressChanged { .. } => "AddressChanged",
            }
        }
    }

    impl Tagged for EnrollmentEvent {
        fn tags(&self) -> Vec<Tag> {
            match self {
                Self::CourseDefined { course, .. } => vec![Tag::of("course", course)],
                Self::Enrolled { course, student } => {
                    vec![Tag::of("course", course), Tag::of("student", student)]
                }
                Self::AddressChanged { student } => vec![Tag::of("student", student)],
            }
        }
    }

    /// Why an enrollment was refused.
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum EnrollmentError {
        /// The course was never defined.
        NoSuchCourse,
        /// The course has no seats left.
        CourseFull,
        /// The student already holds the maximum number of courses.
        StudentAtLimit,
        /// The student is already enrolled in this course.
        AlreadyEnrolled,
    }

    impl fmt::Display for EnrollmentError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Self::NoSuchCourse => "no such course",
                Self::CourseFull => "course is full",
                Self::StudentAtLimit => "student is at the course limit",
                Self::AlreadyEnrolled => "already enrolled",
            })
        }
    }

    /// What the decision folds: the course's capacity and enrolment
    /// count, and the student's course count.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct EnrollmentState {
        /// The course's capacity, once defined.
        pub seats: Option<u32>,
        /// Students enrolled in the course.
        pub enrolled: u32,
        /// Courses the student holds.
        pub student_courses: usize,
        /// Whether this student already holds this course.
        pub already: bool,
    }

    /// The command and its decision: enroll `student` in `course`.
    #[derive(Clone, Debug)]
    pub struct Enroll {
        /// The course.
        pub course: String,
        /// The student.
        pub student: String,
    }

    impl Enroll {
        /// The decision for `student` in `course`.
        pub fn new(course: &str, student: &str) -> Self {
            Self {
                course: String::from(course),
                student: String::from(student),
            }
        }

        /// The stream enrollment events land in: the course's.
        pub fn stream(&self) -> StreamId {
            StreamId::from(alloc::format!("course-{}", self.course))
        }
    }

    impl Decision for Enroll {
        type Event = EnrollmentEvent;
        type State = EnrollmentState;
        type Error = EnrollmentError;

        fn query(&self) -> Query {
            Query::of(
                QueryItem::any()
                    .types(["CourseDefined", "Enrolled"])
                    .tags([Tag::of("course", &self.course)]),
            )
            .or(QueryItem::any()
                .types(["Enrolled"])
                .tags([Tag::of("student", &self.student)]))
        }

        fn initial(&self) -> EnrollmentState {
            EnrollmentState::default()
        }

        fn apply(&self, state: &mut EnrollmentState, event: &EnrollmentEvent) {
            match event {
                EnrollmentEvent::CourseDefined { seats, .. } => state.seats = Some(*seats),
                EnrollmentEvent::Enrolled { course, student } => {
                    let this_course = *course == self.course;
                    let this_student = *student == self.student;
                    if this_course {
                        state.enrolled += 1;
                    }
                    if this_student {
                        state.student_courses += 1;
                    }
                    if this_course && this_student {
                        state.already = true;
                    }
                }
                EnrollmentEvent::AddressChanged { .. } => {}
            }
        }

        fn decide(
            &self,
            state: &EnrollmentState,
        ) -> BoundaryDecision<EnrollmentEvent, EnrollmentError> {
            let Some(seats) = state.seats else {
                return BoundaryDecision::reject(EnrollmentError::NoSuchCourse);
            };
            if state.already {
                return BoundaryDecision::reject(EnrollmentError::AlreadyEnrolled);
            }
            if state.enrolled >= seats {
                return BoundaryDecision::reject(EnrollmentError::CourseFull);
            }
            if state.student_courses >= MAX_COURSES {
                return BoundaryDecision::reject(EnrollmentError::StudentAtLimit);
            }
            BoundaryDecision::to(
                self.stream(),
                vec![EnrollmentEvent::Enrolled {
                    course: self.course.clone(),
                    student: self.student.clone(),
                }],
            )
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
