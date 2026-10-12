//! The loan_eligibility example's mirroring suite (DESIGN §0.5.4): the
//! seeded decisions, plus the narrowed validation query the example
//! only describes — a deposit landing between the read and the append
//! commits without a retry, a withdrawal retries and re-decides. The
//! machine is stepped by hand to the read, a competing write goes in,
//! and the async driver runs the rest.
//!
//! The scenario mirrors `examples/loan_eligibility.rs`; keep the two in
//! step.

use std::fmt;

use eventyr::prelude::*;
use eventyr::store::prelude::*;

/// The applicant/a wallet id.
#[derive(Clone, Debug)]
struct Applicant(u64);
impl fmt::Display for Applicant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, EventName)]
#[eventyr(crate = "eventyr")]
enum MoneyEvent {
    Deposited { account: u64, amount: u64 },
    Withdrawn { account: u64, amount: u64 },
    Approved { account: u64, amount: u64 },
}

impl Tagged for MoneyEvent {
    fn tags(&self) -> Vec<Tag> {
        let (account, kind) = match self {
            MoneyEvent::Deposited { account, .. } => (account, "deposit"),
            MoneyEvent::Withdrawn { account, .. } => (account, "withdrawal"),
            MoneyEvent::Approved { account, .. } => (account, "loan"),
        };
        vec![Tag::of("account", account), Tag::of("kind", kind)]
    }
}

struct ApproveLoan {
    applicant: Applicant,
    amount: u64,
}

#[derive(Default)]
struct LedgerFacts {
    deposits: u64,
    withdrawals: u64,
    approved: bool,
}

#[derive(Debug, PartialEq)]
enum Rejected {
    AlreadyApproved,
    Underfunded,
}
impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyApproved => "a loan is already approved",
            Self::Underfunded => "deposits cover less than half the amount",
        })
    }
}

impl Decision for ApproveLoan {
    type Event = MoneyEvent;
    type State = LedgerFacts;
    type Error = Rejected;

    fn query(&self) -> Query {
        Query::of(
            QueryItem::any()
                .types(["Deposited", "Withdrawn", "Approved"])
                .tags([Tag::of("account", &self.applicant)]),
        )
    }

    /// The *narrowed* validation: a deposit can only help; only a
    /// withdrawal or a prior approval can retry the decision. A
    /// deposit arriving mid-interaction is folded but cannot conflict.
    fn validation(&self) -> Query {
        Query::of(
            QueryItem::any()
                .types(["Withdrawn", "Approved"])
                .tags([Tag::of("account", &self.applicant)]),
        )
    }

    fn initial(&self) -> LedgerFacts {
        LedgerFacts::default()
    }

    fn apply(&self, state: &mut LedgerFacts, event: &MoneyEvent) {
        match event {
            MoneyEvent::Deposited { amount, .. } => state.deposits += amount,
            MoneyEvent::Withdrawn { amount, .. } => state.withdrawals += amount,
            MoneyEvent::Approved { .. } => state.approved = true,
        }
    }

    fn decide(&self, state: &LedgerFacts) -> BoundaryDecision<MoneyEvent, Rejected> {
        if state.approved {
            return BoundaryDecision::reject(Rejected::AlreadyApproved);
        }
        if state.deposits.saturating_sub(state.withdrawals) * 2 < self.amount {
            return BoundaryDecision::reject(Rejected::Underfunded);
        }
        BoundaryDecision::to(
            StreamId::from(format!("loan-{}", self.applicant)),
            [MoneyEvent::Approved {
                account: self.applicant.0,
                amount: self.amount,
            }],
        )
    }
}

fn approve(
    store: &InMemoryStore<MoneyEvent>,
    applicant: u64,
    amount: u64,
) -> BoundaryOutcome<MoneyEvent, Rejected> {
    let mut machine = BoundaryMachine::new(
        ApproveLoan {
            applicant: Applicant(applicant),
            amount,
        },
        RetryPolicy::default(),
    );
    drive_boundary_blocking(&mut machine, store)
}

#[test]
fn the_seed_story_decides_as_advertised() {
    let store = InMemoryStore::new();
    let account = StreamId::from("account-7");
    let deposit = |amount| NewEvent::new(MoneyEvent::Deposited { account: 7, amount });
    let withdraw = |amount| NewEvent::new(MoneyEvent::Withdrawn { account: 7, amount });
    futures::executor::block_on(store.append(
        &account,
        ExpectedVersion::Empty,
        vec![deposit(200), withdraw(50), deposit(100)],
    ))
    .expect("seed");

    // balance 250; under half (125) covers a 400 loan → committed.
    let first = approve(&store, 7, 400);
    assert!(matches!(first, BoundaryOutcome::Committed { .. }));
    // A second loan is refused: the query saw the prior approval.
    let second = approve(&store, 7, 100);
    assert!(matches!(
        second,
        BoundaryOutcome::Rejected(Rejected::AlreadyApproved)
    ));
    // An unknown account has no deposits: underfunded.
    let nobody = approve(&store, 9, 400);
    assert!(matches!(
        nobody,
        BoundaryOutcome::Rejected(Rejected::Underfunded)
    ));
}

/// One driver step, written out: perform `action` against the store and
/// hand the result back to the machine. `drive_boundary` is this in a
/// loop; stepping by hand lets a test put a competing write between
/// the decision and its guarded append.
async fn step(
    machine: &mut BoundaryMachine<ApproveLoan>,
    store: &InMemoryStore<MoneyEvent>,
    action: BoundaryAction<MoneyEvent, Rejected>,
) -> BoundaryAction<MoneyEvent, Rejected> {
    match action {
        BoundaryAction::Read { query, after } => {
            let events = futures::TryStreamExt::try_collect(store.read(&query, after))
                .await
                .expect("the in-memory store reads");
            machine.handle(BoundaryInput::Read { events })
        }
        BoundaryAction::Append { appends, condition } => {
            match store.append_if(appends, condition).await {
                Ok(committed) => machine.handle(BoundaryInput::Appended { committed }),
                Err(error) => machine.handle(BoundaryInput::from(error)),
            }
        }
        done @ BoundaryAction::Done(_) => done,
    }
}

fn seeded(deposit: u64) -> InMemoryStore<MoneyEvent> {
    let store = InMemoryStore::new();
    futures::executor::block_on(store.append(
        &StreamId::from("account-7"),
        ExpectedVersion::Empty,
        vec![NewEvent::new(MoneyEvent::Deposited {
            account: 7,
            amount: deposit,
        })],
    ))
    .expect("seed");
    store
}

fn loan_of(amount: u64) -> BoundaryMachine<ApproveLoan> {
    BoundaryMachine::new(
        ApproveLoan {
            applicant: Applicant(7),
            amount,
        },
        RetryPolicy::default(),
    )
}

#[tokio::test]
async fn a_deposit_between_decide_and_append_does_not_retry() {
    let store = seeded(200);
    let mut machine = loan_of(200);

    // Read and decide: 200 deposited covers a 200 loan.
    let action = machine.start();
    let action = step(&mut machine, &store, action).await;
    assert!(
        matches!(action, BoundaryAction::Append { .. }),
        "the decision appends: {action:?}"
    );

    // A deposit lands between the decision and its append.
    store
        .append(
            &StreamId::from("account-7"),
            ExpectedVersion::Any,
            vec![NewEvent::new(MoneyEvent::Deposited {
                account: 7,
                amount: 1,
            })],
        )
        .await
        .expect("the competing deposit commits");

    // The guard is the validation query — withdrawals and approvals
    // only — so the deposit does not trip it: the append commits on
    // the first try, with no re-read in between.
    let action = step(&mut machine, &store, action).await;
    assert!(
        matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Committed { .. })
        ),
        "a deposit does not retry the decision: {action:?}"
    );
}

#[tokio::test]
async fn a_withdrawal_between_decide_and_append_retries_and_rejects() {
    let store = seeded(200);
    let mut machine = loan_of(200);

    let action = machine.start();
    let action = step(&mut machine, &store, action).await;
    assert!(
        matches!(action, BoundaryAction::Append { .. }),
        "the decision appends: {action:?}"
    );

    // A withdrawal lands between the decision and its append.
    store
        .append(
            &StreamId::from("account-7"),
            ExpectedVersion::Any,
            vec![NewEvent::new(MoneyEvent::Withdrawn {
                account: 7,
                amount: 150,
            })],
        )
        .await
        .expect("the competing withdrawal commits");

    // A withdrawal is in the validation query: the guarded append
    // conflicts, and the machine re-reads instead of committing.
    let action = step(&mut machine, &store, action).await;
    assert!(
        matches!(action, BoundaryAction::Read { .. }),
        "a withdrawal conflicts, so the machine re-reads: {action:?}"
    );

    // The re-read folds the withdrawal: 50 left, under half the loan.
    let action = step(&mut machine, &store, action).await;
    assert!(
        matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Rejected(Rejected::Underfunded))
        ),
        "the re-decision refuses: {action:?}"
    );
}
