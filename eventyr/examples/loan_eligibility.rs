//! # Loan approval — a dynamic consistency boundary with a narrowed
//! validation query, §14 / 0.7.1.
//!
//! A loan is approved against the applicant's financial history, but
//! only *some* later events should force a retry. The fold reads every
//! deposit and withdrawal tagged with the account (the query); the
//! append is guarded only by withdrawals and the approval itself (the
//! validation query) — a deposit arriving during the decision changes
//! the balance but can only *help* the applicant, so it must not
//! conflict (disintegrate's refinement).
//!
//! The boundary is the query, chosen per decision: no aggregate owns
//! "eligibility", and no stream guard is involved.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example loan_eligibility
//! ```

#![allow(dead_code)] // A demo: the point is the focused feature.

use std::fmt;

use eventyr::prelude::*;
use eventyr::store::prelude::*;

/// The account under review.
#[derive(Clone, Debug)]
struct Applicant(u64);
impl fmt::Display for Applicant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The account's money history, plus the loan outcome.
// `crate = "eventyr"`: this example lives in the `eventyr` package
// itself, so the derive cannot resolve the target from the manifest —
// a user's crate would not need the attribute.
#[derive(Clone, Debug, PartialEq, EventName, serde::Serialize, serde::Deserialize)]
#[eventyr(crate = "eventyr")]
enum MoneyEvent {
    Deposited { account: u64, amount: u64 },
    Withdrawn { account: u64, amount: u64 },
    Approved { account: u64, amount: u64 },
}

/// Tags are a pure function of the payload (roadmap 0.7.1): a query is correct
/// on any history, including events written before the tag existed.
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

/// Approve `amount` for the applicant when the account is at least half
/// funded by deposits.
struct ApproveLoan {
    applicant: Applicant,
    amount: u64,
}

/// What the fold needs: the account's flows and its prior approvals.
#[derive(Default)]
struct LedgerFacts {
    deposits: u64,
    withdrawals: u64,
    approved: bool,
}

/// The rejection.
#[derive(Debug, PartialEq)]
enum Rejected {
    /// The account already holds an approved loan.
    AlreadyApproved,
    /// Deposits cover less than half the requested amount.
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

    /// The fold's selection: everything that says something about this
    /// account's money.
    fn query(&self) -> Query {
        Query::of(
            QueryItem::any()
                .types(["Deposited", "Withdrawn", "Approved"])
                .tags([Tag::of("account", &self.applicant)]),
        )
    }

    /// What a concurrent append must match to retry the decision.
    /// Narrower than the fold: a deposit only *raises* the balance, so
    /// it can never invalidate an approval — concurrent deposits land
    /// without a retry, concurrent withdrawals do not.
    fn validation(&self) -> Query {
        Query::of(
            QueryItem::any()
                .types(["Withdrawn", "Approved"])
                .tags([Tag::of("account", &self.applicant)]),
        )
    }

    fn initial(&self) -> Self::State {
        LedgerFacts::default()
    }

    fn apply(&self, state: &mut Self::State, event: &Self::Event) {
        match event {
            MoneyEvent::Deposited { amount, .. } => state.deposits += amount,
            MoneyEvent::Withdrawn { amount, .. } => state.withdrawals += amount,
            MoneyEvent::Approved { .. } => state.approved = true,
        }
    }

    fn decide(&self, state: &Self::State) -> BoundaryDecision<Self::Event, Self::Error> {
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

fn main() {
    // Queries match on the decoded events, so any `QueryAppend` store
    // runs this; the in-memory one needs no runtime to drive.
    let store = InMemoryStore::<MoneyEvent>::new();
    let stream = StreamId::from("account-7");
    let deposit = |amount| NewEvent::new(MoneyEvent::Deposited { account: 7, amount });
    let withdraw = |amount| NewEvent::new(MoneyEvent::Withdrawn { account: 7, amount });
    futures::executor::block_on(store.append(
        &stream,
        ExpectedVersion::Empty,
        vec![deposit(200), withdraw(50), deposit(100)],
    ))
    .expect("seed the history");

    let approve = |amount| {
        let mut machine = BoundaryMachine::new(
            ApproveLoan {
                applicant: Applicant(7),
                amount,
            },
            RetryPolicy::default(),
        );
        // The blocking twin of `drive_boundary` — no runtime at all.
        drive_boundary_blocking(&mut machine, &store)
    };

    // balance 250; half of it (125) covers a 400 loan.
    match approve(400) {
        BoundaryOutcome::Committed { committed } => {
            let approved: u64 = committed
                .iter()
                .flat_map(|stream| stream.events.iter())
                .filter_map(|envelope| match envelope.event {
                    MoneyEvent::Approved { amount, .. } => Some(amount),
                    _ => None,
                })
                .sum();
            println!("approved {approved}");
        }
        other => panic!("a fundable loan commits: {other:?}"),
    }
    assert!(
        matches!(
            approve(100),
            BoundaryOutcome::Rejected(Rejected::AlreadyApproved)
        ),
        "a second loan is refused"
    );

    let mut machine = BoundaryMachine::new(
        ApproveLoan {
            applicant: Applicant(9),
            amount: 400,
        },
        RetryPolicy::default(),
    );
    assert!(
        matches!(
            drive_boundary_blocking(&mut machine, &store),
            BoundaryOutcome::Rejected(Rejected::Underfunded)
        ),
        "an unknown account has no deposits to stand on"
    );
    println!("a second loan is refused; an unfunded applicant is refused");
}
