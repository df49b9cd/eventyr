//! # The bank — the Eventyr features in one domain, §0.5.4.
//!
//! One small, complete example: accounts are opened, funded, debited,
//! and wired between; a transfer runs on the 0.4 batch machine; the
//! ledger is a read model rebuilt off the global stream; a renamed event
//! is upcast end to end; and the write side carries metadata through
//! every committed event.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example bank --all-features
//! ```

#![allow(dead_code)] // A demo: the point is the full vocabulary.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use eventyr::envelope::Metadata;
use eventyr::prelude::*;

use eventyr::projection::prelude::{ClosureUpcaster, UpcasterChain};
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::{
    DriverPorts, InMemoryCheckpointStore, Projection, StoreSubscription, SubscriptionOutcome,
    drive_projector,
};

// -- the domain -----------------------------------------------------

/// An account instance's identifier.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct AccountId(u64);
impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Payload structs (the sourcery pattern §8): a V1 deposit was
// `Amount`; the rename to `Deposited` upcasts the old name on read.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Opened {
    owner: String,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Deposited {
    amount: u64,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Withdrawn {
    amount: u64,
}

/// The account's commands.
#[derive(Debug)]
enum AccountCommand {
    /// Open the account.
    Open { owner: String },
    /// Deposit funds.
    Deposit { amount: u64 },
    /// Withdraw funds.
    Withdraw { amount: u64 },
}

/// The account's rejections.
#[derive(Debug, PartialEq)]
enum AccountError {
    /// The account already exists.
    AlreadyOpen,
    /// The account does not exist yet.
    NotOpen,
    /// The balance is too low.
    InsufficientFunds,
}
impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyOpen => "account is already open",
            Self::NotOpen => "account is not open",
            Self::InsufficientFunds => "insufficient funds",
        })
    }
}

/// The account's folded state.
#[derive(Clone, Debug, Default)]
struct AccountState {
    open: bool,
    balance: u64,
}

fn apply(state: &mut AccountState, event: &AccountEvent) {
    match event {
        AccountEvent::Opened(Opened { .. }) => {
            state.open = true;
            state.balance = 0;
        }
        AccountEvent::Deposited(Deposited { amount }) => state.balance += amount,
        AccountEvent::Withdrawn(Withdrawn { amount }) => state.balance -= amount,
    }
}

fn decide(
    state: &AccountState,
    command: &AccountCommand,
) -> Result<Vec<AccountEvent>, AccountError> {
    match command {
        AccountCommand::Open { .. } if state.open => Err(AccountError::AlreadyOpen),
        AccountCommand::Open { owner } => Ok(vec![
            Opened {
                owner: owner.clone(),
            }
            .into(),
        ]),
        AccountCommand::Deposit { .. } | AccountCommand::Withdraw { .. } if !state.open => {
            Err(AccountError::NotOpen)
        }
        AccountCommand::Deposit { amount } => Ok(vec![Deposited { amount: *amount }.into()]),
        AccountCommand::Withdraw { amount } if *amount > state.balance => {
            Err(AccountError::InsufficientFunds)
        }
        AccountCommand::Withdraw { amount } => Ok(vec![Withdrawn { amount: *amount }.into()]),
    }
}

// The event enum is generated from the payload structs (the §8 sourcery
// pattern: one newtype variant per payload, plus `From` and `EventName`).
// Adjust the shape where the generated enum doesn't fit — here rename
// `Amount` to `Deposited` via the payload, which is what the storage
// keeps and the read path upcasts.
#[derive(Aggregate)]
#[eventyr(
    crate = "eventyr",
    id = AccountId,
    state = AccountState,
    error = AccountError,
    command = AccountCommand,
    events(Opened, Deposited, Withdrawn)
)]
struct Account;

// -- the multi-stream transfer (0.4) --------------------------------

/// A transfer between two accounts — the batch-machine decision.
struct Transfer {
    from: u64,
    to: u64,
    amount: u64,
}

impl Decide<AccountEvent, AccountError> for Transfer {
    type Command = Self;

    fn decide(
        &self,
        folded: &BTreeMap<StreamId, Box<dyn std::any::Any + Send>>,
        command: &Self,
    ) -> BatchDecision<AccountEvent, AccountError> {
        let state_of = |id: u64| -> AccountState {
            let stream = StreamId::for_aggregate::<Account>(&AccountId(id));
            folded
                .get(&stream)
                .and_then(|s| s.downcast_ref::<AccountState>())
                .cloned()
                .expect("the machine folds every boundary stream")
        };
        let from = state_of(command.from);
        let to = state_of(command.to);
        if let Err(error) = Account::decide(
            &from,
            &AccountCommand::Withdraw {
                amount: command.amount,
            },
        ) {
            return BatchDecision::reject(error);
        }
        if let Err(error) = Account::decide(
            &to,
            &AccountCommand::Deposit {
                amount: command.amount,
            },
        ) {
            return BatchDecision::reject(error);
        }
        BatchDecision::of(
            vec![
                Withdrawn {
                    amount: command.amount,
                }
                .into(),
                Deposited {
                    amount: command.amount,
                }
                .into(),
            ],
            vec![
                StreamId::for_aggregate::<Account>(&AccountId(command.from)),
                StreamId::for_aggregate::<Account>(&AccountId(command.to)),
            ],
        )
    }
}

// -- the run ---------------------------------------------------------

/// A balance ledger: one `Projection` impl, at-least-once, idempotent.
struct Ledger {
    balances: Arc<std::sync::Mutex<BTreeMap<u64, u64>>>,
}
impl Projection for Ledger {
    type Event = AccountEvent;
    type Error = std::convert::Infallible;
    async fn apply(&mut self, envelope: &EventEnvelope<AccountEvent>) -> Result<(), Self::Error> {
        let id: u64 = envelope
            .stream_id
            .as_str()
            .trim_start_matches("account-")
            .parse()
            .expect("stream id carries the account id");
        match &envelope.event {
            AccountEvent::Opened(Opened { .. }) => {
                self.balances.lock().expect("the lock").insert(id, 0);
            }
            AccountEvent::Deposited(Deposited { amount }) => {
                *self
                    .balances
                    .lock()
                    .expect("the lock")
                    .entry(id)
                    .or_default() += amount;
            }
            AccountEvent::Withdrawn(Withdrawn { amount }) => {
                *self
                    .balances
                    .lock()
                    .expect("the lock")
                    .entry(id)
                    .or_default() -= amount;
            }
        }
        Ok(())
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(run());
}

async fn run() {
    let store = Arc::new(InMemoryStore::<AccountEvent>::new());
    let repo = AggregateRepository::<Account, _>::new(store.clone(), RetryPolicy::default());

    // Open two accounts, with the request's correlation id stamped on
    // every event (0.5.2).
    repo.execute_with_metadata(
        AccountId(1),
        AccountCommand::Open {
            owner: "alice".into(),
        },
        Metadata {
            correlation_id: Some("open-alice".into()),
            ..Default::default()
        },
    )
    .await
    .expect("open alice");
    repo.execute_with_metadata(
        AccountId(2),
        AccountCommand::Open {
            owner: "bob".into(),
        },
        Metadata {
            correlation_id: Some("open-bob".into()),
            ..Default::default()
        },
    )
    .await
    .expect("open bob");

    // Fund alice.
    repo.execute(AccountId(1), AccountCommand::Deposit { amount: 100 })
        .await
        .expect("fund alice");

    // A cross-account transfer on the batch machine (0.4), with the
    // transfer's causation stamped on both sides (0.5.2).
    let transfer = Transfer {
        from: 1,
        to: 2,
        amount: 30,
    };
    let mut batch = BatchMachine::new(
        vec![
            StreamId::for_aggregate::<Account>(&AccountId(1)),
            StreamId::for_aggregate::<Account>(&AccountId(2)),
        ],
        [1, 2]
            .iter()
            .map(|&id| {
                (
                    StreamId::for_aggregate::<Account>(&AccountId(id)),
                    Box::new(AggregateFold::<Account>(AccountId(id)))
                        as Box<dyn Fold<AccountEvent>>,
                )
            })
            .collect(),
        transfer,
        Transfer {
            from: 1,
            to: 2,
            amount: 30,
        },
        RetryPolicy::default(),
    )
    .with_metadata(Metadata {
        causation_id: Some("transfer-1".into()),
        ..Default::default()
    });
    drive_write_batch(&mut batch, &*store).await;

    // The ledger: rebuild it off the typed global stream.
    let balances = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let ledger = Ledger {
        balances: balances.clone(),
    };
    let outcome = drive_projector(
        &mut SubscriptionMachine::new(
            eventyr::prelude::SubscriptionPolicy::default().stop_at_catch_up(),
            eventyr::prelude::Checkpoint::ORIGIN,
        ),
        "ledger",
        &StoreSubscription::new(store.clone()),
        &InMemoryCheckpointStore::new(),
        ledger,
        |duration| async move { tokio::time::sleep(duration).await },
        DriverPorts::new().with_metrics(&eventyr::store::metrics::NoopMetrics),
    )
    .await;
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    let expected: BTreeMap<u64, u64> = [(1, 70), (2, 30)].into_iter().collect();
    assert_eq!(*balances.lock().expect("the lock"), expected);

    // The upcast story: a deposit stored under its old name `AmountV1`
    // is lifted to the current `Deposited` shape on read (0.3).
    let chain = UpcasterChain::new()
        .with(
            "AmountV1",
            ClosureUpcaster::new(|raw: RawEvent| {
                let amount: u64 =
                    serde_json::from_slice(&raw.payload).map_err(|_| UpcastError {
                        event_type: raw.event_type.clone(),
                        message: "payload is not a number".into(),
                    })?;
                serde_json::to_vec(&Deposited { amount }).map_err(|_| UpcastError {
                    event_type: raw.event_type.clone(),
                    message: "cannot re-encode the current shape".into(),
                })
            }),
        )
        .with(
            "Opened",
            ClosureUpcaster::new(|raw: RawEvent| Ok(raw.payload)),
        )
        .with(
            "Deposited",
            ClosureUpcaster::new(|raw: RawEvent| Ok(raw.payload)),
        )
        .with(
            "Withdrawn",
            ClosureUpcaster::new(|raw: RawEvent| Ok(raw.payload)),
        );

    let upcasted: u64 = serde_json::from_slice::<Deposited>(
        &chain
            .upcast(RawEvent {
                event_type: "AmountV1".into(),
                schema_version: EventSchemaVersion::V1,
                payload: serde_json::to_vec(&42u64).expect("serialize"),
            })
            .expect("upcast"),
    )
    .map(|d| d.amount)
    .expect("decode");
    assert_eq!(upcasted, 42);

    println!("ledger: alice=70 bob=30; upcast AmountV1=42");
}
