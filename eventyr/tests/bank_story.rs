//! The bank example's mirroring suite (DESIGN §0.5.4): the same
//! scenario — open, deposit, transfer, ledger, upcast, snapshot,
//! idempotent replay, saga fee, parking, closure, erasure — run against
//! the library's public API. The example itself is the narrative; this
//! is the gate: it fails `cargo test` when the story breaks, without
//! running `main`.
//!
//! The scenario mirrors `examples/bank.rs`; keep the two in step.

#![cfg(all(
    feature = "subscription",
    feature = "projection",
    feature = "shred_aes_gcm"
))]

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventyr::envelope::Metadata;
use eventyr::prelude::*;
use eventyr::projection::prelude::{ClosureUpcaster, UpcasterChain};
use eventyr::shred::{InMemoryKeyStore, Sensitive, Shredder, ShreddingStore};
use eventyr::shred_aes_gcm::Aes256GcmCipher;
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::{
    EventFilter, FilteredSubscription, InMemoryCheckpointStore, InMemoryParkedStore, ParkedStore,
    Projection, Projector, SagaProjection, StoreSubscription, SubscriptionOutcome,
};

// -- the domain, mirroring examples/bank.rs --------------------------

#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
struct AccountId(u64);
impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Opened {
    owner: Sensitive<String>,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Deposited {
    account: u64,
    amount: u64,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Withdrawn {
    account: u64,
    amount: u64,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Closed {
    account: u64,
}

#[derive(Debug)]
enum AccountCommand {
    Open { owner: String },
    Deposit { amount: u64 },
    Withdraw { amount: u64 },
    Close,
}

#[derive(Debug, PartialEq)]
enum AccountError {
    AlreadyOpen,
    NotOpen,
    InsufficientFunds,
    Closing,
}
impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyOpen => "already open",
            Self::NotOpen => "not open",
            Self::InsufficientFunds => "insufficient funds",
            Self::Closing => "closing",
        })
    }
}

#[derive(Clone, Debug, Default)]
struct AccountState {
    id: AccountId,
    open: bool,
    closed: bool,
    balance: u64,
}
impl AccountState {
    fn new(id: &AccountId) -> Self {
        Self {
            id: id.clone(),
            ..Self::default()
        }
    }
}

fn apply(state: &mut AccountState, event: &AccountEvent) {
    match event {
        AccountEvent::Opened(Opened { .. }) => {
            state.open = true;
            state.balance = 0;
        }
        AccountEvent::Deposited(Deposited { amount, .. }) => state.balance += amount,
        AccountEvent::Withdrawn(Withdrawn { amount, .. }) => state.balance -= amount,
        AccountEvent::Closed(Closed { .. }) => state.closed = true,
    }
}

fn decide(
    state: &AccountState,
    command: &AccountCommand,
) -> Result<Vec<AccountEvent>, AccountError> {
    let account = state.id.0;
    match command {
        AccountCommand::Open { .. } if state.open => Err(AccountError::AlreadyOpen),
        AccountCommand::Close if !state.open => Err(AccountError::NotOpen),
        AccountCommand::Close if state.closed => Err(AccountError::Closing),
        AccountCommand::Close => Ok(vec![Closed { account }.into()]),
        AccountCommand::Open { owner } => Ok(vec![
            Opened {
                owner: Sensitive::new(format!("account-{account}"), owner.clone()),
            }
            .into(),
        ]),
        AccountCommand::Deposit { .. } | AccountCommand::Withdraw { .. } if !state.open => {
            Err(AccountError::NotOpen)
        }
        AccountCommand::Deposit { amount } => Ok(vec![
            Deposited {
                account,
                amount: *amount,
            }
            .into(),
        ]),
        AccountCommand::Withdraw { amount } if *amount > state.balance => {
            Err(AccountError::InsufficientFunds)
        }
        AccountCommand::Withdraw { amount } => Ok(vec![
            Withdrawn {
                account,
                amount: *amount,
            }
            .into(),
        ]),
    }
}

#[derive(Aggregate)]
#[eventyr(
    crate = "eventyr",
    id = AccountId,
    state = AccountState,
    error = AccountError,
    command = AccountCommand,
    initial = AccountState::new(id),
    events(Opened, Deposited, Withdrawn, Closed),
    event_derive(serde::Serialize, serde::Deserialize)
)]
struct Account;

#[derive(Clone)]
struct Transfer {
    from: u64,
    to: u64,
    amount: u64,
}
impl Transfer {
    fn stream_of(&self, id: u64) -> StreamId {
        StreamId::for_aggregate::<Account>(&AccountId(id))
    }
    /// `for_aggregates` derives the streams and the per-stream folds
    /// from the boundary the decider itself names — the same shape the
    /// example ships.
    fn machine(&self, metadata: Metadata) -> BatchMachine<AccountEvent, AccountError, Transfer> {
        BatchMachine::for_aggregates(self, RetryPolicy::default()).with_metadata(metadata)
    }
}

impl AggregateBoundary<Account> for Transfer {
    fn boundary(&self) -> Vec<AccountId> {
        vec![AccountId(self.from), AccountId(self.to)]
    }
}
impl Decide<AccountEvent, AccountError> for Transfer {
    type Command = Self;
    fn decide(
        &self,
        folded: &BTreeMap<StreamId, Box<dyn std::any::Any + Send>>,
        command: &Self,
    ) -> BatchDecision<AccountEvent, AccountError> {
        let state_of = |id: u64| -> AccountState {
            folded
                .get(&self.stream_of(id))
                .and_then(|state| state.downcast_ref::<AccountState>())
                .cloned()
                .expect("the machine folds every boundary stream")
        };
        let from = state_of(command.from);
        let to = state_of(command.to);
        for (state, command) in [
            (
                &from,
                AccountCommand::Withdraw {
                    amount: command.amount,
                },
            ),
            (
                &to,
                AccountCommand::Deposit {
                    amount: command.amount,
                },
            ),
        ] {
            if let Err(error) = Account::decide(state, &command) {
                return BatchDecision::reject(error);
            }
        }
        BatchDecision::of([
            (
                self.stream_of(command.from),
                Withdrawn {
                    account: command.from,
                    amount: command.amount,
                }
                .into(),
            ),
            (
                self.stream_of(command.to),
                Deposited {
                    account: command.to,
                    amount: command.amount,
                }
                .into(),
            ),
        ])
    }
}

fn account_of(stream_id: &StreamId) -> Option<u64> {
    stream_id
        .as_str()
        .strip_prefix("account-")
        .and_then(|id| id.parse().ok())
}

struct Ledger {
    balances: Arc<Mutex<BTreeMap<u64, u64>>>,
}
impl Projection for Ledger {
    type Event = AccountEvent;
    type Error = std::convert::Infallible;
    async fn apply(&mut self, envelope: &EventEnvelope<AccountEvent>) -> Result<(), Self::Error> {
        let Some(id) = account_of(&envelope.stream_id) else {
            return Ok(());
        };
        let mut balances = self.balances.lock().expect("the lock");
        match &envelope.event {
            AccountEvent::Opened(Opened { .. }) => {
                balances.entry(id).or_insert(0);
            }
            AccountEvent::Deposited(Deposited { amount, .. }) => {
                *balances.entry(id).or_default() += amount;
            }
            AccountEvent::Withdrawn(Withdrawn { amount, .. }) => {
                *balances.entry(id).or_default() -= amount;
            }
            AccountEvent::Closed(Closed { .. }) => {
                balances.remove(&id);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct WireFee;
impl Saga for WireFee {
    type Event = AccountEvent;
    type Command = AccountCommand;
    fn name(&self) -> &str {
        "fee"
    }
    fn react(&self, event: &EventEnvelope<AccountEvent>) -> Vec<(StreamId, AccountCommand)> {
        let payer = StreamId::for_aggregate::<Account>(&AccountId(1));
        match &event.event {
            AccountEvent::Deposited(Deposited { account: 2, .. }) if event.stream_id != payer => {
                vec![(payer, AccountCommand::Withdraw { amount: 1 })]
            }
            _ => Vec::new(),
        }
    }
}

type ShreddedStore =
    ShreddingStore<Arc<InMemoryStore<AccountEvent>>, Aes256GcmCipher, InMemoryKeyStore>;

struct Rig {
    store: Arc<ShreddedStore>,
    shredder: Arc<Shredder<Aes256GcmCipher, InMemoryKeyStore>>,
    snapshots: Arc<InMemorySnapshotStore<AccountState>>,
    repo: Arc<
        AggregateRepository<Account, Arc<ShreddedStore>, Arc<InMemorySnapshotStore<AccountState>>>,
    >,
}

fn rig() -> Rig {
    let inner = Arc::new(InMemoryStore::<AccountEvent>::new());
    let shredder = Arc::new(Shredder::new(Aes256GcmCipher, InMemoryKeyStore::new()));
    let store = Arc::new(ShreddingStore::new(inner, shredder.clone()));
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo = Arc::new(
        AggregateRepository::new(store.clone(), RetryPolicy::default()).with_snapshots(
            snapshots.clone(),
            SnapshotPolicy::new(NonZeroU64::new(3).expect("> 0")),
        ),
    );
    Rig {
        store,
        shredder,
        snapshots,
        repo,
    }
}

async fn run_story() -> (Rig, BTreeMap<u64, u64>) {
    let rig = rig();
    let Rig {
        repo,
        store,
        snapshots,
        ..
    } = &rig;
    let correlation = |id: &str| Metadata {
        correlation_id: Some(id.into()),
        ..Default::default()
    };

    repo.execute_with_metadata(
        AccountId(1),
        AccountCommand::Open {
            owner: "alice".into(),
        },
        correlation("open-alice"),
    )
    .await
    .expect("open alice");
    repo.execute_with_metadata(
        AccountId(2),
        AccountCommand::Open {
            owner: "bob".into(),
        },
        correlation("open-bob"),
    )
    .await
    .expect("open bob");

    let mut offered = 0;
    for amount in [100, 10, 10, 10] {
        let outcome = repo
            .execute_with_snapshots(AccountId(1), AccountCommand::Deposit { amount })
            .await
            .expect("deposit");
        if let ExecutionOutcome::Committed {
            snapshot: Some(offer),
            ..
        } = outcome
        {
            offered += 1;
            assert_eq!(offer.into_inner().version, Version::new(3));
        }
    }
    assert_eq!(offered, 1, "the cadence fires once, at version 3");
    let persisted = snapshots
        .load(&StreamId::for_aggregate::<Account>(&AccountId(1)))
        .await
        .expect("load")
        .expect("the offer was persisted");
    assert_eq!(persisted.version, Version::new(3));

    // The keyed transfer commits once, however often it is retried.
    let transfer = Transfer {
        from: 1,
        to: 2,
        amount: 30,
    };
    let transfer_metadata = || {
        Metadata {
            causation_id: Some("transfer-1".into()),
            ..Default::default()
        }
        .with_idempotency_key("transfer-1")
    };
    let committed = drive_write_batch(&mut transfer.machine(transfer_metadata()), &**store).await;
    assert!(matches!(committed, BatchOutcome::Committed { .. }));
    for _ in 0..3 {
        let replayed =
            drive_write_batch(&mut transfer.machine(transfer_metadata()), &**store).await;
        assert!(
            matches!(replayed, BatchOutcome::AlreadyCommitted { .. }),
            "every keyed replay returns the earlier commit: {replayed:?}"
        );
    }

    // The fee saga over the filtered source.
    let dispatcher = {
        let repo = repo.clone();
        move |command: SagaCommand<AccountCommand>| {
            let repo = repo.clone();
            async move {
                let id = account_of(&command.target).expect("account stream");
                repo.execute_with_metadata(AccountId(id), command.command, command.metadata)
                    .await
                    .map_err(|error| StoreError::other(error.to_string()))?;
                Ok(())
            }
        }
    };
    let saga_outcome = Projector::new(
        "fee",
        FilteredSubscription::new(store.clone(), EventFilter::all().stream_prefix("account-")),
        InMemoryCheckpointStore::new(),
        SagaProjection::new(WireFee, dispatcher),
    )
    .with_policy(SubscriptionPolicy::new(64, Duration::ZERO, Duration::ZERO).stop_at_catch_up())
    .run(|_| futures::future::ready(()))
    .await
    .expect("the saga runs");
    assert!(matches!(saga_outcome, SubscriptionOutcome::CaughtUp { .. }));

    // The ledger, woken by commits, catches up.
    let balances = Arc::new(Mutex::new(BTreeMap::new()));
    let outcome = Projector::new(
        "ledger",
        StoreSubscription::new(store.clone()),
        InMemoryCheckpointStore::new(),
        Ledger {
            balances: balances.clone(),
        },
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .wake_on(store.clone())
    .run_woken(|_| futures::future::ready(()))
    .await
    .expect("the ledger rebuilds");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    let ledger = balances.lock().expect("the lock").clone();
    (rig, ledger)
}

#[tokio::test]
async fn the_bank_story_end_to_end() {
    let (rig, ledger) = run_story().await;
    // alice: 100 + 30 - 30 - 1 (fee); bob: +30.
    let expected: BTreeMap<u64, u64> = [(1, 99), (2, 30)].into_iter().collect();
    assert_eq!(ledger, expected);

    // The wire fee was committed exactly once, keyed by the saga.
    let payer: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(EventStore::stream(
        &*rig.store,
        &StreamId::for_aggregate::<Account>(&AccountId(1)),
        Version::EMPTY,
    ))
    .await
    .expect("read alice");
    let fees: Vec<_> = payer
        .iter()
        .filter(|envelope| matches!(envelope.event, AccountEvent::Withdrawn(_)))
        .collect();
    assert_eq!(fees.len(), 2, "the transfer's 30 and the fee's 1");
    assert_eq!(
        fees[1].metadata.idempotency_key.as_deref(),
        Some("fee:8:0"),
        "the saga stamps fee:<sequence of the triggering deposit>:<index>"
    );
}

#[tokio::test]
async fn parking_records_and_replays_the_poison_event() {
    let (rig, _ledger) = run_story().await;
    rig.repo
        .execute(AccountId(1), AccountCommand::Deposit { amount: 10_000 })
        .await
        .expect("fund");
    rig.repo
        .execute(AccountId(1), AccountCommand::Withdraw { amount: 5_000 })
        .await
        .expect("the big withdrawal commits");

    struct Picky;
    impl Projection for Picky {
        type Event = AccountEvent;
        type Error = String;
        async fn apply(
            &mut self,
            envelope: &EventEnvelope<AccountEvent>,
        ) -> Result<(), Self::Error> {
            if let AccountEvent::Withdrawn(Withdrawn { amount, .. }) = &envelope.event
                && *amount > 1_000
            {
                return Err(format!("withdrawal {amount} needs approval"));
            }
            Ok(())
        }
    }

    let parked = Arc::new(InMemoryParkedStore::new());
    let outcome = Projector::new(
        "picky",
        StoreSubscription::new(rig.store.clone()),
        InMemoryCheckpointStore::new(),
        Picky,
    )
    .with_policy(SubscriptionPolicy::new(64, Duration::ZERO, Duration::ZERO).stop_at_catch_up())
    .park_into(parked.clone(), FailurePolicy::Park { retries: 2 })
    .run(|_| futures::future::ready(()))
    .await
    .expect("parks instead of stalling");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));

    let list = parked.list("picky").await.expect("list");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].attempts, 3, "the initial try plus two retries");
    parked
        .remove("picky", list[0].envelope.sequence)
        .await
        .expect("remove");
    assert!(parked.list("picky").await.expect("list").is_empty());
}

#[tokio::test]
async fn closure_and_erasure() {
    let (rig, _ledger) = run_story().await;

    rig.repo
        .execute(
            AccountId(3),
            AccountCommand::Open {
                owner: "carol".into(),
            },
        )
        .await
        .expect("open carol");
    rig.repo
        .execute(AccountId(3), AccountCommand::Close)
        .await
        .expect("record the closure");
    rig.store
        .close_stream(&StreamId::for_aggregate::<Account>(&AccountId(3)))
        .await
        .expect("close");
    let refused = rig
        .repo
        .execute(AccountId(3), AccountCommand::Deposit { amount: 5 })
        .await
        .expect_err("a closed stream refuses appends");
    assert!(matches!(
        refused,
        ExecutionError::Store(StoreError::StreamClosed { .. })
    ));

    rig.shredder.erase("account-2").await.expect("erase bob");
    let bobs: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(EventStore::stream(
        &*rig.store,
        &StreamId::for_aggregate::<Account>(&AccountId(2)),
        Version::EMPTY,
    ))
    .await
    .expect("read bob");
    let AccountEvent::Opened(Opened { owner }) = &bobs[0].event else {
        panic!("the first event opened the account")
    };
    assert!(owner.is_shredded());
}

#[tokio::test]
async fn the_amount_v1_upcast_still_lifts() {
    let chain = UpcasterChain::new().with(
        "AmountV1",
        ClosureUpcaster::new(|raw: RawEvent| {
            let amount: u64 = serde_json::from_slice(&raw.payload).map_err(|_| UpcastError {
                event_type: raw.event_type.clone(),
                message: "payload is not a number".into(),
            })?;
            serde_json::to_vec(&Deposited { account: 0, amount }).map_err(|_| UpcastError {
                event_type: raw.event_type.clone(),
                message: "cannot re-encode".into(),
            })
        }),
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
    .map(|deposited| deposited.amount)
    .expect("decode");
    assert_eq!(upcasted, 42);
}

#[test]
fn the_decide_rules() {
    use eventyr::testing::Scenario;

    let opened = |owner: &str| -> AccountEvent {
        Opened {
            owner: Sensitive::new("account-1", owner.to_owned()),
        }
        .into()
    };
    Scenario::<Account>::given(&AccountId(1), [opened("alice")])
        .when(&AccountCommand::Open {
            owner: "alice".into(),
        })
        .then_error(&AccountError::AlreadyOpen);
    Scenario::<Account>::given(&AccountId(1), [opened("alice")])
        .when(&AccountCommand::Withdraw { amount: 30 })
        .then_error(&AccountError::InsufficientFunds);
    Scenario::<Account>::given(
        &AccountId(1),
        [
            opened("alice"),
            Deposited {
                account: 1,
                amount: 50,
            }
            .into(),
        ],
    )
    .when(&AccountCommand::Withdraw { amount: 30 })
    .then_events(&[Withdrawn {
        account: 1,
        amount: 30,
    }
    .into()])
    .with_state(|state| assert_eq!(state.balance, 50));
    Scenario::<Account>::given(
        &AccountId(1),
        [opened("alice"), Closed { account: 1 }.into()],
    )
    .when(&AccountCommand::Close)
    .then_error(&AccountError::Closing);
}
