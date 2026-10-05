//! # The bank — the Eventyr features in one domain, §0.5.4.
//!
//! One small, complete example covering the shipped surface in a single
//! in-process run: accounts are opened, funded, debited, and wired
//! between; the domain rules are pinned with the `Scenario` DSL; the
//! repository runs snapshot-seeded (0.3); a cross-account transfer rides
//! the 0.4 batch machine and is replayed under its idempotency key
//! (0.7.5); a wire-fee saga reacts to deposits through a filtered
//! subscription (0.6.1 + 0.7.4) with the saga's command keys making
//! redelivery safe; the ledger read model rebuilds off the global
//! stream, woken by commits (0.7.2); a poison event is parked and
//! replayed (0.7.7); a renamed event is upcast end to end (0.3);
//! owner names are sealed by the shredding store and one subject is
//! erased (0.7.6); and an account is closed, its further writes refused
//! (0.7.6).
//!
//! Dynamic consistency boundaries live in `examples/loan_eligibility.rs`
//! and inline views in `examples/inline_view.rs`.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example bank --all-features
//! ```

#![allow(dead_code)] // A demo: the point is the full vocabulary.

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

// -- the domain -----------------------------------------------------

/// An account instance's identifier.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
struct AccountId(u64);
impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Payload structs (the sourcery pattern §8): a V1 deposit was `Amount`;
// the rename to `Deposited` upcasts the old name on read. `Deposited`
// and `Withdrawn` carry the account id so the `Tagged` impl below can
// derive each event's tag from its payload — a stored tag would be
// wrong for every event written before its type gained one (0.7.1).
//
// `Opened.owner` is a `Sensitive` (0.7.6): the account holder is the
// data subject, and the shredding store seals the name before it
// reaches the log. The `AmountV1` upcaster below shows the old deposit
// shape having no `account` field at all.
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
/// The closure fact, appended by the caller before `close_stream` — the
/// store emits no lifecycle marker of its own (0.7.6).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Closed {
    account: u64,
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
    /// Close the account: records the fact, then the stream is closed.
    Close,
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
    /// The account is closing; nothing further applies.
    Closing,
}
impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyOpen => "account is already open",
            Self::NotOpen => "account is not open",
            Self::InsufficientFunds => "insufficient funds",
            Self::Closing => "account is closing",
        })
    }
}

/// The account's folded state. The id rides along so `decide` — which
/// the trait keeps to state and command — can put it into the payloads.
#[derive(Clone, Debug, Default)]
struct AccountState {
    id: AccountId,
    open: bool,
    closed: bool,
    balance: u64,
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
        // Domain-wise a closed account balances any amount; what refuses
        // the write is the closed *stream* (0.7.6 below). Only the
        // domain's own rules reject here.
        AccountCommand::Open { owner } => Ok(vec![
            Opened {
                // The account holds its owner's personal data: the
                // holder is the subject, and erasing the holder shreds
                // the name without rewriting history.
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

// The event enum is generated from the payload structs (the §8 sourcery
// pattern): one newtype variant per payload, plus `From` and `EventName`.
// `initial` receives the id, so the state can carry it; `event_derive`
// adds the serde glue the shredding store and SQL stores need.
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

impl AccountState {
    fn new(id: &AccountId) -> Self {
        Self {
            id: id.clone(),
            ..Self::default()
        }
    }
}

// Tags (0.7.1): a pure function of the event, the same as its stored
// name. Boundary decisions query the log through tags (see
// `examples/loan_eligibility.rs`); the fee saga below reads through a
// filtered subscription, the read-side cousin.
impl Tagged for AccountEvent {
    fn tags(&self) -> Vec<Tag> {
        let account = match self {
            AccountEvent::Opened(Opened { .. }) => return Vec::new(),
            AccountEvent::Deposited(Deposited { account, .. })
            | AccountEvent::Withdrawn(Withdrawn { account, .. })
            | AccountEvent::Closed(Closed { account }) => *account,
        };
        vec![Tag::of("account", account)]
    }
}

// -- the multi-stream transfer (0.4) --------------------------------

/// A transfer between two accounts — the batch-machine decision.
struct Transfer {
    from: u64,
    to: u64,
    amount: u64,
}

impl Transfer {
    fn stream_of(&self, id: u64) -> StreamId {
        StreamId::for_aggregate::<Account>(&AccountId(id))
    }

    /// The boundary names its streams twice (once for the loads, once
    /// for the folds), so one constructor keeps the call honest.
    fn machine(&self, metadata: Metadata) -> BatchMachine<AccountEvent, AccountError, Transfer> {
        BatchMachine::new(
            vec![self.stream_of(self.from), self.stream_of(self.to)],
            [self.from, self.to]
                .iter()
                .map(|&id| {
                    (
                        self.stream_of(id),
                        Box::new(AggregateFold::<Account>(AccountId(id)))
                            as Box<dyn Fold<AccountEvent>>,
                    )
                })
                .collect(),
            Transfer {
                from: self.from,
                to: self.to,
                amount: self.amount,
            },
            Transfer {
                from: self.from,
                to: self.to,
                amount: self.amount,
            },
            RetryPolicy::default(),
        )
        .with_metadata(metadata)
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

// -- projections ----------------------------------------------------

fn account_of(stream_id: &StreamId) -> Option<u64> {
    stream_id
        .as_str()
        .strip_prefix("account-")
        .and_then(|id| id.parse().ok())
}

/// A balance ledger: one `Projection` impl, at-least-once, idempotent.
/// An event from a stream that is not an account is skipped, not
/// fatal — a projection decides what it handles, and redelivery must
/// never corrupt it.
struct Ledger {
    balances: Arc<Mutex<BTreeMap<u64, u64>>>,
}
impl Ledger {
    fn fold(&mut self, envelope: &EventEnvelope<AccountEvent>) {
        let Some(id) = account_of(&envelope.stream_id) else {
            return;
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
    }
}
impl Projection for Ledger {
    type Event = AccountEvent;
    type Error = std::convert::Infallible;
    async fn apply(&mut self, envelope: &EventEnvelope<AccountEvent>) -> Result<(), Self::Error> {
        self.fold(envelope);
        Ok(())
    }
}

/// A ledger that refuses any withdrawal over 1_000 — the poison event
/// for the parking story (0.7.7). Its `Error` is a plain `String`:
/// projections reject with whatever they can display.
///
/// At-least-once is literal: when one event keeps failing, the batch is
/// redelivered from the last ack, and the events *before* the poison
/// one arrive again each time. So a projection that can see redelivery
/// dedupes — here by stream version (each event's `version` is its
/// position in one stream, so anything at or below the last applied
/// version is a duplicate fold).
struct PickyLedger {
    balances: Arc<Mutex<BTreeMap<u64, u64>>>,
    applied: Arc<Mutex<BTreeMap<u64, u64>>>,
    /// The approval list: the fixed projection lets these through.
    approved: Vec<u64>,
}
impl Projection for PickyLedger {
    type Event = AccountEvent;
    type Error = String;
    async fn apply(&mut self, envelope: &EventEnvelope<AccountEvent>) -> Result<(), Self::Error> {
        let Some(id) = account_of(&envelope.stream_id) else {
            return Ok(());
        };
        {
            let applied = self.applied.lock().expect("the lock");
            if envelope.version.as_u64() <= *applied.get(&id).unwrap_or(&0) {
                return Ok(()); // a redelivery: already folded
            }
        }
        if let AccountEvent::Withdrawn(Withdrawn { amount, .. }) = &envelope.event
            && *amount > 1_000
            && !self.approved.contains(amount)
        {
            return Err(format!("withdrawal {amount} needs approval"));
        }
        self.applied
            .lock()
            .expect("the lock")
            .insert(id, envelope.version.as_u64());
        Ledger {
            balances: self.balances.clone(),
        }
        .fold(envelope);
        Ok(())
    }
}

// -- the wire-fee saga (0.6.1, keys 0.7.5) ---------------------------

/// Every deposit to account 2 charges account 1 a 1-unit wire fee.
///
/// `SagaMachine` stamps each command with the idempotency key
/// `"fee:<sequence>:<index>"` (0.7.5): if the run crashes between
/// dispatch and ack, the redelivered deposit re-issues the command, and
/// the target stream's key check returns the earlier commit instead of
/// charging twice. The dispatcher must forward `command.metadata` into
/// `execute_with_metadata`, or the key never reaches the write side.
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

// -- the run ---------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() {
    domain_rules();

    // The write side: an in-memory store sealed by the shredding
    // wrapper (0.7.6). The log itself holds ciphertext; readers through
    // the wrapper see plain values until a subject's key is erased.
    let inner = Arc::new(InMemoryStore::<AccountEvent>::new());
    let shredder = Arc::new(Shredder::new(Aes256GcmCipher, InMemoryKeyStore::new()));
    let store = Arc::new(ShreddingStore::new(inner.clone(), shredder.clone()));

    // Snapshots on (0.3): the long-lived account's state is persisted
    // every 3 committed versions, and later loads start from it.
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(store.clone(), RetryPolicy::default()).with_snapshots(
            snapshots.clone(),
            SnapshotPolicy::new(NonZeroU64::new(3).expect("> 0")),
        );
    let correlation = |id: &str| Metadata {
        correlation_id: Some(id.into()),
        ..Default::default()
    };

    // Open two accounts, with the request's correlation id stamped on
    // every event (0.5.2).
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

    // Fund alice, snapshot-seeded: the cadence offer rides the commit
    // once the stream has moved three versions since the last snapshot.
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
            println!("snapshot offered at version {}", offer.into_inner().version);
        }
    }

    // A cross-account transfer on the batch machine (0.4), under an
    // idempotency key (0.7.5). The key travels in the metadata and its
    // record is the streams the command writes to — no side table, and
    // nothing new to keep consistent.
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
    let mut first_attempt = transfer.machine(transfer_metadata());
    let first = drive_write_batch(&mut first_attempt, &*store).await;
    assert!(matches!(first, BatchOutcome::Committed { .. }), "{first:?}");

    // The retried API call: same key, same boundary. Nothing is
    // re-decided and nothing appended — the earlier commit comes back.
    let mut second_attempt = transfer.machine(transfer_metadata());
    let second = drive_write_batch(&mut second_attempt, &*store).await;
    assert!(
        matches!(second, BatchOutcome::AlreadyCommitted { .. }),
        "a keyed replay transfers nothing twice: {second:?}"
    );

    // The fee saga (0.6.1): bob's deposit charges alice the wire fee.
    // Its subscription is filtered (0.7.4) — only `account-*` streams
    // are read, and the checkpoint still advances past long runs where
    // nothing matched.
    let repo = Arc::new(repo);
    let dispatcher = {
        let repo = repo.clone();
        move |command: SagaCommand<AccountCommand>| {
            let repo = repo.clone();
            async move {
                let id = account_of(&command.target).expect("the saga targets account streams");
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

    // The ledger: rebuild it off the global stream, woken by commits
    // (0.7.2). `wake_on` ends idle sleeps early when the store commits;
    // here catch-up is immediate because the log is already full.
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
    // alice: 100 + 30 (top-ups) - 30 (transfer) - 1 (wire fee) = 99.
    let expected: BTreeMap<u64, u64> = [(1, 99), (2, 30)].into_iter().collect();
    assert_eq!(*balances.lock().expect("the lock"), expected);

    // Poison events (0.7.7): a 5_000-unit withdrawal the picky ledger
    // refuses. `Park { retries: 2 }` lets it reject three times, records
    // the event in the parked store, and carries on past it — one bad
    // event never stalls the projection (Halt stays the default;
    // skipping is a decision the caller makes).
    repo.execute(AccountId(1), AccountCommand::Deposit { amount: 10_000 })
        .await
        .expect("fund for the big withdrawal");
    repo.execute(AccountId(1), AccountCommand::Withdraw { amount: 5_000 })
        .await
        .expect("the big withdrawal commits as an event");
    let picky_balances = Arc::new(Mutex::new(BTreeMap::new()));
    let picky_applied = Arc::new(Mutex::new(BTreeMap::new()));
    let parked = Arc::new(InMemoryParkedStore::new());
    let sensitive = PickyLedger {
        balances: picky_balances.clone(),
        applied: picky_applied.clone(),
        approved: Vec::new(),
    };
    let outcome = Projector::new(
        "picky",
        StoreSubscription::new(store.clone()),
        InMemoryCheckpointStore::new(),
        sensitive,
    )
    .with_policy(SubscriptionPolicy::new(64, Duration::ZERO, Duration::ZERO).stop_at_catch_up())
    .park_into(parked.clone(), FailurePolicy::Park { retries: 2 })
    .run(|_| futures::future::ready(()))
    .await
    .expect("parks instead of stalling");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    let poisoned = parked.list("picky").await.expect("list parked");
    assert_eq!(poisoned.len(), 1, "exactly the 5_000 withdrawal parked");
    assert_eq!(
        picky_balances.lock().expect("the lock")[&1],
        99 + 10_000,
        "the parked withdrawal never applied"
    );

    // Replay is the caller's: the withdrawal is approved, the fixed
    // projection applies each parked event, and its record is removed.
    let mut fixed = PickyLedger {
        balances: picky_balances.clone(),
        // A fresh ledger with its own dedupe map: the parked envelope is
        // replayed from scratch, not redelivered by the subscription.
        applied: Arc::new(Mutex::new(BTreeMap::new())),
        approved: vec![5_000],
    };
    for entry in poisoned {
        Projection::apply(&mut fixed, &entry.envelope)
            .await
            .expect("the fixed projection applies it");
        parked
            .remove("picky", entry.envelope.sequence)
            .await
            .expect("remove");
    }
    assert_eq!(
        picky_balances.lock().expect("the lock")[&1],
        99 + 10_000 - 5_000,
        "the replay applies the once-poisoned event"
    );

    // The upcast story (0.3): a deposit stored under its old name
    // `AmountV1` — a bare number, from before deposits knew their
    // account — is lifted to the current `Deposited` shape on read.
    let chain = UpcasterChain::new()
        .with(
            "AmountV1",
            ClosureUpcaster::new(|raw: RawEvent| {
                let amount: u64 =
                    serde_json::from_slice(&raw.payload).map_err(|_| UpcastError {
                        event_type: raw.event_type.clone(),
                        message: "payload is not a number".into(),
                    })?;
                serde_json::to_vec(&Deposited { account: 0, amount }).map_err(|_| UpcastError {
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
        )
        .with(
            "Closed",
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
    .map(|deposited| deposited.amount)
    .expect("decode");
    assert_eq!(upcasted, 42);

    // Lifecycle (0.7.6): carol's account is closed. The domain records
    // its own `Closed` event first; the store then refuses every append
    // path while the history stays readable.
    repo.execute_with_metadata(
        AccountId(3),
        AccountCommand::Open {
            owner: "carol".into(),
        },
        correlation("open-carol"),
    )
    .await
    .expect("open carol");
    repo.execute(AccountId(3), AccountCommand::Close)
        .await
        .expect("record the closure");
    store
        .close_stream(&StreamId::for_aggregate::<Account>(&AccountId(3)))
        .await
        .expect("close");
    let refused = repo
        .execute(AccountId(3), AccountCommand::Deposit { amount: 5 })
        .await
        .expect_err("a closed stream refuses appends");
    assert!(matches!(
        refused,
        ExecutionError::Store(StoreError::StreamClosed { .. })
    ));

    // Erasure (0.7.6): bob closes his account and exercises his right
    // to be forgotten. Deleting his key turns every sealed `owner` of
    // his into `Shredded` — history still folds, only the personal data
    // is gone. And nothing new for an erased subject is ever written in
    // the clear: the seal fails and the append is refused.
    shredder.erase("account-2").await.expect("erase bob");
    let bobs_history: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(EventStore::stream(
        &*store,
        &StreamId::for_aggregate::<Account>(&AccountId(2)),
        Version::EMPTY,
    ))
    .await
    .expect("read bob");
    let AccountEvent::Opened(Opened { owner }) = &bobs_history[0].event else {
        panic!("the first event opened the account")
    };
    assert!(owner.is_shredded(), "bob's name is gone from the log");

    println!(
        "ledger: alice=99 bob=30 (fee charged); transfer replayed once; \
         parked withdrawal replayed; upcast AmountV1=42; carol closed; bob erased",
    );
}

/// The decide rules, pinned with the `Scenario` DSL (0.2) instead of
/// ad-hoc asserts: `given` a history, `when` a command, `then` the
/// events or the rejection.
fn domain_rules() {
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
}
