//! The mediator on eventyr: an aggregate derived with eventyr-macros,
//! executed and loaded through a composition, with interaction metadata
//! stamped by a behavior, idempotent commands, a snapshotting repository,
//! and a saga dispatching its commands back through the mediator.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use std::num::NonZeroU64;

use eventyr_core::prelude::*;
use eventyr_mediator::prelude::*;
use eventyr_mediator::store::{
    Execute, HasMetadata, Load, LoadAt, StampMetadata, parse_id, saga_dispatcher,
};
use eventyr_store::prelude::{
    AggregateRepository, EventStore, ExecutionError, ExecutionOutcome, InMemorySnapshotStore,
    InMemoryStore, SnapshotStore, StreamsAll,
};
use eventyr_subscription::prelude::{
    InMemoryCheckpointStore, Projector, Saga, SagaProjection, StoreSubscription,
};
use futures::TryStreamExt;
use futures::executor::block_on;

// -- the domain: a wallet, derived ----------------------------------------

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct WalletId(u64);

impl fmt::Display for WalletId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WalletId {
    type Err = std::num::ParseIntError;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse().map(WalletId)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Opened;

#[derive(Clone, Debug, PartialEq)]
struct Credited {
    amount: u64,
}

#[derive(Clone, Debug)]
enum WalletCommand {
    Open,
    Credit { amount: u64 },
}

#[derive(Debug, PartialEq)]
enum WalletError {
    AlreadyOpen,
    NotOpen,
}

impl fmt::Display for WalletError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyOpen => "the wallet is already open",
            Self::NotOpen => "the wallet is not open",
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct WalletState {
    open: bool,
    balance: u64,
}

fn apply(state: &mut WalletState, event: &WalletEvent) {
    match event {
        WalletEvent::Opened(Opened) => state.open = true,
        WalletEvent::Credited(Credited { amount }) => state.balance += amount,
    }
}

fn decide(state: &WalletState, command: &WalletCommand) -> Result<Vec<WalletEvent>, WalletError> {
    match command {
        WalletCommand::Open if state.open => Err(WalletError::AlreadyOpen),
        WalletCommand::Open => Ok(vec![Opened.into()]),
        WalletCommand::Credit { .. } if !state.open => Err(WalletError::NotOpen),
        WalletCommand::Credit { amount } => Ok(vec![Credited { amount: *amount }.into()]),
    }
}

#[derive(Aggregate)]
#[eventyr(state = WalletState, events(Opened, Credited))]
struct Wallet;

type Store = Arc<InMemoryStore<WalletEvent>>;

// -- the edge's own request, reaching the repository through a handler --

/// What the HTTP edge sends: a deposit, in the edge's own vocabulary.
/// The edge never names the aggregate, the repository, or the store.
#[derive(Clone)]
struct Deposit {
    wallet: u64,
    amount: u64,
    metadata: Metadata,
}

impl Request for Deposit {
    type Response = Result<Version, String>;
}

impl HasMetadata for Deposit {
    fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
}

/// Translates the edge's request into the aggregate's command and sends
/// it on — sub-dispatch through the mediator, so `Execute<Wallet>`'s own
/// pipeline (and its metadata stamping) applies too.
#[derive(Default)]
struct DepositHandler;

impl<M: Sender<Execute<Wallet>>> RequestHandler<Deposit, M> for DepositHandler {
    async fn handle(&self, mediator: &M, request: Deposit) -> Result<Version, String> {
        let command = Execute::<Wallet>::new(
            WalletId(request.wallet),
            WalletCommand::Credit {
                amount: request.amount,
            },
        )
        .with_metadata(request.metadata);
        match mediator.send(command).await {
            Ok(ExecutionOutcome::Committed { committed, .. })
            | Ok(ExecutionOutcome::AlreadyCommitted { committed }) => Ok(committed
                .last()
                .map(|envelope| envelope.version)
                .unwrap_or(Version::EMPTY)),
            Ok(ExecutionOutcome::Noop) => Ok(Version::EMPTY),
            Err(ExecutionError::Domain(rejection)) => Err(rejection.to_string()),
            Err(ExecutionError::Store(failure)) => Err(failure.to_string()),
        }
    }
}

/// The edge's request id, as the interaction's correlation id.
fn request_id() -> Option<String> {
    Some("req-7".into())
}

eventyr_mediator::mediator! {
    /// The application's mediator: the edge sends requests, the
    /// repository behind it is the composition root's choice.
    #[derive(Clone)]
    struct App {
        wallets: Arc<AggregateRepository<Wallet, Store>>,
        stamp: StampMetadata<fn() -> Option<String>>,
    }

    command Execute<Wallet> as ExecuteWallet => self.wallets;
    command Deposit => DepositHandler;
    query Load<Wallet> as LoadWallet => self.wallets;
    query LoadAt<Wallet> as LoadWalletAt => self.wallets;

    behaviors { self.stamp }
}

fn app() -> (App, Store) {
    let store: Store = Arc::new(InMemoryStore::new());
    let repository = AggregateRepository::<Wallet, _>::new(store.clone(), RetryPolicy::default());
    let app = App::new(
        Arc::new(repository),
        StampMetadata(request_id as fn() -> Option<String>),
    );
    (app, store)
}

fn metadata_of(store: &Store) -> Vec<Metadata> {
    block_on(store.stream_all(Sequence::START).try_collect::<Vec<_>>())
        .expect("read")
        .into_iter()
        .map(|envelope| envelope.metadata)
        .collect()
}

#[test]
fn commands_and_queries_reach_the_repository() {
    let (app, _) = app();
    let opened = block_on(app.send(Execute::<Wallet>::new(WalletId(1), WalletCommand::Open)));
    assert!(matches!(opened, Ok(ExecutionOutcome::Committed { .. })));

    assert_eq!(
        block_on(app.send(Deposit {
            wallet: 1,
            amount: 30,
            metadata: Metadata::default(),
        })),
        Ok(Version::new(2))
    );

    let loaded = block_on(app.send(Load::<Wallet>::new(WalletId(1)))).expect("load");
    assert_eq!(loaded.state.balance, 30);
    assert_eq!(loaded.version, Version::new(2));

    let before =
        block_on(app.send(LoadAt::<Wallet>::new(WalletId(1), Version::new(1)))).expect("load at");
    assert_eq!(before.state.balance, 0);
    assert!(before.state.open);
}

#[test]
fn a_domain_rejection_is_a_value() {
    let (app, _) = app();
    let refused = block_on(app.send(Deposit {
        wallet: 9,
        amount: 5,
        metadata: Metadata::default(),
    }));
    assert_eq!(refused, Err("the wallet is not open".into()));
}

#[test]
fn the_behavior_stamps_every_appended_event() {
    let (app, store) = app();
    block_on(app.send(Execute::<Wallet>::new(WalletId(1), WalletCommand::Open))).expect("open");
    let caused = Metadata {
        causation_id: Some("upstream-event".into()),
        ..Metadata::default()
    };
    block_on(app.send(Deposit {
        wallet: 1,
        amount: 5,
        metadata: caused,
    }))
    .expect("deposit");

    let stamped = metadata_of(&store);
    assert_eq!(stamped.len(), 2);
    for metadata in &stamped {
        assert_eq!(metadata.correlation_id.as_deref(), Some("req-7"));
    }
    // What the caller set is kept; what it left empty is filled.
    assert_eq!(stamped[0].causation_id.as_deref(), Some("req-7"));
    assert_eq!(stamped[1].causation_id.as_deref(), Some("upstream-event"));
}

#[test]
fn an_idempotent_command_commits_once() {
    let (app, store) = app();
    block_on(app.send(Execute::<Wallet>::new(WalletId(1), WalletCommand::Open))).expect("open");
    let credit = || {
        Execute::<Wallet>::new(WalletId(1), WalletCommand::Credit { amount: 10 })
            .idempotent("deposit-42")
    };
    assert!(matches!(
        block_on(app.send(credit())),
        Ok(ExecutionOutcome::Committed { .. })
    ));
    assert!(matches!(
        block_on(app.send(credit())),
        Ok(ExecutionOutcome::AlreadyCommitted { .. })
    ));
    assert_eq!(metadata_of(&store).len(), 2);
    let loaded = block_on(app.send(Load::<Wallet>::new(WalletId(1)))).expect("load");
    assert_eq!(loaded.state.balance, 10);
}

#[test]
fn with_metadata_keeps_a_preset_idempotency_key() {
    let (app, store) = app();
    block_on(app.send(Execute::<Wallet>::new(WalletId(1), WalletCommand::Open))).expect("open");
    // The key is set first; the interaction's metadata carries none, so
    // it must not clobber it — a repeat still matches the earlier commit.
    let credit = || {
        Execute::<Wallet>::new(WalletId(1), WalletCommand::Credit { amount: 10 })
            .idempotent("deposit-42")
            .with_metadata(Metadata {
                correlation_id: Some("req-9".into()),
                ..Metadata::default()
            })
    };
    assert!(matches!(
        block_on(app.send(credit())),
        Ok(ExecutionOutcome::Committed { .. })
    ));
    assert!(matches!(
        block_on(app.send(credit())),
        Ok(ExecutionOutcome::AlreadyCommitted { .. })
    ));
    assert_eq!(metadata_of(&store).len(), 2);
    let loaded = block_on(app.send(Load::<Wallet>::new(WalletId(1)))).expect("load");
    assert_eq!(loaded.state.balance, 10);
}

// -- the same routes over a snapshotting repository ------------------------

type Snapshots = Arc<InMemorySnapshotStore<WalletState>>;

eventyr_mediator::mediator! {
    struct Snapshotted {
        wallets: AggregateRepository<Wallet, Store, Snapshots>,
    }

    command Execute<Wallet> => self.wallets;
    query Load<Wallet> => self.wallets;
}

#[test]
fn a_snapshotting_repository_handles_the_same_requests() {
    let store: Store = Arc::new(InMemoryStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let repository = AggregateRepository::<Wallet, _>::new(store, RetryPolicy::default())
        .with_snapshots(
            snapshots.clone(),
            SnapshotPolicy::new(NonZeroU64::new(2).expect("non-zero")),
        );
    let app = Snapshotted::new(repository);

    block_on(app.send(Execute::<Wallet>::new(WalletId(3), WalletCommand::Open))).expect("open");
    for _ in 0..3 {
        block_on(app.send(Execute::<Wallet>::new(
            WalletId(3),
            WalletCommand::Credit { amount: 1 },
        )))
        .expect("credit");
    }
    let loaded = block_on(app.send(Load::<Wallet>::new(WalletId(3)))).expect("load");
    assert_eq!(loaded.state.balance, 3);
    assert_eq!(loaded.version, Version::new(4));
    let stream = StreamId::for_aggregate::<Wallet>(&WalletId(3));
    let snapshot = block_on(SnapshotStore::load(&*snapshots, &stream))
        .expect("snapshot read")
        .expect("the policy fired");
    assert!(snapshot.version >= Version::new(2));
}

// -- a saga dispatching through the mediator --------------------------------

/// Every credit to wallet 1 credits wallet 2 a bonus.
#[derive(Clone)]
struct Bonus;

impl Saga for Bonus {
    type Event = WalletEvent;
    type Command = WalletCommand;

    fn name(&self) -> &str {
        "bonus"
    }

    fn react(&self, event: &EventEnvelope<WalletEvent>) -> Vec<(StreamId, WalletCommand)> {
        let source = StreamId::for_aggregate::<Wallet>(&WalletId(1));
        match &event.event {
            WalletEvent::Credited(Credited { amount }) if event.stream_id == source => vec![(
                StreamId::for_aggregate::<Wallet>(&WalletId(2)),
                WalletCommand::Credit {
                    amount: amount / 10,
                },
            )],
            _ => Vec::new(),
        }
    }
}

fn run_saga(app: &Arc<App>, store: &Store) -> Result<SubscriptionOutcome, StoreError> {
    block_on(
        Projector::new(
            "bonus",
            StoreSubscription::new(store.clone()),
            InMemoryCheckpointStore::new(),
            SagaProjection::new(
                Bonus,
                saga_dispatcher::<Wallet, _, _>(app.clone(), parse_id::<WalletId>),
            ),
        )
        .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
        .run(|_| futures::future::ready(())),
    )
}

#[test]
fn a_saga_dispatches_through_the_mediator_idempotently() {
    let (app, store) = app();
    let app = Arc::new(app);
    for wallet in [1, 2] {
        block_on(app.send(Execute::<Wallet>::new(
            WalletId(wallet),
            WalletCommand::Open,
        )))
        .expect("open");
    }
    block_on(app.send(Execute::<Wallet>::new(
        WalletId(1),
        WalletCommand::Credit { amount: 100 },
    )))
    .expect("credit");

    let outcome = run_saga(&app, &store).expect("the saga runs");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    let bonus = block_on(app.send(Load::<Wallet>::new(WalletId(2)))).expect("load");
    assert_eq!(bonus.state.balance, 10);

    // A fresh checkpoint store replays every event — the at-least-once
    // redelivery a crash causes. The saga's keys travel through the
    // mediator to the repository, so nothing executes twice.
    run_saga(&app, &store).expect("the replay runs");
    let replayed = block_on(app.send(Load::<Wallet>::new(WalletId(2)))).expect("load");
    assert_eq!(replayed.state.balance, 10);

    // The saga's command went through the pipeline: it carries the
    // saga's idempotency key and the stamped correlation id.
    let events = block_on(
        store
            .stream(
                &StreamId::for_aggregate::<Wallet>(&WalletId(2)),
                Version::EMPTY,
            )
            .try_collect::<Vec<_>>(),
    )
    .expect("read");
    assert_eq!(events.len(), 2, "opened, and one bonus — not two");
    let credit = events.last().expect("an event");
    assert!(
        credit
            .metadata
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.starts_with("bonus:"))
    );
    assert_eq!(credit.metadata.correlation_id.as_deref(), Some("req-7"));
}

#[test]
fn a_saga_command_to_a_foreign_stream_fails_the_dispatch() {
    let (app, _) = app();
    let mut dispatch = saga_dispatcher::<Wallet, _, _>(Arc::new(app), parse_id::<WalletId>);
    let failure = block_on(dispatch(SagaCommand {
        command: WalletCommand::Open,
        target: StreamId::from("ledger-1"),
        metadata: Metadata::default(),
    }));
    assert!(failure.is_err());
}

#[test]
fn a_saga_rejection_is_other_never_unavailable() {
    let (app, _) = app();
    let mut dispatch = saga_dispatcher::<Wallet, _, _>(Arc::new(app), parse_id::<WalletId>);
    // Wallet 9 was never opened: the credit is a domain rejection. The
    // dispatcher maps it to `StoreError::other`, and the runner treats a
    // projection failure as `Other` too — a rejection is a failure by
    // policy, not a retriable `Unavailable`.
    let failure = block_on(dispatch(SagaCommand {
        command: WalletCommand::Credit { amount: 5 },
        target: StreamId::for_aggregate::<Wallet>(&WalletId(9)),
        metadata: Metadata::default(),
    }))
    .expect_err("a rejection fails the dispatch");
    assert!(
        matches!(&failure, StoreError::Other(_)),
        "expected StoreError::Other, got {failure:?}"
    );
    assert!(
        failure.to_string().contains("the wallet is not open"),
        "the rejection is named: {failure}"
    );
}
