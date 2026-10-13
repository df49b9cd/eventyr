//! The composition root: the one module that names the store.
//!
//! `compose` builds the whole application on one backend and returns the
//! [`Composed`] — which store it picked, plus the [`Sample`] over it: the
//! mediator every slice routes through, and the inputs the runners drive.
//! Replicas of `main` each compose one of these; the edge and the runners
//! never know which store is underneath.
//!
//! The crate's axis is one backend end to end: the in-memory store, so
//! `cargo run` and the tests compose out of the box, or the Postgres
//! store the compose-managed cluster's replicas share. The store's other
//! adapters (SQLite, fjall) apply the same composition; the umbrella
//! examples already cover them.

use std::sync::Arc;

use eventyr::projection::prelude::ViewStore;
use eventyr::store::prelude::{
    AggregateRepository, EventStore, InMemorySnapshotStore, InMemoryStore, SnapshotStore,
    StreamsAll,
};
use eventyr::subscription::prelude::{
    CheckpointStore, CommitSignal, InMemoryCheckpointStore, InMemoryLeaseStore, ProjectorLease,
};
use eventyr_mediator::store::{Execute, Load, LoadAt, StampMetadata};
use tokio::sync::watch;

use crate::domain::{Account, AccountEvent, AccountState};
use crate::features::{
    Balance, BalanceHandler, Deposit, DepositHandler, GetBalance, OpenAccount, OpenAccountHandler,
    Transfer, TransferHandler, Withdraw, WithdrawHandler,
};

pub mod compose;
pub mod runners;

/// The in-memory backend's store, so `compose` runs out of the box and
/// the file is readable end to end.
type MemoryStore = InMemoryStore<AccountEvent>;

/// The account repository the commands route to, generic over the event
/// store and the snapshot store the backend supplies — `Arc` so the
/// composition's `self.accounts` field is pointer-delegated to it.
type Accounts<Store, Snapshots> = Arc<AggregateRepository<Account, Arc<Store>, Arc<Snapshots>>>;

/// The sample: what `compose` hands the edge and the runners, generic
/// over the store's concrete types so either backend builds the same
/// mediator shape. The app is the mediator the edge sends through;
/// `inputs` is what the runners drive over the store, the view, and the
/// lease.
pub struct Sample<Store, Snapshots, Checkpoints, Leases, Views, Signal>
where
    Store:
        EventStore<Event = AccountEvent> + StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Checkpoints: CheckpointStore + Send + Sync + 'static,
    Leases: ProjectorLease + Send + Sync + 'static,
    Views: ViewStore<Balance> + Send + Sync + 'static,
    Signal: CommitSignal + Send + Sync + 'static,
    Arc<Store>: StreamsAll<Event = AccountEvent> + Send + Sync,
    Arc<Snapshots>: SnapshotStore<State = AccountState> + Send + Sync,
    Arc<Signal>: CommitSignal + Send + Sync,
{
    /// The mediator every slice routes through.
    pub app: App<Store, Snapshots, Views>,
    /// The projection's and saga's shared inputs — the store's checkpoint
    /// port, the lease store, the balances' view store, the source, and
    /// the commit signal, all over one backend.
    pub inputs: runners::Inputs<Store, Checkpoints, Leases, Views, Signal>,
    /// Shutdown: when this closes, the runners stop.
    pub shutdown: watch::Sender<bool>,
}

impl<Store, Snapshots, Checkpoints, Leases, Views, Signal> Clone
    for Sample<Store, Snapshots, Checkpoints, Leases, Views, Signal>
where
    Store:
        EventStore<Event = AccountEvent> + StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Checkpoints: CheckpointStore + Send + Sync + 'static,
    Leases: ProjectorLease + Send + Sync + 'static,
    Views: ViewStore<Balance> + Send + Sync + 'static,
    Signal: CommitSignal + Send + Sync + 'static,
    Arc<Store>: StreamsAll<Event = AccountEvent> + Send + Sync,
    Arc<Snapshots>: SnapshotStore<State = AccountState> + Send + Sync,
    Arc<Signal>: CommitSignal + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
            inputs: self.inputs.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

eventyr_mediator::mediator! {
    /// The sample's mediator: one type, composed in `compose` per
    /// backend.
    ///
    /// The command routes go to the account repository, pointer-
    /// delegated through its `Arc`; the queries ride the same
    /// repository's `Load`/`LoadAt`. `StampMetadata` fills every
    /// `HasMetadata` request's ids before the handler runs.
    pub struct App<Store, Snapshots, Views>
    where
        Store: EventStore<Event = AccountEvent> + Send + Sync + 'static,
        Snapshots: SnapshotStore<State = AccountState> + Send + Sync + 'static,
        Views: ViewStore<Balance> + Send + Sync + 'static,
        Arc<Store>: EventStore<Event = AccountEvent> + Send + Sync,
        Arc<Snapshots>: SnapshotStore<State = AccountState> + Send + Sync,
    {
        /// The account repository the commands route to.
        accounts: Accounts<Store, Snapshots>,
        /// The balance read path, over the projection's store.
        balances: BalanceHandler<Views>,
        /// The transfer's batch handler.
        transfer: Arc<TransferHandler<Store>>,
        /// The stamp on every request that carries metadata.
        stamp: StampMetadata<MetricsStamp>,
    }

    command Execute<Account> => self.accounts;
    query Load<Account> => self.accounts;
    query LoadAt<Account> => self.accounts;
    command OpenAccount => OpenAccountHandler;
    command Deposit => DepositHandler;
    command Withdraw => WithdrawHandler;
    command Transfer => self.transfer;
    query GetBalance => self.balances;
    behaviors { self.stamp }
}

impl<Store, Snapshots, Views> Clone for App<Store, Snapshots, Views>
where
    Store: EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: ViewStore<Balance> + Send + Sync + 'static,
    Arc<Store>: EventStore<Event = AccountEvent> + Send + Sync,
    Arc<Snapshots>: SnapshotStore<State = AccountState> + Send + Sync,
{
    fn clone(&self) -> Self {
        Self::new(
            self.accounts.clone(),
            self.balances.clone(),
            self.transfer.clone(),
            self.stamp,
        )
    }
}

/// The metadata stamp the mediator applies to every request that carries
/// any: a fresh correlation id per request, and that id as the causation
/// id when the request has none.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsStamp;

impl eventyr_mediator::store::CorrelationSource for MetricsStamp {
    fn correlation_id(&self) -> Option<String> {
        Some(uuid::Uuid::new_v4().to_string())
    }
}

/// Which store composes. The tests and `cargo run` use the in-memory
/// backend out of the box; `DATABASE_URL` names the compose-managed
/// Postgres cluster.
#[derive(Clone, Debug, Default)]
pub enum Backend {
    /// The in-memory store — one process, no dependencies.
    #[default]
    Memory,
    /// The Postgres store the compose-managed cluster's replicas share,
    /// at this DSN. Only with the `postgres` feature.
    #[cfg(feature = "postgres")]
    Postgres(String),
}

impl Backend {
    /// Pick the backend from the environment: the compose-managed
    /// Postgres when `DATABASE_URL` is set, the in-memory store
    /// otherwise. On a build without the `postgres` feature a set
    /// `DATABASE_URL` is ignored — the binary cannot serve it.
    pub fn from_env() -> Self {
        #[cfg(feature = "postgres")]
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return Self::Postgres(url);
        }
        Self::Memory
    }
}

/// The composed application, tagged by which store it runs on. `main`
/// matches on it to drive the runners and the edge over the one backend
/// `compose` picked; the tests match it to name the concrete sample.
pub enum Composed {
    /// The in-memory backend.
    Memory(MemorySample),
    /// The Postgres backend.
    #[cfg(feature = "postgres")]
    Postgres(compose::PgSample),
}

impl Composed {
    /// The in-memory sample, when the backend is the in-memory one; the
    /// tests match it out of the enum themselves.
    pub fn memory(self) -> Option<MemorySample> {
        match self {
            Self::Memory(sample) => Some(sample),
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => None,
        }
    }

    /// The Postgres sample, when the backend is the compose-managed one.
    #[cfg(feature = "postgres")]
    pub fn postgres(self) -> Option<compose::PgSample> {
        match self {
            Self::Memory(_) => None,
            Self::Postgres(sample) => Some(sample),
        }
    }
}

/// The composition root: build the sample on the backend `from_env`
/// picked. The Postgres branch connects, migrates, and builds its stores;
/// the in-memory branch composes in place.
pub async fn compose(backend: &Backend) -> anyhow::Result<Composed> {
    match backend {
        Backend::Memory => Ok(Composed::Memory(compose::memory())),
        #[cfg(feature = "postgres")]
        Backend::Postgres(url) => Ok(Composed::Postgres(compose::postgres(url).await?)),
    }
}

/// The composed in-memory sample: the concrete [`Sample`] the in-memory
/// backend builds. Re-exported so the tests name it without the generic
/// parameters.
pub type MemorySample = Sample<
    MemoryStore,
    InMemorySnapshotStore<AccountState>,
    InMemoryCheckpointStore,
    InMemoryLeaseStore,
    crate::features::InMemoryBalances,
    MemoryStore,
>;
