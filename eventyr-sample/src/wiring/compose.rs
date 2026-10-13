//! One `compose` per backend: each builds every field of the same
//! mediator type — the account repository the commands route to, the
//! balance read path, the transfer's batch — and the inputs the runners
//! drive, over the store the backend names.
//!
//! The functions are async where the store is: the in-memory backend
//! composes in place; the Postgres backend connects a pool, runs the
//! store's migration, and builds its snapshots, checkpoints, leases,
//! views, and commit signal over it.

use std::sync::Arc;

#[cfg(feature = "postgres")]
use anyhow::Context;
use eventyr::prelude::{RetryPolicy, SnapshotPolicy};
use eventyr::store::prelude::{AggregateRepository, InMemorySnapshotStore, InMemoryStore};
use eventyr::subscription::prelude::{InMemoryCheckpointStore, InMemoryLeaseStore};
use eventyr_mediator::store::StampMetadata;
use tokio::sync::watch;

#[cfg(feature = "postgres")]
use crate::domain::AccountState;
use crate::domain::{Account, AccountEvent};
use crate::features::{BalanceHandler, InMemoryBalances, TransferHandler};
use crate::wiring::{App, MemorySample, MetricsStamp, Sample};

#[cfg(feature = "postgres")]
use eventyr::postgres::{
    PgCheckpointStore, PgCommitSignal, PgLeaseStore, PgSnapshotStore, PgStore,
};

/// The composition root's in-memory branch: the whole application over
/// the process's own store, so `compose` runs out of the box.
pub fn memory() -> MemorySample {
    let store: Arc<InMemoryStore<AccountEvent>> = Arc::new(InMemoryStore::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let accounts = Arc::new(
        AggregateRepository::<Account, _, _>::new(store.clone(), RetryPolicy::default())
            .with_snapshots(
                snapshots,
                SnapshotPolicy::new(std::num::NonZeroU64::new(5).expect("non-zero")),
            ),
    );
    let views = Arc::new(InMemoryBalances::new());
    let app = App::new(
        accounts,
        BalanceHandler::new(views.clone()),
        Arc::new(TransferHandler::new(store.clone())),
        StampMetadata(MetricsStamp),
    );
    Sample {
        app,
        inputs: crate::wiring::runners::Inputs {
            source: store.clone(),
            signal: store,
            checkpoints: Arc::new(InMemoryCheckpointStore::new()),
            leases: Arc::new(InMemoryLeaseStore::new()),
            views,
        },
        shutdown: watch::channel(false).0,
    }
}

/// The composed Postgres sample. The read path's view store is the
/// projection's own in-process table: on the clustered deployment the
/// replica that holds the lease maintains it, and the read serves it
/// without a network round-trip.
#[cfg(feature = "postgres")]
pub type PgSample = Sample<
    PgStore<AccountEvent>,
    PgSnapshotStore<AccountState>,
    PgCheckpointStore,
    PgLeaseStore,
    InMemoryBalances,
    PgCommitSignal,
>;

/// The composition root's Postgres branch: connect the pool, migrate the
/// store, and build the same mediator over it — its snapshots, its
/// checkpoints, its leases, its views, and its `LISTEN`/`NOTIFY` commit
/// signal.
#[cfg(feature = "postgres")]
pub async fn postgres(url: &str) -> anyhow::Result<PgSample> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await
        .context("connect to DATABASE_URL")?;
    eventyr::postgres::store::migrate(&pool)
        .await
        .context("migrate the event store")?;

    let store = PgStore::<AccountEvent>::new(pool);
    let snapshots = Arc::new(PgSnapshotStore::<AccountState>::new(&store));
    let signal = Arc::new(PgCommitSignal::new(&store));
    let checkpoints = Arc::new(PgCheckpointStore::new(&store));
    let leases = Arc::new(PgLeaseStore::new(&store));
    let store = Arc::new(store);
    let accounts = Arc::new(
        AggregateRepository::<Account, _, _>::new(store.clone(), RetryPolicy::default())
            .with_snapshots(
                snapshots,
                SnapshotPolicy::new(std::num::NonZeroU64::new(5).expect("non-zero")),
            ),
    );
    let views = Arc::new(InMemoryBalances::new());
    let app = App::new(
        accounts,
        BalanceHandler::new(views.clone()),
        Arc::new(TransferHandler::new(store.clone())),
        StampMetadata(MetricsStamp),
    );
    Ok(Sample {
        app,
        inputs: crate::wiring::runners::Inputs {
            source: store,
            signal,
            checkpoints,
            leases,
            views,
        },
        shutdown: watch::channel(false).0,
    })
}
