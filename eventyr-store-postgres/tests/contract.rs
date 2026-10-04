//! The store contract against a real Postgres: the proof that `PgStore`
//! honours the protocol the in-memory store is compared to.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

use serde::{Deserialize, Serialize};

use eventyr_core::event_name::EventName;
use eventyr_store_postgres::PgStore;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
enum ContractEvent {
    Payload { value: u64 },
}

impl EventName for ContractEvent {
    fn event_name(&self) -> &'static str {
        match self {
            ContractEvent::Payload { .. } => "Payload",
        }
    }
}

impl From<u64> for ContractEvent {
    fn from(value: u64) -> Self {
        ContractEvent::Payload { value }
    }
}

/// A store over a fresh, empty schema.
///
/// The suite calls `make_store()` once per check and expects an empty
/// store each time — `stream_all` reads the whole log, so a shared
/// database would hand later checks every earlier check's events. Each
/// call creates its own schema, pins a one-connection pool's
/// `search_path` to it, and migrates into it.
fn fresh_store(runtime: &tokio::runtime::Runtime, url: &str) -> PgStore<ContractEvent> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let schema = format!(
        "contract_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    runtime.block_on(async {
        let admin = sqlx::PgPool::connect(url).await.expect("connect");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .expect("create schema");
        admin.close().await;
        let options: sqlx::postgres::PgConnectOptions = url.parse().expect("url");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .expect("connect to schema");
        eventyr_store_postgres::store::migrate(&pool)
            .await
            .expect("migrate");
        PgStore::new(pool)
    })
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_contract() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    // Multi-thread, not current-thread: the suite drives each future
    // with `futures::executor::block_on`, which parks this thread, so
    // the I/O reactor has to run on the runtime's own worker threads.
    // A current-thread runtime only drives its reactor inside its own
    // `block_on`, and sqlx's first socket wait would never wake.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let make_store = || fresh_store(&runtime, &url);
    // Entered so the sqlx futures the suite polls find this runtime.
    let _guard = runtime.enter();
    eventyr_store_testing::event_store_contract::<ContractEvent, _>(make_store);
    eventyr_store_testing::streams_all_contract::<ContractEvent, _>(make_store);
    eventyr_store_testing::event_store_batch_contract::<ContractEvent, _>(make_store);
}
