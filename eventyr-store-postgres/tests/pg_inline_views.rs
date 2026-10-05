//! Inline views (0.7.3) against a real Postgres: rows written in the
//! append's transaction.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

use std::sync::Arc;

use eventyr_core::batch::StreamAppend;
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
use eventyr_projection::inline::{Inline, InlineView};
use eventyr_projection::view::{View, ViewStore};
use eventyr_store::store::EventStore;
use eventyr_store_postgres::PgStore;
use eventyr_store_postgres::views::PgViewStore;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
enum Event {
    Deposited { amount: u64 },
}

impl EventName for Event {
    fn event_name(&self) -> &'static str {
        "Deposited"
    }
}

/// Per-account balance: one row per stream.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct Balance(u64);

impl View<Event> for Balance {
    fn initial() -> Self {
        Self(0)
    }

    fn apply(&mut self, event: &EventEnvelope<Event>) {
        let Event::Deposited { amount } = event.event;
        self.0 += amount;
    }
}

/// Per-account deposit count, refusing to fold past a ceiling — the
/// way a view whose row stops decoding fails, without corrupting data
/// by hand.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct Capped(u64);

impl View<Event> for Capped {
    fn initial() -> Self {
        Self(0)
    }

    fn apply(&mut self, _: &EventEnvelope<Event>) {
        self.0 += 1;
    }
}

fn balance() -> Arc<dyn InlineView<Event>> {
    Arc::new(Inline::<Balance, _>::new(
        "balance",
        |e: &EventEnvelope<Event>| Some(e.stream_id.as_str().to_owned()),
    ))
}

/// One row every stream folds into: the multi-stream case.
fn total() -> Arc<dyn InlineView<Event>> {
    Arc::new(Inline::<Balance, _>::new(
        "total",
        |_: &EventEnvelope<Event>| Some("all".to_owned()),
    ))
}

/// Schemas this file's tests created, dropped by [`drop_schemas`] so a
/// dev database does not fill with them.
static SCHEMAS: std::sync::Mutex<Vec<(sqlx::PgPool, sqlx::PgPool, String)>> =
    std::sync::Mutex::new(Vec::new());

async fn fresh_pool(url: &str, connections: u32) -> sqlx::PgPool {
    let schema = format!("inline_{}", uuid::Uuid::new_v4().simple());
    // One connection, closed promptly when idle: a test's schemas live
    // until its cleanup, and the default 100-connection server must
    // serve every concurrently running test.
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(std::time::Duration::from_secs(5))
        .connect(url)
        .await
        .expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    let options: sqlx::postgres::PgConnectOptions = url.parse().expect("url");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(connections)
        // Bounded waits and prompt idle closes: a failing drop must say
        // so quickly instead of hanging the suite, and the schemas all
        // stay open until their test's cleanup — the server's 100
        // connections serve every concurrently running test.
        .acquire_timeout(std::time::Duration::from_secs(30))
        .idle_timeout(std::time::Duration::from_secs(5))
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .expect("connect to schema");
    eventyr_store_postgres::store::migrate(&pool)
        .await
        .expect("migrate");
    SCHEMAS
        .lock()
        .expect("registry")
        .push((pool.clone(), admin, schema));
    pool
}

/// Drop every schema created so far; call at the end of a test.
async fn drop_schemas() {
    let schemas: Vec<_> = std::mem::take(&mut *SCHEMAS.lock().expect("registry"));
    for (pool, admin, name) in schemas {
        pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {name} CASCADE"
        )))
        .execute(&admin)
        .await
        .expect("drop schema");
        admin.close().await;
    }
}

fn url() -> String {
    std::env::var("EVENTYR_TEST_PG_URL").expect("EVENTYR_TEST_PG_URL must point at a real Postgres")
}

fn deposit(amount: u64) -> NewEvent<Event> {
    NewEvent::new(Event::Deposited { amount })
}

async fn row(views: &PgViewStore<Balance>, name: &str, id: &str) -> Option<(u64, Sequence)> {
    views
        .load(name, id)
        .await
        .expect("load")
        .map(|row| (row.value.0, row.version))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn the_row_is_there_when_the_append_returns() {
    let pool = fresh_pool(&url(), 2).await;
    let store = PgStore::<Event>::new(pool).with_inline_views(vec![balance(), total()]);
    let views = PgViewStore::<Balance>::new(&store);

    let committed = store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Empty,
            vec![deposit(10), deposit(5)],
        )
        .await
        .expect("append");
    // No projector ran: the rows were written by the append itself.
    assert_eq!(
        row(&views, "balance", "account-1").await,
        Some((15, committed[1].sequence))
    );
    assert_eq!(row(&views, "total", "all").await.map(|r| r.0), Some(15));

    // Batches and every later append fold on top.
    store
        .append_batch(vec![
            StreamAppend {
                stream_id: StreamId::from("account-1"),
                expected: ExpectedVersion::Any,
                events: vec![deposit(1)],
            },
            StreamAppend {
                stream_id: StreamId::from("account-2"),
                expected: ExpectedVersion::Empty,
                events: vec![deposit(100)],
            },
        ])
        .await
        .expect("batch");
    assert_eq!(
        row(&views, "balance", "account-1").await.map(|r| r.0),
        Some(16)
    );
    assert_eq!(
        row(&views, "balance", "account-2").await.map(|r| r.0),
        Some(100)
    );
    assert_eq!(row(&views, "total", "all").await.map(|r| r.0), Some(116));
    drop_schemas().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn a_view_that_cannot_fold_fails_the_append() {
    let pool = fresh_pool(&url(), 2).await;
    let plain = PgStore::<Event>::new(pool.clone());
    // A stored row the view's type cannot decode.
    sqlx::query(
        "INSERT INTO views (view_name, view_id, version, payload) \
         VALUES ('capped', 'account-1', 0, '\"not a number\"'::jsonb)",
    )
    .execute(&pool)
    .await
    .expect("seed a bad row");

    let capped: Arc<dyn InlineView<Event>> = Arc::new(Inline::<Capped, _>::new(
        "capped",
        |e: &EventEnvelope<Event>| Some(e.stream_id.as_str().to_owned()),
    ));
    let store = PgStore::<Event>::new(pool).with_inline_views(vec![capped]);
    let error = store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Empty,
            vec![deposit(1)],
        )
        .await
        .expect_err("the view cannot fold");
    assert!(error.to_string().contains("capped"), "{error}");

    // The append rolled back with the view.
    let events: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
        eventyr_store::store::StreamsAll::stream_all(&plain, Sequence::START),
    )
    .await
    .expect("read");
    assert!(events.is_empty(), "no event outlives its failed view write");
    drop_schemas().await;
}

/// Many appends to *different* streams race to fold into one shared
/// row. Without the row lock two of them would read the same old total
/// and one deposit would vanish.
///
/// The heaviest pool the file opens: run it alone with the other heavy
/// test binary's suites out of the way, so a 100-connection server
/// always has room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn concurrent_folds_into_one_row_lose_nothing() {
    const WRITERS: u64 = 24;
    let pool = fresh_pool(&url(), WRITERS as u32).await;
    let store = PgStore::<Event>::new(pool).with_inline_views(vec![total()]);
    let views = PgViewStore::<Balance>::new(&store);

    let tasks: Vec<_> = (0..WRITERS)
        .map(|n| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append(
                        &StreamId::from(format!("account-{n}")),
                        ExpectedVersion::Empty,
                        vec![deposit(1)],
                    )
                    .await
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task").expect("append");
    }
    assert_eq!(
        row(&views, "total", "all").await.map(|r| r.0),
        Some(WRITERS)
    );
    drop_schemas().await;
}

/// A plain store (no inline views) still writes no view rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn without_inline_views_nothing_is_written() {
    let pool = fresh_pool(&url(), 2).await;
    let store = PgStore::<Event>::new(pool);
    let views = PgViewStore::<Balance>::new(&store);
    store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Empty,
            vec![deposit(1)],
        )
        .await
        .expect("append");
    assert!(row(&views, "balance", "account-1").await.is_none());
    drop_schemas().await;
}
