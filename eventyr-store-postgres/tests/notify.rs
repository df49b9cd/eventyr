//! The Postgres commit signal (0.7.2) against a real Postgres.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::event_name::EventName;
use eventyr_core::subscription::SubscriptionPolicy;
use eventyr_core::vocabulary::{ExpectedVersion, StreamId};
use eventyr_store::notify::{CommitListener, CommitSignal};
use eventyr_store::store::EventStore;
use eventyr_store_postgres::{PgCommitSignal, PgStore};
use eventyr_subscription::prelude::{
    InMemoryCheckpointStore, Projection, Projector, StoreSubscription,
};
use futures::FutureExt;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
enum Event {
    Happened { value: u64 },
}

impl EventName for Event {
    fn event_name(&self) -> &'static str {
        "Happened"
    }
}

/// The channel is database-wide, so concurrently running tests would
/// wake each other's listeners, and the tests that assert *silence*
/// would see a neighbour's commit. Every test here holds this lock for
/// its whole body.
static QUIET: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn fresh_store(url: &str) -> PgStore<Event> {
    let schema = format!("notify_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::PgPool::connect(url).await.expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    admin.close().await;
    let options: sqlx::postgres::PgConnectOptions = url.parse().expect("url");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .expect("connect to schema");
    eventyr_store_postgres::store::migrate(&pool)
        .await
        .expect("migrate");
    PgStore::new(pool)
}

async fn append(store: &PgStore<Event>, stream: &str, value: u64) {
    store
        .append(
            &StreamId::from(stream),
            ExpectedVersion::Any,
            vec![NewEvent::new(Event::Happened { value })],
        )
        .await
        .expect("append");
}

fn url() -> String {
    std::env::var("EVENTYR_TEST_PG_URL").expect("EVENTYR_TEST_PG_URL must point at a real Postgres")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn a_commit_resolves_an_armed_listener() {
    let _quiet = QUIET.lock().await;
    let store = fresh_store(&url()).await;
    let mut listener = PgCommitSignal::new(&store)
        .subscribe()
        .await
        .expect("listen");
    // Nothing yet.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), listener.committed())
            .await
            .is_err()
    );
    append(&store, "s-1", 1).await;
    tokio::time::timeout(Duration::from_secs(5), listener.committed())
        .await
        .expect("woken within 5s")
        .expect("no error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn a_commit_between_arming_and_waiting_is_not_lost() {
    let _quiet = QUIET.lock().await;
    let store = fresh_store(&url()).await;
    let mut listener = PgCommitSignal::new(&store)
        .subscribe()
        .await
        .expect("listen");
    append(&store, "s-1", 1).await;
    // The commit happened before anyone waited.
    tokio::time::timeout(Duration::from_secs(5), listener.committed())
        .await
        .expect("the earlier commit is remembered")
        .expect("no error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn a_rolled_back_append_does_not_notify() {
    let _quiet = QUIET.lock().await;
    let store = fresh_store(&url()).await;
    let mut listener = PgCommitSignal::new(&store)
        .subscribe()
        .await
        .expect("listen");
    let mut tx = store.pool().begin().await.expect("begin");
    sqlx::query(
        "SELECT * FROM append_events(0::smallint, 0, 's-1', ARRAY['Happened'], \
         ARRAY['{\"Happened\": {\"value\": 1}}'::jsonb], ARRAY[NULL]::text[], ARRAY[NULL]::text[], ARRAY[NULL]::text[])",
    )
    .execute(&mut *tx)
    .await
    .expect("append in tx");
    tx.rollback().await.expect("rollback");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), listener.committed())
            .await
            .is_err(),
        "a rollback must not wake listeners"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn a_dropped_wait_does_not_lose_the_notification() {
    let _quiet = QUIET.lock().await;
    let store = fresh_store(&url()).await;
    let mut listener = PgCommitSignal::new(&store)
        .subscribe()
        .await
        .expect("listen");
    // Start a wait and abandon it, the way the driver's select does.
    assert!(listener.committed().now_or_never().is_none());
    append(&store, "s-1", 1).await;
    tokio::time::timeout(Duration::from_secs(5), listener.committed())
        .await
        .expect("woken after the abandoned wait")
        .expect("no error");
}

/// End to end: a caught-up projector over Postgres with a one-hour idle
/// sleep applies a new event within seconds when woken by the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn a_commit_wakes_an_idle_projector_over_postgres() {
    let _quiet = QUIET.lock().await;
    #[derive(Clone, Default)]
    struct Seen(Arc<Mutex<Vec<u64>>>);
    impl Projection for Seen {
        type Event = Event;
        type Error = core::convert::Infallible;
        async fn apply(&mut self, event: &EventEnvelope<Event>) -> Result<(), Self::Error> {
            let Event::Happened { value } = event.event;
            self.0.lock().expect("poisoned").push(value);
            Ok(())
        }
    }

    let store = fresh_store(&url()).await;
    append(&store, "s-1", 1).await;

    let seen = Seen::default();
    let projector = Projector::new(
        "pg-woken",
        StoreSubscription::new(store.clone()),
        InMemoryCheckpointStore::new(),
        seen.clone(),
    )
    .with_policy(SubscriptionPolicy::new(
        64,
        Duration::from_secs(3600),
        Duration::from_secs(1),
    ))
    .wake_on(PgCommitSignal::new(&store));
    let run = tokio::spawn(projector.run_woken(tokio::time::sleep));

    let wait_for = |count: usize| {
        let seen = seen.clone();
        async move {
            while seen.0.lock().expect("poisoned").len() < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait_for(1))
        .await
        .expect("catch-up");
    append(&store, "s-2", 2).await;
    tokio::time::timeout(Duration::from_secs(5), wait_for(2))
        .await
        .expect("the NOTIFY woke the projector well before its idle sleep");
    assert_eq!(*seen.0.lock().expect("poisoned"), vec![1, 2]);
    run.abort();
}
