//! The Postgres commit signal (0.7.2) against a real Postgres: the
//! shared [`commit_signal_contract`] over a timed adapter, plus the
//! behaviour only Postgres has (a `NOTIFY` whose transaction rolls back
//! must not fire).
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::subscription::SubscriptionPolicy;
use eventyr_core::vocabulary::{ExpectedVersion, StreamId, Version};
use eventyr_store::notify::{CommitListener, CommitSignal};
use eventyr_store::store::EventStore;
use eventyr_store_postgres::{PgCommitSignal, PgStore};
use eventyr_store_testing::PayloadEvent;
use eventyr_subscription::prelude::{
    InMemoryCheckpointStore, Projection, Projector, StoreSubscription,
};

/// The channel is database-wide, so concurrently running tests would
/// wake each other's listeners, and the tests that assert *silence*
/// would see a neighbour's commit. Every test here holds this lock for
/// its whole body.
static QUIET: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Schemas this file's tests created, dropped by [`drop_schemas`] so a
/// dev database does not fill with them.
static SCHEMAS: Mutex<Vec<(sqlx::PgPool, sqlx::PgPool, String)>> = Mutex::new(Vec::new());

fn url() -> String {
    std::env::var("EVENTYR_TEST_PG_URL").expect("EVENTYR_TEST_PG_URL must point at a real Postgres")
}

async fn fresh_store(url: &str) -> PgStore<PayloadEvent> {
    let schema = format!("notify_{}", uuid::Uuid::new_v4().simple());
    // One connection, closed promptly when idle: a test's schemas live
    // until its cleanup, and the default 100-connection server must
    // serve every concurrently running test.
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(Duration::from_secs(5))
        .connect(url)
        .await
        .expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    let options: sqlx::postgres::PgConnectOptions = url.parse().expect("url");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        // Bounded waits and prompt idle closes: a failing drop must say
        // so quickly instead of hanging the suite, and the schemas all
        // stay open until their test's cleanup — the server's 100
        // connections serve every concurrently running test.
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(5))
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
    PgStore::new(pool)
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

async fn append(store: &PgStore<PayloadEvent>, stream: &str, value: u64) {
    store
        .append(
            &StreamId::from(stream),
            ExpectedVersion::Any,
            vec![NewEvent::new(PayloadEvent::from(value))],
        )
        .await
        .expect("append");
}

/// The pair the shared contract's bounds ask for: [`EventStore`] through
/// the store, [`CommitSignal`] through the `PgCommitSignal` the NOTIFY
/// feeds.
struct PgWithSignal {
    store: PgStore<PayloadEvent>,
    signal: PgCommitSignal,
}

impl EventStore for PgWithSignal {
    type Event = PayloadEvent;

    async fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<PayloadEvent>>,
    ) -> Result<Vec<EventEnvelope<PayloadEvent>>, StoreError> {
        self.store.append(stream_id, expected, events).await
    }

    async fn append_batch(
        &self,
        appends: Vec<eventyr_core::batch::StreamAppend<PayloadEvent>>,
    ) -> Result<Vec<eventyr_core::batch::CommittedStream<PayloadEvent>>, StoreError> {
        self.store.append_batch(appends).await
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl futures::Stream<Item = Result<EventEnvelope<PayloadEvent>, StoreError>> + Send {
        self.store.stream(stream_id, from)
    }
}

impl CommitSignal for PgWithSignal {
    type Listener = BufferedListener;

    async fn subscribe(&self) -> Result<Self::Listener, StoreError> {
        Ok(BufferedListener {
            inner: self.signal.subscribe().await?,
        })
    }
}

/// A [`CommitListener`] with the synchronous polling semantics the
/// shared contract assumes — unlike a network NOTIFY, whose delivery is
/// asynchronous.
///
/// `committed` re-polls the inner listener after one-millisecond sleeps
/// for up to half a second: a NOTIFY sent by a just-committed append
/// arrives within milliseconds, so the positive checks see `Ready` on an
/// early poll; a check that expects silence runs out the deadline and
/// then parks forever, which the contract's `now_or_never` reads as
/// "did not fire". Polling in a loop with the caller's waker is legal,
/// just impatient — exactly what a test adapter wants.
struct BufferedListener {
    inner: <PgCommitSignal as CommitSignal>::Listener,
}

impl CommitListener for BufferedListener {
    async fn committed(&mut self) -> Result<(), StoreError> {
        use futures::FutureExt;
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if let Some(result) = self.inner.committed().now_or_never() {
                return result;
            }
            if Instant::now() >= deadline {
                return core::future::pending().await;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_commit_signal_passes_the_contract() {
    // Multi-thread, not current-thread: the contract drives each future
    // with `futures::executor::block_on`, which parks this thread, so
    // the I/O reactor has to run on the runtime's own worker thread.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let _quiet = runtime.block_on(QUIET.lock());
    let url = url();
    // Entered for the sync suite so the sqlx futures it polls find
    // this runtime's reactor — and `futures::executor::block_on`, not
    // `Runtime::block_on`, drives them, because an entered runtime
    // refuses to be block_on'd from the same thread.
    {
        let _guard = runtime.enter();
        eventyr_store_testing::commit_signal_contract::<PayloadEvent, _>(|| {
            let store = futures::executor::block_on(fresh_store(&url));
            let signal = PgCommitSignal::new(&store);
            PgWithSignal { store, signal }
        });
    }
    runtime.block_on(drop_schemas());
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
    // INSERT under the migration's function: the NOTIFY it raises is
    // transaction-scoped like everything else, so the rollback eats it.
    sqlx::query(
        "SELECT * FROM append_events(0::smallint, 0, 's-1', ARRAY['Payload'], \
         ARRAY['{\"Payload\": {\"value\": 1}}'::jsonb], \
         ARRAY[NULL]::text[], ARRAY[NULL]::text[], ARRAY[NULL]::text[])",
    )
    .execute(&mut *tx)
    .await
    .expect("append in tx");
    tx.rollback().await.expect("rollback");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.committed())
            .await
            .is_err(),
        "a rollback must not wake listeners"
    );
    drop_schemas().await;
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
        type Event = PayloadEvent;
        type Error = core::convert::Infallible;
        async fn apply(&mut self, event: &EventEnvelope<PayloadEvent>) -> Result<(), Self::Error> {
            let PayloadEvent::Payload { value } = event.event;
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
    drop_schemas().await;
}
