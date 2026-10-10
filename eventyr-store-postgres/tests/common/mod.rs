//! The shared schema lifecycle for the Postgres test binaries: one
//! fresh schema per store, each owned by the test thread that created
//! it, so a dev database does not fill with test schemas.
//!
//! Every `#[ignore]`d test file here runs against a real Postgres
//! (`EVENTYR_TEST_PG_URL`); each store it builds lives in its own
//! schema. [`SchemaScope`] owns the schemas one test creates and drops
//! them — at [`finish`](SchemaScope::finish), on the test's own
//! runtime — so tests in one binary may run in parallel: a finishing
//! test drops only its own schemas, never another test's live ones.
//!
//! Each test binary compiles this module and uses the subset of
//! helpers its tests need — the rest stay for the next file.

use std::time::Duration;

/// A created schema: the pools and the name, until its scope drops it.
pub struct Schema {
    /// The pool bound to the schema (the store's).
    pub pool: sqlx::PgPool,
    /// A second pool on the default schema, for the `DROP SCHEMA`.
    pub admin: sqlx::PgPool,
    pub name: String,
}

impl Schema {
    /// Close the schema's pools and drop it. Awaited on the caller's
    /// runtime at the test's end; a failure is the test's failure.
    pub async fn drop(self) {
        self.pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.name
        )))
        .execute(&self.admin)
        .await
        .expect("drop schema");
        self.admin.close().await;
    }
}

/// The schemas one test created. The helpers register into the
/// *current thread's* scope, and libtest runs each test on one thread,
/// so parallel tests in one binary never see each other's schemas.
///
/// [`finish`](Self::finish) drops exactly this scope's schemas and
/// must be awaited inside the test's runtime. A test that fails
/// before its `finish` leaves its scope behind on the thread; the
/// next `open` on that thread (the next test, in a serial run) takes
/// the slot over and drops the leftover pools — its schemas stay as
/// clutter in the dev database until the next run against it prunes,
/// which is the lesser evil to a suite that cannot run in parallel.
pub struct SchemaScope {
    schemas: Vec<Schema>,
}

thread_local! {
    static SCOPE: std::cell::RefCell<Option<std::rc::Rc<std::cell::RefCell<Vec<Schema>>>>> =
        const { std::cell::RefCell::new(None) };
}

impl SchemaScope {
    /// Open the scope the current thread's schema creations register
    /// into. One per test: a leftover scope (its test failed before
    /// finishing) is replaced — its pools close, best-effort — so a
    /// failed test never breaks the next one's run.
    pub fn open() -> Self {
        SCOPE.with(|scope| {
            *scope.borrow_mut() = Some(std::rc::Rc::new(std::cell::RefCell::new(Vec::new())));
        });
        Self {
            schemas: Vec::new(),
        }
    }

    fn register(schema: Schema) {
        SCOPE.with(|scope| {
            let slot = scope.borrow();
            if let Some(registry) = slot.as_ref() {
                registry.borrow_mut().push(schema);
            } else {
                panic!("a schema was created outside a schema scope");
            }
        });
    }

    /// Take this scope's schemas back from the thread's registry and
    /// drop them all, awaited on the test's runtime. Call once, at the
    /// test's end.
    pub async fn finish(mut self) {
        let mut schemas = Vec::new();
        SCOPE.with(|scope| {
            let mut slot = scope.borrow_mut();
            if let Some(registry) = slot.take() {
                schemas = std::mem::take(&mut *registry.borrow_mut());
            }
        });
        self.schemas.append(&mut schemas);
        for schema in self.schemas.drain(..) {
            schema.drop().await;
        }
    }
}

/// Create a fresh schema named `{prefix}_{uuid}`, pin a pool's
/// `search_path` to it, migrate into it, and register it in the
/// current thread's scope.
///
/// One connection on the admin pool, closed promptly when idle: a
/// contract run holds one of these per check until its test's cleanup,
/// and the default 100-connection server must serve every concurrently
/// running test.
#[allow(dead_code)] // each test binary uses a subset of the helpers
pub async fn make_schema(url: &str, prefix: &str, connections: u32) -> Schema {
    let name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(Duration::from_secs(5))
        .connect(url)
        .await
        .expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {name}")))
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
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(5))
        .connect_with(options.options([("search_path", name.as_str())]))
        .await
        .expect("connect to schema");
    eventyr_store_postgres::store::migrate(&pool)
        .await
        .expect("migrate");
    Schema { pool, admin, name }
}

/// A store over a fresh, empty schema, registered into the current
/// thread's scope.
///
/// The suite calls this once per check and expects an empty store each
/// time — `stream_all` reads the whole log, so a shared database would
/// hand later checks every earlier check's events.
///
/// `futures::executor::block_on`, not `Runtime::block_on`: the caller
/// holds the runtime's enter guard, and an entered runtime refuses to
/// be block_on'd again from the same thread. The executor parks the
/// thread; the guard's reactor drives the sqlx futures' I/O. The
/// runtime parameter names which reactor that is.
#[allow(dead_code)] // each test binary uses a subset of the helpers
pub fn fresh_store<E>(
    _runtime: &tokio::runtime::Runtime,
    url: &str,
    prefix: &str,
) -> eventyr_store_postgres::PgStore<E> {
    let schema = futures::executor::block_on(make_schema(url, prefix, 1));
    let store = eventyr_store_postgres::PgStore::new(schema.pool.clone());
    SchemaScope::register(schema);
    store
}

/// [`fresh_store`] for callers already inside a runtime, with a pool
/// wide enough for real concurrency.
#[allow(dead_code)] // each test binary uses a subset of the helpers
pub async fn fresh_store_async<E>(
    url: &str,
    prefix: &str,
    connections: u32,
) -> eventyr_store_postgres::PgStore<E> {
    let schema = make_schema(url, prefix, connections).await;
    let store = eventyr_store_postgres::PgStore::new(schema.pool.clone());
    SchemaScope::register(schema);
    store
}

/// A fresh, empty schema's pool, registered into the current thread's
/// scope — for tests that build more than a store over it (view
/// stores, lease stores).
#[allow(dead_code)] // each test binary uses a subset of the helpers
pub async fn fresh_pool(url: &str, prefix: &str, connections: u32) -> sqlx::PgPool {
    let schema = make_schema(url, prefix, connections).await;
    let pool = schema.pool.clone();
    SchemaScope::register(schema);
    pool
}

/// The sync-driven suites run under this runtime, entered for the
/// caller: the suite drives each future with
/// `futures::executor::block_on`, which parks this thread, so the I/O
/// reactor has to run on the runtime's own worker threads. A
/// current-thread runtime only drives its reactor inside its own
/// `block_on`, and sqlx's first socket wait would never wake. Two
/// workers, not one: a worker is parked for the whole suite's
/// `block_on`, and the notification I/O a commit wakes still needs one
/// spare.
#[allow(dead_code)] // each test binary uses a subset of the helpers
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

#[allow(dead_code)] // each test binary uses a subset of the helpers
pub fn url() -> String {
    std::env::var("EVENTYR_TEST_PG_URL").expect("EVENTYR_TEST_PG_URL must point at a real Postgres")
}
