//! The shared schema lifecycle for the Postgres test binaries: one
//! fresh schema per store, registered for cleanup, so a dev database
//! does not fill with test schemas.
//!
//! Every `#[ignore]`d test file here runs against a real Postgres
//! (`EVENTYR_TEST_PG_URL`); each store it builds lives in its own
//! schema, and [`cleanup_schemas`] drops them all at the test's end.
//!
//! Each test binary compiles this module and uses the subset of
//! helpers its tests need — the rest stay for the next file.

use std::time::Duration;

/// A created schema, kept around until [`cleanup_schemas`] drops it at
/// the test's end, so a dev database does not fill with test schemas.
pub struct Schema {
    /// The pool bound to the schema (the store's).
    pub pool: sqlx::PgPool,
    /// A second pool on the default schema, for the `DROP SCHEMA`.
    pub admin: sqlx::PgPool,
    pub name: String,
}

static SCHEMAS: std::sync::Mutex<Vec<Schema>> = std::sync::Mutex::new(Vec::new());

/// Create a fresh schema named `{prefix}_{uuid}`, pin a pool's
/// `search_path` to it, and migrate into it.
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

/// A store over a fresh, empty schema, registered for cleanup.
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
    SCHEMAS.lock().expect("registry").push(schema);
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
    SCHEMAS.lock().expect("registry").push(schema);
    store
}

/// A fresh, empty schema's pool, registered for cleanup — for tests
/// that build more than a store over it (view stores, lease stores).
#[allow(dead_code)] // each test binary uses a subset of the helpers
pub async fn fresh_pool(url: &str, prefix: &str, connections: u32) -> sqlx::PgPool {
    let schema = make_schema(url, prefix, connections).await;
    let pool = schema.pool.clone();
    SCHEMAS.lock().expect("registry").push(schema);
    pool
}

/// Drop every schema a test created; call at the test's end.
#[allow(dead_code)] // each test binary uses a subset of the helpers
pub async fn cleanup_schemas() {
    let schemas: Vec<Schema> = std::mem::take(&mut *SCHEMAS.lock().expect("registry"));
    for schema in schemas {
        schema.pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            schema.name
        )))
        .execute(&schema.admin)
        .await
        .expect("drop schema");
        schema.admin.close().await;
    }
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
