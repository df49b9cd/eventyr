//! The `leases` implementation against a real Postgres: two pools, one
//! name — exactly one driver holds it.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

#![cfg(feature = "leases")]

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use eventyr_store_postgres::PgStore;
use eventyr_store_postgres::leases::PgLeaseStore;
use eventyr_store_postgres::store::migrate;
use eventyr_subscription::lease::{LeaseError, LeasePolicy, ProjectorLease, lease_store_contract};

struct Schema {
    pool: sqlx::PgPool,
    admin: sqlx::PgPool,
    name: String,
}

static SCHEMAS: OnceLock<Mutex<Vec<Schema>>> = OnceLock::new();

fn registry() -> &'static Mutex<Vec<Schema>> {
    SCHEMAS.get_or_init(|| Mutex::new(Vec::new()))
}

async fn make_schema(url: &str, connections: u32) -> Schema {
    let name = format!("leases_{}", uuid::Uuid::new_v4().simple());
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
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(5))
        .connect_with(options.options([("search_path", name.as_str())]))
        .await
        .expect("connect to schema");
    migrate(&pool).await.expect("migrate");
    Schema { pool, admin, name }
}

fn fresh_store<E>(_runtime: &tokio::runtime::Runtime, url: &str, connections: u32) -> PgStore<E> {
    let schema = futures::executor::block_on(make_schema(url, connections));
    let store = PgStore::new(schema.pool.clone());
    registry().lock().expect("registry").push(schema);
    store
}

async fn cleanup_schemas() {
    let schemas: Vec<Schema> = std::mem::take(&mut *registry().lock().expect("registry"));
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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

fn url() -> String {
    std::env::var("EVENTYR_TEST_PG_URL").expect("EVENTYR_TEST_PG_URL must point at a real Postgres")
}

#[test]
#[ignore = "needs the Postgres test database"]
fn pg_lease_store_passes_the_lease_store_contract() {
    let url = url();
    let runtime = runtime();
    {
        let _guard = runtime.enter();
        let lease_store = PgLeaseStore::new(&fresh_store::<eventyr_store_testing::PayloadEvent>(
            &runtime, &url, 1,
        ));
        lease_store_contract(|| lease_store.clone());
    }
    runtime.block_on(cleanup_schemas());
}

#[test]
#[ignore = "needs the Postgres test database"]
fn two_pools_cannot_hold_one_name() {
    let url = url();
    let runtime = runtime();
    // Two lease stores over the *same* database: the point of the lease
    // is that two drivers behind separate pools still share the row.
    let schema = runtime.block_on(make_schema(&url, 4));
    let one = PgLeaseStore::from_pool(schema.pool.clone());
    let two = PgLeaseStore::from_pool(schema.pool.clone());
    let policy = LeasePolicy {
        ttl: Duration::from_secs(30),
        grace: 3,
        max_grace: 12,
    };
    runtime.block_on(async {
        let first = one
            .acquire("balance", policy.ttl, policy.grace, policy.max_grace)
            .await
            .expect("the first acquire claims the name");
        assert!(matches!(
            two.acquire("balance", policy.ttl, policy.grace, policy.max_grace)
                .await,
            Err(LeaseError::Taken)
        ));
        one.release(first).await.expect("release");
    });
    // The stores above hold pool handles into the schema's pool; drop
    // them before its admin drops the schema.
    // Close this test's pool itself and tear the schema down; the
    // shared registry cleanup would wait on these two stores' clones.
    runtime.block_on(async move {
        schema.pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            schema.name
        )))
        .execute(&schema.admin)
        .await
        .expect("drop schema");
        schema.admin.close().await;
    });
}
