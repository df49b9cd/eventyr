//! The `leases` implementation against a real Postgres: two pools, one
//! name — exactly one driver holds it.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

#![cfg(feature = "leases")]

mod common;

use std::time::Duration;

use eventyr_store_postgres::leases::PgLeaseStore;
use eventyr_subscription::lease::{LeaseError, LeasePolicy, ProjectorLease, lease_store_contract};

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_lease_store_passes_the_lease_store_contract() {
    let url = common::url();
    let runtime = common::runtime();
    {
        let _guard = runtime.enter();
        let lease_store = PgLeaseStore::new(&common::fresh_store::<
            eventyr_store_testing::PayloadEvent,
        >(&runtime, &url, "leases"));
        lease_store_contract(|| lease_store.clone());
    }
    runtime.block_on(common::cleanup_schemas());
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn two_pools_cannot_hold_one_name() {
    let url = common::url();
    let runtime = common::runtime();
    // Two lease stores over genuinely separate pools on the same
    // schema: the point of the lease is that two drivers behind
    // separate pools still share the row — the reason it is a row, not
    // an advisory lock (advisory locks are connection-pinned, and a
    // pool hands renewals to whichever connection is free).
    let schema = runtime.block_on(common::make_schema(&url, "leases", 4));
    let one = PgLeaseStore::from_pool(schema.pool.clone());
    let two = {
        // A second pool pinned to the same schema, the way every
        // `make_schema` pins one: the startup `search_path` option.
        let options: sqlx::postgres::PgConnectOptions = url.parse().expect("url");
        let pool = runtime
            .block_on(async {
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(4)
                    .acquire_timeout(Duration::from_secs(30))
                    .idle_timeout(Duration::from_secs(5))
                    .connect_with(options.options([("search_path", schema.name.as_str())]))
                    .await
            })
            .expect("connect to schema");
        PgLeaseStore::from_pool(pool)
    };
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
    // Close this test's pools and tear the schema down; the shared
    // registry cleanup would wait on the stores' pool clones.
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
