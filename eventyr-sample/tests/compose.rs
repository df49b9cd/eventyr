//! The compose-managed deployment: the whole sample — commands, the
//! projection, the read — over a real Postgres its replicas would share.
//!
//! `#[ignore]`d like every Postgres test in the workspace: run
//! `./scripts/dev-services.sh up` (or point `DATABASE_URL` at the
//! compose-managed primary the README describes), then
//!
//! ```sh
//! cargo test -p eventyr-sample --features postgres --test compose -- --include-ignored
//! ```
//!
//! Each run composes into a fresh, throwaway schema and drops it
//! afterwards, so a repeat run against the same database starts empty.

#![cfg(feature = "postgres")]

use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;

use eventyr_sample::domain::Cash;
use eventyr_sample::features::{Deposit, GetBalance, OpenAccount, Transfer, Withdraw};
use eventyr_sample::wiring::{Backend, compose, runners};

/// The `DATABASE_URL` of a reachable Postgres, skipping the test when
/// the variable is not set — the compose-managed deployment and
/// `./scripts/dev-services.sh up` both supply one.
fn database_url() -> Option<String> {
    std::env::var("DATABASE_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

/// Compose the Postgres sample against a throwaway schema, unwrapping
/// the backend tag.
async fn postgres_sample(url: &str) -> eventyr_sample::wiring::compose::PgSample {
    compose(&Backend::Postgres(url.into()))
        .await
        .expect("the Postgres sample composes")
        .postgres()
        .expect("the Postgres backend composes Postgres")
}

/// A schema name that is a valid Postgres identifier: letters, digits,
/// and underscores only — the throwaway name a UUID-simple produces.
fn assert_schema_ident(schema: &str) {
    assert!(
        !schema.is_empty()
            && schema
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "the throwaway schema's name must be an identifier: {schema}"
    );
}

/// Open a fresh schema named for this run and hand back its DSN (with
/// the search_path pinned to it) plus the admin pool that will drop it.
/// The sample's compose migrates into the schema it lands on.
async fn fresh_schema(url: &str) -> anyhow::Result<(String, sqlx::PgPool, String)> {
    let schema = format!("compose_{}", uuid::Uuid::new_v4().simple());
    assert_schema_ident(&schema);
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .context("connect to DATABASE_URL")?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .context("create the throwaway schema")?;
    // Pin the search_path through the DSN's `options`, so the compose's
    // own pool lands on, and migrates, the throwaway schema.
    let separator = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{separator}options=-c%20search_path%3D{schema}");
    Ok((scoped, admin, schema))
}

/// Drop the throwaway schema, best-effort, at the run's end.
async fn drop_schema(admin: sqlx::PgPool, schema: &str) {
    assert_schema_ident(schema);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&admin)
    .await
    .expect("drop the throwaway schema");
    admin.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs DATABASE_URL pointing at the compose-managed Postgres"]
async fn the_compose_managed_backend_runs_the_whole_sample() {
    let Some(url) = database_url() else {
        eprintln!("skipping: DATABASE_URL is not set");
        return;
    };
    let (scoped_url, admin, schema) = fresh_schema(&url).await.expect("open a throwaway schema");

    let run = async {
        let sample = postgres_sample(&scoped_url).await;
        let app = &sample.app;

        for account in [1u64, 2] {
            app.send(OpenAccount {
                account,
                owner: "the bank".into(),
                stamped: Default::default(),
            })
            .await
            .expect("the account opens");
            app.send(Deposit {
                account,
                amount: Cash(100),
                stamped: Default::default(),
            })
            .await
            .expect("the deposit commits");
        }
        app.send(Transfer {
            from: 1,
            to: 2,
            amount: Cash(40),
            key: Some("xfer-1".into()),
            stamped: Default::default(),
        })
        .await
        .expect("the transfer commits");
        app.send(Withdraw {
            account: 2,
            amount: Cash(10),
            stamped: Default::default(),
        })
        .await
        .expect("the withdrawal commits");

        // The projection catches up, then the read path serves the rows
        // the write side committed — over the same store the API replicas
        // share.
        runners::run_projection(&sample.inputs, &sample.shutdown)
            .await
            .expect("the projection catches up");

        let balance_of = |account: u64| async move {
            app.send(GetBalance { account })
                .await
                .expect("the read serves")
                .expect("the account is known")
                .balance
                .0
        };
        // The saga is not run in this test, so the balances reflect only
        // the commands above: 100 - 40, and 100 + 40 - 10.
        assert_eq!(balance_of(1).await, 60, "account 1: the withdrawal of 40");
        assert_eq!(
            balance_of(2).await,
            130,
            "account 2: the deposit of 40, then a 10 withdrawal"
        );
    }
    .await;

    drop_schema(admin, &schema).await;
    run
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs DATABASE_URL pointing at the compose-managed Postgres"]
async fn the_saga_runs_against_the_shared_store() {
    let Some(url) = database_url() else {
        eprintln!("skipping: DATABASE_URL is not set");
        return;
    };
    let (scoped_url, admin, schema) = fresh_schema(&url).await.expect("open a throwaway schema");

    let run = async {
        let sample = postgres_sample(&scoped_url).await;
        let app = &sample.app;

        const ACCOUNT: u64 = 7;
        const DEPOSIT: u64 = 100;
        // The wire fee is a tenth; the deposit crosses the minimum, so
        // one charge lands.
        const FEE: u64 = DEPOSIT / 10;
        const EXPECTED: u64 = DEPOSIT - FEE;
        // The saga gets a bounded window to react.
        const SAGA_WINDOW: Duration = Duration::from_secs(10);
        const RETRY: Duration = Duration::from_millis(50);

        app.send(OpenAccount {
            account: ACCOUNT,
            owner: "the bank".into(),
            stamped: Default::default(),
        })
        .await
        .expect("the account opens");
        app.send(Deposit {
            account: ACCOUNT,
            amount: Cash(DEPOSIT),
            stamped: Default::default(),
        })
        .await
        .expect("the deposit commits");

        let saga = tokio::spawn({
            let inputs = sample.inputs.clone();
            let app = sample.app.clone();
            let shutdown = sample.shutdown.clone();
            async move { runners::run_saga(&inputs, &app, &shutdown).await }
        });
        let deadline = std::time::Instant::now() + SAGA_WINDOW;
        let balance = loop {
            runners::run_projection(&sample.inputs, &sample.shutdown)
                .await
                .expect("the projection catches up");
            let balance = app
                .send(GetBalance { account: ACCOUNT })
                .await
                .expect("the read serves")
                .expect("the account is known")
                .balance
                .0;
            if balance == EXPECTED {
                break balance;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the wire fee of {FEE} committed within {SAGA_WINDOW:?} (balance is {balance}, want {EXPECTED})"
            );
            tokio::time::sleep(RETRY).await;
        };
        let _ = sample.shutdown.send(true);
        let _ = saga.await;
        assert_eq!(
            balance, EXPECTED,
            "the deposit of {DEPOSIT} less the wire fee of {FEE}"
        );
    }
    .await;

    drop_schema(admin, &schema).await;
    run
}
