//! # Hello, Postgres — the smallest Postgres deployment, §9.
//!
//! One aggregate, one store, three commands: connect, migrate, write,
//! read back. `distributed.rs` is the full multi-node story; this is
//! the first step before it.
//!
//! The run uses a throwaway schema — created on start, dropped at the
//! end — so it can share a database with anything else, CI included.
//! Without `EVENTYR_PG_URL` it says so and exits.
//!
//! Run with:
//! ```sh
//! EVENTYR_PG_URL=postgres://postgres:postgres@localhost:5432/eventyr \
//!     cargo run -p eventyr --example hello_postgres --features postgres
//! ```

use std::fmt;

use eventyr::postgres::PgStore;
use eventyr::prelude::*;
use eventyr::store::prelude::*;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

// -- the domain -----------------------------------------------------------

/// A counter's identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct CounterId(u64);
impl fmt::Display for CounterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Created {
    name: String,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Bumped;

#[derive(Debug)]
enum CounterCommand {
    Create { name: String },
    Bump,
}

#[derive(Debug, PartialEq)]
enum CounterError {
    AlreadyCreated,
    NotCreated,
}
impl fmt::Display for CounterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyCreated => "the counter already exists",
            Self::NotCreated => "the counter does not exist",
        })
    }
}

#[derive(Clone, Debug, Default)]
struct CounterState {
    created: bool,
    total: u64,
}

// `crate = "eventyr"`: this example lives in the `eventyr` package
// itself, so the derive cannot resolve the target from the manifest —
// a user's crate would not need the attribute.
#[derive(Aggregate)]
#[eventyr(
    crate = "eventyr",
    id = CounterId,
    state = CounterState,
    error = CounterError,
    command = CounterCommand,
    events(Created, Bumped),
    event_derive(serde::Serialize, serde::Deserialize)
)]
struct Counter;

fn apply(state: &mut CounterState, event: &CounterEvent) {
    match event {
        CounterEvent::Created(_) => state.created = true,
        CounterEvent::Bumped(_) => state.total += 1,
    }
}

fn decide(
    state: &CounterState,
    command: &CounterCommand,
) -> Result<Vec<CounterEvent>, CounterError> {
    match command {
        CounterCommand::Create { .. } if state.created => Err(CounterError::AlreadyCreated),
        CounterCommand::Create { name } => Ok(vec![Created { name: name.clone() }.into()]),
        CounterCommand::Bump if !state.created => Err(CounterError::NotCreated),
        CounterCommand::Bump => Ok(vec![Bumped.into()]),
    }
}

// -- the run ------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let Ok(url) = std::env::var("EVENTYR_PG_URL") else {
        println!("hello_postgres: set EVENTYR_PG_URL to run against a Postgres");
        return;
    };

    // A schema of our own, so the migrations and the events table never
    // meet anything else in the database.
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970");
    let schema = format!(
        "hello_postgres_{}_{}",
        std::process::id(),
        since_epoch.as_micros()
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect to EVENTYR_PG_URL");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create the schema");
    let options: PgConnectOptions = url.parse().expect("a Postgres URL");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .expect("connect in the schema");

    // The store assumes its schema: migrate once, then wrap the pool.
    eventyr::postgres::store::migrate(&pool)
        .await
        .expect("migrate");
    let repository = AggregateRepository::<Counter, _>::new(
        PgStore::<CounterEvent>::new(pool.clone()),
        RetryPolicy::default(),
    );

    repository
        .execute(
            CounterId(1),
            CounterCommand::Create {
                name: "visits".into(),
            },
        )
        .await
        .expect("create");
    for _ in 0..2 {
        repository
            .execute(CounterId(1), CounterCommand::Bump)
            .await
            .expect("bump");
    }
    // The domain still decides: creating it twice is refused.
    assert!(matches!(
        repository
            .execute(
                CounterId(1),
                CounterCommand::Create {
                    name: "again".into()
                }
            )
            .await,
        Err(ExecutionError::Domain(CounterError::AlreadyCreated))
    ));

    // Read it back: the state is folded from the rows in Postgres.
    let loaded = repository.load(CounterId(1)).await.expect("load");
    assert_eq!(loaded.version, Version::new(3));
    assert_eq!(loaded.state.total, 2);

    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&admin)
    .await
    .expect("drop the schema");
    println!("hello_postgres: created and bumped twice; version 3 read back from Postgres");
}
