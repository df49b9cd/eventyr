//! Inline views (0.7.3) against a real Postgres: rows written in the
//! append's transaction.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres (each test's
//! schemas live in its own `SchemaScope`, so a parallel run is safe;
//! CI pins `--test-threads=1` anyway, because the contract suites'
//! open transactions would exhaust a 100-connection container).

use std::sync::Arc;

use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
use eventyr_projection::inline::{Inline, InlineView};
use eventyr_projection::view::{View, ViewStore};
use eventyr_store::store::EventStore;
mod common;

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
async fn the_store_passes_the_inline_view_contract() {
    let url = common::url();
    let schemas = common::SchemaScope::open();
    // The contract's reads are `block_on`-driven: this runtime is
    // entered, and its workers drive the sqlx I/O while the executor
    // parks this thread (common::runtime's documented shape).
    let _entered = tokio::runtime::Handle::current().enter();
    eventyr_projection::inline::inline_view_contract(
        || vec![balance(), total()],
        |views| {
            let pool = futures::executor::block_on(common::fresh_pool(&url, "inline", 2));
            PgStore::<Event>::new(pool).with_inline_views(views)
        },
        || {
            let pool = futures::executor::block_on(common::fresh_pool(&url, "inline", 2));
            PgStore::<Event>::new(pool)
        },
        |store, stream, events| {
            futures::executor::block_on(store.append(&stream, ExpectedVersion::Empty, events))
                .expect("append")
        },
        |store, appends| {
            let committed =
                futures::executor::block_on(store.append_batch(appends)).expect("batch");
            let mut envelopes = Vec::new();
            for stream in committed {
                envelopes.extend(stream.events);
            }
            envelopes
        },
        |store, name, id| {
            futures::executor::block_on(PgViewStore::<Balance>::new(store).load(name, id))
                .expect("load")
                .map(|row| (Balance(row.value.0), row.version))
        },
        |amount| Event::Deposited { amount },
        Balance,
    );
    schemas.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn a_view_that_cannot_fold_fails_the_append() {
    let schemas = common::SchemaScope::open();
    let pool = common::fresh_pool(&common::url(), "inline", 2).await;
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
    schemas.finish().await;
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
    let schemas = common::SchemaScope::open();
    const WRITERS: u64 = 24;
    let pool = common::fresh_pool(&common::url(), "inline", WRITERS as u32).await;
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
    schemas.finish().await;
}
