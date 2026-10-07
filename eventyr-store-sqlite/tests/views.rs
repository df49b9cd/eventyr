//! Views over SQLite: the `ViewStore` port and inline views (0.7.3).

use std::sync::Arc;

use eventyr_core::batch::StreamAppend;
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
use eventyr_projection::inline::{Inline, InlineView};
use eventyr_projection::view::{View, ViewRow, ViewStore};
use eventyr_store::store::{EventStore, StreamsAll};
use eventyr_store_sqlite::{SqliteStore, SqliteViewStore};
use futures::TryStreamExt;
use futures::executor::block_on;
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

fn balance() -> Arc<dyn InlineView<Event>> {
    Arc::new(Inline::<Balance, _>::new(
        "balance",
        |e: &EventEnvelope<Event>| Some(e.stream_id.as_str().to_owned()),
    ))
}

fn total() -> Arc<dyn InlineView<Event>> {
    Arc::new(Inline::<Balance, _>::new(
        "total",
        |_: &EventEnvelope<Event>| Some("all".to_owned()),
    ))
}

fn deposit(amount: u64) -> NewEvent<Event> {
    NewEvent::new(Event::Deposited { amount })
}

fn row(views: &SqliteViewStore<Balance>, name: &str, id: &str) -> Option<(u64, Sequence)> {
    block_on(views.load(name, id))
        .expect("load")
        .map(|row| (row.value.0, row.version))
}

#[test]
fn the_view_store_passes_the_contract() {
    eventyr_projection::view::view_store_contract(
        || SqliteViewStore::<Balance>::new(&SqliteStore::<Event>::open_in_memory().expect("open")),
        |version| Balance(version * 10),
    );
}

#[test]
fn inline_rows_are_written_by_the_append() {
    let store = SqliteStore::<Event>::open_in_memory()
        .expect("open")
        .with_inline_views(vec![balance(), total()]);
    let views = SqliteViewStore::<Balance>::new(&store);

    let committed = block_on(store.append(
        &StreamId::from("account-1"),
        ExpectedVersion::Empty,
        vec![deposit(10), deposit(5)],
    ))
    .expect("append");
    assert_eq!(
        row(&views, "balance", "account-1"),
        Some((15, committed[1].sequence))
    );

    block_on(store.append_batch(vec![
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
    ]))
    .expect("batch");
    assert_eq!(row(&views, "balance", "account-1").map(|r| r.0), Some(16));
    assert_eq!(row(&views, "balance", "account-2").map(|r| r.0), Some(100));
    assert_eq!(row(&views, "total", "all").map(|r| r.0), Some(116));
}

#[test]
fn a_view_that_cannot_fold_fails_the_append() {
    let conn = rusqlite::Connection::open_in_memory().expect("open");
    let store = SqliteStore::<Event>::from_connection(conn)
        .expect("schema")
        .with_inline_views(vec![balance()]);
    // A stored row the view's type cannot decode, written through the
    // async port (any JSON is a valid row there).
    block_on(SqliteViewStore::<String>::new(&store).save(
        "balance",
        "account-1",
        ViewRow {
            version: Sequence::START,
            value: "not a number".to_owned(),
        },
    ))
    .expect("seed");

    let error = block_on(store.append(
        &StreamId::from("account-1"),
        ExpectedVersion::Empty,
        vec![deposit(1)],
    ))
    .expect_err("the view cannot fold");
    assert!(error.to_string().contains("balance"), "{error}");
    let events: Vec<_> = block_on(store.stream_all(Sequence::START).try_collect()).expect("read");
    assert!(events.is_empty(), "no event outlives its failed view write");
}

#[test]
fn without_inline_views_nothing_is_written() {
    let store = SqliteStore::<Event>::open_in_memory().expect("open");
    let views = SqliteViewStore::<Balance>::new(&store);
    block_on(store.append(
        &StreamId::from("account-1"),
        ExpectedVersion::Empty,
        vec![deposit(1)],
    ))
    .expect("append");
    assert!(row(&views, "balance", "account-1").is_none());
}

/// Inline and async maintain the same rows: an async `ViewProjection`
/// replaying the whole log over rows an inline view already folded
/// changes nothing: the newest-wins guard absorbs every event. That is
/// what lets a view move from inline to async without a rebuild.
#[test]
fn an_async_replay_over_inline_rows_folds_nothing_twice() {
    use eventyr_projection::view::ViewProjection;
    use eventyr_subscription::runner::Projection;

    let inline = SqliteStore::<Event>::open_in_memory()
        .expect("open")
        .with_inline_views(vec![balance()]);
    block_on(inline.append(
        &StreamId::from("account-1"),
        ExpectedVersion::Empty,
        vec![deposit(10), deposit(5)],
    ))
    .expect("append");

    let views = SqliteViewStore::<Balance>::new(&inline);
    let mut projection = ViewProjection::<Balance, _, _, Event>::new(
        "balance",
        views.clone(),
        |e: &EventEnvelope<Event>| Some(e.stream_id.as_str().to_owned()),
    );

    // Replay everything inline already folded: nothing changes.
    let log: Vec<_> = block_on(inline.stream_all(Sequence::START).try_collect()).expect("read");
    for event in &log {
        block_on(projection.apply(event)).expect("apply");
    }
    assert_eq!(row(&views, "balance", "account-1").map(|r| r.0), Some(15));
}
