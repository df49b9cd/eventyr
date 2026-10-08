//! # Balances as an inline view — the transactional read model, §14 /
//! 0.7.3.
//!
//! Two answers to "what is account 7's balance, right now?":
//!
//! - **Inline** (roadmap 0.7.3): the store folds every committed event into the
//!   view's row inside the append's own transaction, so a read straight
//!   after the write sees it. SQLite and Postgres maintain it; a row
//!   that cannot be folded fails the append. Keep inline views cheap —
//!   they run inside the append's serialized section.
//! - **Async** (roadmap 0.6.3): a `ViewProjection` replays the log into the
//!   same row shape, eventually consistently — the same `View`,
//!   the same rows, so a view moves between the two without a rewrite.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example inline_view --features sqlite_views
//! ```

#![allow(dead_code)] // A demo: the point is the focused feature.

use std::sync::Arc;

use eventyr::prelude::*;
use eventyr::projection::prelude::{InMemoryViewStore, View, ViewProjection, ViewStore};
use eventyr::sqlite::{SqliteStore, SqliteViewStore};
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::Projection;

/// One account's money history.
// `crate = "eventyr"`: this example lives in the `eventyr` package
// itself, so the derive cannot resolve the target from the manifest —
// a user's crate would not need the attribute.
#[derive(Clone, Debug, PartialEq, EventName, serde::Serialize, serde::Deserialize)]
#[eventyr(crate = "eventyr")]
enum MoneyEvent {
    Opened { account: u64 },
    Deposited { account: u64, amount: u64 },
    Withdrawn { account: u64, amount: u64 },
}

/// The row: a running balance. `View::apply` never fails — anything an
/// event cannot contribute is decided upstream.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Balance {
    total: u64,
}
impl View<MoneyEvent> for Balance {
    fn initial() -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &EventEnvelope<MoneyEvent>) {
        match &event.event {
            MoneyEvent::Opened { .. } => {}
            MoneyEvent::Deposited { amount, .. } => self.total += amount,
            MoneyEvent::Withdrawn { amount, .. } => self.total -= amount,
        }
    }
}

/// A row per `account-N` — the same key function drives both paths.
fn row_of(event: &EventEnvelope<MoneyEvent>) -> Option<String> {
    Some(event.stream_id.as_str().to_owned())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // -- inline: the row is written in the append's transaction -------
    let store = SqliteStore::<MoneyEvent>::open_in_memory()
        .expect("open")
        .with_inline_views(vec![Arc::new(eventyr::projection::inline::Inline::<
            Balance,
            _,
        >::new("balance", row_of))]);
    let view_store = SqliteViewStore::<Balance>::new(&store);

    let account = StreamId::from("account-7");
    store
        .append(
            &account,
            ExpectedVersion::Empty,
            vec![
                NewEvent::new(MoneyEvent::Opened { account: 7 }),
                NewEvent::new(MoneyEvent::Deposited {
                    account: 7,
                    amount: 100,
                }),
            ],
        )
        .await
        .expect("append");
    // No projector has run, and none needs to: the append's own
    // transaction wrote the row.
    let row = view_store
        .load("balance", "account-7")
        .await
        .expect("load")
        .expect("the row exists the moment the append commits");
    assert_eq!(row.value.total, 100);
    println!("inline: after the append commits, balance = 100 with no projector run");

    store
        .append(
            &account,
            ExpectedVersion::Any,
            vec![NewEvent::new(MoneyEvent::Withdrawn {
                account: 7,
                amount: 30,
            })],
        )
        .await
        .expect("append");
    assert_eq!(
        view_store
            .load("balance", "account-7")
            .await
            .expect("load")
            .expect("row")
            .value
            .total,
        70
    );

    // -- async: the same `View` driven as a projection ----------------
    // Rebuild-first: fold the global stream, checkpoint, done. Rows are
    // read back through the projection's own store — an in-memory one
    // here, so the example stays about the two driving modes, not the
    // second storage backend. (Any `ViewStore` works, including the
    // SQLite one above; a view moves between inline and async without
    // a rewrite.)
    let memory = InMemoryViewStore::<Balance>::new();
    let mut async_view = ViewProjection::new("balance", memory, row_of);
    for envelope in futures::TryStreamExt::try_collect::<Vec<_>>(store.stream_all(Sequence::START))
        .await
        .expect("replay")
    {
        Projection::apply(&mut async_view, &envelope)
            .await
            .expect("fold");
    }
    let rebuilt = async_view
        .store()
        .load("balance", "account-7")
        .await
        .expect("load")
        .expect("rebuilt row");
    assert_eq!(
        rebuilt.value,
        70.into(),
        "inline and async fold the same log into the same row"
    );
    println!("async: replaying the log folds the same row — a view moves between the two paths");
}

// `u64` is not a `Balance`; keep the assert above legible.
impl From<u64> for Balance {
    fn from(total: u64) -> Self {
        Self { total }
    }
}
