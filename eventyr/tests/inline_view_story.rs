//! The inline_view example's mirroring suite (DESIGN §0.5.4): the same
//! scenario — same-fold inline and async, the same View, the same
//! rows — pinned by tests, without running the example's `main`.
//!
//! The scenario mirrors `examples/inline_view.rs`; keep the two in step.

#![cfg(feature = "sqlite_views")]

use std::sync::Arc;

use eventyr::prelude::*;
use eventyr::projection::prelude::{InMemoryViewStore, View, ViewProjection, ViewStore};
use eventyr::sqlite::{SqliteStore, SqliteViewStore};
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::Projection;

#[derive(Clone, Debug, PartialEq, EventName, serde::Serialize, serde::Deserialize)]
#[eventyr(crate = "eventyr")]
enum MoneyEvent {
    Opened { account: u64 },
    Deposited { account: u64, amount: u64 },
    Withdrawn { account: u64, amount: u64 },
}

impl Tagged for MoneyEvent {
    fn tags(&self) -> Vec<Tag> {
        let (account, kind) = match self {
            MoneyEvent::Opened { account } => (account, "open"),
            MoneyEvent::Deposited { account, .. } => (account, "deposit"),
            MoneyEvent::Withdrawn { account, .. } => (account, "withdrawal"),
        };
        vec![Tag::of("account", account), Tag::of("kind", kind)]
    }
}

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

fn row_of(event: &EventEnvelope<MoneyEvent>) -> Option<String> {
    Some(event.stream_id.as_str().to_owned())
}

#[tokio::test]
async fn the_inline_row_is_current_inside_the_append() {
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
            vec![NewEvent::new(MoneyEvent::Deposited {
                account: 7,
                amount: 100,
            })],
        )
        .await
        .expect("append");

    // The row exists the moment the append commits — no projector ran.
    let row = view_store
        .load("balance", "account-7")
        .await
        .expect("load")
        .expect("the row exists");
    assert_eq!(row.value.total, 100);
}

#[tokio::test]
async fn the_async_fold_matches_the_inline_one() {
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

    // The same `View` folded as a projection: one store drives the
    // inline row, the replayed envelope drives the in-memory one. The
    // fold is the point of the example, so the totals must match.
    let inline_total = view_store
        .load("balance", "account-7")
        .await
        .expect("load")
        .expect("inline row")
        .value
        .total;

    let memory = InMemoryViewStore::<Balance>::new();
    let mut projection = ViewProjection::new("balance", memory, row_of);
    for envelope in futures::TryStreamExt::try_collect::<Vec<_>>(store.stream_all(Sequence::START))
        .await
        .expect("replay")
    {
        Projection::apply(&mut projection, &envelope)
            .await
            .expect("fold");
    }
    let replayed_total = projection
        .store()
        .load("balance", "account-7")
        .await
        .expect("load")
        .expect("replayed row")
        .value
        .total;

    assert_eq!(inline_total, 70);
    assert_eq!(replayed_total, 70, "inline and async fold the same log");
}
