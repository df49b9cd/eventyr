//! End-to-end for the view store: save and load against a real Postgres
//! (migration `0003_views`).
//!
//! Requires a database; `EVENTYR_TEST_PG_URL` points at it. Ignored by
//! default — CI runs a service container and sets the URL.

use eventyr_core::vocabulary::Sequence;
use eventyr_projection::view::{ViewRow, ViewStore};
use eventyr_store_postgres::PgStore;
use eventyr_store_postgres::views::PgViewStore;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Balance {
    balance: u64,
}

fn fresh_name() -> String {
    format!("test-{}", Uuid::new_v4())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn view_save_load_and_newest_wins() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let events = PgStore::<serde_json::Value>::connect(&url)
        .await
        .expect("connect and migrate");
    let store = PgViewStore::<Balance>::new(&events);
    let view_name = fresh_name();

    // Unknown row: None, not an error.
    assert!(
        ViewStore::load(&store, &view_name, "account-1")
            .await
            .expect("load")
            .is_none()
    );

    // Save and read back.
    let row = ViewRow {
        version: Sequence::new(3),
        value: Balance { balance: 42 },
    };
    ViewStore::save(&store, &view_name, "account-1", row.clone())
        .await
        .expect("save");
    let loaded = ViewStore::load(&store, &view_name, "account-1")
        .await
        .expect("load")
        .expect("saved");
    assert_eq!(loaded.version, row.version);
    assert_eq!(loaded.value, row.value);

    // A newer save replaces; an older one is dropped (replay guard).
    let newer = ViewRow {
        version: Sequence::new(7),
        value: Balance { balance: 50 },
    };
    ViewStore::save(&store, &view_name, "account-1", newer.clone())
        .await
        .expect("save newer");
    let stale = ViewRow {
        version: Sequence::new(4),
        value: Balance { balance: 1 },
    };
    ViewStore::save(&store, &view_name, "account-1", stale)
        .await
        .expect("a stale save is absorbed");
    let loaded = ViewStore::load(&store, &view_name, "account-1")
        .await
        .expect("load")
        .expect("saved");
    assert_eq!(loaded.version, Sequence::new(7), "never regresses");
    assert_eq!(loaded.value, newer.value);
}
