//! End-to-end for the snapshot store: save and load against a real
//! Postgres (`snapshots` feature; migration `0002_snapshots`).
//!
//! Requires a database; `EVENTYR_TEST_PG_URL` points at it. The test is
//! `#[ignore]`d by default — run it with
//! `cargo test -p eventyr-store-postgres --features snapshots -- --ignored`
//! once a Postgres is reachable.

#![cfg(feature = "snapshots")]

use eventyr_core::snapshot::Snapshot;
use eventyr_core::vocabulary::{StreamId, Version};
use eventyr_store::snapshot_store::SnapshotStore;
use eventyr_store_postgres::PgStore;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct AccountState {
    open: bool,
    balance: u64,
}

fn new_stream() -> StreamId {
    StreamId::from(format!("account-{}", Uuid::new_v4()))
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn snapshot_save_and_load_roundtrip() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let store = PgStore::<()>::connect(&url)
        .await
        .expect("connect and migrate");
    let store = PgStore::<AccountState>::new(store.pool().clone());
    let stream = new_stream();

    // Unknown stream: no snapshot, not an error.
    let missing = SnapshotStore::load(&store, &stream)
        .await
        .expect("load against the table");
    assert!(missing.is_none());

    // Save, read back: the row decodes into the state and version.
    let snapshot = Snapshot {
        stream_id: stream.clone(),
        version: Version::new(4),
        state: AccountState {
            open: true,
            balance: 42,
        },
    };
    SnapshotStore::save(&store, snapshot.clone())
        .await
        .expect("save");
    let loaded = SnapshotStore::load(&store, &stream)
        .await
        .expect("load")
        .expect("saved");
    assert_eq!(loaded, snapshot);

    // A newer save replaces — the newest wins, in one upsert.
    let newer = Snapshot {
        stream_id: stream.clone(),
        version: Version::new(9),
        state: AccountState {
            open: true,
            balance: 100,
        },
    };
    SnapshotStore::save(&store, newer.clone())
        .await
        .expect("save newer");
    let loaded = SnapshotStore::load(&store, &stream)
        .await
        .expect("load")
        .expect("saved");
    assert_eq!(loaded, newer);

    // An out-of-order offer (two commits racing, stale save landing
    // last) cannot regress the row: the upsert's WHERE drops it.
    let stale = Snapshot {
        stream_id: stream.clone(),
        version: Version::new(5),
        state: AccountState {
            open: true,
            balance: 1,
        },
    };
    SnapshotStore::save(&store, stale)
        .await
        .expect("stale save");
    let loaded = SnapshotStore::load(&store, &stream)
        .await
        .expect("load")
        .expect("saved");
    assert_eq!(loaded, newer, "an older offer never regresses the row");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
async fn snapshot_offers_persist_through_the_store_directly() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let store = PgStore::<AccountState>::connect(&url)
        .await
        .expect("connect and migrate");
    let stream = new_stream();

    // `PgStore<E>` *is* the `SnapshotStore<State = E>`: no adapter
    // layer. A commit's offer routes straight to `save` on the store —
    // the fire-and-forget caller drops the result.
    let offer = Snapshot {
        stream_id: stream.clone(),
        version: Version::new(3),
        state: AccountState {
            open: true,
            balance: 7,
        },
    };
    SnapshotStore::save(&store, offer)
        .await
        .expect("the offer persists through the store's pool");
    let loaded = SnapshotStore::load(&store, &stream)
        .await
        .expect("load")
        .expect("persisted");
    assert_eq!(loaded.version, Version::new(3));
    assert_eq!(loaded.state.balance, 7);

    // Fire-and-forget stays safe under a stale offer: the store's
    // monotonic upsert drops it and the persisted snapshot is untouched.
    let stale = Snapshot {
        stream_id: stream.clone(),
        version: Version::new(2),
        state: AccountState {
            open: true,
            balance: 0,
        },
    };
    SnapshotStore::save(&store, stale)
        .await
        .expect("stale offer");
    let loaded = SnapshotStore::load(&store, &stream)
        .await
        .expect("load")
        .expect("persisted");
    assert_eq!(loaded.version, Version::new(3), "regression did not land");
    assert_eq!(loaded.state.balance, 7);
}
