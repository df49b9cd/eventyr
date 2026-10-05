//! The SQLite store against the shared contract suite: the
//! roadmap-0.6.4 proof that a third-party store implements the port by
//! calling the suite, not by shipping its own tests of the same rules.

use eventyr_store_sqlite::SqliteStore;
use eventyr_store_testing::{ParityEvent, PayloadEvent as ContractEvent};

// In memory: one connection, one store, no setup cost per check.
#[test]
fn sqlite_store_passes_the_event_store_contract() {
    eventyr_store_testing::event_store_contract::<ContractEvent, _>(|| {
        SqliteStore::<ContractEvent>::open_in_memory().expect("open")
    });
}

#[test]
fn sqlite_store_passes_the_streams_all_contract() {
    eventyr_store_testing::streams_all_contract::<ContractEvent, _>(|| {
        SqliteStore::<ContractEvent>::open_in_memory().expect("open")
    });
}

#[test]
fn sqlite_store_passes_the_append_batch_contract() {
    eventyr_store_testing::event_store_batch_contract::<ContractEvent, _>(|| {
        SqliteStore::<ContractEvent>::open_in_memory().expect("open")
    });
}

#[cfg(feature = "snapshots")]
#[test]
fn sqlite_store_passes_the_snapshot_contract() {
    use eventyr_store_sqlite::SqliteSnapshotStore;

    eventyr_store_testing::snapshot_contract::<u64, _>(|| {
        let store = SqliteStore::<ContractEvent>::open_in_memory().expect("open");
        SqliteSnapshotStore::new(&store).expect("snapshots")
    });
}

#[test]
fn sqlite_store_passes_the_query_append_contract() {
    eventyr_store_testing::query_append_contract(|| SqliteStore::open_in_memory().expect("open"));
}

#[test]
fn sqlite_store_passes_the_commit_signal_contract() {
    eventyr_store_testing::commit_signal_contract::<ContractEvent, _>(|| {
        SqliteStore::<ContractEvent>::open_in_memory().expect("open")
    });
}

#[test]
fn sqlite_store_passes_the_filtered_read_contract() {
    eventyr_store_testing::filtered_read_contract::<ParityEvent, _>(|| {
        SqliteStore::<ParityEvent>::open_in_memory().expect("open")
    });
}

/// A database created before 0.7.5 has no `idempotency_key` column: the
/// store adds it on open, old rows read back without a key, and new
/// rows keep theirs.
#[test]
fn a_pre_0_7_5_database_gains_the_key_column_on_open() {
    use eventyr_core::envelope::{Metadata, NewEvent};
    use eventyr_core::vocabulary::{ExpectedVersion, StreamId, Version};
    use eventyr_store::store::EventStore;
    use futures::TryStreamExt;
    use futures::executor::block_on;

    let dir = tempfile_dir();
    let path = dir.join("old.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "CREATE TABLE events (
                global_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id       TEXT    NOT NULL,
                stream_version  INTEGER NOT NULL,
                event_type      TEXT    NOT NULL,
                payload         TEXT    NOT NULL,
                causation_id    TEXT,
                correlation_id  TEXT,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE (stream_id, stream_version)
            );
            INSERT INTO events (stream_id, stream_version, event_type, payload)
            VALUES ('s-1', 1, 'Payload', '{\"Payload\":{\"value\":1}}');",
        )
        .expect("old schema");
    }

    let store = SqliteStore::<ContractEvent>::open(&path).expect("reopen migrates");
    block_on(store.append(
        &StreamId::from("s-1"),
        ExpectedVersion::Exact(Version::new(1)),
        vec![NewEvent {
            event: ContractEvent::from(2),
            metadata: Metadata::default().with_idempotency_key("k"),
        }],
    ))
    .expect("append after upgrade");
    let events: Vec<_> = block_on(
        store
            .stream(&StreamId::from("s-1"), Version::EMPTY)
            .try_collect(),
    )
    .expect("read");
    assert_eq!(events[0].metadata.idempotency_key, None);
    assert_eq!(events[1].metadata.idempotency_key.as_deref(), Some("k"));

    // Opening again is a no-op.
    drop(store);
    SqliteStore::<ContractEvent>::open(&path).expect("idempotent open");
    std::fs::remove_dir_all(dir).ok();
}

fn tempfile_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "eventyr-sqlite-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

#[test]
fn sqlite_store_passes_the_lifecycle_contract() {
    eventyr_store_testing::lifecycle_contract::<ContractEvent, _>(|| {
        SqliteStore::<ContractEvent>::open_in_memory().expect("open")
    });
    eventyr_store_testing::lifecycle_query_append_contract(|| {
        SqliteStore::open_in_memory().expect("open")
    });
}

#[test]
fn sqlite_parked_store_passes_the_parked_store_contract() {
    eventyr_subscription::parked::parked_store_contract(|| {
        eventyr_store_sqlite::SqliteParkedStore::<u64>::from_connection(
            rusqlite::Connection::open_in_memory().expect("open"),
        )
        .expect("schema")
    });
}

#[test]
fn sqlite_checkpoint_store_passes_the_checkpoint_store_contract() {
    eventyr_subscription::checkpoint::checkpoint_store_contract(|| {
        eventyr_store_sqlite::SqliteCheckpointStore::from_connection(
            rusqlite::Connection::open_in_memory().expect("open"),
        )
        .expect("schema")
    });
    // Beside an event store, on its connection.
    eventyr_subscription::checkpoint::checkpoint_store_contract(|| {
        let store = SqliteStore::<ContractEvent>::open_in_memory().expect("open");
        eventyr_store_sqlite::SqliteCheckpointStore::beside(&store).expect("schema")
    });
}

/// A checkpoint written through one handle is there after the database
/// is reopened: the point of a durable checkpoint store.
#[test]
fn a_checkpoint_survives_reopening_the_database() {
    use eventyr_core::subscription::Checkpoint;
    use eventyr_core::vocabulary::Sequence;
    use eventyr_store_sqlite::SqliteCheckpointStore;
    use eventyr_subscription::checkpoint::CheckpointStore;
    use futures::executor::block_on;

    let dir = tempfile_dir();
    let path = dir.join("checkpoints.db");
    {
        let store = SqliteCheckpointStore::from_connection(
            rusqlite::Connection::open(&path).expect("open"),
        )
        .expect("schema");
        block_on(store.store("balance", Checkpoint::new(Sequence::new(42)))).expect("store");
    }
    let reopened =
        SqliteCheckpointStore::from_connection(rusqlite::Connection::open(&path).expect("reopen"))
            .expect("schema");
    assert_eq!(
        block_on(reopened.load("balance")).expect("load"),
        Checkpoint::new(Sequence::new(42))
    );
    std::fs::remove_dir_all(dir).ok();
}

/// An event that counts how often it is decoded.
#[derive(Clone, PartialEq, Debug, serde::Serialize)]
struct Counted(u64);

static DECODED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl<'de> serde::Deserialize<'de> for Counted {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        DECODED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        <u64 as serde::Deserialize>::deserialize(deserializer).map(Self)
    }
}

impl eventyr_core::event_name::EventName for Counted {
    fn event_name(&self) -> &'static str {
        "Counted"
    }
}

/// Reading a few events from the global stream reads one page, not the
/// log's tail: a subscriber polls `stream_all(checkpoint).take(batch)`
/// every round, so a read that decoded the whole tail would make
/// catch-up quadratic.
#[test]
fn a_short_global_read_stops_at_its_page() {
    use eventyr_core::envelope::NewEvent;
    use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
    use eventyr_store::store::{EventStore, StreamsAll};
    use futures::StreamExt;
    use futures::executor::block_on;
    use std::sync::atomic::Ordering;

    const EVENTS: u64 = 2000;
    let store = SqliteStore::<Counted>::open_in_memory().expect("open");
    block_on(store.append(
        &StreamId::from("lazy-1"),
        ExpectedVersion::Empty,
        (1..=EVENTS).map(|v| NewEvent::new(Counted(v))).collect(),
    ))
    .expect("append");

    DECODED.store(0, Ordering::SeqCst);
    let first: Vec<_> = block_on(store.stream_all(Sequence::START).take(3).collect());
    assert_eq!(first.len(), 3);
    let decoded = DECODED.load(Ordering::SeqCst);
    assert!(
        decoded < EVENTS as usize / 2,
        "a read of three events decoded {decoded} rows"
    );
    // The full read still pages through everything, in order.
    let all: Vec<_> = block_on(store.stream_all(Sequence::START).collect());
    assert_eq!(all.len(), EVENTS as usize);
    assert!(
        all.windows(2).all(|w| match (&w[0], &w[1]) {
            (Ok(a), Ok(b)) => a.sequence < b.sequence,
            _ => false,
        }),
        "pages join without gaps or repeats"
    );
}
