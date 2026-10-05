//! The SQLite store against the shared contract suite: the
//! roadmap-0.6.4 proof that a third-party store implements the port by
//! calling the suite, not by shipping its own tests of the same rules.

use eventyr_core::event_name::EventName;
use eventyr_store_sqlite::SqliteStore;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
enum ContractEvent {
    Payload { value: u64 },
}

impl EventName for ContractEvent {
    fn event_name(&self) -> &'static str {
        match self {
            ContractEvent::Payload { .. } => "Payload",
        }
    }
}

impl From<u64> for ContractEvent {
    fn from(value: u64) -> Self {
        ContractEvent::Payload { value }
    }
}

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

/// The filtered-read contract needs a stored name that depends on the
/// payload: even values are `"Even"`, odd ones `"Odd"`.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct ParityEvent(u64);

impl EventName for ParityEvent {
    fn event_name(&self) -> &'static str {
        if self.0.is_multiple_of(2) {
            "Even"
        } else {
            "Odd"
        }
    }
}

impl From<u64> for ParityEvent {
    fn from(value: u64) -> Self {
        Self(value)
    }
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
}
