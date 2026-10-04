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
