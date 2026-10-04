//! The embedded snapshot store, behind the `snapshots` feature: a
//! fourth keyspace, `"{stream_id}\0{version:016}"` → JSON
//! [`Snapshot`]-shaped rows, read through a reverse range scan so the
//! newest version is the first hit — the port's newest-wins semantics
//! fall straight out of the key layout.

use std::sync::Mutex;

use fjall::{Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use serde::{Deserialize, Serialize};

use eventyr_core::error::StoreError;
use eventyr_core::snapshot::Snapshot;
use eventyr_core::vocabulary::StreamId;
use eventyr_store::snapshot_store::SnapshotStore;

use crate::FjallStoreError;

const PARTITION_SNAPSHOTS: &str = "snapshots";

#[derive(Serialize, Deserialize)]
struct StoredSnapshot<S> {
    version: u64,
    state: S,
}

/// The fjall [`SnapshotStore`]: newest-per-stream snapshots in their
/// own keyspace of the same database the event store uses.
pub struct FjallSnapshotStore<S> {
    keyspace: SingleWriterTxDatabase,
    snapshots: SingleWriterTxKeyspace,
    // `save` is a single-writer transaction against one keyspace;
    // the lock mirrors the event store's.
    write_lock: Mutex<()>,
    _state: std::marker::PhantomData<fn() -> S>,
}

impl<S> FjallSnapshotStore<S> {
    /// Open (or create) the snapshots keyspace on `keyspace` (fjall 3’s name for fjall 2’s “partition”).
    ///
    /// Pair this with a [`FjallStore`](crate::FjallStore) opened on the
    /// same database: the snapshots live beside the log.
    pub fn open(keyspace: &SingleWriterTxDatabase) -> Result<Self, FjallStoreError> {
        Ok(Self {
            snapshots: keyspace
                .keyspace(PARTITION_SNAPSHOTS, fjall::KeyspaceCreateOptions::default)?,
            keyspace: keyspace.clone(),
            write_lock: Mutex::new(()),
            _state: std::marker::PhantomData,
        })
    }

    fn key(stream_id: &StreamId, version: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(stream_id.as_str().len() + 1 + 16);
        key.extend_from_slice(stream_id.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(format!("{version:016}").as_bytes());
        key
    }
}

impl<S> SnapshotStore for FjallSnapshotStore<S>
where
    S: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned,
{
    type State = S;

    fn load(
        &self,
        stream_id: &StreamId,
    ) -> impl std::future::Future<Output = Result<Option<Snapshot<S>>, StoreError>> + Send {
        let stream_id = stream_id.clone();
        async move {
            let mut lower = stream_id.as_str().as_bytes().to_vec();
            lower.push(0);
            let mut upper = stream_id.as_str().as_bytes().to_vec();
            upper.push(0);
            upper.push(0xff);

            let tx = self.keyspace.read_tx();
            let guard = match tx.range(&self.snapshots, lower..upper).next_back() {
                Some(guard) => guard,
                None => return Ok(None),
            };
            let key = guard.key().map_err(|error| {
                StoreError::Other(std::sync::Arc::new(FjallStoreError::Engine(error)))
            })?;
            let value = tx.get(&self.snapshots, &key).map_err(|error| {
                StoreError::Other(std::sync::Arc::new(FjallStoreError::Engine(error)))
            })?;
            let value = value.ok_or_else(|| {
                StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(
                    "a snapshot key pointed at no row".into(),
                )))
            })?;
            // The key's last 16 chars are the version; the row's own
            // copy is what we deserialize, but read it from the key.
            let version = String::from_utf8_lossy(&key[key.len().saturating_sub(16)..])
                .parse::<u64>()
                .map_err(|error| {
                    StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(format!(
                        "a snapshot key's version did not parse: {error}"
                    ))))
                })?;
            let stored: StoredSnapshot<S> = serde_json::from_slice(&value).map_err(|error| {
                StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(format!(
                    "{error}"
                ))))
            })?;
            Ok(Some(Snapshot {
                stream_id,
                version: eventyr_core::vocabulary::Version::new(version),
                state: stored.state,
            }))
        }
    }

    fn save(
        &self,
        snapshot: Snapshot<S>,
    ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        let snapshot = snapshot.clone();
        async move {
            let _guard = self
                .write_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tx = self.keyspace.write_tx();
            let key = Self::key(&snapshot.stream_id, snapshot.version.as_u64());
            let value = serde_json::to_vec(&StoredSnapshot {
                version: snapshot.version.as_u64(),
                state: snapshot.state,
            })
            .map_err(|error| {
                StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(format!(
                    "{error}"
                ))))
            })?;
            tx.insert(&self.snapshots, key, value);
            tx.commit().map_err(|error| {
                StoreError::Other(std::sync::Arc::new(FjallStoreError::Engine(error)))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The in-memory store can stand in while these compile; the real
    // coverage is the contract test in this crate's tests/.
    // Keep the key layout exercised without a database.
    #[test]
    fn key_layout_orders_by_version() {
        let stream = StreamId::from("account-1");
        let v1 = FjallSnapshotStore::<u64>::key(&stream, 1);
        let v9 = FjallSnapshotStore::<u64>::key(&stream, 9);
        let v10 = FjallSnapshotStore::<u64>::key(&stream, 10);
        assert!(v1 < v9 && v9 < v10, "zero-padded versions sort lexically");
    }
}
