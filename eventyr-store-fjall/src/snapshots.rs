//! The embedded snapshot store, behind the `snapshots` feature: a
//! fourth keyspace, `"{stream_id}\0{version:016}"` → JSON
//! [`Snapshot`]-shaped rows.
//!
//! The rows are read through a reverse range scan so the newest
//! version is the first hit. The port's newest-wins semantics
//! fall out of the key layout, and `save` drops an offer at or below the
//! newest stored version so an equal-version offer cannot replace it.

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
///
/// Saves are fjall write transactions, serialized with the event
/// store's by the database's single-writer lock and fsynced on commit
/// like every write in this crate.
pub struct FjallSnapshotStore<S> {
    keyspace: SingleWriterTxDatabase,
    snapshots: SingleWriterTxKeyspace,
    _state: std::marker::PhantomData<fn() -> S>,
}

impl<S> FjallSnapshotStore<S> {
    /// Open (or create) the snapshots keyspace on `keyspace` (fjall 3’s name for fjall 2’s “partition”).
    ///
    /// Pair this with a [`FjallStore`](crate::FjallStore) opened on the
    /// same database: the snapshots live beside the log.
    ///
    /// # Errors
    ///
    /// The snapshots keyspace could not be created on the database —
    /// an engine (I/O, corruption) failure, or a keyspace of the same
    /// name existing with an incompatible configuration.
    pub fn open(keyspace: &SingleWriterTxDatabase) -> Result<Self, FjallStoreError> {
        Ok(Self {
            snapshots: keyspace
                .keyspace(PARTITION_SNAPSHOTS, fjall::KeyspaceCreateOptions::default)?,
            keyspace: keyspace.clone(),
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

    async fn save(&self, snapshot: Snapshot<S>) -> Result<(), StoreError> {
        let mut tx = crate::store::durable_write_tx(&self.keyspace);
        let key = Self::key(&snapshot.stream_id, snapshot.version.as_u64());
        // Newest wins: an offer at or below a stored version is
        // dropped, not written — the write driver saves
        // fire-and-forget, so a late offer must not replace a newer
        // (or equal) snapshot. The check runs in the write
        // transaction, so no save can land between it and the insert.
        let mut upper = snapshot.stream_id.as_str().as_bytes().to_vec();
        upper.push(0);
        upper.push(0xff);
        if tx
            .range(&self.snapshots, key.clone()..upper)
            .next()
            .is_some()
        {
            return Ok(());
        }
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
        tx.commit()
            .map_err(|error| StoreError::Other(std::sync::Arc::new(FjallStoreError::Engine(error))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The port's behaviour is covered by `snapshot_contract` in this
    // crate's tests/; this pins the key layout it relies on.
    #[test]
    fn key_layout_orders_by_version() {
        let stream = StreamId::from("account-1");
        let v1 = FjallSnapshotStore::<u64>::key(&stream, 1);
        let v9 = FjallSnapshotStore::<u64>::key(&stream, 9);
        let v10 = FjallSnapshotStore::<u64>::key(&stream, 10);
        assert!(v1 < v9 && v9 < v10, "zero-padded versions sort lexically");
    }
}
