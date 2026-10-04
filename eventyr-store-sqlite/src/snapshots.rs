//! The snapshot store over the same connection, behind the
//! `snapshots` feature: a `snapshots` table with one row per stream,
//! newest-wins on the stream version — the same contract the Postgres
//! snapshot store carries (DESIGN §6.3).

use std::sync::Arc;

use eventyr_core::error::StoreError;
use eventyr_core::snapshot::Snapshot;
use eventyr_core::vocabulary::{StreamId, Version};
use eventyr_store::snapshot_store::SnapshotStore;

use crate::{SqliteStore, SqliteStoreError};

/// The SQLite snapshot store, generic in the state type.
pub struct SqliteSnapshotStore<S> {
    conn: Arc<std::sync::Mutex<rusqlite::Connection>>,
    _state: std::marker::PhantomData<fn() -> S>,
}

impl<S> Clone for SqliteSnapshotStore<S> {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            _state: std::marker::PhantomData,
        }
    }
}

impl<S> SqliteSnapshotStore<S> {
    /// A snapshot store over the same connection the event store uses —
    /// the snapshots live beside the log.
    pub fn new<E>(store: &SqliteStore<E>) -> Result<Self, SqliteStoreError> {
        let conn = store.conn();
        conn.lock()
            .map_err(|e| SqliteStoreError::CorruptRow(format!("snapshot open: lock poisoned: {e}")))?
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS snapshots (
                    stream_id   TEXT    PRIMARY KEY,
                    version     INTEGER NOT NULL CHECK (version >= 0),
                    payload     TEXT    NOT NULL,
                    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
                );",
            )?;
        Ok(Self {
            conn: Arc::clone(&conn),
            _state: std::marker::PhantomData,
        })
    }
}

impl<S> SnapshotStore for SqliteSnapshotStore<S>
where
    S: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
{
    type State = S;

    async fn load(&self, stream_id: &StreamId) -> Result<Option<Snapshot<S>>, StoreError> {
        let conn = self.conn.lock().map_err(|e| StoreError::other(format!("snapshot lock poisoned: {e}")))?;
        let mut stmt = conn
            .prepare("SELECT version, payload FROM snapshots WHERE stream_id = ?1")
            .map_err(SqliteStoreError::into_store)?;
        let row = stmt
            .query_row([stream_id.as_str()], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()
            .map_err(SqliteStoreError::into_store)?;
        let Some((version, payload)) = row else { return Ok(None) };
        let version = u64::try_from(version).map_err(|_| {
            StoreError::from(SqliteStoreError::CorruptRow(format!(
                "negative snapshot version: {version}"
            )))
        })?;
        let state: S = serde_json::from_str(&payload).map_err(|error| {
            StoreError::from(SqliteStoreError::CorruptRow(format!(
                "the snapshot payload does not decode: {error}"
            )))
        })?;
        Ok(Some(Snapshot {
            stream_id: stream_id.clone(),
            version: Version::new(version),
            state,
        }))
    }

    async fn save(&self, snapshot: Snapshot<S>) -> Result<(), StoreError> {
        let payload = serde_json::to_string(&snapshot.state).map_err(SqliteStoreError::from)?;
        let version = i64::try_from(snapshot.version.as_u64()).map_err(|_| {
            StoreError::from(SqliteStoreError::CorruptRow(
                "snapshot version beyond i64".into(),
            ))
        })?;
        let conn = self.conn.lock().map_err(|e| StoreError::other(format!("snapshot lock poisoned: {e}")))?;
        // Newest wins: a stale offer's UPDATE never matches, so the row
        // cannot regress.
        conn.execute(
            "INSERT INTO snapshots (stream_id, version, payload) VALUES (?1, ?2, ?3) \
             ON CONFLICT (stream_id) DO UPDATE \
             SET version = excluded.version, payload = excluded.payload, created_at = datetime('now') \
             WHERE snapshots.version < excluded.version",
            rusqlite::params![snapshot.stream_id.as_str(), version, payload],
        )
        .map_err(SqliteStoreError::into_store)?;
        Ok(())
    }
}

use rusqlite::OptionalExtension;
