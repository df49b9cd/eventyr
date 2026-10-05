//! A [`CheckpointStore`], behind the `checkpoints` feature: each
//! subscription's last-acked position in a `checkpoints` table on the
//! store's connection, so a restart resumes where the projection left
//! off — beside the parked events and the read models it writes.

use std::sync::{Arc, Mutex};

use rusqlite::OptionalExtension;

use eventyr_core::error::StoreError;
use eventyr_core::subscription::Checkpoint;
use eventyr_core::vocabulary::Sequence;
use eventyr_store::store::sql_position;
use eventyr_subscription::checkpoint::CheckpointStore;

use crate::{SqliteStoreError, lock_conn, position};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS checkpoints (
    name            TEXT    PRIMARY KEY,
    global_sequence INTEGER NOT NULL CHECK (global_sequence >= 0),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);
";

/// Subscription checkpoints in SQLite, one row per subscription name.
///
/// [`store`](CheckpointStore::store) is one autocommitted upsert, durable
/// when it returns (as durable as the connection's `synchronous` pragma
/// makes any SQLite commit).
#[derive(Clone)]
pub struct SqliteCheckpointStore {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteCheckpointStore {
    /// A checkpoint store beside an event store, on its connection.
    pub fn beside<E>(store: &crate::SqliteStore<E>) -> Result<Self, SqliteStoreError> {
        let conn = store.conn();
        lock_conn(&conn).execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// A checkpoint store on its own connection.
    pub fn from_connection(conn: rusqlite::Connection) -> Result<Self, SqliteStoreError> {
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl CheckpointStore for SqliteCheckpointStore {
    async fn load(&self, name: &str) -> Result<Checkpoint, StoreError> {
        let stored: Option<i64> = lock_conn(&self.conn)
            .query_row(
                "SELECT global_sequence FROM checkpoints WHERE name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(SqliteStoreError::into_store)?;
        match stored {
            Some(sequence) => Ok(Checkpoint::new(Sequence::new(position(sequence)?))),
            None => Ok(Checkpoint::ORIGIN),
        }
    }

    async fn store(&self, name: &str, checkpoint: Checkpoint) -> Result<(), StoreError> {
        lock_conn(&self.conn)
            .execute(
                "INSERT INTO checkpoints (name, global_sequence) VALUES (?1, ?2) \
                 ON CONFLICT (name) DO UPDATE SET \
                 global_sequence = excluded.global_sequence, updated_at = datetime('now')",
                rusqlite::params![name, sql_position(checkpoint.as_sequence().as_u64())],
            )
            .map_err(SqliteStoreError::into_store)?;
        Ok(())
    }
}
