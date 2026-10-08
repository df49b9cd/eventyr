//! A [`KeyStore`] for crypto-shredding (roadmap 0.7.6), behind the `shred`
//! feature: subject keys in a `subject_keys` table on the store's own
//! connection — or on a separate database, which is better: a key that
//! lives beside the ciphertext in one backup is only as erased as that
//! backup.
//!
//! An erased subject keeps its row with the key bytes overwritten by
//! `NULL`, so it cannot be given a new key and its old data stays
//! unreadable even if a sealed field is replayed.

use std::sync::{Arc, Mutex};

use rusqlite::OptionalExtension;

use eventyr_core::error::StoreError;
use eventyr_shred::{KeyStore, SubjectKey};

use crate::SqliteStoreError;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS subject_keys (
    subject     TEXT PRIMARY KEY,
    key         BLOB,
    erased_at   TEXT
);
";

/// Subject keys in SQLite.
pub struct SqliteKeyStore {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl Clone for SqliteKeyStore {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
        }
    }
}

impl SqliteKeyStore {
    /// A key store on its own database file — the recommended layout.
    ///
    /// # Errors
    ///
    /// The key database could not be opened (missing directory,
    /// permissions, a corrupt or non-SQLite file), or its schema
    /// could not be created on it.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, SqliteStoreError> {
        Self::from_connection(rusqlite::Connection::open(path)?)
    }

    /// A key store on an existing connection.
    ///
    /// # Errors
    ///
    /// The key schema could not be created on the connection's
    /// database (read-only file, a foreign table of the same name, or
    /// the database locked by another writer).
    pub fn from_connection(conn: rusqlite::Connection) -> Result<Self, SqliteStoreError> {
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// A key store beside an event store, on its connection.
    ///
    /// # Errors
    ///
    /// The key schema could not be created on the shared database
    /// (read-only file, a foreign table of the same name, or the
    /// database locked by another writer).
    pub fn beside<E>(store: &crate::SqliteStore<E>) -> Result<Self, SqliteStoreError> {
        let conn = store.conn();
        crate::lock_conn(&conn).execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, rusqlite::Connection> {
        crate::lock_conn(&self.conn)
    }
}

/// `None`: no row. `Some(None)`: erased. `Some(Some(key))`: live.
fn row(
    conn: &rusqlite::Connection,
    subject: &str,
) -> Result<Option<Option<SubjectKey>>, StoreError> {
    conn.query_row(
        "SELECT key FROM subject_keys WHERE subject = ?1",
        [subject],
        |row| row.get::<_, Option<Vec<u8>>>(0),
    )
    .optional()
    .map(|found| found.map(|key| key.map(SubjectKey::from_bytes)))
    .map_err(SqliteStoreError::into_store)
}

impl KeyStore for SqliteKeyStore {
    async fn load(&self, subject: &str) -> Result<Option<SubjectKey>, StoreError> {
        Ok(row(&self.lock(), subject)?.flatten())
    }

    async fn create(
        &self,
        subject: &str,
        key: SubjectKey,
    ) -> Result<Option<SubjectKey>, StoreError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO subject_keys (subject, key) VALUES (?1, ?2) ON CONFLICT (subject) DO NOTHING",
            rusqlite::params![subject, key.as_bytes()],
        )
        .map_err(SqliteStoreError::into_store)?;
        Ok(row(&conn, subject)?.flatten())
    }

    async fn delete(&self, subject: &str) -> Result<(), StoreError> {
        self.lock()
            .execute(
                "INSERT INTO subject_keys (subject, key, erased_at) VALUES (?1, NULL, datetime('now')) \
                 ON CONFLICT (subject) DO UPDATE SET key = NULL, \
                 erased_at = COALESCE(subject_keys.erased_at, datetime('now'))",
                [subject],
            )
            .map_err(SqliteStoreError::into_store)?;
        Ok(())
    }
}
