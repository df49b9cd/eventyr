//! # eventyr-store-sqlite
//!
//! The SQLite [`EventStore`](eventyr_store::store::EventStore) — the roadmap-0.6.4 reference port: a
//! second durable, embeddable database proving the `eventyr-store-testing`
//! contract suite is implementable without a server and without an
//! async runtime.
//!
//! One table, one-row-per-event, the same shape as the Postgres store
//! (DESIGN §9: the boring, standard design): an `INTEGER PRIMARY KEY`
//! rowid serves as the global sequence — strictly increasing per
//! single-write transaction, gap-burning on rollback exactly as the
//! `SubscriptionMachine`'s contract allows — and a
//! `UNIQUE (stream_id, stream_version)` index enforces the
//! optimistic-concurrency contract in the schema, with the store
//! checking the expectation first inside the same transaction so the
//! conflict names the version instead of a constraint violation.
//!
//! `SqliteStore` is synchronous over a `rusqlite::Connection`; the
//! port's futures resolve immediately. All writes go through one
//! connection behind a mutex — SQLite serializes writers exactly as
//! fjall's single-writer database does, and `append_batch`'s one
//! transaction across streams is atomic for the same reason.

#[cfg(feature = "checkpoints")]
mod checkpoints;
#[cfg(feature = "shred")]
mod keys;
#[cfg(feature = "parked")]
mod parked;
#[cfg(feature = "snapshots")]
mod snapshots;
mod store;
#[cfg(feature = "views")]
pub mod views;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use eventyr_core::error::StoreError;

pub use store::SqliteStore;

#[cfg(feature = "snapshots")]
pub use snapshots::SqliteSnapshotStore;

#[cfg(feature = "views")]
pub use views::SqliteViewStore;

#[cfg(feature = "shred")]
pub use keys::SqliteKeyStore;

#[cfg(feature = "parked")]
pub use parked::SqliteParkedStore;

#[cfg(feature = "checkpoints")]
pub use checkpoints::SqliteCheckpointStore;

/// Lock a connection shared by this crate's stores — the one poisoning
/// policy for all of them.
///
/// A poisoned lock is taken, not reported. No code under it can leave
/// the database half-written: rusqlite reports failures as `Result`s,
/// and every multi-statement write runs in a transaction that rolls back
/// when its guard drops during the panic. So the panic that poisoned the
/// lock left SQLite consistent, and refusing every later call — on this
/// store and on every sibling sharing its connection — would turn one
/// failed call into a dead database handle.
pub(crate) fn lock_conn(
    conn: &Mutex<rusqlite::Connection>,
) -> MutexGuard<'_, rusqlite::Connection> {
    conn.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A stored position (version or sequence) back as a `u64`: a negative
/// one is corrupt data, not a number to wrap.
pub(crate) fn position(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| {
        StoreError::from(SqliteStoreError::CorruptRow(format!(
            "negative position: {value}"
        )))
    })
}

/// The crate-level error: store failures are [`StoreError`] once they
/// leave the store; this is what those `Other` variants wrap.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteStoreError {
    /// The event payload could not be serialized to JSON.
    #[error("cannot serialize event payload: {0}")]
    Payload(#[from] serde_json::Error),
    /// A row read back could not be decoded — treated as data
    /// corruption, never silently skipped.
    #[error("corrupt row: {0}")]
    CorruptRow(String),
    /// SQLite returned an error.
    #[error("sqlite: {0}")]
    Db(#[from] rusqlite::Error),
}

impl From<SqliteStoreError> for StoreError {
    fn from(error: SqliteStoreError) -> Self {
        Self::Other(Arc::new(error))
    }
}

impl SqliteStoreError {
    /// Map a rusqlite error: a locked database is transient
    /// (`Unavailable` — the caller may retry), anything else fatal.
    fn into_store(error: rusqlite::Error) -> StoreError {
        match error {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::DatabaseBusy,
                    ..
                }
                | rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::DatabaseLocked,
                    ..
                },
                _,
            ) => StoreError::Unavailable,
            other => SqliteStoreError::from(other).into(),
        }
    }
}
