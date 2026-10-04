//! # eventyr-store-postgres
//!
//! The Postgres implementation of the [`EventStore`](eventyr_store::store::EventStore)
//! port (DESIGN §9). One table — `events(stream_id, stream_version,
//! event_type, payload, metadata, global_sequence, created_at)` — with
//! `UNIQUE(stream_id, stream_version)` as the optimistic-concurrency
//! contract and a `global_sequence` IDENTITY column as the projection
//! backbone.
//!
//! Appends run through the `append_events` PL/pgSQL function installed
//! by the migration: it takes a per-stream advisory lock (serializing
//! writers to one stream; others proceed), checks the expected version
//! against the live max, and inserts the batch in one statement,
//! returning the rows as the server recorded them. A mismatch raises an
//! error whose `hint` carries the conflict's actual version, mapped
//! here to [`StoreError::Conflict`].
//!
//! Events serialize as `{ name: <EventName>, payload }`; the `name` is
//! the event enum's [`EventName`](eventyr_core::event_name::EventName),
//! so payload-struct renames are free and historical payloads stay
//! readable by an upcaster.

pub mod store;

use std::sync::Arc;

use eventyr_core::error::StoreError;
use sqlx::postgres::PgDatabaseError;

/// PostgreSQL's SQLSTATE for a user-raised exception — the
/// `RAISE EXCEPTION '...'` the migration's `append_events` uses to
/// signal a version-conflict (its `hint` carries the actual version).
const RAISE_EXCEPTION: &str = "P0001";

pub use store::PgStore;

/// The crate-level error: store failures are [`StoreError`] once they
/// leave the store; this is what those `Other` variants wrap.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgStoreError {
    /// The event payload could not be serialized to JSON.
    #[error("cannot serialize event payload: {0}")]
    Payload(#[from] serde_json::Error),
    /// A row read back could not be decoded into the event enum —
    /// treated as data corruption, never silently skipped.
    #[error("corrupt row: {0}")]
    CorruptRow(String),
    /// The database returned an error that is not a version conflict.
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    /// Migration failed.
    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

impl PgStoreError {
    /// Map a raw sqlx error to [`StoreError`]: the migration's
    /// version-conflict is surfaced as a [`StoreError::Conflict`], a
    /// connection-level failure as [`StoreError::Unavailable`], anything
    /// else as fatal-by-construction.
    pub(crate) fn into_store(error: sqlx::Error) -> StoreError {
        // The function raises a version conflict as `USING HINT =
        // <current-version>`; read the hint, not the message text.
        let conflict_current = error
            .as_database_error()
            .and_then(|db| db.try_downcast_ref::<PgDatabaseError>())
            .filter(|pg| pg.code() == RAISE_EXCEPTION)
            .and_then(|pg| pg.hint())
            .and_then(|hint| hint.trim().parse().ok());
        match conflict_current {
            Some(current) => StoreError::Conflict {
                current: eventyr_core::vocabulary::Version::new(current),
            },
            None => match error {
                sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => {
                    StoreError::Unavailable
                }
                other => StoreError::Other(Arc::new(Self::from(other))),
            },
        }
    }
}

impl From<PgStoreError> for StoreError {
    fn from(error: PgStoreError) -> Self {
        Self::Other(Arc::new(error))
    }
}
