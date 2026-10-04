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
//! Each event persists as one row: `payload` holds the event enum's
//! externally-tagged serde (`{variant: args}`), and `event_type` holds the
//! enum variant's [`EventName`](eventyr_core::event_name::EventName) in a
//! separate column — so payload-struct renames are free and historical
//! payloads stay selectable by an upcaster.

#[cfg(feature = "snapshots")]
pub mod snapshots;
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
        let conflict_current = error
            .as_database_error()
            .and_then(|db| db.try_downcast_ref::<PgDatabaseError>())
            .and_then(|pg| parse_conflict_hint(pg.code(), pg.hint()));
        Self::classify(conflict_current, error)
    }

    /// The classification itself, split from the sqlx plumbing so the
    /// branches are unit-testable without a database.
    fn classify(conflict_current: Option<u64>, error: sqlx::Error) -> StoreError {
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

/// The function raises a version conflict as `RAISE EXCEPTION ... USING
/// HINT = <current-version>`; read the hint, not the message text. Any
/// other SQLSTATE — or a hint that is not a version — is not a conflict.
fn parse_conflict_hint(code: &str, hint: Option<&str>) -> Option<u64> {
    if code != RAISE_EXCEPTION {
        return None;
    }
    hint?.trim().parse().ok()
}

impl From<PgStoreError> for StoreError {
    fn from(error: PgStoreError) -> Self {
        Self::Other(Arc::new(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_hint_only_for_raised_exception_with_numeric_hint() {
        assert_eq!(parse_conflict_hint(RAISE_EXCEPTION, Some("1")), Some(1));
        assert_eq!(parse_conflict_hint(RAISE_EXCEPTION, Some(" 42 ")), Some(42));
        // The same hint on any other SQLSTATE is not a conflict.
        assert_eq!(parse_conflict_hint("23505", Some("1")), None);
        // A raised exception without a numeric hint carries no version.
        assert_eq!(parse_conflict_hint(RAISE_EXCEPTION, None), None);
        assert_eq!(parse_conflict_hint(RAISE_EXCEPTION, Some("oops")), None);
    }

    #[test]
    fn classify_conflict_reads_the_hint_version() {
        let before = sqlx::Error::RowNotFound;
        match PgStoreError::classify(Some(7), before) {
            StoreError::Conflict { current } => {
                assert_eq!(current, eventyr_core::vocabulary::Version::new(7));
            }
            other => panic!("expected a conflict, got {other:?}"),
        }
    }

    #[test]
    fn classify_connection_failures_as_unavailable() {
        let io: std::io::Error = std::io::ErrorKind::ConnectionReset.into();
        assert!(matches!(
            PgStoreError::classify(None, sqlx::Error::Io(io)),
            StoreError::Unavailable
        ));
        assert!(matches!(
            PgStoreError::classify(None, sqlx::Error::PoolTimedOut),
            StoreError::Unavailable
        ));
        assert!(matches!(
            PgStoreError::classify(None, sqlx::Error::PoolClosed),
            StoreError::Unavailable
        ));
    }

    #[test]
    fn classify_anything_else_as_other() {
        match PgStoreError::classify(None, sqlx::Error::RowNotFound) {
            StoreError::Other(error) => {
                let pg = error
                    .downcast_ref::<PgStoreError>()
                    .expect("Other wraps a PgStoreError");
                assert!(matches!(pg, PgStoreError::Db(sqlx::Error::RowNotFound)));
            }
            other => panic!("expected Other, got {other:?}"),
        }
    }
}
