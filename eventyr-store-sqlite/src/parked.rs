//! A [`ParkedStore`] (0.7.7), behind the `parked` feature: parked events
//! in a `parked_events` table on the store's connection, so a park is
//! durable in the same database the projection reads from.

use std::sync::{Arc, Mutex};

use eventyr_core::envelope::{EventEnvelope, Metadata};
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::{Sequence, StreamId, Version};
use eventyr_subscription::parked::{ParkedEvent, ParkedStore};

use crate::SqliteStoreError;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS parked_events (
    subscription    TEXT    NOT NULL,
    global_sequence INTEGER NOT NULL,
    stream_id       TEXT    NOT NULL,
    stream_version  INTEGER NOT NULL,
    payload         TEXT    NOT NULL,
    causation_id    TEXT,
    correlation_id  TEXT,
    idempotency_key TEXT,
    attempts        INTEGER NOT NULL,
    error           TEXT    NOT NULL,
    parked_at       TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (subscription, global_sequence)
);
";

/// Parked events in SQLite, generic in the event type (stored as JSON).
pub struct SqliteParkedStore<E> {
    conn: Arc<Mutex<rusqlite::Connection>>,
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> Clone for SqliteParkedStore<E> {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            _event: std::marker::PhantomData,
        }
    }
}

impl<E> SqliteParkedStore<E> {
    /// A parked store beside an event store, on its connection.
    pub fn beside<X>(store: &crate::SqliteStore<X>) -> Result<Self, SqliteStoreError> {
        let conn = store.conn();
        conn.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .execute_batch(SCHEMA)?;
        Ok(Self {
            conn,
            _event: std::marker::PhantomData,
        })
    }

    /// A parked store on its own connection.
    pub fn from_connection(conn: rusqlite::Connection) -> Result<Self, SqliteStoreError> {
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _event: std::marker::PhantomData,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, rusqlite::Connection> {
        self.conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn position(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| {
        StoreError::from(SqliteStoreError::CorruptRow(format!(
            "negative position: {value}"
        )))
    })
}

impl<E> ParkedStore<E> for SqliteParkedStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + Send,
{
    async fn park(&self, event: ParkedEvent<E>) -> Result<(), StoreError> {
        let payload =
            serde_json::to_string(&event.envelope.event).map_err(SqliteStoreError::from)?;
        let metadata = &event.envelope.metadata;
        self.lock()
            .execute(
                "INSERT INTO parked_events (subscription, global_sequence, stream_id, \
                 stream_version, payload, causation_id, correlation_id, idempotency_key, \
                 attempts, error) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
                 ON CONFLICT (subscription, global_sequence) DO UPDATE SET \
                 attempts = excluded.attempts, error = excluded.error, \
                 parked_at = datetime('now')",
                rusqlite::params![
                    event.subscription,
                    event.envelope.sequence.as_u64() as i64,
                    event.envelope.stream_id.as_str(),
                    event.envelope.version.as_u64() as i64,
                    payload,
                    metadata.causation_id,
                    metadata.correlation_id,
                    metadata.idempotency_key,
                    i64::from(event.attempts),
                    event.error,
                ],
            )
            .map_err(SqliteStoreError::into_store)?;
        Ok(())
    }

    async fn list(&self, subscription: &str) -> Result<Vec<ParkedEvent<E>>, StoreError> {
        type Row = (
            i64,
            String,
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            String,
        );
        let conn = self.lock();
        let rows: Vec<Row> = conn
            .prepare(
                "SELECT global_sequence, stream_id, stream_version, payload, causation_id, \
                 correlation_id, idempotency_key, attempts, error FROM parked_events \
                 WHERE subscription = ?1 ORDER BY global_sequence",
            )
            .and_then(|mut stmt| {
                stmt.query_map([subscription], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                    ))
                })?
                .collect()
            })
            .map_err(SqliteStoreError::into_store)?;
        rows.into_iter()
            .map(
                |(sequence, stream, version, payload, cause, corr, key, attempts, error)| {
                    let event = serde_json::from_str(&payload).map_err(|e| {
                        StoreError::from(SqliteStoreError::CorruptRow(format!(
                            "a parked payload does not decode: {e}"
                        )))
                    })?;
                    Ok(ParkedEvent {
                        subscription: subscription.to_owned(),
                        envelope: EventEnvelope {
                            sequence: Sequence::new(position(sequence)?),
                            stream_id: StreamId::from(stream),
                            version: Version::new(position(version)?),
                            event,
                            metadata: Metadata {
                                idempotency_key: key,
                                ..Metadata::of_ids(cause, corr)
                            },
                        },
                        attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
                        error,
                    })
                },
            )
            .collect()
    }

    async fn remove(&self, subscription: &str, sequence: Sequence) -> Result<(), StoreError> {
        self.lock()
            .execute(
                "DELETE FROM parked_events WHERE subscription = ?1 AND global_sequence = ?2",
                rusqlite::params![subscription, sequence.as_u64() as i64],
            )
            .map_err(SqliteStoreError::into_store)?;
        Ok(())
    }
}
