//! The store proper: schema, append and read paths.

use std::sync::{Arc, Mutex};

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{EventStore, StreamsAll};
use futures::Stream;
use futures::stream::iter;

use crate::SqliteStoreError;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    global_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id       TEXT    NOT NULL,
    stream_version  INTEGER NOT NULL,
    event_type      TEXT    NOT NULL,
    payload         TEXT    NOT NULL,
    causation_id    TEXT,
    correlation_id  TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (stream_id, stream_version)
);
";

/// The `events` columns every read shares — one place, so adding a
/// column can't drift across the append/read/read-all queries.
const EVENT_COLUMNS: &str =
    "global_sequence, stream_id, stream_version, payload, causation_id, correlation_id";

/// An embedded [`EventStore`] and [`StreamsAll`] over SQLite, via
/// rusqlite.
///
/// Synchronous under one mutex: the port's future signatures resolve
/// immediately, and a `Mutex<Connection>` serializes writers the way
/// SQLite's own single-writer rule would anyway. Cloneable — clones
/// share the one connection.
///
/// Construct with [`open`](SqliteStore::open) (a path) or
/// [`open_in_memory`](SqliteStore::open_in_memory) (a per-test transient
/// store — named vs. `:memory:` matters: rusqlite opens a fresh
/// database per `:memory:` connection, and the contract suite expects
/// one store handle to see all its writes).
pub struct SqliteStore<E> {
    conn: Arc<Mutex<rusqlite::Connection>>,
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> Clone for SqliteStore<E> {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            _event: std::marker::PhantomData,
        }
    }
}

impl<E> SqliteStore<E> {
    /// Open (or create) a store at `path`, migrating the schema.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, SqliteStoreError> {
        let conn = rusqlite::Connection::open(path)?;
        Self::from_connection(conn)
    }

    /// An in-memory store: tests, examples, the contract suite.
    pub fn open_in_memory() -> Result<Self, SqliteStoreError> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    /// Wrap an existing connection: the caller owns pragmas (WAL,
    /// busy_timeout, foreign keys); the store only owns the schema its
    /// tables stand on. Run on the same connection the app reads with.
    pub fn from_connection(conn: rusqlite::Connection) -> Result<Self, SqliteStoreError> {
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _event: std::marker::PhantomData,
        })
    }

    /// The connection, for sibling stores on the same database (the
    /// snapshot store behind the `snapshots` feature).
    #[cfg(feature = "snapshots")]
    pub(crate) fn conn(&self) -> Arc<Mutex<rusqlite::Connection>> {
        Arc::clone(&self.conn)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, rusqlite::Connection> {
        // No code under this lock can panic meaningfully (rusqlite's
        // error path is a Result, not a panic), and a panicking holder
        // leaves the database itself consistent — take the lock like
        // the other embedded stores do.
        self.conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One row of the events table, as read back.
struct EventRow {
    global_sequence: i64,
    stream_id: String,
    stream_version: i64,
    payload: String,
    causation_id: Option<String>,
    correlation_id: Option<String>,
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        global_sequence: row.get(0)?,
        stream_id: row.get(1)?,
        stream_version: row.get(2)?,
        payload: row.get(3)?,
        causation_id: row.get(4)?,
        correlation_id: row.get(5)?,
    })
}

impl<E> TryFrom<EventRow> for EventEnvelope<E>
where
    E: serde::de::DeserializeOwned,
{
    type Error = StoreError;

    fn try_from(row: EventRow) -> Result<Self, Self::Error> {
        let corrupt = |m: String| SqliteStoreError::CorruptRow(m);
        let event: E = serde_json::from_str(&row.payload)
            .map_err(|error| corrupt(format!("the payload does not decode: {error}")))?;
        let invalid = |value: i64| {
            StoreError::from(corrupt(format!("negative position in the log: {value}")))
        };
        Ok(EventEnvelope {
            sequence: Sequence::new(u64::try_from(row.global_sequence).map_err(|_| invalid(row.global_sequence))?),
            stream_id: StreamId::from(row.stream_id),
            version: Version::new(u64::try_from(row.stream_version).map_err(|_| invalid(row.stream_version))?),
            event,
            metadata: Metadata::of_ids(row.causation_id, row.correlation_id),
        })
    }
}

/// The write path, shared by the single-stream and batch appends:
/// check every head, insert every event, commit. All-or-nothing under
/// SQLite's transaction; the expectation check runs inside it so a
/// conflict rolls back rather than violating the unique index mid-way.
fn append_within<E>(
    tx: &rusqlite::Transaction<'_>,
    appends: &[(StreamId, ExpectedVersion, Vec<NewEvent<E>>)],
) -> Result<Vec<Vec<EventEnvelope<E>>>, StoreError>
where
    E: EventName + serde::Serialize + Clone,
{
    let mut committed = Vec::with_capacity(appends.len());
    for (stream_id, expected, events) in appends {
        // The head, inside the transaction: the current version is the
        // count of rows the stream holds.
        let current: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(stream_version), 0) FROM events WHERE stream_id = ?1",
                [stream_id.as_str()],
                |row| row.get(0),
            )
            .map_err(SqliteStoreError::into_store)?;
        if !eventyr_store::store::expected_version_matches(*expected, current.max(0) as u64) {
            return Err(StoreError::Conflict {
                stream_id: Some(stream_id.clone()),
                current: Version::new(current.max(0) as u64),
            });
        }
        let mut written = Vec::with_capacity(events.len());
        for (index, new_event) in events.iter().enumerate() {
            let version = current.max(0) as u64 + index as u64 + 1;
            let payload = serde_json::to_string(&new_event.event).map_err(SqliteStoreError::from)?;
            tx.execute(
                "INSERT INTO events (stream_id, stream_version, event_type, payload, causation_id, correlation_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    stream_id.as_str(),
                    version as i64,
                    new_event.event.event_name(),
                    payload,
                    new_event.metadata.causation_id,
                    new_event.metadata.correlation_id,
                ],
            )
            .map_err(SqliteStoreError::into_store)?;
            let sequence = tx.last_insert_rowid();
            written.push(EventEnvelope {
                sequence: Sequence::new(sequence as u64),
                stream_id: stream_id.clone(),
                version: Version::new(version),
                event: new_event.event.clone(),
                metadata: Metadata::of_ids(
                    new_event.metadata.causation_id.clone(),
                    new_event.metadata.correlation_id.clone(),
                ),
            });
        }
        committed.push(written);
    }
    Ok(committed)
}

impl<E> EventStore for SqliteStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    type Event = E;

    async fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<E>>,
    ) -> Result<Vec<EventEnvelope<E>>, StoreError> {
        let mut conn = self.lock();
        let tx = conn.transaction().map_err(SqliteStoreError::into_store)?;
        let mut committed = append_within(
            &tx,
            &[(stream_id.clone(), expected, events)],
        )?;
        tx.commit().map_err(SqliteStoreError::into_store)?;
        // One stream in the batch: the writes above are the caller's
        // single answer.
        Ok(committed.remove(0))
    }

    /// Append the whole batch in one transaction: every expectation
    /// checked before any write commits, so the batch is all-or-nothing
    /// across streams exactly as the port promises.
    async fn append_batch(
        &self,
        appends: Vec<StreamAppend<E>>,
    ) -> Result<Vec<CommittedStream<E>>, StoreError> {
        let mut conn = self.lock();
        let tx = conn.transaction().map_err(SqliteStoreError::into_store)?;
        let appends: Vec<(StreamId, ExpectedVersion, Vec<NewEvent<E>>)> = appends
            .into_iter()
            .map(|a| (a.stream_id, a.expected, a.events))
            .collect();
        let committed = append_within(&tx, &appends)?;
        tx.commit().map_err(SqliteStoreError::into_store)?;
        Ok(appends
            .into_iter()
            .zip(committed)
            .map(|((stream_id, _, _), events)| CommittedStream { stream_id, events })
            .collect())
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let conn = self.lock();
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE stream_id = ?1 AND stream_version > ?2 \
             ORDER BY stream_version"
        );
        let events: Vec<Result<EventEnvelope<E>, StoreError>> = conn
            .prepare(&sql)
            .map_err(SqliteStoreError::into_store)
            .and_then(|mut stmt| {
                stmt.query_map(rusqlite::params![stream_id.as_str(), from.as_u64() as i64], read_row)
                    .map_err(SqliteStoreError::into_store)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(SqliteStoreError::into_store)
            })
            .map(|rows| {
                rows.into_iter()
                    .map(EventEnvelope::try_from)
                    .collect()
            })
            .unwrap_or_else(|error| vec![Err(error)]);
        iter(events)
    }
}

impl<E> StreamsAll for SqliteStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let conn = self.lock();
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE global_sequence > ?1 \
             ORDER BY global_sequence"
        );
        // SQLite has no identity columns to burn on rollback: gaps come
        // from deleted rows or failed transactions neither of which the
        // schema allows — the port's gap-tolerance contract rides free.
        let events: Vec<Result<EventEnvelope<E>, StoreError>> = conn
            .prepare(&sql)
            .map_err(SqliteStoreError::into_store)
            .and_then(|mut stmt| {
                stmt.query_map(rusqlite::params![from.as_u64() as i64], read_row)
                    .map_err(SqliteStoreError::into_store)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(SqliteStoreError::into_store)
            })
            .map(|rows| {
                rows.into_iter()
                    .map(EventEnvelope::try_from)
                    .collect()
            })
            .unwrap_or_else(|error| vec![Err(error)]);
        iter(events)
    }
}
