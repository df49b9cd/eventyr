//! The store proper: schema, append and read paths.

use std::sync::{Arc, Mutex};

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::notify::{CommitSignal, LocalCommitListener, LocalCommitSignal};
use eventyr_store::store::{
    EventFilter, EventStore, FilteredRead, QueryAppend, StreamLifecycle, StreamsAll,
};
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
    idempotency_key TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (stream_id, stream_version)
);
-- Stream lifecycle (0.7.6): a row only for streams that were closed or
-- truncated. `head` keeps a truncated stream's version when its rows
-- are gone; `first_kept` is the first version left.
CREATE TABLE IF NOT EXISTS stream_lifecycle (
    stream_id   TEXT    PRIMARY KEY,
    closed      INTEGER NOT NULL DEFAULT 0,
    first_kept  INTEGER NOT NULL DEFAULT 1,
    head        INTEGER NOT NULL DEFAULT 0
);
";

/// The `events` columns every read shares — one place, so adding a
/// column can't drift across the append/read/read-all queries.
const EVENT_COLUMNS: &str = "global_sequence, stream_id, stream_version, payload, causation_id, \
     correlation_id, idempotency_key";

/// An embedded [`EventStore`] and [`StreamsAll`] over SQLite, via
/// rusqlite — and, for [`Tagged`] events, [`QueryAppend`] (0.7.1),
/// answered by scanning the log. Its [`CommitSignal`] (0.7.2) wakes
/// subscribers on commits made through this store or its clones;
/// another connection writing the same file is seen at the next timed
/// poll.
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
    /// Views folded inside every append transaction (0.7.3).
    #[cfg(feature = "views")]
    inline_views: eventyr_projection::inline::InlineViews<E>,
    /// Raised after every commit (0.7.2); shared by clones.
    signal: LocalCommitSignal,
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> Clone for SqliteStore<E> {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            #[cfg(feature = "views")]
            inline_views: Arc::clone(&self.inline_views),
            signal: self.signal.clone(),
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
        // Databases created before 0.7.5 lack the column.
        let has_key: bool = conn.query_row(
            "SELECT count(*) FROM pragma_table_info('events') WHERE name = 'idempotency_key'",
            [],
            |row| row.get::<_, i64>(0).map(|n| n > 0),
        )?;
        if !has_key {
            conn.execute_batch("ALTER TABLE events ADD COLUMN idempotency_key TEXT")?;
        }
        #[cfg(feature = "views")]
        conn.execute_batch(crate::views::VIEWS_SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            #[cfg(feature = "views")]
            inline_views: Arc::from([]),
            signal: LocalCommitSignal::new(),
            _event: std::marker::PhantomData,
        })
    }

    /// Maintain `views` inline (0.7.3): every append folds its committed
    /// events into the views' rows in the `views` table, inside the
    /// append's transaction, and a row that cannot be written fails the
    /// append. Read the rows with [`SqliteViewStore`](crate::SqliteViewStore).
    #[cfg(feature = "views")]
    pub fn with_inline_views(
        mut self,
        views: Vec<Arc<dyn eventyr_projection::inline::InlineView<E>>>,
    ) -> Self {
        self.inline_views = views.into();
        self
    }

    /// Fold `committed` into the inline views inside `tx` — a no-op
    /// without the `views` feature.
    #[allow(clippy::unused_self, reason = "no-op without the `views` feature")]
    fn write_views(
        &self,
        tx: &rusqlite::Transaction<'_>,
        committed: &[EventEnvelope<E>],
    ) -> Result<(), StoreError> {
        #[cfg(feature = "views")]
        {
            crate::views::write_inline_views(tx, &self.inline_views, committed)
        }
        #[cfg(not(feature = "views"))]
        {
            let _ = (tx, committed);
            Ok(())
        }
    }

    /// The connection, for sibling stores on the same database (the
    /// snapshot and view stores behind their features).
    #[cfg(any(
        feature = "snapshots",
        feature = "views",
        feature = "shred",
        feature = "parked"
    ))]
    pub(crate) fn conn(&self) -> Arc<Mutex<rusqlite::Connection>> {
        Arc::clone(&self.conn)
    }

    /// Raise the commit signal when the commit wrote any event — after
    /// the commit, so a woken reader sees it.
    fn notify_if_written<T>(&self, committed: &[Vec<T>]) {
        if committed.iter().any(|events| !events.is_empty()) {
            self.signal.notify();
        }
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
    idempotency_key: Option<String>,
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        global_sequence: row.get(0)?,
        stream_id: row.get(1)?,
        stream_version: row.get(2)?,
        payload: row.get(3)?,
        causation_id: row.get(4)?,
        correlation_id: row.get(5)?,
        idempotency_key: row.get(6)?,
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
            sequence: Sequence::new(
                u64::try_from(row.global_sequence).map_err(|_| invalid(row.global_sequence))?,
            ),
            stream_id: StreamId::from(row.stream_id),
            version: Version::new(
                u64::try_from(row.stream_version).map_err(|_| invalid(row.stream_version))?,
            ),
            event,
            metadata: Metadata {
                idempotency_key: row.idempotency_key,
                ..Metadata::of_ids(row.causation_id, row.correlation_id)
            },
        })
    }
}

/// A stream's lifecycle row, or the defaults for a stream that has none.
struct Lifecycle {
    closed: bool,
    first_kept: i64,
    head: i64,
}

fn lifecycle(conn: &rusqlite::Connection, stream_id: &StreamId) -> Result<Lifecycle, StoreError> {
    use rusqlite::OptionalExtension;
    Ok(conn
        .query_row(
            "SELECT closed, first_kept, head FROM stream_lifecycle WHERE stream_id = ?1",
            [stream_id.as_str()],
            |row| {
                Ok(Lifecycle {
                    closed: row.get::<_, i64>(0)? != 0,
                    first_kept: row.get(1)?,
                    head: row.get(2)?,
                })
            },
        )
        .optional()
        .map_err(SqliteStoreError::into_store)?
        .unwrap_or(Lifecycle {
            closed: false,
            first_kept: 1,
            head: 0,
        }))
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
        let life = lifecycle(tx, stream_id)?;
        if life.closed {
            return Err(StoreError::StreamClosed {
                stream_id: stream_id.clone(),
            });
        }
        // The head, inside the transaction: the newest row's version, or
        // the recorded head of a stream truncated to nothing.
        let current: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(stream_version), 0) FROM events WHERE stream_id = ?1",
                [stream_id.as_str()],
                |row| row.get(0),
            )
            .map_err(SqliteStoreError::into_store)?;
        let current = current.max(life.head);
        if !eventyr_store::store::expected_version_matches(*expected, current.max(0) as u64) {
            return Err(StoreError::Conflict {
                stream_id: Some(stream_id.clone()),
                current: Version::new(current.max(0) as u64),
            });
        }
        let mut written = Vec::with_capacity(events.len());
        for (index, new_event) in events.iter().enumerate() {
            let version = current.max(0) as u64 + index as u64 + 1;
            let payload =
                serde_json::to_string(&new_event.event).map_err(SqliteStoreError::from)?;
            tx.execute(
                "INSERT INTO events (stream_id, stream_version, event_type, payload, causation_id, \
                 correlation_id, idempotency_key) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    stream_id.as_str(),
                    version as i64,
                    new_event.event.event_name(),
                    payload,
                    new_event.metadata.causation_id,
                    new_event.metadata.correlation_id,
                    new_event.metadata.idempotency_key,
                ],
            )
            .map_err(SqliteStoreError::into_store)?;
            let sequence = tx.last_insert_rowid();
            written.push(EventEnvelope {
                sequence: Sequence::new(sequence as u64),
                stream_id: stream_id.clone(),
                version: Version::new(version),
                event: new_event.event.clone(),
                metadata: Metadata {
                    idempotency_key: new_event.metadata.idempotency_key.clone(),
                    ..Metadata::of_ids(
                        new_event.metadata.causation_id.clone(),
                        new_event.metadata.correlation_id.clone(),
                    )
                },
            });
        }
        committed.push(written);
    }
    Ok(committed)
}

/// Every envelope after `from` (exclusive), in global order — the body
/// of `stream_all`, and the scan behind the query reads.
///
/// SQLite has no identity columns to burn on rollback: gaps come from
/// deleted rows or failed transactions, neither of which the schema
/// allows — the port's gap-tolerance contract rides free.
fn read_after<E>(
    conn: &rusqlite::Connection,
    from: Sequence,
) -> Vec<Result<EventEnvelope<E>, StoreError>>
where
    E: serde::de::DeserializeOwned,
{
    let sql = format!(
        "SELECT {EVENT_COLUMNS} FROM events WHERE global_sequence > ?1 \
         ORDER BY global_sequence"
    );
    conn.prepare(&sql)
        .map_err(SqliteStoreError::into_store)
        .and_then(|mut stmt| {
            stmt.query_map(rusqlite::params![from.as_u64() as i64], read_row)
                .map_err(SqliteStoreError::into_store)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(SqliteStoreError::into_store)
        })
        .map(|rows| rows.into_iter().map(EventEnvelope::try_from).collect())
        .unwrap_or_else(|error| vec![Err(error)])
}

/// The envelopes after `from` that `query` selects, matched on the
/// decoded event. Tags are a pure function of the payload, so a tag
/// column written at append time would be wrong for every event stored
/// before it existed; the scan is correct on any history.
fn read_query<E>(
    conn: &rusqlite::Connection,
    query: &Query,
    from: Sequence,
) -> Vec<Result<EventEnvelope<E>, StoreError>>
where
    E: serde::de::DeserializeOwned + EventName + Tagged,
{
    if query.items.is_empty() {
        return Vec::new();
    }
    read_after(conn, from)
        .into_iter()
        .filter(|result| match result {
            Ok(envelope) => query.selects(&envelope.event),
            Err(_) => true,
        })
        .collect()
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
        let mut committed = append_within(&tx, &[(stream_id.clone(), expected, events)])?;
        self.write_views(&tx, &committed[0])?;
        tx.commit().map_err(SqliteStoreError::into_store)?;
        self.notify_if_written(&committed);
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
        self.write_views(&tx, &committed.concat())?;
        tx.commit().map_err(SqliteStoreError::into_store)?;
        self.notify_if_written(&committed);
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
        match lifecycle(&conn, stream_id) {
            Ok(life) if (from.as_u64() as i64) + 1 < life.first_kept => {
                return iter(vec![Err(StoreError::Truncated {
                    stream_id: stream_id.clone(),
                    first: Version::new(life.first_kept as u64),
                })]);
            }
            Ok(_) => {}
            Err(error) => return iter(vec![Err(error)]),
        }
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE stream_id = ?1 AND stream_version > ?2 \
             ORDER BY stream_version"
        );
        let events: Vec<Result<EventEnvelope<E>, StoreError>> = conn
            .prepare(&sql)
            .map_err(SqliteStoreError::into_store)
            .and_then(|mut stmt| {
                stmt.query_map(
                    rusqlite::params![stream_id.as_str(), from.as_u64() as i64],
                    read_row,
                )
                .map_err(SqliteStoreError::into_store)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(SqliteStoreError::into_store)
            })
            .map(|rows| rows.into_iter().map(EventEnvelope::try_from).collect())
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
        iter(read_after(&self.lock(), from))
    }

    /// Filter in the database (0.7.4): one connection, so the scan bound
    /// and the matching rows come from the same state. The bound is the
    /// `scan_limit`-th row after `from` (or the head, if nearer).
    async fn stream_all_filtered(
        &self,
        from: Sequence,
        filter: &EventFilter,
        max: usize,
        scan_limit: usize,
    ) -> Result<FilteredRead<E>, StoreError> {
        let conn = self.lock();
        let from_i64 = from.as_u64() as i64;
        let bound: Option<i64> = conn
            .query_row(
                "SELECT max(global_sequence) FROM ( \
                     SELECT global_sequence FROM events WHERE global_sequence > ?1 \
                     ORDER BY global_sequence LIMIT ?2)",
                rusqlite::params![from_i64, scan_limit.max(1) as i64],
                |row| row.get(0),
            )
            .map_err(SqliteStoreError::into_store)?;
        let Some(bound) = bound else {
            return Ok(FilteredRead {
                events: Vec::new(),
                scanned: from,
            });
        };
        // Built from fixed fragments and numbered placeholders only;
        // every filter value is a bound parameter.
        let mut sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE global_sequence > ?1 AND global_sequence <= ?2"
        );
        let mut params: Vec<rusqlite::types::Value> = vec![from_i64.into(), bound.into()];
        if !filter.stream_prefixes.is_empty() {
            let clauses: Vec<String> = filter
                .stream_prefixes
                .iter()
                .map(|prefix| {
                    params.push(prefix.clone().into());
                    // substr compares bytes exactly — LIKE would treat
                    // `%`/`_` in a prefix as wildcards.
                    format!(
                        "substr(stream_id, 1, length(?{n})) = ?{n}",
                        n = params.len()
                    )
                })
                .collect();
            sql.push_str(&format!(" AND ({})", clauses.join(" OR ")));
        }
        if !filter.event_types.is_empty() {
            let placeholders: Vec<String> = filter
                .event_types
                .iter()
                .map(|name| {
                    params.push(name.clone().into());
                    format!("?{}", params.len())
                })
                .collect();
            sql.push_str(&format!(" AND event_type IN ({})", placeholders.join(", ")));
        }
        params.push((max as i64).into());
        sql.push_str(&format!(
            " ORDER BY global_sequence LIMIT ?{}",
            params.len()
        ));
        let rows = conn
            .prepare(&sql)
            .and_then(|mut stmt| {
                stmt.query_map(rusqlite::params_from_iter(params), read_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(SqliteStoreError::into_store)?;
        let full = rows.len() == max;
        let events = rows
            .into_iter()
            .map(EventEnvelope::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let scanned = match events.last() {
            Some(last) if full => last.sequence,
            _ => Sequence::new(bound.max(0) as u64),
        };
        Ok(FilteredRead { events, scanned })
    }
}

impl<E> QueryAppend for SqliteStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Tagged + Clone + Send + Sync,
{
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        iter(read_query(&self.lock(), query, after))
    }

    /// Check the condition and write in one `IMMEDIATE` transaction:
    /// SQLite's write lock is taken before the check reads, so no
    /// writer — through this store, a clone, or another connection to
    /// the same file — can commit between the check and the write.
    async fn append_if(
        &self,
        appends: Vec<StreamAppend<E>>,
        condition: AppendCondition,
    ) -> Result<Vec<CommittedStream<E>>, StoreError> {
        let mut conn = self.lock();
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(SqliteStoreError::into_store)?;
        let mut latest = None;
        for envelope in read_query::<E>(&tx, &condition.query, condition.after) {
            latest = Some(envelope?.sequence);
        }
        if let Some(sequence) = latest {
            return Err(StoreError::QueryConflict { sequence });
        }
        let appends: Vec<(StreamId, ExpectedVersion, Vec<NewEvent<E>>)> = appends
            .into_iter()
            .map(|a| (a.stream_id, a.expected, a.events))
            .collect();
        let committed = append_within(&tx, &appends)?;
        self.write_views(&tx, &committed.concat())?;
        tx.commit().map_err(SqliteStoreError::into_store)?;
        self.notify_if_written(&committed);
        Ok(appends
            .into_iter()
            .zip(committed)
            .map(|((stream_id, _, _), events)| CommittedStream { stream_id, events })
            .collect())
    }
}

impl<E> CommitSignal for SqliteStore<E>
where
    E: Send,
{
    type Listener = LocalCommitListener;

    fn subscribe(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Listener, StoreError>> + Send {
        self.signal.subscribe()
    }
}

impl<E> StreamLifecycle for SqliteStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    async fn close_stream(&self, stream_id: &StreamId) -> Result<(), StoreError> {
        self.lock()
            .execute(
                "INSERT INTO stream_lifecycle (stream_id, closed) VALUES (?1, 1) \
                 ON CONFLICT (stream_id) DO UPDATE SET closed = 1",
                [stream_id.as_str()],
            )
            .map_err(SqliteStoreError::into_store)?;
        Ok(())
    }

    /// One `IMMEDIATE` transaction: record the head and the cut, then
    /// delete the rows below it. The head is recorded so a stream
    /// truncated to nothing keeps its version.
    async fn truncate_before(
        &self,
        stream_id: &StreamId,
        version: Version,
    ) -> Result<(), StoreError> {
        let mut conn = self.lock();
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(SqliteStoreError::into_store)?;
        let life = lifecycle(&tx, stream_id)?;
        let rows_head: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(stream_version), 0) FROM events WHERE stream_id = ?1",
                [stream_id.as_str()],
                |row| row.get(0),
            )
            .map_err(SqliteStoreError::into_store)?;
        let head = rows_head.max(life.head);
        let cut = version.as_u64() as i64;
        if cut > head + 1 {
            return Err(StoreError::other(format!(
                "cannot truncate {stream_id} before {version}: it ends at {head}"
            )));
        }
        if cut <= life.first_kept {
            return Ok(());
        }
        tx.execute(
            "INSERT INTO stream_lifecycle (stream_id, first_kept, head) VALUES (?1, ?2, ?3) \
             ON CONFLICT (stream_id) DO UPDATE SET first_kept = ?2, head = ?3",
            rusqlite::params![stream_id.as_str(), cut, head],
        )
        .map_err(SqliteStoreError::into_store)?;
        tx.execute(
            "DELETE FROM events WHERE stream_id = ?1 AND stream_version < ?2",
            rusqlite::params![stream_id.as_str(), cut],
        )
        .map_err(SqliteStoreError::into_store)?;
        tx.commit().map_err(SqliteStoreError::into_store)?;
        Ok(())
    }
}
