//! The Postgres [`EventStore`] — the 0.2 store.
//!
//! One table, one PL/pgSQL function: `append_events` takes a per-stream
//! advisory lock, checks the expected version against the live max, and
//! inserts the batch. See the [crate-level docs](crate) for the schema
//! and the wire format.

use std::future::Future;
use std::path::Path;

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{EventFilter, EventStore, FilteredRead, QueryAppend, StreamsAll};
use futures::Stream;
use sqlx::postgres::PgPool;

use crate::PgStoreError;
use std::collections::BTreeMap;
use std::sync::Arc;

use eventyr_projection::inline::{
    InlineView, InlineViews, RowKey, StoredRow, fold_inline, rows_touched,
};

/// An [`EventStore`] over Postgres, via sqlx, for one event enum `E` —
/// and, for [`Tagged`] events, [`QueryAppend`] (0.7.1).
///
/// Shareable and cloneable (it wraps a [`PgPool`]): the pool owns the
/// connection count; the store holds no other state.
pub struct PgStore<E> {
    pool: PgPool,
    /// Views folded inside every append transaction (0.7.3).
    inline_views: InlineViews<E>,
}

impl<E> Clone for PgStore<E> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            inline_views: Arc::clone(&self.inline_views),
        }
    }
}

impl<E> PgStore<E> {
    /// Wrap an existing pool. Run the migration first
    /// ([`migrate`]) — the store assumes the schema.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            inline_views: Arc::from([]),
        }
    }

    /// Maintain `views` inline (0.7.3): every append folds its committed
    /// events into the views' rows in the `views` table, inside the
    /// append's transaction. A row that cannot be written fails the
    /// append. Read the rows with [`PgViewStore`](crate::views::PgViewStore)
    /// — they are the same rows an async
    /// [`ViewProjection`](eventyr_projection::view::ViewProjection) of the
    /// same view writes, under the same newest-wins guard.
    ///
    /// Every append commits one at a time on Postgres already (the
    /// commit-order lock, migration 0006), so inline folds never race;
    /// their cost is added to that serialized section. Keep inline views
    /// cheap, and steer heavy or rarely-read ones to the async path.
    pub fn with_inline_views(mut self, views: Vec<Arc<dyn InlineView<E>>>) -> Self {
        self.inline_views = views.into();
        self
    }

    /// Build a pool and run the migration.
    pub async fn connect(url: &str) -> Result<Self, PgStoreError> {
        let pool = PgPool::connect(url).await?;
        migrate(&pool).await?;
        Ok(Self::new(pool))
    }

    /// The pool, for projections and queries that share the database.
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Run the migrations against a pool.
pub async fn migrate(pool: &PgPool) -> Result<(), PgStoreError> {
    let migrations = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    sqlx::migrate::Migrator::new(migrations)
        .await?
        .run(pool)
        .await?;
    Ok(())
}

/// The `events` columns every read shares — one place, so adding a
/// column can't drift across the append/read/read-all queries.
const EVENT_COLUMNS: &str =
    "global_sequence, stream_id, stream_version, payload, metadata, created_at";

/// One `append_events` call's arguments: the expectation kind, the
/// exact version, and the per-event arrays fanned out from one source
/// of truth so the four stay correlated.
type AppendArgs = (
    i16,
    i64,
    Vec<String>,
    Vec<serde_json::Value>,
    Vec<Option<String>>,
    Vec<Option<String>>,
    Vec<Option<String>>,
);

fn append_args<E: serde::Serialize + EventName>(
    expected: ExpectedVersion,
    events: Vec<NewEvent<E>>,
) -> Result<AppendArgs, PgStoreError> {
    let (kind, exact) = expectation_args(expected);
    let mut names = Vec::with_capacity(events.len());
    let mut payloads = Vec::with_capacity(events.len());
    let mut causations = Vec::with_capacity(events.len());
    let mut correlations = Vec::with_capacity(events.len());
    let mut keys = Vec::with_capacity(events.len());
    for new_event in events {
        names.push(new_event.event.event_name().to_string());
        payloads.push(serde_json::to_value(&new_event.event).map_err(PgStoreError::from)?);
        causations.push(new_event.metadata.causation_id);
        correlations.push(new_event.metadata.correlation_id);
        keys.push(new_event.metadata.idempotency_key);
    }
    Ok((kind, exact, names, payloads, causations, correlations, keys))
}

/// Run one `append_events` call against `conn` within an open
/// transaction.
async fn append_events_tx(
    conn: &mut sqlx::PgConnection,
    stream_id: &str,
    args: AppendArgs,
) -> Result<Vec<EventRow>, sqlx::Error> {
    let (kind, exact, names, payloads, causations, correlations, keys) = args;
    let query = sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_COLUMNS} FROM append_events($1, $2, $3, $4, $5, $6, $7, $8)"
    ));
    sqlx::query_as::<_, EventRow>(query)
        .bind(kind)
        .bind(exact)
        .bind(stream_id)
        .bind(&names[..])
        .bind(&payloads[..])
        .bind(&causations[..])
        .bind(&correlations[..])
        .bind(&keys[..])
        .fetch_all(conn)
        .await
}

/// One row of the events table, as read back. The `payload` column holds
/// `{variant, args}` (externally-tagged serde) (the stored event); `metadata` is the JSONB
/// envelope with the two ids.
#[derive(sqlx::FromRow)]
struct EventRow {
    global_sequence: i64,
    stream_id: String,
    stream_version: i64,
    payload: serde_json::Value,
    metadata: serde_json::Value,
    #[cfg(feature = "time")]
    created_at: time::OffsetDateTime,
}

/// What the `metadata` column carries — just the ids; `created_at` is
/// the envelope's timestamp, not part of the JSON.
#[derive(serde::Deserialize)]
struct StoredMetadata {
    causation_id: Option<String>,
    correlation_id: Option<String>,
    /// 0.7.5; rows written before it have none.
    #[serde(default)]
    idempotency_key: Option<String>,
}

impl<E> TryFrom<EventRow> for EventEnvelope<E>
where
    E: serde::de::DeserializeOwned + EventName,
{
    type Error = PgStoreError;

    fn try_from(row: EventRow) -> Result<Self, Self::Error> {
        // The payload column is the event's externally-tagged serde
        // (`{variant: args}`), so we deserialize it directly back into
        // the enum. `event_type` lives on the row for the upcaster to
        // select by — it isn't needed to decode the payload.
        let event: E = serde_json::from_value(row.payload).map_err(|error| {
            PgStoreError::CorruptRow(format!("the payload does not decode: {error}"))
        })?;
        let metadata: StoredMetadata = serde_json::from_value(row.metadata).map_err(|error| {
            PgStoreError::CorruptRow(format!("the metadata does not decode: {error}"))
        })?;

        // Positions must be non-negative — a negative one is corrupt
        // data, not a number to wrap.
        let invalid =
            |value: i64| PgStoreError::CorruptRow(format!("negative position in the log: {value}"));
        // The two ids ride the `metadata` column; the timestamp is
        // `created_at`, set only when this crate's `time` feature is on.
        let metadata = Metadata {
            idempotency_key: metadata.idempotency_key,
            ..Metadata::of_ids(metadata.causation_id, metadata.correlation_id)
        };
        #[cfg(feature = "time")]
        let metadata = Metadata {
            timestamp: Some(row.created_at),
            ..metadata
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
            metadata,
        })
    }
}

/// The expectation as the function's leading arguments:
/// 0 = Any, 1 = Empty, 2 = Exact. The wire encoding mirrors the port's
/// [`expected_version_matches`](eventyr_store::store::expected_version_matches)
/// — the `append_events` function applies the same rule in SQL.
fn expectation_args(expected: ExpectedVersion) -> (i16, i64) {
    match expected {
        ExpectedVersion::Any => (0, 0),
        ExpectedVersion::Empty => (1, 0),
        ExpectedVersion::Exact(v) => (2, v.as_u64().try_into().unwrap_or(i64::MAX)),
    }
}

/// Take every appended stream's advisory lock, sorted, before writing
/// anything — concurrent batches over overlapping streams queue on the
/// same first lock instead of deadlocking.
async fn lock_streams<E>(
    conn: &mut sqlx::PgConnection,
    appends: &[StreamAppend<E>],
) -> Result<(), StoreError> {
    let mut ids: Vec<&str> = appends.iter().map(|a| a.stream_id.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    for id in &ids {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(id)
            .execute(&mut *conn)
            .await
            .map_err(PgStoreError::into_store)?;
    }
    Ok(())
}

fn decode_rows<E>(rows: Vec<EventRow>) -> Result<Vec<EventEnvelope<E>>, StoreError>
where
    E: serde::de::DeserializeOwned + EventName,
{
    rows.into_iter()
        .map(EventEnvelope::try_from)
        .collect::<Result<Vec<_>, PgStoreError>>()
        .map_err(StoreError::from)
}

/// Run each append's `append_events` inside the caller's open
/// transaction, in input order.
async fn append_all_tx<E>(
    conn: &mut sqlx::PgConnection,
    appends: Vec<StreamAppend<E>>,
) -> Result<Vec<CommittedStream<E>>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName,
{
    let mut committed = Vec::with_capacity(appends.len());
    for append in appends {
        let args = append_args(append.expected, append.events).map_err(StoreError::from)?;
        let rows = append_events_tx(&mut *conn, append.stream_id.as_str(), args)
            .await
            .map_err(PgStoreError::into_store)?;
        let events = rows
            .into_iter()
            .map(EventEnvelope::try_from)
            .collect::<Result<Vec<_>, PgStoreError>>()
            .map_err(StoreError::from)?;
        committed.push(CommittedStream {
            stream_id: append.stream_id,
            events,
        });
    }
    Ok(committed)
}

/// The SQL prefilter for a query: the stored names it can select, or
/// `None` when some item accepts any type (no prefilter possible).
fn query_types(query: &Query) -> Option<Vec<String>> {
    let mut types = Vec::new();
    for item in &query.items {
        if item.types.is_empty() {
            return None;
        }
        types.extend(item.types.iter().map(|name| (*name).to_string()));
    }
    types.sort_unstable();
    types.dedup();
    Some(types)
}

/// One page of the rows a query may select after `cursor`: every row,
/// or only rows of the query's types when it names them all.
async fn query_page<'c>(
    conn: impl sqlx::PgExecutor<'c>,
    types: Option<&[String]>,
    cursor: i64,
    page: i64,
) -> Result<Vec<EventRow>, StoreError> {
    let rows = match types {
        Some(types) => {
            let query = sqlx::AssertSqlSafe(format!(
                "SELECT {EVENT_COLUMNS} FROM events \
                 WHERE event_type = ANY($1) AND global_sequence > $2 \
                 ORDER BY global_sequence LIMIT $3"
            ));
            sqlx::query_as::<_, EventRow>(query)
                .bind(types)
                .bind(cursor)
                .bind(page)
                .fetch_all(conn)
                .await
        }
        None => {
            let query = sqlx::AssertSqlSafe(format!(
                "SELECT {EVENT_COLUMNS} FROM events WHERE global_sequence > $1 \
                 ORDER BY global_sequence LIMIT $2"
            ));
            sqlx::query_as::<_, EventRow>(query)
                .bind(cursor)
                .bind(page)
                .fetch_all(conn)
                .await
        }
    };
    rows.map_err(PgStoreError::into_store)
}

/// Fold `committed` into the inline views and write the changed rows,
/// inside the caller's open transaction.
///
/// No lock of its own: every append path holds the commit-order lock
/// (migration 0006) by the time it gets here, until commit, so view
/// folds are already serialized — two appends to different streams
/// folding into one row cannot both read its old value. If that lock is
/// ever relaxed (per-tag locking, §14), these rows need their own;
/// `concurrent_folds_into_one_row_lose_nothing` fails without either.
async fn write_inline_views<E>(
    conn: &mut sqlx::PgConnection,
    views: &[Arc<dyn InlineView<E>>],
    committed: &[EventEnvelope<E>],
) -> Result<(), StoreError> {
    if views.is_empty() || committed.is_empty() {
        return Ok(());
    }
    let touched = rows_touched(views, committed);
    if touched.is_empty() {
        return Ok(());
    }
    let mut loaded: BTreeMap<RowKey, StoredRow> = BTreeMap::new();
    for (name, id) in &touched {
        let row: Option<(i64, serde_json::Value)> = sqlx::query_as(
            "SELECT version, payload FROM views WHERE view_name = $1 AND view_id = $2",
        )
        .bind(name)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(PgStoreError::into_store)?;
        if let Some((version, payload)) = row {
            let version = u64::try_from(version).map_err(|_| {
                StoreError::from(PgStoreError::CorruptRow(format!(
                    "negative view version: {version}"
                )))
            })?;
            loaded.insert(
                (name.clone(), id.clone()),
                StoredRow {
                    version: Sequence::new(version),
                    payload,
                },
            );
        }
    }
    let rows = fold_inline(views, committed, loaded)
        .map_err(|error| StoreError::Other(Arc::new(error)))?;
    for ((name, id), row) in rows {
        let version = i64::try_from(row.version.as_u64()).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow("view version beyond i64".into()))
        })?;
        sqlx::query(
            "INSERT INTO views (view_name, view_id, version, payload) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (view_name, view_id) DO UPDATE \
             SET version = EXCLUDED.version, payload = EXCLUDED.payload, updated_at = now() \
             WHERE views.version < EXCLUDED.version",
        )
        .bind(&name)
        .bind(&id)
        .bind(version)
        .bind(&row.payload)
        .execute(&mut *conn)
        .await
        .map_err(PgStoreError::into_store)?;
    }
    Ok(())
}

const QUERY_PAGE: i64 = 512;

/// The advisory lock every append holds from drawing its global
/// sequence until it commits (migration 0006): sequences become visible
/// in order, so `stream_all` never shows a later one before an earlier
/// one that will still commit. Must match the key in `append_events`.
const COMMIT_ORDER_LOCK: i64 = 7_300_160_413_598_463_541;

impl<E> EventStore for PgStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    type Event = E;

    fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<E>>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        let pool = self.pool.clone();
        let views = Arc::clone(&self.inline_views);
        let stream_id = stream_id.clone();
        async move {
            if events.is_empty() {
                return Ok(Vec::new());
            }
            let args = append_args(expected, events).map_err(StoreError::from)?;
            let mut conn = pool.acquire().await.map_err(PgStoreError::into_store)?;
            if views.is_empty() {
                // No inline views: one autocommitted statement.
                let rows = append_events_tx(&mut conn, stream_id.as_str(), args)
                    .await
                    .map_err(PgStoreError::into_store)?;
                return decode_rows(rows);
            }
            use sqlx::Acquire;
            let mut tx = conn.begin().await.map_err(PgStoreError::into_store)?;
            let rows = append_events_tx(&mut tx, stream_id.as_str(), args)
                .await
                .map_err(PgStoreError::into_store)?;
            let committed = decode_rows(rows)?;
            write_inline_views(&mut tx, &views, &committed).await?;
            tx.commit().await.map_err(PgStoreError::into_store)?;
            Ok(committed)
        }
    }

    /// Append the whole batch in one transaction. The per-stream
    /// advisory locks are first taken on every stream in sorted order
    /// (so two conflicting batches over the same streams cannot
    /// deadlock — they queue on the same first lock), then each
    /// stream's `append_events` runs against one pooled connection
    /// inside the open transaction: every expectation is checked by the
    /// same function the single-stream path uses, and all locks are
    /// held until the one commit — the batch is all-or-nothing across
    /// streams.
    fn append_batch(
        &self,
        appends: Vec<StreamAppend<E>>,
    ) -> impl Future<Output = Result<Vec<CommittedStream<E>>, StoreError>> + Send {
        let pool = self.pool.clone();
        let views = Arc::clone(&self.inline_views);
        async move {
            use sqlx::Acquire;
            let mut conn = pool.acquire().await.map_err(PgStoreError::into_store)?;
            let mut tx = conn.begin().await.map_err(PgStoreError::into_store)?;

            lock_streams(&mut tx, &appends).await?;
            let committed = append_all_tx(&mut tx, appends).await?;
            let events: Vec<_> = committed
                .iter()
                .flat_map(|c| c.events.iter().cloned())
                .collect();
            write_inline_views(&mut tx, &views, &events).await?;
            tx.commit().await.map_err(PgStoreError::into_store)?;
            Ok(committed)
        }
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let pool = self.pool.clone();
        let stream_id = stream_id.clone();
        Box::pin(async_stream::stream! {
            let query = sqlx::AssertSqlSafe(format!(
                "SELECT {EVENT_COLUMNS} FROM events WHERE stream_id = $1 AND stream_version > $2 \
                 ORDER BY stream_version"
            ));
            let rows = sqlx::query_as::<_, EventRow>(query)
            .bind(stream_id.as_str())
            .bind(from.as_u64().try_into().unwrap_or(i64::MAX))
            .fetch_all(&pool)
            .await
            .map_err(PgStoreError::into_store);

            let rows = match rows {
                Ok(rows) => rows,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            for row in rows {
                yield EventEnvelope::try_from(row).map_err(StoreError::from);
            }
        })
    }
}

impl<E> StreamsAll for PgStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let pool = self.pool.clone();
        Box::pin(async_stream::stream! {
            const PAGE: i64 = 512;
            let mut cursor = from.as_u64().try_into().unwrap_or(i64::MAX);
            loop {
                let query = sqlx::AssertSqlSafe(format!(
                    "SELECT {EVENT_COLUMNS} FROM events WHERE global_sequence > $1 \
                     ORDER BY global_sequence LIMIT $2"
                ));
                let rows = sqlx::query_as::<_, EventRow>(query)
                    .bind(cursor)
                    .bind(PAGE)
                    .fetch_all(&pool)
                    .await
                    .map_err(PgStoreError::into_store);

                let rows = match rows {
                    Ok(rows) => rows,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                let page_len = rows.len();
                for row in rows {
                    cursor = row.global_sequence;
                    yield EventEnvelope::try_from(row).map_err(StoreError::from);
                }
                // An empty (or short) page is the end of the log.
                if page_len < PAGE as usize {
                    return;
                }
            }
        })
    }

    /// Filter in the database (0.7.4). Two queries, one snapshot each:
    /// the scan bound is the `scan_limit`-th row after `from` (or the
    /// head, if nearer), and the events are the matching rows up to that
    /// bound. The bound is read first, so a commit landing between the
    /// queries is beyond it and is never skipped. The commit-order lock
    /// (0006) is what makes "no later sequence before an earlier one"
    /// hold for the bound itself.
    ///
    /// Prefixes match with `starts_with`, which uses the
    /// `(stream_id, stream_version)` index for a single prefix; names
    /// use the `(event_type, global_sequence)` index from 0005.
    fn stream_all_filtered(
        &self,
        from: Sequence,
        filter: &EventFilter,
        max: usize,
        scan_limit: usize,
    ) -> impl Future<Output = Result<FilteredRead<E>, StoreError>> + Send {
        let pool = self.pool.clone();
        let filter = filter.clone();
        async move {
            let from_i64: i64 = from.as_u64().try_into().unwrap_or(i64::MAX);
            let scan_limit = i64::try_from(scan_limit.max(1)).unwrap_or(i64::MAX);
            let bound: Option<i64> = sqlx::query_scalar(
                "SELECT max(global_sequence) FROM ( \
                     SELECT global_sequence FROM events WHERE global_sequence > $1 \
                     ORDER BY global_sequence LIMIT $2 \
                 ) AS scan_window",
            )
            .bind(from_i64)
            .bind(scan_limit)
            .fetch_one(&pool)
            .await
            .map_err(PgStoreError::into_store)?;
            let Some(bound) = bound else {
                return Ok(FilteredRead {
                    events: Vec::new(),
                    scanned: from,
                });
            };
            let max = i64::try_from(max).unwrap_or(i64::MAX);
            let query = sqlx::AssertSqlSafe(format!(
                "SELECT {EVENT_COLUMNS} FROM events \
                 WHERE global_sequence > $1 AND global_sequence <= $2 \
                   AND (cardinality($3::text[]) = 0 \
                        OR EXISTS (SELECT 1 FROM unnest($3::text[]) AS p \
                                   WHERE starts_with(stream_id, p))) \
                   AND (cardinality($4::text[]) = 0 OR event_type = ANY($4)) \
                 ORDER BY global_sequence LIMIT $5"
            ));
            let rows = sqlx::query_as::<_, EventRow>(query)
                .bind(from_i64)
                .bind(bound)
                .bind(&filter.stream_prefixes)
                .bind(&filter.event_types)
                .bind(max)
                .fetch_all(&pool)
                .await
                .map_err(PgStoreError::into_store)?;
            let full = rows.len() as i64 == max;
            let events = decode_rows::<E>(rows)?;
            // A read that filled `max` stopped at its last event; one
            // that did not looked at everything up to the bound.
            let scanned = match events.last() {
                Some(last) if full => last.sequence,
                _ => Sequence::new(u64::try_from(bound).unwrap_or(0)),
            };
            Ok(FilteredRead { events, scanned })
        }
    }
}

impl<E> QueryAppend for PgStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Tagged + Clone + Send + Sync,
{
    /// Page through the rows of the query's types (all rows when an
    /// item accepts any type), matching tags on the decoded event.
    /// Tags are a pure function of the payload, so matching the decoded
    /// event is correct on any history — including rows written before
    /// the event type grew a tag.
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let pool = self.pool.clone();
        let query = query.clone();
        Box::pin(async_stream::stream! {
            if query.items.is_empty() {
                return;
            }
            let types = query_types(&query);
            let mut cursor = after.as_u64().try_into().unwrap_or(i64::MAX);
            loop {
                let rows = match query_page(&pool, types.as_deref(), cursor, QUERY_PAGE).await {
                    Ok(rows) => rows,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                let page_len = rows.len();
                for row in rows {
                    cursor = row.global_sequence;
                    match EventEnvelope::<E>::try_from(row) {
                        Ok(envelope) if query.selects(&envelope.event) => yield Ok(envelope),
                        Ok(_) => {}
                        Err(error) => yield Err(StoreError::from(error)),
                    }
                }
                if page_len < QUERY_PAGE as usize {
                    return;
                }
            }
        })
    }

    /// One transaction: the appended streams' advisory locks (sorted,
    /// as `append_batch` takes them), then the commit-order lock
    /// (migration 0006), then the condition check, then the writes.
    ///
    /// Every append holds the commit-order lock from drawing its
    /// sequence until it commits, so once this transaction holds it, no
    /// matching event is in flight: the check sees every committed event,
    /// and nothing can commit between the check and the write. Without
    /// it, a READ COMMITTED check reads past an uncommitted matching row
    /// and oversells. The lock order (stream locks, then the commit-order
    /// lock) is the one every append takes, so no two writers deadlock.
    fn append_if(
        &self,
        appends: Vec<StreamAppend<E>>,
        condition: AppendCondition,
    ) -> impl Future<Output = Result<Vec<CommittedStream<E>>, StoreError>> + Send {
        let pool = self.pool.clone();
        let views = Arc::clone(&self.inline_views);
        async move {
            use sqlx::Acquire;
            let mut conn = pool.acquire().await.map_err(PgStoreError::into_store)?;
            let mut tx = conn.begin().await.map_err(PgStoreError::into_store)?;

            lock_streams(&mut tx, &appends).await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(COMMIT_ORDER_LOCK)
                .execute(&mut *tx)
                .await
                .map_err(PgStoreError::into_store)?;

            if !condition.query.items.is_empty() {
                let types = query_types(&condition.query);
                let mut cursor: i64 = condition.after.as_u64().try_into().unwrap_or(i64::MAX);
                let mut latest = None;
                loop {
                    let rows = query_page(&mut *tx, types.as_deref(), cursor, QUERY_PAGE).await?;
                    let page_len = rows.len();
                    for row in rows {
                        cursor = row.global_sequence;
                        let envelope =
                            EventEnvelope::<E>::try_from(row).map_err(StoreError::from)?;
                        if condition.query.selects(&envelope.event) {
                            latest = Some(envelope.sequence);
                        }
                    }
                    if page_len < QUERY_PAGE as usize {
                        break;
                    }
                }
                if let Some(sequence) = latest {
                    // Dropping `tx` rolls back and releases every lock.
                    return Err(StoreError::QueryConflict { sequence });
                }
            }

            let committed = append_all_tx(&mut tx, appends).await?;
            let events: Vec<_> = committed
                .iter()
                .flat_map(|c| c.events.iter().cloned())
                .collect();
            write_inline_views(&mut tx, &views, &events).await?;
            tx.commit().await.map_err(PgStoreError::into_store)?;
            Ok(committed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_core::error::StoreError;
    use serde::Deserialize;

    #[derive(Clone, Deserialize, Debug, PartialEq)]
    enum TestEvent {
        Ping { value: u64 },
    }

    impl EventName for TestEvent {
        fn event_name(&self) -> &'static str {
            match self {
                TestEvent::Ping { .. } => "Ping",
            }
        }
    }

    fn row(payload: serde_json::Value, metadata: serde_json::Value) -> EventRow {
        EventRow {
            global_sequence: 1,
            stream_id: "stream-1".into(),
            stream_version: 1,
            payload,
            metadata,
            #[cfg(feature = "time")]
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn good_payload() -> serde_json::Value {
        serde_json::json!({"Ping": {"value": 9}})
    }

    fn good_metadata() -> serde_json::Value {
        serde_json::json!({"causation_id": "cmd-1", "correlation_id": "corr-1"})
    }

    fn corrupt_row(error: PgStoreError, needle: &str) {
        match error {
            PgStoreError::CorruptRow(message) => {
                assert!(message.contains(needle), "message: {message}");
            }
            other => panic!("expected CorruptRow, got {other:?}"),
        }
    }

    #[test]
    fn a_well_formed_row_decodes() {
        // A full pass through the happy path pins the decode contract
        // the failure tests below interrupt.
        let envelope = EventEnvelope::<TestEvent>::try_from(row(good_payload(), good_metadata()))
            .expect("decodes");
        assert_eq!(envelope.event, TestEvent::Ping { value: 9 });
        assert_eq!(envelope.sequence, Sequence::new(1));
        assert_eq!(envelope.version, Version::new(1));
        assert_eq!(envelope.stream_id.as_str(), "stream-1");
        assert_eq!(envelope.metadata.causation_id.as_deref(), Some("cmd-1"));
    }

    #[test]
    fn an_undecodable_payload_is_a_corrupt_row() {
        // Not the variant the enum offers.
        let payload = serde_json::json!({"Bounce": {}});
        corrupt_row(
            EventEnvelope::<TestEvent>::try_from(row(payload, good_metadata()))
                .expect_err("the payload does not decode"),
            "payload",
        );
    }

    #[test]
    fn undecodable_metadata_is_a_corrupt_row() {
        // ids arrive as the wrong shape.
        let metadata = serde_json::json!({"causation_id": 42, "correlation_id": null});
        corrupt_row(
            EventEnvelope::<TestEvent>::try_from(row(good_payload(), metadata))
                .expect_err("the metadata does not decode"),
            "metadata",
        );
    }

    #[test]
    fn a_negative_sequence_is_a_corrupt_row() {
        corrupt_row(
            EventEnvelope::<TestEvent>::try_from(EventRow {
                global_sequence: -1,
                ..row(good_payload(), good_metadata())
            })
            .expect_err("negative sequence"),
            "negative position",
        );
    }

    #[test]
    fn a_negative_version_is_a_corrupt_row() {
        corrupt_row(
            EventEnvelope::<TestEvent>::try_from(EventRow {
                stream_version: -1,
                ..row(good_payload(), good_metadata())
            })
            .expect_err("negative version"),
            "negative position",
        );
    }

    #[test]
    fn a_corrupt_row_maps_to_store_error_other() {
        // The store-level conversion the streams lean on: a corrupt row
        // is fatal-by-construction, not a conflict or an outage.
        let error = StoreError::from(PgStoreError::CorruptRow(
            "the payload does not decode".into(),
        ));
        assert!(matches!(error, StoreError::Other(_)));
    }

    #[test]
    fn expectation_args_encode_kind_and_version() {
        assert_eq!(expectation_args(ExpectedVersion::Any), (0, 0));
        assert_eq!(expectation_args(ExpectedVersion::Empty), (1, 0));
        assert_eq!(
            expectation_args(ExpectedVersion::Exact(Version::new(3))),
            (2, 3)
        );
        // A version beyond i64 saturates rather than wrapping.
        assert_eq!(
            expectation_args(ExpectedVersion::Exact(Version::new(u64::MAX))),
            (2, i64::MAX)
        );
    }
}
