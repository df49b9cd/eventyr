//! The Postgres [`EventStore`] — the 0.2 store.
//!
//! One table, one PL/pgSQL function: `append_events` takes a per-stream
//! advisory lock, checks the expected version against the live max, and
//! inserts the batch. See the [crate-level docs](crate) for the schema
//! and the wire format.

use std::future::Future;

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{
    EventFilter, EventStore, FilteredRead, QueryAppend, StreamLifecycle, StreamsAll, TruncatePlan,
    all_events, plan_truncate, read_starts_before_cut, sql_position, validate_batch,
};
use futures::Stream;
use sqlx::postgres::PgPool;

use crate::PgStoreError;
#[cfg(feature = "views")]
use std::sync::Arc;

#[cfg(feature = "views")]
use eventyr_projection::inline::{InlineView, InlineViews};

/// An [`EventStore`] over Postgres, via sqlx, for one event enum `E` —
/// and, for [`Tagged`] events, [`QueryAppend`] (roadmap 0.7.1).
///
/// Shareable and cloneable (it wraps a [`PgPool`]): the pool owns the
/// connection count; the store holds no other state.
pub struct PgStore<E> {
    pool: PgPool,
    /// Views folded inside every append transaction (roadmap 0.7.3).
    #[cfg(feature = "views")]
    inline_views: InlineViews<E>,
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> Clone for PgStore<E> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            #[cfg(feature = "views")]
            inline_views: Arc::clone(&self.inline_views),
            _event: std::marker::PhantomData,
        }
    }
}

impl<E> PgStore<E> {
    /// Wrap an existing pool. Run the migration first
    /// ([`migrate`]) — the store assumes the schema.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            #[cfg(feature = "views")]
            inline_views: Arc::from([]),
            _event: std::marker::PhantomData,
        }
    }

    /// Maintain `views` inline (roadmap 0.7.3): every append folds its committed
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
    #[cfg(feature = "views")]
    pub fn with_inline_views(mut self, views: Vec<Arc<dyn InlineView<E>>>) -> Self {
        self.inline_views = views.into();
        self
    }

    /// Whether appends have inline views to fold — always `false`
    /// without the `views` feature.
    #[allow(clippy::unused_self, reason = "constant without the `views` feature")]
    fn has_inline_views(&self) -> bool {
        #[cfg(feature = "views")]
        {
            !self.inline_views.is_empty()
        }
        #[cfg(not(feature = "views"))]
        {
            false
        }
    }

    /// Fold `committed` into the inline views inside the caller's open
    /// transaction — a no-op without the `views` feature.
    async fn write_views(
        &self,
        conn: &mut sqlx::PgConnection,
        committed: &[EventEnvelope<E>],
    ) -> Result<(), StoreError> {
        #[cfg(feature = "views")]
        {
            crate::views::write_inline_views(conn, &self.inline_views, committed).await
        }
        #[cfg(not(feature = "views"))]
        {
            let _ = (conn, committed);
            Ok(())
        }
    }

    /// Build a pool and run the migration.
    ///
    /// # Errors
    ///
    /// The pool could not connect (bad URL, database unreachable,
    /// authentication refused), or a migration failed — the schema is
    /// half-applied only in the sense sqlx records: applied migrations
    /// stay applied, and a retry resumes where it stopped.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use eventyr_store_postgres::PgStore;
    ///
    /// // Connect, migrate, and the store assumes the schema.
    /// let store: PgStore<String> = PgStore::connect(
    ///     "postgres://eventyr:eventyr@localhost:5432/eventyr"
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
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

/// The migrations, compiled into the binary: running them needs no
/// source tree at run time.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run the migrations against a pool.
///
/// # Errors
///
/// A migration failed against the database: a rejected DDL statement,
/// a conflicting existing table (`_sqlx_migrations` already held by
/// another tool — see the crate docs on schemas), or a connection
/// failure mid-run. Applied migrations stay applied; a retry resumes.
pub async fn migrate(pool: &PgPool) -> Result<(), PgStoreError> {
    MIGRATOR.run(pool).await?;
    Ok(())
}

/// The `events` columns every read shares — one place, so adding a
/// column can't drift across the append/read/read-all queries.
const EVENT_COLUMNS: &str =
    "global_sequence, stream_id, stream_version, payload, metadata, created_at";

/// One `append_events` call's arguments: the expectation, and the
/// per-event columns fanned out into parallel arrays — one entry per
/// event in each, built together so they stay correlated.
struct AppendArgs {
    /// 0 = Any, 1 = Empty, 2 = Exact (see [`expectation_args`]).
    expected_kind: i16,
    /// The version an `Exact` expectation names; 0 otherwise.
    expected_version: i64,
    event_types: Vec<String>,
    payloads: Vec<serde_json::Value>,
    causation_ids: Vec<Option<String>>,
    correlation_ids: Vec<Option<String>>,
    idempotency_keys: Vec<Option<String>>,
}

impl AppendArgs {
    fn new<E: serde::Serialize + EventName>(
        expected: ExpectedVersion,
        events: Vec<NewEvent<E>>,
    ) -> Result<Self, PgStoreError> {
        let (expected_kind, expected_version) = expectation_args(expected);
        let mut args = Self {
            expected_kind,
            expected_version,
            event_types: Vec::with_capacity(events.len()),
            payloads: Vec::with_capacity(events.len()),
            causation_ids: Vec::with_capacity(events.len()),
            correlation_ids: Vec::with_capacity(events.len()),
            idempotency_keys: Vec::with_capacity(events.len()),
        };
        for new_event in events {
            args.event_types
                .push(new_event.event.event_name().to_string());
            args.payloads
                .push(serde_json::to_value(&new_event.event).map_err(PgStoreError::from)?);
            args.causation_ids.push(new_event.metadata.causation_id);
            args.correlation_ids.push(new_event.metadata.correlation_id);
            args.idempotency_keys
                .push(new_event.metadata.idempotency_key);
        }
        Ok(args)
    }
}

/// Run one `append_events` call against `conn` within an open
/// transaction.
async fn append_events_tx(
    conn: &mut sqlx::PgConnection,
    stream_id: &str,
    args: AppendArgs,
) -> Result<Vec<EventRow>, sqlx::Error> {
    let query = sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_COLUMNS} FROM append_events($1, $2, $3, $4, $5, $6, $7, $8)"
    ));
    sqlx::query_as::<_, EventRow>(query)
        .bind(args.expected_kind)
        .bind(args.expected_version)
        .bind(stream_id)
        .bind(&args.event_types[..])
        .bind(&args.payloads[..])
        .bind(&args.causation_ids[..])
        .bind(&args.correlation_ids[..])
        .bind(&args.idempotency_keys[..])
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
        // The ids and the key ride the `metadata` column; the timestamp
        // is `created_at`, set only when this crate's `time` feature is on.
        #[cfg_attr(not(feature = "time"), allow(unused_mut))]
        let mut metadata = Metadata::stored(
            metadata.causation_id,
            metadata.correlation_id,
            metadata.idempotency_key,
        );
        #[cfg(feature = "time")]
        {
            metadata.timestamp = Some(row.created_at);
        }
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
        ExpectedVersion::Exact(v) => (2, sql_position(v.as_u64())),
    }
}

/// Take one stream's advisory lock until the transaction ends — the
/// lock `append_events` takes, through the same SQL function
/// (migration 0010), so the key is defined once.
async fn lock_stream(conn: &mut sqlx::PgConnection, stream_id: &str) -> Result<(), StoreError> {
    sqlx::query("SELECT eventyr_lock_stream($1)")
        .bind(stream_id)
        .execute(conn)
        .await
        .map_err(PgStoreError::into_store)?;
    Ok(())
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
    for id in ids {
        lock_stream(&mut *conn, id).await?;
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
        let args = AppendArgs::new(append.expected, append.events).map_err(StoreError::from)?;
        let rows = append_events_tx(&mut *conn, append.stream_id.as_str(), args)
            .await
            .map_err(PgStoreError::into_store)?;
        let events = decode_rows(rows)?;
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

const QUERY_PAGE: i64 = 512;

impl<E> EventStore for PgStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    type Event = E;

    /// One `append_events` call: the closed check, the expectation
    /// check and the insert, under the stream's advisory lock. An empty
    /// append runs it too, so it is refused exactly when a non-empty one
    /// would be, and writes nothing.
    fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<E>>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        let this = self.clone();
        let stream_id = stream_id.clone();
        async move {
            let args = AppendArgs::new(expected, events).map_err(StoreError::from)?;
            let mut conn = this
                .pool
                .acquire()
                .await
                .map_err(PgStoreError::into_store)?;
            if !this.has_inline_views() {
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
            this.write_views(&mut tx, &committed).await?;
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
        let this = self.clone();
        async move {
            // `append_events` reads each stream's head inside the
            // transaction, so a repeated stream would see its own first
            // write and append after it — a batch other stores refuse.
            validate_batch(&appends)?;
            use sqlx::Acquire;
            let mut conn = this
                .pool
                .acquire()
                .await
                .map_err(PgStoreError::into_store)?;
            let mut tx = conn.begin().await.map_err(PgStoreError::into_store)?;

            lock_streams(&mut tx, &appends).await?;
            let committed = append_all_tx(&mut tx, appends).await?;
            this.write_views(&mut tx, &all_events(&committed)).await?;
            tx.commit().await.map_err(PgStoreError::into_store)?;
            Ok(committed)
        }
    }

    /// The truncation cut and the rows, read in one `REPEATABLE READ`
    /// snapshot: a truncation committing between the two reads would
    /// otherwise pass the cut check and then return the rows left after
    /// it — a partial history instead of [`StoreError::Truncated`].
    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let pool = self.pool.clone();
        let stream_id = stream_id.clone();
        Box::pin(async_stream::stream! {
            let rows = read_stream(&pool, &stream_id, from).await;
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

/// The body of [`EventStore::stream`] for [`PgStore`]: check the cut and
/// read the rows inside one read-only `REPEATABLE READ` transaction.
async fn read_stream(
    pool: &PgPool,
    stream_id: &StreamId,
    from: Version,
) -> Result<Vec<EventRow>, StoreError> {
    let mut tx = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(PgStoreError::into_store)?;
    let first_kept: Option<i64> =
        sqlx::query_scalar("SELECT first_kept FROM stream_lifecycle WHERE stream_id = $1")
            .bind(stream_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(PgStoreError::into_store)?;
    let first_kept = first_kept.map_or(Ok(1), position)?;
    read_starts_before_cut(stream_id, from, first_kept)?;
    let query = sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_COLUMNS} FROM events WHERE stream_id = $1 AND stream_version > $2 \
         ORDER BY stream_version"
    ));
    let rows = sqlx::query_as::<_, EventRow>(query)
        .bind(stream_id.as_str())
        .bind(sql_position(from.as_u64()))
        .fetch_all(&mut *tx)
        .await
        .map_err(PgStoreError::into_store)?;
    tx.commit().await.map_err(PgStoreError::into_store)?;
    Ok(rows)
}

/// A stored position back as a `u64`: a negative one is corrupt data.
fn position(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| {
        StoreError::from(PgStoreError::CorruptRow(format!(
            "negative position: {value}"
        )))
    })
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
            let mut cursor = sql_position(from.as_u64());
            loop {
                let rows = match query_page(&pool, None, cursor, QUERY_PAGE).await {
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
                if page_len < QUERY_PAGE as usize {
                    return;
                }
            }
        })
    }

    /// Filter in the database (roadmap 0.7.4). Two queries, one snapshot each:
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
            let from_i64 = sql_position(from.as_u64());
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
            let mut cursor = sql_position(after.as_u64());
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
        let this = self.clone();
        async move {
            validate_batch(&appends)?;
            use sqlx::Acquire;
            let mut conn = this
                .pool
                .acquire()
                .await
                .map_err(PgStoreError::into_store)?;
            let mut tx = conn.begin().await.map_err(PgStoreError::into_store)?;

            lock_streams(&mut tx, &appends).await?;
            // The same lock `append_events` takes, through the same SQL
            // function (migration 0010): one definition of the key.
            sqlx::query("SELECT eventyr_commit_order_lock()")
                .execute(&mut *tx)
                .await
                .map_err(PgStoreError::into_store)?;

            if !condition.query.items.is_empty() {
                let types = query_types(&condition.query);
                let mut cursor = sql_position(condition.after.as_u64());
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
            this.write_views(&mut tx, &all_events(&committed)).await?;
            tx.commit().await.map_err(PgStoreError::into_store)?;
            Ok(committed)
        }
    }
}

impl<E> StreamLifecycle for PgStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    /// Under the stream's advisory lock, so a close cannot interleave
    /// with an append that already passed its closed check.
    fn close_stream(
        &self,
        stream_id: &StreamId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        let pool = self.pool.clone();
        let stream_id = stream_id.clone();
        async move {
            let mut tx = pool.begin().await.map_err(PgStoreError::into_store)?;
            lock_stream(&mut tx, stream_id.as_str()).await?;
            sqlx::query(
                "INSERT INTO stream_lifecycle (stream_id, closed) VALUES ($1, TRUE) \
                 ON CONFLICT (stream_id) DO UPDATE SET closed = TRUE",
            )
            .bind(stream_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(PgStoreError::into_store)?;
            tx.commit().await.map_err(PgStoreError::into_store)
        }
    }

    /// One transaction under the stream's advisory lock: record the head
    /// and the cut, then delete the rows below it.
    fn truncate_before(
        &self,
        stream_id: &StreamId,
        version: Version,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        let pool = self.pool.clone();
        let stream_id = stream_id.clone();
        async move {
            let mut tx = pool.begin().await.map_err(PgStoreError::into_store)?;
            lock_stream(&mut tx, stream_id.as_str()).await?;
            // The head the same way `append_events` reads it (0010).
            let (head, first): (i64, i64) = sqlx::query_as(
                "SELECT eventyr_stream_head($1), \
                     COALESCE((SELECT first_kept FROM stream_lifecycle WHERE stream_id = $1), 1)",
            )
            .bind(stream_id.as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(PgStoreError::into_store)?;
            let TruncatePlan::Cut(cut) =
                plan_truncate(&stream_id, position(head)?, position(first)?, version)?
            else {
                return Ok(());
            };
            let cut = sql_position(cut);
            sqlx::query(
                "INSERT INTO stream_lifecycle (stream_id, first_kept, head) VALUES ($1, $2, $3) \
                 ON CONFLICT (stream_id) DO UPDATE SET first_kept = $2, head = $3",
            )
            .bind(stream_id.as_str())
            .bind(cut)
            .bind(head)
            .execute(&mut *tx)
            .await
            .map_err(PgStoreError::into_store)?;
            sqlx::query("DELETE FROM events WHERE stream_id = $1 AND stream_version < $2")
                .bind(stream_id.as_str())
                .bind(cut)
                .execute(&mut *tx)
                .await
                .map_err(PgStoreError::into_store)?;
            tx.commit().await.map_err(PgStoreError::into_store)
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
