//! The Postgres [`EventStore`] — the 0.2 store.
//!
//! One table, one PL/pgSQL function: `append_events` takes a per-stream
//! advisory lock, checks the expected version against the live max, and
//! inserts the batch. See the [crate-level docs](crate) for the schema
//! and the wire format.

use std::future::Future;
use std::path::Path;

use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{EventStore, StreamsAll};
use futures::Stream;
use sqlx::postgres::PgPool;

use crate::PgStoreError;

/// An [`EventStore`] over Postgres, via sqlx, for one event enum `E`.
///
/// Shareable and cloneable (it wraps a [`PgPool`]): the pool owns the
/// connection count; the store holds no other state.
#[derive(Clone)]
pub struct PgStore<E> {
    pool: PgPool,
    _event: std::marker::PhantomData<fn(E)>,
}

impl<E> PgStore<E> {
    /// Wrap an existing pool. Run the migration first
    /// ([`migrate`]) — the store assumes the schema.
    pub const fn new(pool: PgPool) -> Self {
        Self {
            pool,
            _event: std::marker::PhantomData,
        }
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
        // `Metadata::default()` fills `timestamp` — present only when
        // core's `time` feature is on (it can be switched on by the
        // umbrella crate without this crate's `time`; the constructor
        // must not depend on that). With `time` off the update is
        // needless but must stay for the `time`-on build.
        #[allow(clippy::needless_update)]
        let metadata = Metadata {
            causation_id: metadata.causation_id,
            correlation_id: metadata.correlation_id,
            ..Metadata::default()
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
/// 0 = Any, 1 = Empty, 2 = Exact.
fn expectation_args(expected: ExpectedVersion) -> (i16, i64) {
    match expected {
        ExpectedVersion::Any => (0, 0),
        ExpectedVersion::Empty => (1, 0),
        ExpectedVersion::Exact(v) => (2, v.as_u64().try_into().unwrap_or(i64::MAX)),
    }
}

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
        let stream_id = stream_id.clone();
        async move {
            if events.is_empty() {
                return Ok(Vec::new());
            }

            let (kind, exact) = expectation_args(expected);

            // One source of truth per event, fanned out into the four
            // arrays the function takes — the correlation across the four
            // stays in the type, not in four lockstep pushes.
            let mut names = Vec::with_capacity(events.len());
            let mut payloads = Vec::with_capacity(events.len());
            let mut causations = Vec::with_capacity(events.len());
            let mut correlations = Vec::with_capacity(events.len());
            for new_event in events {
                names.push(new_event.event.event_name().to_string());
                payloads.push(serde_json::to_value(&new_event.event).map_err(PgStoreError::from)?);
                causations.push(new_event.metadata.causation_id);
                correlations.push(new_event.metadata.correlation_id);
            }

            let query = sqlx::AssertSqlSafe(format!(
                "SELECT {EVENT_COLUMNS} FROM append_events($1, $2, $3, $4, $5, $6, $7)"
            ));
            let rows = sqlx::query_as::<_, EventRow>(query)
                .bind(kind)
                .bind(exact)
                .bind(stream_id.as_str())
                .bind(&names[..])
                .bind(&payloads[..])
                .bind(&causations[..])
                .bind(&correlations[..])
                .fetch_all(&pool)
                .await
                .map_err(PgStoreError::into_store)?;

            rows.into_iter()
                .map(EventEnvelope::try_from)
                .collect::<Result<Vec<_>, PgStoreError>>()
                .map_err(StoreError::from)
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
