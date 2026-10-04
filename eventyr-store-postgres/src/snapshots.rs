//! The [`SnapshotStore`](eventyr_store::snapshot_store::SnapshotStore)
//! implementation over the `snapshots` table
//! (migration `0002_snapshots`), behind the `snapshots` feature.
//!
//! A [`PgSnapshotStore`] is its own handle, generic in the *state* type
//! `S` — it does not share the event store's event type. Snapshotting an
//! aggregate whose `A::State` differs in shape from its event enum uses
//! the same table; construct one `PgSnapshotStore` per state type over
//! the pool. One snapshot row per stream, the newest wins: `save` is a
//! monotonicity-guarded upsert (`ON CONFLICT ... DO UPDATE ... WHERE
//! snapshots.version < excluded.version`) — an offer that lands behind
//! the persisted version (two post-commit offers racing) updates
//! nothing, so the stored snapshot can never regress; `load` reads the
//! one row. The state is stored as JSON in its externally-tagged serde
//! shape, the same convention the `events.payload` column uses.

use eventyr_core::error::StoreError;
use eventyr_core::snapshot::Snapshot;
use eventyr_core::vocabulary::{StreamId, Version};

use crate::{PgStore, PgStoreError};

/// The row shape: `payload` is the materialized state as JSON;
/// `version` the stream version at snapshot time (CHECK-positive in
/// the schema).
#[derive(sqlx::FromRow)]
struct SnapshotRow {
    version: i64,
    payload: serde_json::Value,
}

/// The Postgres [`SnapshotStore`](eventyr_store::snapshot_store::SnapshotStore):
/// one row per stream, over the same
/// pool as the event log.
///
/// Generic in the snapshot's state type, free of the event store's
/// event type. Wrap a [`PgStore`]'s pool with
/// [`new`](PgSnapshotStore::new).
pub struct PgSnapshotStore<S> {
    pool: sqlx::postgres::PgPool,
    _state: std::marker::PhantomData<fn() -> S>,
}

impl<S> PgSnapshotStore<S> {
    /// A snapshot store over `store`'s pool.
    pub fn new(store: &PgStore<impl Send>) -> Self {
        Self {
            pool: store.pool().clone(),
            _state: std::marker::PhantomData,
        }
    }
}

impl<S> Clone for PgSnapshotStore<S> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            _state: std::marker::PhantomData,
        }
    }
}

impl<S> eventyr_store::snapshot_store::SnapshotStore for PgSnapshotStore<S>
where
    S: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
{
    type State = S;

    async fn load(&self, stream_id: &StreamId) -> Result<Option<Snapshot<S>>, StoreError> {
        let row = sqlx::query_as::<_, SnapshotRow>(
            "SELECT version, payload FROM snapshots WHERE stream_id = $1",
        )
        .bind(stream_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(PgStoreError::into_store)?;

        let Some(row) = row else { return Ok(None) };
        let version = u64::try_from(row.version).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "negative snapshot version: {}",
                row.version
            )))
        })?;
        let state: S = serde_json::from_value(row.payload).map_err(|error| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "the snapshot payload does not decode: {error}"
            )))
        })?;
        Ok(Some(Snapshot {
            stream_id: stream_id.clone(),
            version: Version::new(version),
            state,
        }))
    }

    async fn save(&self, snapshot: Snapshot<S>) -> Result<(), StoreError> {
        let payload = serde_json::to_value(&snapshot.state).map_err(PgStoreError::from)?;
        let version = i64::try_from(snapshot.version.as_u64()).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow(
                "snapshot version beyond i64".into(),
            ))
        })?;
        // Newest wins, atomically: the WHERE keeps a racing offer that
        // lands behind from regressing the row — a stale save affects
        // zero rows, and whatever staleness remains self-corrects on
        // the next load's delta fold.
        sqlx::query(
            "INSERT INTO snapshots (stream_id, version, payload) VALUES ($1, $2, $3) \
             ON CONFLICT (stream_id) DO UPDATE \
             SET version = EXCLUDED.version, payload = EXCLUDED.payload, created_at = now() \
             WHERE snapshots.version < EXCLUDED.version",
        )
        .bind(snapshot.stream_id.as_str())
        .bind(version)
        .bind(&payload)
        .execute(&self.pool)
        .await
        .map_err(PgStoreError::into_store)?;
        Ok(())
    }
}
