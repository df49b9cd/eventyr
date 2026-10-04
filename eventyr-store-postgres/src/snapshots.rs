//! The [`SnapshotStore`] implementation over the `snapshots` table
//! (migration `0002_snapshots`), behind the `snapshots` feature.
//!
//! One snapshot row per stream, the newest wins: `save` is a
//! monotonicity-guarded upsert (`ON CONFLICT ... DO UPDATE ... WHERE
//! snapshots.version < excluded.version`) — an offer that lands behind
//! the persisted version (two post-commit offers racing) updates
//! nothing, so the stored snapshot can never regress; `load` reads the
//! one row. The snapshot's `version` is the stream version the folded
//! state covers — the machine's monotonicity guard checks it against
//! the delta it then loads. The state is stored as JSON in its
//! externally-tagged serde shape, the same convention the
//! `events.payload` column uses.

use eventyr_core::error::StoreError;
use eventyr_core::snapshot::Snapshot;
use eventyr_core::vocabulary::StreamId;
use eventyr_core::vocabulary::Version;

use crate::{PgStore, PgStoreError};

/// The row shape: `payload` is the materialized state as JSON;
/// `version` the stream version at snapshot time (CHECK-positive in
/// the schema).
#[derive(sqlx::FromRow)]
struct SnapshotRow {
    version: i64,
    payload: serde_json::Value,
}

impl<E> eventyr_store::snapshot_store::SnapshotStore for PgStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
{
    type State = E;

    async fn load(&self, stream_id: &StreamId) -> Result<Option<Snapshot<E>>, StoreError> {
        let row = sqlx::query_as::<_, SnapshotRow>(
            "SELECT version, payload FROM snapshots WHERE stream_id = $1",
        )
        .bind(stream_id.as_str())
        .fetch_optional(self.pool())
        .await
        .map_err(PgStoreError::into_store)?;

        let Some(row) = row else { return Ok(None) };
        let version = u64::try_from(row.version).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "negative snapshot version: {}",
                row.version
            )))
        })?;
        let state: E = serde_json::from_value(row.payload).map_err(|error| {
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

    async fn save(&self, snapshot: Snapshot<E>) -> Result<(), StoreError> {
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
        .execute(self.pool())
        .await
        .map_err(PgStoreError::into_store)?;
        Ok(())
    }
}
