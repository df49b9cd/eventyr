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

/// Wire snapshot persistence alongside a store-backed subscription:
/// the subscription keeps reading the event stream, and
/// [`SubscriptionSnapshots::save_snapshot`] routes a snapshot offer
/// straight through to the [`SnapshotStore`](eventyr_store::snapshot_store::SnapshotStore)
/// the same pool provides.
///
/// Behind the `snapshots` feature. Construct via
/// [`SubscriptionSnapshots::new`] with the store (or a `StoreSubscription`
/// over it), then route commit-offered snapshots through it.
pub struct SubscriptionSnapshots<S> {
    inner: S,
}

impl<S> SubscriptionSnapshots<S> {
    /// Wrap a `StoreSubscription<PgStore<E>>` (or any wrapper around
    /// it). The snapshots persist against the same pool the
    /// subscription reads from.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    /// The wrapped subscription.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// The wrapped subscription, for polling.
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S, E> SubscriptionSnapshots<S>
where
    S: std::ops::Deref,
    S::Target: eventyr_store::snapshot_store::SnapshotStore<State = E>,
    E: Clone + Send,
{
    /// Persist a snapshot offer against the subscription's store —
    /// fire-and-forget at the caller's side: the error is reported but
    /// the caller (the write driver) deliberately drops it.
    pub async fn save_snapshot(
        &self,
        snapshot: Snapshot<E>,
    ) -> Result<(), StoreError> {
        eventyr_store::snapshot_store::SnapshotStore::save(&*self.inner, snapshot).await
    }
}

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
