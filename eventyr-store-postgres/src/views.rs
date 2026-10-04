//! The Postgres [`ViewStore`]: one row per `(view_name, view_id)` over
//! the `views` table (migration `0003_views`), newest-wins by folded
//! sequence.
//!
//! Same contract as the snapshot store's upsert: `save` is an
//! `ON CONFLICT ... DO UPDATE ... WHERE views.version < excluded.version`
//! — a redelivered event the subscription runner replays folds to a row
//! at its own older sequence and updates nothing, so replays can never
//! regress a row. `load` reads the one row.

use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::Sequence;
use eventyr_projection::view::{ViewRow, ViewStore};

use crate::{PgStore, PgStoreError};

/// The Postgres view store, generic in the row's value type — like
/// [`PgSnapshotStore`](crate::snapshots::PgSnapshotStore), it does not
/// share the event store's event type.
pub struct PgViewStore<V> {
    pool: sqlx::postgres::PgPool,
    _value: std::marker::PhantomData<fn() -> V>,
}

impl<V> PgViewStore<V> {
    /// A view store over `store`'s pool.
    pub fn new(store: &PgStore<impl Send>) -> Self {
        Self {
            pool: store.pool().clone(),
            _value: std::marker::PhantomData,
        }
    }
}

impl<V> Clone for PgViewStore<V> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            _value: std::marker::PhantomData,
        }
    }
}

#[derive(sqlx::FromRow)]
struct StoredViewRow {
    version: i64,
    payload: serde_json::Value,
}

impl<V> ViewStore<V> for PgViewStore<V>
where
    V: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
{
    async fn load(&self, view_name: &str, view_id: &str) -> Result<Option<ViewRow<V>>, StoreError> {
        let row = sqlx::query_as::<_, StoredViewRow>(
            "SELECT version, payload FROM views WHERE view_name = $1 AND view_id = $2",
        )
        .bind(view_name)
        .bind(view_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(PgStoreError::into_store)?;

        let Some(row) = row else { return Ok(None) };
        let version = u64::try_from(row.version).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "negative view version: {}",
                row.version
            )))
        })?;
        let value: V = serde_json::from_value(row.payload).map_err(|error| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "the view payload does not decode: {error}"
            )))
        })?;
        Ok(Some(ViewRow {
            version: Sequence::new(version),
            value,
        }))
    }

    async fn save(&self, view_name: &str, view_id: &str, row: ViewRow<V>) -> Result<(), StoreError> {
        let payload = serde_json::to_value(&row.value).map_err(PgStoreError::from)?;
        let version = i64::try_from(row.version.as_u64()).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow(
                "view version beyond i64".into(),
            ))
        })?;
        // Newest wins, atomically: a replayed event's row lands behind
        // and affects zero rows.
        sqlx::query(
            "INSERT INTO views (view_name, view_id, version, payload) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (view_name, view_id) DO UPDATE \
             SET version = EXCLUDED.version, payload = EXCLUDED.payload, updated_at = now() \
             WHERE views.version < EXCLUDED.version",
        )
        .bind(view_name)
        .bind(view_id)
        .bind(version)
        .bind(&payload)
        .execute(&self.pool)
        .await
        .map_err(PgStoreError::into_store)?;
        Ok(())
    }
}
