//! The Postgres [`ViewStore`]: one row per `(view_name, view_id)` over
//! the `views` table (migration `0003_views`), newest-wins by folded
//! sequence.
//!
//! Same contract as the snapshot store's upsert: `save` is an
//! `ON CONFLICT ... DO UPDATE ... WHERE views.version < excluded.version`
//! — a redelivered event the subscription runner replays folds to a row
//! at its own older sequence and updates nothing, so replays can never
//! regress a row. `load` reads the one row.
//!
//! Behind the `views` feature, with inline views (0.7.3): a
//! [`PgStore`] built
//! [`with_inline_views`](crate::PgStore::with_inline_views) folds every
//! append's events into the same rows inside the append's transaction.

use std::collections::BTreeMap;
use std::sync::Arc;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::Sequence;
use eventyr_projection::inline::{InlineView, RowKey, StoredRow, fold_inline, rows_touched};
use eventyr_projection::view::{ViewRow, ViewStore};

use crate::{PgStore, PgStoreError};

/// Fold `committed` into the inline views and write the changed rows,
/// inside the caller's open transaction: one read of every touched row
/// and one upsert of every changed one, however many rows the append
/// touches.
///
/// No lock of its own: every append path holds the commit-order lock
/// (migration 0006) by the time it gets here, until commit, so view
/// folds are already serialized — two appends to different streams
/// folding into one row cannot both read its old value. If that lock is
/// ever relaxed (per-tag locking, §14), these rows need their own;
/// `concurrent_folds_into_one_row_lose_nothing` fails without either.
pub(crate) async fn write_inline_views<E>(
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
    let (names, ids): (Vec<String>, Vec<String>) = touched.into_iter().unzip();
    let found: Vec<(String, String, i64, serde_json::Value)> = sqlx::query_as(
        "SELECT v.view_name, v.view_id, v.version, v.payload \
         FROM views AS v \
         JOIN unnest($1::text[], $2::text[]) AS t(view_name, view_id) \
           ON v.view_name = t.view_name AND v.view_id = t.view_id",
    )
    .bind(&names)
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await
    .map_err(PgStoreError::into_store)?;
    let mut loaded: BTreeMap<RowKey, StoredRow> = BTreeMap::new();
    for (name, id, version, payload) in found {
        let version = u64::try_from(version).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "negative view version: {version}"
            )))
        })?;
        loaded.insert(
            (name, id),
            StoredRow {
                version: Sequence::new(version),
                payload,
            },
        );
    }
    let rows = fold_inline(views, committed, loaded)
        .map_err(|error| StoreError::Other(Arc::new(error)))?;
    if rows.is_empty() {
        return Ok(());
    }
    let mut names = Vec::with_capacity(rows.len());
    let mut ids = Vec::with_capacity(rows.len());
    let mut versions = Vec::with_capacity(rows.len());
    let mut payloads = Vec::with_capacity(rows.len());
    for ((name, id), row) in rows {
        versions.push(i64::try_from(row.version.as_u64()).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow("view version beyond i64".into()))
        })?);
        names.push(name);
        ids.push(id);
        payloads.push(row.payload);
    }
    // Newest wins per row, as in `save`: the WHERE keeps a row that is
    // already past this fold.
    sqlx::query(
        "INSERT INTO views (view_name, view_id, version, payload) \
         SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[], $4::jsonb[]) \
         ON CONFLICT (view_name, view_id) DO UPDATE \
         SET version = EXCLUDED.version, payload = EXCLUDED.payload, updated_at = now() \
         WHERE views.version < EXCLUDED.version",
    )
    .bind(&names)
    .bind(&ids)
    .bind(&versions)
    .bind(&payloads)
    .execute(&mut *conn)
    .await
    .map_err(PgStoreError::into_store)?;
    Ok(())
}

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

    async fn save(
        &self,
        view_name: &str,
        view_id: &str,
        row: ViewRow<V>,
    ) -> Result<(), StoreError> {
        let payload = serde_json::to_value(&row.value).map_err(PgStoreError::from)?;
        let version = i64::try_from(row.version.as_u64()).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow("view version beyond i64".into()))
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
