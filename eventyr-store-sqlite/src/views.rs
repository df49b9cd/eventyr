//! Views over the same connection, behind the `views` feature: the
//! [`ViewStore`] port (0.6.3) and inline views (0.7.3).
//!
//! One `views` table, one row per `(view_name, view_id)`, newest-wins
//! on the folded sequence — the contract the Postgres view store
//! carries. A [`SqliteViewStore`] reads and writes rows for async
//! [`ViewProjection`](eventyr_projection::view::ViewProjection)s; a store
//! built [`with_inline_views`](crate::SqliteStore::with_inline_views)
//! folds every append's events into the same rows inside the append's
//! transaction.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rusqlite::OptionalExtension;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::Sequence;
use eventyr_projection::inline::{InlineView, RowKey, StoredRow, fold_inline, rows_touched};
use eventyr_projection::view::{ViewRow, ViewStore};

use crate::{SqliteStore, SqliteStoreError};

pub(crate) const VIEWS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS views (
    view_name   TEXT    NOT NULL,
    view_id     TEXT    NOT NULL,
    version     INTEGER NOT NULL CHECK (version >= 0),
    payload     TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (view_name, view_id)
);
";

/// The SQLite view store, generic in the row's value type.
pub struct SqliteViewStore<V> {
    conn: Arc<Mutex<rusqlite::Connection>>,
    _value: std::marker::PhantomData<fn() -> V>,
}

impl<V> Clone for SqliteViewStore<V> {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            _value: std::marker::PhantomData,
        }
    }
}

impl<V> SqliteViewStore<V> {
    /// A view store over the event store's connection — the rows live
    /// beside the log, and inline views write the same table.
    pub fn new<E>(store: &SqliteStore<E>) -> Self {
        Self {
            conn: store.conn(),
            _value: std::marker::PhantomData,
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>, StoreError> {
        self.conn
            .lock()
            .map_err(|e| StoreError::other(format!("view store lock poisoned: {e}")))
    }
}

fn load_row(
    conn: &rusqlite::Connection,
    view_name: &str,
    view_id: &str,
) -> Result<Option<StoredRow>, StoreError> {
    let row = conn
        .query_row(
            "SELECT version, payload FROM views WHERE view_name = ?1 AND view_id = ?2",
            [view_name, view_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(SqliteStoreError::into_store)?;
    let Some((version, payload)) = row else {
        return Ok(None);
    };
    let version = u64::try_from(version).map_err(|_| {
        StoreError::from(SqliteStoreError::CorruptRow(format!(
            "negative view version: {version}"
        )))
    })?;
    let payload = serde_json::from_str(&payload).map_err(|error| {
        StoreError::from(SqliteStoreError::CorruptRow(format!(
            "the view payload is not JSON: {error}"
        )))
    })?;
    Ok(Some(StoredRow {
        version: Sequence::new(version),
        payload,
    }))
}

fn save_row(
    conn: &rusqlite::Connection,
    view_name: &str,
    view_id: &str,
    row: &StoredRow,
) -> Result<(), StoreError> {
    let version = i64::try_from(row.version.as_u64()).map_err(|_| {
        StoreError::from(SqliteStoreError::CorruptRow(
            "view version beyond i64".into(),
        ))
    })?;
    // Newest wins: a replayed row's UPDATE never matches.
    conn.execute(
        "INSERT INTO views (view_name, view_id, version, payload) VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT (view_name, view_id) DO UPDATE \
         SET version = excluded.version, payload = excluded.payload, updated_at = datetime('now') \
         WHERE views.version < excluded.version",
        rusqlite::params![view_name, view_id, version, row.payload.to_string()],
    )
    .map_err(SqliteStoreError::into_store)?;
    Ok(())
}

/// Fold `committed` into the inline views and write the changed rows,
/// inside the caller's open transaction. SQLite has one writer, so the
/// read-fold-write cannot race another append.
pub(crate) fn write_inline_views<E>(
    conn: &rusqlite::Connection,
    views: &[Arc<dyn InlineView<E>>],
    committed: &[EventEnvelope<E>],
) -> Result<(), StoreError> {
    if views.is_empty() || committed.is_empty() {
        return Ok(());
    }
    let mut loaded: BTreeMap<RowKey, StoredRow> = BTreeMap::new();
    for (name, id) in rows_touched(views, committed) {
        if let Some(row) = load_row(conn, &name, &id)? {
            loaded.insert((name, id), row);
        }
    }
    let rows = fold_inline(views, committed, loaded)
        .map_err(|error| StoreError::Other(Arc::new(error)))?;
    for ((name, id), row) in rows {
        save_row(conn, &name, &id, &row)?;
    }
    Ok(())
}

impl<V> ViewStore<V> for SqliteViewStore<V>
where
    V: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
{
    async fn load(&self, view_name: &str, view_id: &str) -> Result<Option<ViewRow<V>>, StoreError> {
        let Some(row) = load_row(&*self.lock()?, view_name, view_id)? else {
            return Ok(None);
        };
        let value = serde_json::from_value(row.payload).map_err(|error| {
            StoreError::from(SqliteStoreError::CorruptRow(format!(
                "the view payload does not decode: {error}"
            )))
        })?;
        Ok(Some(ViewRow {
            version: row.version,
            value,
        }))
    }

    async fn save(
        &self,
        view_name: &str,
        view_id: &str,
        row: ViewRow<V>,
    ) -> Result<(), StoreError> {
        let payload = serde_json::to_value(&row.value).map_err(SqliteStoreError::from)?;
        save_row(
            &*self.lock()?,
            view_name,
            view_id,
            &StoredRow {
                version: row.version,
                payload,
            },
        )
    }
}
