//! Inline views (roadmap 0.7.3): view rows written in the same
//! transaction as the append that produced their events.
//!
//! A [`ViewProjection`](crate::view::ViewProjection) runs behind the
//! subscription runner, so its rows trail the write: a read straight
//! after a command may miss it. An inline view closes that gap — the
//! store folds the committed events into the registered views before it
//! commits, and a view that cannot be written fails the append. Same
//! [`View`] fold, same rows, same newest-wins guard: a view can move
//! between inline and async without a rewrite.
//!
//! This module is the store-agnostic half, and it does no I/O:
//!
//! - [`InlineView`] is the object-safe face a store holds. JSON is the
//!   erasure boundary: rows travel as [`serde_json::Value`], the shape
//!   both durable stores persist.
//! - [`Inline`] adapts any serde-able [`View`] plus a key function.
//! - [`rows_touched`] names the rows a commit's events fold into, so
//!   the store can load exactly those inside its transaction;
//!   [`fold_inline`] folds the events over the loaded rows and returns
//!   the rows to write back.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::vocabulary::Sequence;

use crate::view::View;

/// A view a store folds inline: the type-erased form of a [`View`] and
/// its key function.
pub trait InlineView<E>: Send + Sync {
    /// The view name rows persist under — the `view_name` a
    /// [`ViewStore`](crate::view::ViewStore) loads by.
    fn name(&self) -> &str;

    /// The row `event` folds into, or `None` to pass it by.
    fn key(&self, event: &EventEnvelope<E>) -> Option<String>;

    /// Fold `event` into the stored row (`None`: the view's initial
    /// row) and return the new row.
    ///
    /// # Errors
    ///
    /// [`InlineViewError`] when the stored row's payload does not
    /// decode into the view's type, or the new row does not serialize —
    /// the append carrying it must fail rather than write a broken row.
    fn fold(
        &self,
        stored: Option<serde_json::Value>,
        event: &EventEnvelope<E>,
    ) -> Result<serde_json::Value, InlineViewError>;
}

/// An inline row could not be read or written: a stored payload that
/// no longer decodes into the view's type, or a value that does not
/// serialize.
///
/// The store fails the append with it — an inline view that
/// cannot be maintained must not let the write through.
#[derive(Debug)]
pub struct InlineViewError {
    /// The view.
    pub view_name: String,
    /// The row.
    pub view_id: String,
    /// What went wrong.
    pub message: String,
}

impl core::fmt::Display for InlineViewError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "inline view `{}` row `{}`: {}",
            self.view_name, self.view_id, self.message
        )
    }
}

impl core::error::Error for InlineViewError {}

/// A [`View`] maintained inline: `name` scopes its rows, `key_of`
/// names the row each event folds into (as for
/// [`ViewProjection`](crate::view::ViewProjection)).
pub struct Inline<V, K> {
    name: String,
    key_of: K,
    _view: core::marker::PhantomData<fn() -> V>,
}

impl<V, K> Inline<V, K> {
    /// An inline view `name`, keyed by `key_of`.
    pub fn new(name: impl Into<String>, key_of: K) -> Self {
        Self {
            name: name.into(),
            key_of,
            _view: core::marker::PhantomData,
        }
    }
}

impl<V, K, E> InlineView<E> for Inline<V, K>
where
    V: View<E> + serde::Serialize + serde::de::DeserializeOwned,
    K: Fn(&EventEnvelope<E>) -> Option<String> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn key(&self, event: &EventEnvelope<E>) -> Option<String> {
        (self.key_of)(event)
    }

    fn fold(
        &self,
        stored: Option<serde_json::Value>,
        event: &EventEnvelope<E>,
    ) -> Result<serde_json::Value, InlineViewError> {
        let error = |message: String| InlineViewError {
            view_name: self.name.clone(),
            view_id: (self.key_of)(event).unwrap_or_default(),
            message,
        };
        let mut value = match stored {
            Some(stored) => serde_json::from_value::<V>(stored)
                .map_err(|e| error(format!("the stored row does not decode: {e}")))?,
            None => V::initial(),
        };
        value.apply(event);
        serde_json::to_value(&value).map_err(|e| error(format!("the row does not serialize: {e}")))
    }
}

/// The registered inline views of a store, shared by its clones.
pub type InlineViews<E> = Arc<[Arc<dyn InlineView<E>>]>;

/// `(view_name, view_id)`.
pub type RowKey = (String, String);

/// One view row as stored: the sequence of the newest folded event and
/// the JSON value.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredRow {
    /// The global sequence of the newest event folded into the row.
    pub version: Sequence,
    /// The row's value.
    pub payload: serde_json::Value,
}

/// The rows `events` fold into across `views` — what a store loads
/// inside its transaction before calling [`fold_inline`].
pub fn rows_touched<E>(
    views: &[Arc<dyn InlineView<E>>],
    events: &[EventEnvelope<E>],
) -> BTreeSet<RowKey> {
    let mut rows = BTreeSet::new();
    for event in events {
        for view in views {
            if let Some(key) = view.key(event) {
                rows.insert((view.name().to_owned(), key));
            }
        }
    }
    rows
}

/// Fold `events` (in sequence order) into the `loaded` rows across
/// `views`, and return every row that changed, to be written back.
///
/// `loaded` holds the stored rows [`rows_touched`] named — a missing
/// entry is a row not yet written. Newest wins: an event at or below a
/// row's version folds nothing, so a row written by an async
/// [`ViewProjection`](crate::view::ViewProjection) of the same view is
/// never regressed.
///
/// # Errors
///
/// [`InlineViewError`] when any view's fold rejected its event — the
/// append carrying the batch must fail rather than write broken rows.
///
/// # Panics
///
/// Unreachable by construction: a dirty row's key was just folded
/// into `loaded`, so the look-up cannot miss. A panic here is a bug in
/// this function, not a store condition.
pub fn fold_inline<E>(
    views: &[Arc<dyn InlineView<E>>],
    events: &[EventEnvelope<E>],
    mut loaded: BTreeMap<RowKey, StoredRow>,
) -> Result<Vec<(RowKey, StoredRow)>, InlineViewError> {
    let mut dirty = BTreeSet::new();
    for event in events {
        for view in views {
            let Some(key) = view.key(event) else { continue };
            let row_key = (view.name().to_owned(), key);
            let stored = loaded.get(&row_key);
            if stored.is_some_and(|row| row.version >= event.sequence) {
                continue;
            }
            let payload = view.fold(stored.map(|row| row.payload.clone()), event)?;
            loaded.insert(
                row_key.clone(),
                StoredRow {
                    version: event.sequence,
                    payload,
                },
            );
            dirty.insert(row_key);
        }
    }
    Ok(dirty
        .into_iter()
        .map(|key| {
            let row = loaded.remove(&key).expect("a dirty row was just folded");
            (key, row)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_core::vocabulary::{StreamId, Version};

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Balance(u64);

    impl View<u64> for Balance {
        fn initial() -> Self {
            Self(0)
        }

        fn apply(&mut self, event: &EventEnvelope<u64>) {
            self.0 += event.event;
        }
    }

    fn envelope(sequence: u64, stream: &str, amount: u64) -> EventEnvelope<u64> {
        EventEnvelope {
            sequence: Sequence::new(sequence),
            stream_id: StreamId::from(stream),
            version: Version::new(sequence),
            event: amount,
            metadata: Default::default(),
        }
    }

    fn balance() -> Arc<dyn InlineView<u64>> {
        Arc::new(Inline::<Balance, _>::new(
            "balance",
            |e: &EventEnvelope<u64>| Some(e.stream_id.as_str().to_owned()),
        ))
    }

    fn total() -> Arc<dyn InlineView<u64>> {
        Arc::new(Inline::<Balance, _>::new(
            "total",
            |_: &EventEnvelope<u64>| Some("all".to_owned()),
        ))
    }

    fn key(name: &str, id: &str) -> RowKey {
        (name.to_owned(), id.to_owned())
    }

    #[test]
    fn rows_touched_names_every_view_row_once() {
        let views = [balance(), total()];
        let events = [
            envelope(1, "a", 1),
            envelope(2, "b", 1),
            envelope(3, "a", 1),
        ];
        let rows = rows_touched(&views, &events);
        assert_eq!(
            rows.into_iter().collect::<Vec<_>>(),
            vec![
                key("balance", "a"),
                key("balance", "b"),
                key("total", "all")
            ]
        );
    }

    #[test]
    fn events_fold_over_the_loaded_rows() {
        let views = [balance(), total()];
        let events = [
            envelope(5, "a", 10),
            envelope(6, "b", 7),
            envelope(7, "a", 1),
        ];
        let loaded = BTreeMap::from([(
            key("balance", "a"),
            StoredRow {
                version: Sequence::new(2),
                payload: serde_json::json!(100),
            },
        )]);
        let rows: BTreeMap<_, _> = fold_inline(&views, &events, loaded)
            .expect("fold")
            .into_iter()
            .collect();
        assert_eq!(rows[&key("balance", "a")].payload, serde_json::json!(111));
        assert_eq!(rows[&key("balance", "a")].version, Sequence::new(7));
        assert_eq!(rows[&key("balance", "b")].payload, serde_json::json!(7));
        assert_eq!(rows[&key("total", "all")].payload, serde_json::json!(18));
    }

    #[test]
    fn a_row_already_at_or_past_the_event_is_left_alone() {
        let views = [balance()];
        let loaded = BTreeMap::from([(
            key("balance", "a"),
            StoredRow {
                version: Sequence::new(9),
                payload: serde_json::json!(50),
            },
        )]);
        let rows = fold_inline(&views, &[envelope(9, "a", 1)], loaded).expect("fold");
        assert!(rows.is_empty(), "nothing changed, nothing to write");
    }

    #[test]
    fn a_stored_row_that_does_not_decode_fails_the_fold() {
        let views = [balance()];
        let loaded = BTreeMap::from([(
            key("balance", "a"),
            StoredRow {
                version: Sequence::new(1),
                payload: serde_json::json!("not a number"),
            },
        )]);
        let error = fold_inline(&views, &[envelope(2, "a", 1)], loaded).expect_err("decode");
        assert_eq!(error.view_name, "balance");
        assert_eq!(error.view_id, "a");
    }

    #[test]
    fn unkeyed_events_pass_by() {
        let views: [Arc<dyn InlineView<u64>>; 1] = [Arc::new(Inline::<Balance, _>::new(
            "balance",
            |_: &EventEnvelope<u64>| None,
        ))];
        let events = [envelope(1, "a", 1)];
        assert!(rows_touched(&views, &events).is_empty());
        assert!(
            fold_inline(&views, &events, BTreeMap::new())
                .expect("fold")
                .is_empty()
        );
    }
}
