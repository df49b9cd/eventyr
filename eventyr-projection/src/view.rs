//! The view store: a projection whose state is a persisted row.
//!
//! Roadmap 0.6.3. The subscriptions side of eventyr is rebuild-first —
//! fold the global stream, checkpoint, keep the read model in whatever
//! storage the caller owns. That covers models read in bulk; it does
//! not cover the query every CQRS demo has: *look this projection up by
//! id, fast, right after the write*. A **view** answers that: one row
//! per view id, folded event-by-event, persisted newest-wins.
//!
//! Three pieces, each boring on its own:
//!
//! - [`View`] is the pure fold — `apply` one event into the row's
//!   value. Exactly [`Aggregate::apply`](eventyr_core::aggregate::Aggregate)
//!   for the read side: total, never fails — a mismatch between row and
//!   event shape is the codec's/upcaster's worry, settled before the
//!   projection sees the event.
//! - [`ViewStore`] is the port: `load` by `(view_name, view_id)`,
//!   `save` the newest version back. Newest wins by *sequence*: a
//!   redelivered event (the at-least-once contract) carries an older
//!   sequence than the row already has and is dropped, so the fold
//!   itself stays idempotent-by-construction for the common
//!   "projection is one aggregate's state" case.
//! - [`ViewProjection`] is the adapter: a [`Projection`] whose `apply`
//!   is load → fold → save, keyed by a caller-supplied extractor. Plug
//!   it into any [`Projector`](eventyr_subscription::runner::Projector);
//!   the subscription machine owns catch-up, checkpoints, and
//!   redelivery exactly as for any other projection.
//!
//! Placement: everything here is read-side glue and lives
//! store-side — §3's rule. The port's Postgres implementation is
//! `eventyr_store_postgres::views::PgViewStore`.

use std::collections::HashMap;
use std::sync::Mutex;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::Sequence;
use eventyr_subscription::runner::Projection;

/// A materialized row keyed by id: the read side's smallest unit.
///
/// Like [`Aggregate::apply`](eventyr_core::aggregate::Aggregate), `apply`
/// is pure and total — events are facts a view folds from. Unlike an
/// aggregate, the *identity* of the view is the caller's: the
/// [`ViewProjection`] names it per event, so one `View` type can serve
/// a per-aggregate model (`view_id = stream id`) or a
/// bucketed/denormalized one (`view_id = "owner-7"`).
pub trait View<E>: Send {
    /// The state before any event — the row a `view_id` starts from
    /// when the store has none.
    fn initial() -> Self;

    /// Fold one event into the row. Never fails: anything this event
    /// cannot contribute has been decided upstream (upcasters, the
    /// store's codec), and at-least-once duplicates of it are absorbed
    /// by the store's newest-wins guard, not by branching here.
    fn apply(&mut self, event: &EventEnvelope<E>);
}

/// One view row as stored: the value plus the sequence of the last
/// event folded into it.
///
/// The sequence is the optimistic-concurrency and redelivery guard in
/// one: `save` replaces only when it is newer, so a replayed batch
/// writes rows it has already folded with no effect.
#[derive(Clone, Debug)]
pub struct ViewRow<V> {
    /// The global sequence of the newest folded event.
    pub version: Sequence,
    /// The row's value.
    pub value: V,
}

/// Where views persist. One row per `(view_name, view_id)`, newest wins.
///
/// `view_name` scopes the model ("balance", "statement_by_owner");
/// `view_id` names the row inside it. The contract mirrors the
/// snapshot port's (DESIGN §6.3): a save behind the stored sequence is
/// dropped, never written over it — out-of-order offers from
/// overlapping rebuild-and-follow runs cannot regress a row.
pub trait ViewStore<V>: Send + Sync {
    /// The newest stored row for `(view_name, view_id)`, or `None`.
    fn load(
        &self,
        view_name: &str,
        view_id: &str,
    ) -> impl core::future::Future<Output = Result<Option<ViewRow<V>>, StoreError>> + Send;

    /// Persist `row` at `(view_name, view_id)`, replacing only older
    /// rows.
    fn save(
        &self,
        view_name: &str,
        view_id: &str,
        row: ViewRow<V>,
    ) -> impl core::future::Future<Output = Result<(), StoreError>> + Send;
}

/// A [`Projection`] that maintains one [`View`] row per key.
///
/// Construction carries the *mapping*: `name` is the view's stable
/// table/key prefix, `key_of` names the row each event folds into.
/// Apply is the whole contract:
///
/// ```ignore
/// let row = self.store.load(name, key_of(event))?.unwrap_or(View::initial);
/// row.value.apply(event);
/// self.store.save(name, key, ViewRow { version: event.sequence, value: row.value })
/// ```
///
/// Persisting under the event's *global sequence* is what makes the
/// adapter replay-safe: the subscription runner re-delivers after a
/// crash between apply and ack, and the store's newest-wins guard
/// absorbs every row whose key it already folded.
pub struct ViewProjection<V, VS, K, E> {
    name: String,
    store: VS,
    key_of: K,
    _view: core::marker::PhantomData<fn() -> (V, E)>,
}

impl<V, VS, K, E> ViewProjection<V, VS, K, E> {
    /// A projection maintaining the view `name` through `store`: each
    /// event folds into the row `key_of(event)` names.
    pub fn new(name: impl Into<String>, store: VS, key_of: K) -> Self {
        Self {
            name: name.into(),
            store,
            key_of,
            _view: core::marker::PhantomData,
        }
    }

    /// The view name rows persist under.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The store rows persist through.
    pub fn store(&self) -> &VS {
        &self.store
    }
}

impl<V, VS, K, E> Projection for ViewProjection<V, VS, K, E>
where
    V: View<E> + Send,
    VS: ViewStore<V>,
    K: Fn(&EventEnvelope<E>) -> Option<String> + Send,
    E: Send + Sync,
{
    type Event = E;
    type Error = StoreError;

    async fn apply(&mut self, event: &EventEnvelope<E>) -> Result<(), Self::Error> {
        // A key of `None` passes the event by untouched: views are
        // selective by construction (a per-owner statement ignores
        // every owner it does not track).
        let key = match (self.key_of)(event) {
            Some(key) => key,
            None => return Ok(()),
        };
        let mut row = self
            .store
            .load(&self.name, &key)
            .await?
            .map(|stored| (stored.version, stored.value))
            .unwrap_or((Sequence::START, V::initial()));
        // Newest-wins locally too: a re-delivered event at or below the
        // row's version folds nothing. The store repeats the guard for
        // concurrent writers.
        if event.sequence <= row.0 {
            return Ok(());
        }
        row.1.apply(event);
        self.store
            .save(
                &self.name,
                &key,
                ViewRow {
                    version: event.sequence,
                    value: row.1,
                },
            )
            .await
    }
}

/// The in-memory [`ViewStore`]: for tests, examples, and the reference
/// implementation the contract pins.
///
/// No eviction: like the in-memory event store, it grows with the log
/// it feeds on; per the same docs, production wires a durable backend.
pub struct InMemoryViewStore<V> {
    inner: Mutex<HashMap<(String, String), ViewRow<V>>>,
}

impl<V> InMemoryViewStore<V> {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl<V> Default for InMemoryViewStore<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone + Send + Sync> ViewStore<V> for InMemoryViewStore<V> {
    async fn load(&self, view_name: &str, view_id: &str) -> Result<Option<ViewRow<V>>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| StoreError::other(format!("view store lock poisoned: {e}")))?;
        Ok(guard
            .get(&(view_name.to_owned(), view_id.to_owned()))
            .cloned())
    }

    async fn save(
        &self,
        view_name: &str,
        view_id: &str,
        row: ViewRow<V>,
    ) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| StoreError::other(format!("view store lock poisoned: {e}")))?;
        let key = (view_name.to_owned(), view_id.to_owned());
        // Newest wins: a redelivered event folds to a row at an older
        // sequence and is dropped — never written over the newer value.
        match guard.get(&key) {
            Some(existing) if existing.version >= row.version => Ok(()),
            _ => {
                guard.insert(key, row);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_core::vocabulary::{StreamId, Version};

    /// A balance view over u64 deposit events.
    #[derive(Clone, Default, Debug, PartialEq)]
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

    fn projection(
        store: InMemoryViewStore<Balance>,
    ) -> ViewProjection<
        Balance,
        InMemoryViewStore<Balance>,
        impl Fn(&EventEnvelope<u64>) -> Option<String>,
        u64,
    > {
        ViewProjection::new("balance", store, |event: &EventEnvelope<u64>| {
            Some(event.stream_id.as_str().to_owned())
        })
    }

    #[tokio::test]
    async fn events_fold_into_rows_per_key() {
        let mut view = projection(InMemoryViewStore::new());
        view.apply(&envelope(1, "account-1", 10))
            .await
            .expect("fold");
        view.apply(&envelope(2, "account-2", 7))
            .await
            .expect("fold");
        view.apply(&envelope(3, "account-1", 5))
            .await
            .expect("fold");

        let one = ProjectionRow::load(&view, "account-1").await.expect("load");
        assert_eq!(one.0.0, 15, "10 + 5 folded");
        assert_eq!(one.1, Sequence::new(3));
    }

    #[tokio::test]
    async fn a_redelivered_event_is_absorbed() {
        let mut view = projection(InMemoryViewStore::new());
        view.apply(&envelope(1, "account-1", 10))
            .await
            .expect("fold");
        // ApplyFailed boundary: the event redelivers under the same
        // sequence. The row's guard absorbs it.
        view.apply(&envelope(1, "account-1", 10))
            .await
            .expect("replay");

        let (value, version) = ProjectionRow::load(&view, "account-1").await.expect("load");
        assert_eq!(value.0, 10, "replayed, not double-folded");
        assert_eq!(version, Sequence::new(1));
    }

    #[tokio::test]
    async fn an_unkeyed_event_passes_by() {
        let mut view: ViewProjection<Balance, _, _, u64> = ViewProjection::new(
            "balance",
            InMemoryViewStore::new(),
            // Only account streams key this view.
            |event: &EventEnvelope<u64>| {
                event
                    .stream_id
                    .as_str()
                    .starts_with("account-")
                    .then(|| event.stream_id.as_str().to_owned())
            },
        );
        view.apply(&envelope(1, "gingerbread-1", 10))
            .await
            .expect("pass");
        assert!(ProjectionRow::load(&view, "gingerbread-1").await.is_none());
    }

    /// Test-side handle for reading what a [`ViewProjection`] wrote.
    struct ProjectionRow;

    impl ProjectionRow {
        async fn load(
            view: &ViewProjection<
                Balance,
                InMemoryViewStore<Balance>,
                impl Fn(&EventEnvelope<u64>) -> Option<String>,
                u64,
            >,
            key: &str,
        ) -> Option<(Balance, Sequence)> {
            view.store()
                .load(view.name(), key)
                .await
                .expect("load")
                .map(|row| (row.value, row.version))
        }
    }
}
