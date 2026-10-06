//! The snapshot port: where the state a snapshot carries is persisted,
//! out of band from the event log.
//!
//! Snapshots are read-side shortcuts, never the source of truth: the
//! event log answers every question a snapshot can, so this port is
//! deliberately small — newest-per-stream `load`, replace `save` — and
//! its failures never fail a write. The machine's
//! [`Committed`](eventyr_core::write::WriteOutcome::Committed) outcome
//! carries the offer; the driver may persist it here and drops a save
//! error, because a snapshot's staleness is self-correcting on the next
//! load (the delta fold covers whatever the snapshot missed).
//!
//! `save` is **newest-wins**: a snapshot whose `version` is behind the
//! one already persisted is dropped, never stored over it. Concurrent
//! post-commit offers can arrive out of order; the monotonic replace
//! keeps that harmless — a stale offer can only ever be ignored, so the
//! persisted snapshot never regresses.
//!
//! The state is represented as [`Snapshot<S>`] over the in-memory state
//! type `S` — serialization to bytes (and back) is each *store*'s
//! concern, keeping `eventyr-core` serde-free per DESIGN §3's placement
//! rule.

use core::future::Future;

extern crate alloc;

use eventyr_core::error::StoreError;
use eventyr_core::snapshot::Snapshot;
use eventyr_core::vocabulary::StreamId;

/// A durable home for the newest snapshot of each stream.
///
/// One snapshot per stream (the newest): `save` replaces, `load` reads
/// it back. Implementations are shared (`&self`) like every other port.
pub trait SnapshotStore {
    /// The materialized state type the snapshots carry.
    type State: Clone + Send;

    /// The newest persisted snapshot for `stream_id`, or `None` when the
    /// store has none (an unknown stream is `None`, not an error).
    fn load(
        &self,
        stream_id: &StreamId,
    ) -> impl Future<Output = Result<Option<Snapshot<Self::State>>, StoreError>> + Send;

    /// Persist `snapshot`, replacing any older one for the same stream.
    ///
    /// Newest wins: an incoming snapshot whose `version` is behind (or
    /// equal to) the persisted one is dropped, so concurrent
    /// fire-and-forget offers arriving out of order can never regress
    /// the stored snapshot.
    ///
    /// Callers persist fire-and-forget — an error means the next reader
    /// folds a longer delta, nothing more. Implementations may still
    /// report it (for metrics), but the write outcome never carries it.
    fn save(
        &self,
        snapshot: Snapshot<Self::State>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// In-memory snapshot store: for tests, examples, and the repository's
/// default. Keyed newest-per-stream behind the same kind of lock the
/// in-memory event store uses.
#[derive(Debug)]
pub struct InMemorySnapshotStore<S> {
    inner: std::sync::Mutex<std::collections::HashMap<StreamId, Snapshot<S>>>,
}

impl<S> InMemorySnapshotStore<S> {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl<S> Default for InMemorySnapshotStore<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Clone + Send> SnapshotStore for InMemorySnapshotStore<S> {
    type State = S;

    async fn load(&self, stream_id: &StreamId) -> Result<Option<Snapshot<S>>, StoreError> {
        // No user code runs under this lock beyond a `HashMap` get, and
        // a panic there leaves the store consistent enough to continue —
        // the recover-on-poison policy the other in-memory stores share.
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(guard.get(stream_id).cloned())
    }

    async fn save(&self, snapshot: Snapshot<S>) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Newest wins: an offer arriving behind the persisted version
        // (two commits racing, offers applied out of order) is dropped,
        // so the stored snapshot can never regress.
        match guard.get(&snapshot.stream_id) {
            Some(existing) if existing.version >= snapshot.version => {}
            _ => {
                guard.insert(snapshot.stream_id.clone(), snapshot);
            }
        }
        Ok(())
    }
}

// Blanket impls: stores are shared (references, `Arc`) — the same
// delegation pattern the event-store ports use.

macro_rules! impl_snapshot_delegation {
    ($pointer:ty) => {
        impl<T: SnapshotStore + ?Sized> SnapshotStore for $pointer {
            type State = T::State;

            fn load(
                &self,
                stream_id: &StreamId,
            ) -> impl Future<Output = Result<Option<Snapshot<Self::State>>, StoreError>> + Send
            {
                (**self).load(stream_id)
            }

            fn save(
                &self,
                snapshot: Snapshot<Self::State>,
            ) -> impl Future<Output = Result<(), StoreError>> + Send {
                (**self).save(snapshot)
            }
        }
    };
}

impl_snapshot_delegation!(&T);
impl_snapshot_delegation!(std::sync::Arc<T>);
