//! The `CheckpointStore` port: where the runner persists the last-acked
//! position.
//!
//! §6: "the projector runner persists the last-acked global sequence, so
//! restarts resume without reprocessing." Checkpoints are keyed by
//! subscription name (the first argument of
//! [`Projector::new`](crate::runner::Projector::new)),
//! so one store serves many projections.

use std::collections::HashMap;
use std::sync::Mutex;

use core::future::Future;

use eventyr_core::error::StoreError;
use eventyr_core::subscription_machine::Checkpoint;

/// Where the runner persists last-acked checkpoints.
///
/// `store` is called only after a full batch applied — the at-least-once
/// boundary — so a crash mid-batch restarts after the last persisted
/// checkpoint and re-delivers what followed it.
pub trait CheckpointStore: Send + Sync {
    /// The persisted position of `name`,
    /// [`ORIGIN`](Checkpoint::ORIGIN) if never written.
    fn load(&self, name: &str) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send;

    /// Persist `checkpoint` as `name`'s position, replacing whatever was
    /// there: the last store wins. The runner only ever moves a name
    /// forward; a lower store is an operator rewinding the projection.
    fn store(
        &self,
        name: &str,
        checkpoint: Checkpoint,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Process-memory checkpoints: no persistence across restarts.
///
/// Convenient for tests and examples. On a restart every projection
/// resumes from [`ORIGIN`](Checkpoint::ORIGIN) and replays the whole
/// log — correct under the at-least-once idempotent-apply contract, but
/// usually not what production wants; use a durable store there:
/// `SqliteCheckpointStore` (`eventyr-store-sqlite`'s `checkpoints`
/// feature) or `PgCheckpointStore` (`eventyr-store-postgres`'s
/// `checkpoints` feature), beside the parked events and the read
/// models they guard.
#[derive(Default)]
pub struct InMemoryCheckpointStore {
    inner: Mutex<HashMap<String, Checkpoint>>,
}

impl InMemoryCheckpointStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

// `String` → `&str` lookup via `HashMap::get` avoids allocating on the
// hot read; the lock itself is uncontended except at the ack boundary.
// No user code runs under this lock beyond a `HashMap` get/insert, and
// a panic there leaves the store consistent enough to continue — the
// recover-on-poison policy the other in-memory stores share.
impl CheckpointStore for InMemoryCheckpointStore {
    async fn load(&self, name: &str) -> Result<Checkpoint, StoreError> {
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(guard.get(name).copied().unwrap_or(Checkpoint::ORIGIN))
    }

    async fn store(&self, name: &str, checkpoint: Checkpoint) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.insert(name.to_owned(), checkpoint);
        Ok(())
    }
}

/// Run the [`CheckpointStore`] contract against `make_store`'s fresh
/// stores. Every implementation runs it, behind this crate's `testing`
/// feature.
///
/// The contract is the trait's: an unwritten name loads
/// [`ORIGIN`](Checkpoint::ORIGIN), a stored checkpoint loads back,
/// names are independent, and the last store wins — including one that
/// moves a name back, which is how an operator rewinds a projection.
///
/// # Panics
///
/// When the store broke the trait's contract: an unwritten name
/// answered off the origin, a stored checkpoint did not load back,
/// names leaked into each other, or a later store did not win. The
/// message names the broken check.
#[cfg(any(test, feature = "testing"))]
pub fn checkpoint_store_contract<C: CheckpointStore>(make_store: impl Fn() -> C) {
    use eventyr_core::vocabulary::Sequence;
    use futures::executor::block_on;

    let at = |sequence: u64| Checkpoint::new(Sequence::new(sequence));

    let store = make_store();
    assert_eq!(
        block_on(store.load("projection-a")).expect("load"),
        Checkpoint::ORIGIN,
        "an unwritten name loads the origin"
    );

    block_on(store.store("projection-a", at(7))).expect("store");
    assert_eq!(block_on(store.load("projection-a")).expect("load"), at(7));
    assert_eq!(
        block_on(store.load("projection-b")).expect("load"),
        Checkpoint::ORIGIN,
        "names are independent"
    );

    block_on(store.store("projection-a", at(9))).expect("store again");
    assert_eq!(
        block_on(store.load("projection-a")).expect("load"),
        at(9),
        "a later store replaces the position"
    );
    block_on(store.store("projection-a", at(3))).expect("rewind");
    assert_eq!(
        block_on(store.load("projection-a")).expect("load"),
        at(3),
        "the last store wins, even a lower one"
    );

    block_on(store.store("projection-b", at(u64::from(u32::MAX) + 5))).expect("store b");
    assert_eq!(
        block_on(store.load("projection-b")).expect("load"),
        at(u64::from(u32::MAX) + 5),
        "a position beyond 32 bits round-trips"
    );
    assert_eq!(
        block_on(store.load("projection-a")).expect("load"),
        at(3),
        "storing one name leaves the others alone"
    );
}

// Blanket impls so references and `Arc` work wherever a store does.

macro_rules! impl_checkpoint_delegation {
    ($pointer:ty) => {
        impl<C: CheckpointStore + ?Sized> CheckpointStore for $pointer {
            fn load(
                &self,
                name: &str,
            ) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send {
                (**self).load(name)
            }

            fn store(
                &self,
                name: &str,
                checkpoint: Checkpoint,
            ) -> impl Future<Output = Result<(), StoreError>> + Send {
                (**self).store(name, checkpoint)
            }
        }
    };
}

impl_checkpoint_delegation!(&C);
impl_checkpoint_delegation!(std::sync::Arc<C>);

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_core::vocabulary::Sequence;

    #[test]
    fn the_in_memory_checkpoint_store_passes_the_contract() {
        super::checkpoint_store_contract(InMemoryCheckpointStore::new);
    }

    #[tokio::test]
    async fn an_unwritten_name_loads_the_origin() {
        let store = InMemoryCheckpointStore::new();
        assert_eq!(store.load("miss").await.expect("load"), Checkpoint::ORIGIN);
    }

    #[tokio::test]
    async fn names_are_scoped() {
        let store = InMemoryCheckpointStore::new();
        store
            .store("a", Checkpoint::new(Sequence::new(7)))
            .await
            .expect("store");
        assert_eq!(
            store.load("a").await.expect("load"),
            Checkpoint::new(Sequence::new(7))
        );
        assert_eq!(store.load("b").await.expect("load"), Checkpoint::ORIGIN);
    }
}
