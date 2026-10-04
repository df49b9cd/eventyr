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
use eventyr_core::subscription::Checkpoint;

/// Where the runner persists last-acked checkpoints.
///
/// `store` is called only after a full batch applied — the at-least-once
/// boundary — so a crash mid-batch restarts after the last persisted
/// checkpoint and re-delivers what followed it.
pub trait CheckpointStore: Send + Sync {
    /// The persisted position of `name`,
    /// [`ORIGIN`](Checkpoint::ORIGIN) if never written.
    fn load(&self, name: &str) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send;

    /// Persist `checkpoint` as `name`'s position.
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
/// usually not what production wants; use a durable store there (a
/// Postgres one arrives with `eventyr-store-postgres`).
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
impl CheckpointStore for InMemoryCheckpointStore {
    async fn load(&self, name: &str) -> Result<Checkpoint, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| StoreError::other(format!("checkpoint store lock poisoned: {e}")))?;
        Ok(guard.get(name).copied().unwrap_or(Checkpoint::ORIGIN))
    }

    async fn store(&self, name: &str, checkpoint: Checkpoint) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| StoreError::other(format!("checkpoint store lock poisoned: {e}")))?;
        guard.insert(name.to_owned(), checkpoint);
        Ok(())
    }
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
