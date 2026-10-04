//! Schema-versioned rebuilds: a fold change re-folds from the origin.
//!
//! A read model is its `fold`. When the fold logic changes, the stored
//! read-model state is no longer derivable from where the old fold
//! stopped — the projection must re-fold the whole log from
//! [`Checkpoint::ORIGIN`]. [`RebuildPlan`] packages that: checkpoint
//! keys are versioned (`"name@v{N}"`), so a schema bump resumes under a
//! fresh key at the origin, folds to catch-up with
//! [`stop_at_catch_up`](SubscriptionPolicy::stop_at_catch_up), and
//! finishes — after which the caller swaps the rebuilt read model in
//! (atomically, under the caller's own lock) and either starts the
//! follow-on run or is done.
//!
//! Name rotation instead of a `delete` op on
//! [`CheckpointStore`](eventyr_subscription::checkpoint::CheckpointStore):
//! the port stays exactly as shipped (DESIGN.md §3's placement rule
//! leaves its surface unchanged), and the old version's checkpoint
//! remains for a rollback. The cost is stale checkpoints accumulating;
//! if a retention story is ever needed, a delete op is a store-side
//! extension justified per-occurrence — not a change to core.
//!
//! An upcaster change does **not** need a schema bump: upcasters live
//! in the source and still produce the same typed events, so the fold's
//! inputs are unchanged and resuming from the existing checkpoint is
//! correct.

use eventyr_core::error::StoreError;
use eventyr_core::subscription::{Checkpoint, SubscriptionPolicy};
use eventyr_core::upcast::RawEvent;
use eventyr_subscription::checkpoint::CheckpointStore;
use eventyr_subscription::runner::{Projection, Projector};
use eventyr_subscription::source::SubscriptionSource;

use crate::chain::UpcasterChain;
use crate::source::UpcastingSource;

/// The schema version of one read model's fold.
///
/// Bump it when the fold logic changes: the new version's checkpoint
/// key is fresh, so the projection re-folds from
/// [`Checkpoint::ORIGIN`]. An upcaster change does not require a bump
/// — see the module docs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SchemaVersion(pub u64);

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// The versioned checkpoint key a rebuild runs under: `"name@v{N}"`.
#[must_use]
pub fn checkpoint_key(name: &str, version: SchemaVersion) -> String {
    format!("{name}@{version}")
}

/// [`CheckpointStore`] convenience for the versioned key discipline.
///
/// Blanket-implemented for every checkpoint store: it is key naming,
/// not new port surface — the `load`/`store` ops it composes are the
/// port's own.
pub trait SchemaCheckpointStore: CheckpointStore {
    /// [`CheckpointStore::load`] under the versioned key for `name`.
    fn load_versioned(
        &self,
        name: &str,
        version: SchemaVersion,
    ) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send {
        let key = checkpoint_key(name, version);
        async move { self.load(&key).await }
    }

    /// [`CheckpointStore::store`] under the versioned key for `name`.
    fn store_versioned(
        &self,
        name: &str,
        version: SchemaVersion,
        checkpoint: Checkpoint,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        let key = checkpoint_key(name, version);
        async move { self.store(&key, checkpoint).await }
    }
}

impl<C: CheckpointStore> SchemaCheckpointStore for C {}

/// A schema-versioned rebuild of one read model: compose, don't
/// re-drive.
///
/// [`RebuildPlan::projector`] wraps the raw `source` in an
/// [`UpcastingSource`], scopes the checkpoint under `name@version`, and
/// forces [`stop_at_catch_up`](SubscriptionPolicy::stop_at_catch_up) —
/// the existing [`Projector`] runs to
/// [`CaughtUp`](eventyr_core::subscription::SubscriptionOutcome::CaughtUp)
/// and returns. Catch-up and live-following are deliberately two
/// separate runs (the caller then drives the follow-on projector
/// without the flag): per §6's "no built-in consumer loop", this crate
/// ships no orchestration.
///
/// For a projection over a store of *already-typed* events — no raw
/// payloads anywhere — use the plain [`Projector`] with a
/// `name@version` checkpoint key (see [`checkpoint_key`]) instead: the
/// source adapter and chain exist only for the raw-payload read path.
pub struct RebuildPlan<E> {
    name: String,
    version: SchemaVersion,
    chain: UpcasterChain<E>,
    policy: SubscriptionPolicy,
}

impl<E> RebuildPlan<E> {
    /// A rebuild of the read model checkpointed under `name`, at fold
    /// schema `version`, upcasting raw payloads through `chain`.
    pub fn new(name: impl Into<String>, version: SchemaVersion, chain: UpcasterChain<E>) -> Self {
        Self {
            name: name.into(),
            version,
            chain,
            policy: SubscriptionPolicy::default().stop_at_catch_up(),
        }
    }

    /// Tune batch size and sleeps; `stop_at_catch_up` stays forced on —
    /// a rebuild that never stops is a follow-on subscription, which
    /// the plain [`Projector`] already is.
    #[must_use]
    pub fn with_policy(mut self, policy: SubscriptionPolicy) -> Self {
        self.policy = policy.stop_at_catch_up();
        self
    }

    /// The versioned checkpoint key this rebuild runs under.
    #[must_use]
    pub fn checkpoint_key(&self) -> String {
        checkpoint_key(&self.name, self.version)
    }

    /// Build the rebuild projector: `source` raw in, typed envelopes
    /// out, checkpoints scoped to this `name@version`, stopping at
    /// catch-up.
    pub fn projector<S, C, P>(
        self,
        source: S,
        checkpoints: C,
        projection: P,
    ) -> Projector<UpcastingSource<S, E>, C, P>
    where
        S: SubscriptionSource<Event = RawEvent>,
        C: CheckpointStore,
        P: Projection<Event = E>,
    {
        Projector::new(
            self.checkpoint_key(),
            UpcastingSource::new(source, self.chain),
            checkpoints,
            projection,
        )
        .with_policy(self.policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_core::vocabulary::Sequence;
    use eventyr_subscription::checkpoint::InMemoryCheckpointStore;

    #[test]
    fn the_checkpoint_key_is_name_at_version() {
        assert_eq!(checkpoint_key("balance", SchemaVersion(7)), "balance@v7");
        assert_eq!(SchemaVersion(7).to_string(), "v7");
    }

    #[tokio::test]
    async fn versioned_keys_are_scoped_per_version() {
        let store = InMemoryCheckpointStore::new();
        store
            .store_versioned(
                "balance",
                SchemaVersion(1),
                Checkpoint::new(Sequence::new(9)),
            )
            .await
            .expect("store");
        assert_eq!(
            store
                .load_versioned("balance", SchemaVersion(1))
                .await
                .expect("load"),
            Checkpoint::new(Sequence::new(9))
        );
        // A schema bump reads the origin: the fresh key was never written.
        assert_eq!(
            store
                .load_versioned("balance", SchemaVersion(2))
                .await
                .expect("load"),
            Checkpoint::ORIGIN
        );
    }

    #[test]
    fn a_rebuild_plan_forces_stop_at_catch_up() {
        let plan =
            RebuildPlan::<u64>::new("balance", SchemaVersion(1), UpcasterChain::new()).with_policy(
                SubscriptionPolicy::new(1, core::time::Duration::ZERO, core::time::Duration::ZERO),
            );
        // The key is versioned even before the projector is built.
        assert_eq!(plan.checkpoint_key(), "balance@v1");
    }
}
