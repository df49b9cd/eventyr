//! # eventyr-projection
//!
//! The read-path correctness layer: turns raw stored payloads into
//! typed events through composable [upcaster chains](chain), and
//! rebuilds read models across fold-schema changes via
//! [versioned checkpoints](rebuild) — composing with, never
//! duplicating, `eventyr-subscription`'s shipped machinery (the
//! [`Projector`](eventyr_subscription::runner::Projector) runner, the
//! [`SubscriptionSource`](eventyr_subscription::source::SubscriptionSource)
//! and [`CheckpointStore`](eventyr_subscription::checkpoint::CheckpointStore)
//! ports).
//!
//! DESIGN.md's crate layout gives this crate the read path's
//! correctness layer — the projector runner itself shipped as
//! `eventyr-subscription`. What remains here is versioning on the read
//! path. `eventyr-core` ships [`Upcaster`](eventyr_core::upcast::Upcaster)
//! / [`RawEvent`](eventyr_core::upcast::RawEvent) /
//! [`UpcastError`](eventyr_core::error::UpcastError), and nothing read
//! them until now; this crate is the read side of that pipeline:
//!
//! - [`chain::UpcasterChain`] selects an upcaster by
//!   [`event_type`](eventyr_core::upcast::RawEvent::event_type). A miss
//!   or a failed upcast is a loud
//!   [`UpcastError`](eventyr_core::error::UpcastError), never a skipped
//!   event — silently dropping a stored fact is data loss.
//! - [`source::UpcastingSource`] adapts any raw
//!   [`SubscriptionSource`](eventyr_subscription::source::SubscriptionSource)
//!   into a typed one, upcasting inside the runner's existing `Fetch`
//!   action. A poison payload becomes a store error on that fetch: the
//!   subscription machine aborts, the checkpoint never advances past
//!   the poison event, and the projection **stops loudly by design**.
//!   There is no dead-letter mode — the operator fixes the upcaster or
//!   the data and rebuilds.
//! - [`rebuild::RebuildPlan`] re-folds a read model from
//!   the origin when its fold changes: checkpoint keys are versioned
//!   (`"name@v{N}"`), so a schema bump starts fresh with zero changes
//!   to the checkpoint port, and the old version's checkpoint remains
//!   for rollback.
//!
//! ## Cargo features
//!
//! - `inline` — inline views (roadmap 0.7.3): the store-agnostic fold the
//!   durable stores run inside their append transactions; JSON is the
//!   erasure boundary.
//! - `testing` — the `ViewStore` contract, for view-store
//!   implementations' tests; not for production builds.
//!
//! ## Read models are plain `Projection` impls
//!
//! A typed in-memory read model needs no new trait. Folding is pure
//! domain logic — synchronous, infallible at the domain layer, done
//! inside `apply` — while `eventyr-subscription`'s
//! [`Projection`](eventyr_subscription::runner::Projection) is the
//! async, at-least-once boundary. So a read model is a struct holding
//! its state plus a hand-written `impl Projection` that calls a pure
//! `fold` function, and the query side reads behind whatever lock or
//! snapshot discipline the caller chose. If a shared-state handle with
//! typed errors ever proves valuable, it lands as a small local adapter
//! struct over `Projection` (the orphan rule forbids a blanket impl
//! over `Arc<RwLock<R>>`) — never a parallel trait axis.
//!
//! ## A taste
//!
//! Append raw payloads, upcast them on the read path, and rebuild when
//! the fold changes:
//!
//! ```
//! # async fn demo() {
//! use eventyr_core::prelude::*;
//! use eventyr_store::prelude::*;
//! use eventyr_subscription::prelude::*;
//! use eventyr_projection::prelude::*;
//!
//! // The store keeps event_type + payload raw — upcasting is the
//! // read path's job.
//! let store = InMemoryStore::<RawEvent>::new();
//! store
//!     .append(
//!         &StreamId::from("account-1"),
//!         ExpectedVersion::Empty,
//!         vec![NewEvent::new(RawEvent {
//!             event_type: "AmountV1".into(),
//!             schema_version: EventSchemaVersion::V1,
//!             payload: b"10".to_vec(),
//!         })],
//!     )
//!     .await
//!     .expect("append");
//!
//! // The chain: one registered upcaster per historical shape.
//! let chain = UpcasterChain::new().with(
//!     "AmountV1",
//!     ClosureUpcaster::new(|raw: RawEvent| {
//!         std::str::from_utf8(&raw.payload)
//!             .ok()
//!             .and_then(|text| text.parse::<u64>().ok())
//!             .ok_or_else(|| UpcastError {
//!                 event_type: raw.event_type.clone(),
//!                 message: "payload is not a number".into(),
//!             })
//!     }),
//! );
//!
//! // The read model: state plus a documented, hand-written Projection.
//! struct Balance(u64);
//! impl Projection for Balance {
//!     type Event = u64;
//!     type Error = core::convert::Infallible;
//!     async fn apply(&mut self, envelope: &EventEnvelope<u64>) -> Result<(), Self::Error> {
//!         self.0 += envelope.event; // idempotent folding is the model's to keep
//!         Ok(())
//!     }
//! }
//!
//! // Rebuild "balance" at fold-schema v1: from the origin, to catch-up.
//! let plan = RebuildPlan::new("balance", SchemaVersion(1), chain);
//! let outcome = plan
//!     .projector(StoreSubscription::new(store), InMemoryCheckpointStore::new(), Balance(0))
//!     .run(|duration| async move { tokio::time::sleep(duration).await })
//!     .await
//!     .expect("drive");
//! assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
//! # }
//! # fn main() {
//! #     tokio::runtime::Builder::new_current_thread()
//! #         .enable_all()
//! #         .build()
//! #         .expect("runtime")
//! #         .block_on(demo());
//! # }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
pub mod chain;
#[cfg(feature = "inline")]
pub mod inline;
pub mod rebuild;
pub mod registry;
pub mod source;
pub mod view;

pub mod prelude {
    //! The read path: upcaster chains, the raw→typed source adapter,
    //! the versioned registry, schema-versioned rebuilds — and the
    //! view store for per-id read models.

    pub use crate::chain::{ClosureUpcaster, UpcasterChain};
    #[cfg(feature = "inline")]
    pub use crate::inline::{
        Inline, InlineView, InlineViewError, InlineViews, RowKey, StoredRow, fold_inline,
        rows_touched,
    };
    pub use crate::rebuild::{RebuildPlan, SchemaCheckpointStore, SchemaVersion, checkpoint_key};
    pub use crate::registry::{RegistryError, UpcasterRegistry, VersionRung, VersionUpcaster};
    pub use crate::source::{UpcastingSource, VersionedSource};
    pub use crate::view::{InMemoryViewStore, View, ViewProjection, ViewRow, ViewStore};
    pub use eventyr_core::upcast::{RawEvent, Upcaster};
    pub use eventyr_core::version_registry::{EventSchemaVersion, VersionedRaw};
}
