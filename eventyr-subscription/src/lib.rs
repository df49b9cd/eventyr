//! # eventyr-subscription
//!
//! Catch-up subscriptions and the event bus trait: the projector
//! runner, a checkpoint store, and the adapters over any
//! [`StreamsAll`](eventyr_store::store::StreamsAll) store.
//!
//! The machine itself —
//! [`SubscriptionMachine`](eventyr_core::subscription::SubscriptionMachine)
//! — lives in `eventyr-core` (DESIGN.md §3: a machine and the types it
//! transitions on live in core). This crate is the store side: the I/O
//! ports ([`source::SubscriptionSource`], [`checkpoint::CheckpointStore`],
//! [`runner::Projection`]) and the driver ([`runner::Projector`] /
//! [`runner::drive_projector`]) it runs against. The `bus` feature
//! carries the §6 [`bus::Subscription`] shape and the §2
//! [`bus::EventBus`] trait — the 0.3 live-push seam — off by default.
//!
//! ## A taste
//!
//! A projection folds envelopes at-least-once; the runner resumes it
//! from its persisted checkpoint:
//!
//! ```
//! # async fn demo() {
//! use std::fmt;
//! use eventyr_core::prelude::*;
//! use eventyr_store::prelude::*;
//! use eventyr_subscription::prelude::*;
//!
//! // A projection over the canonical account events.
//! struct BalanceOf(u64);
//! impl Projection for BalanceOf {
//!     type Event = u64;
//!     type Error = core::convert::Infallible;
//!
//!     async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), Self::Error> {
//!         // A read model. Idempotency is the projection's to keep —
//!         // here, folded keys are just u64s for the taste of it.
//!         self.0 += event.sequence.as_u64();
//!         Ok(())
//!     }
//! }
//!
//! let store = InMemoryStore::new();
//! store
//!     .append(
//!         &StreamId::from("account-1"),
//!         ExpectedVersion::Empty,
//!         vec![NewEvent::new(1), NewEvent::new(2)],
//!     )
//!     .await
//!     .expect("append");
//!
//! let projector = Projector::new(
//!     "balance",                         // the checkpoint name
//!     StoreSubscription::new(store),     // over any StreamsAll store
//!     InMemoryCheckpointStore::new(),    // restart replays from ORIGIN
//!     BalanceOf(0),
//! )
//! .with_policy(SubscriptionPolicy::default().stop_at_catch_up());
//!
//! // No built-in consumer loop: the caller spawns the run.
//! let outcome = projector
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

#[cfg(feature = "bus")]
pub mod bus;
pub mod checkpoint;
pub mod runner;
pub mod saga;
pub mod source;

pub mod prelude {
    //! The subscription side: the projection runner, the store-source
    //! adapter, the checkpoint port, and the saga runner.

    #[cfg(feature = "bus")]
    pub use crate::bus::{EventBus, Subscription};
    pub use crate::checkpoint::{CheckpointStore, InMemoryCheckpointStore};
    pub use crate::runner::{
        Projection, Projector, drive_projector, drive_projector_with_metrics, drive_projector_woken,
    };
    pub use crate::saga::{SagaProjection, drive_saga};
    pub use crate::source::{StoreSubscription, SubscriptionSource};
    pub use eventyr_core::saga::{
        Saga, SagaAction, SagaCommand, SagaInput, SagaMachine, SagaOutcome,
    };
    pub use eventyr_core::subscription::{
        Batch, Checkpoint, SubscriptionAction, SubscriptionInput, SubscriptionMachine,
        SubscriptionOutcome, SubscriptionPolicy,
    };
    pub use eventyr_store::notify::{CommitListener, CommitSignal, LocalCommitSignal, NoSignal};
}
