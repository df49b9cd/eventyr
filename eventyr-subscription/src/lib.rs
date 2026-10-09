//! # eventyr-subscription
//!
//! Catch-up subscriptions and the event bus trait: the projector
//! runner, a checkpoint store, and the adapters over any
//! [`StreamsAll`](eventyr_store::store::StreamsAll) store.
//!
//! The machine itself —
//! [`SubscriptionMachine`](eventyr_core::subscription_machine::SubscriptionMachine)
//! — lives in `eventyr-core` (DESIGN.md §3: a machine and the types it
//! transitions on live in core). This crate is the store side: the I/O
//! ports ([`source::SubscriptionSource`], [`checkpoint::CheckpointStore`],
//! [`lease::ProjectorLease`] — one driver per checkpoint name, 0.7.9 —
//! [`runner::Projection`]) and the driver ([`runner::Projector`] /
//! [`runner::drive_projector`], or [`runner::drive_projector_leased`])
//! it runs against.
//!
//! ## Cargo features
//!
//! - `bus` — the §6 [`bus::Subscription`] shape and the §2
//!   [`bus::EventBus`] trait — the 0.3 live-push seam; off by default,
//!   shipping as transport adapters land.
//! - `testing` — exports [`checkpoint::checkpoint_store_contract`],
//!   [`parked::parked_store_contract`], and
//!   [`lease::lease_store_contract`], for store implementations'
//!   tests; not for production builds.
//! - `tokio_notify` — a [`Catch`](runner::Catch) impl for
//!   `Arc<tokio::sync::Notify>`: the `caught_up` port's ready-made
//!   listener for tokio-based runners; off by default — the machine
//!   runs without tokio, this is the tests' and services' seam.
//!
//! ## A taste
//!
//! A projection folds envelopes at-least-once; the runner resumes it
//! from its persisted checkpoint:
//!
//! ```
//! # async fn demo() {
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

#![cfg_attr(docsrs, feature(doc_cfg))]
#[cfg(feature = "bus")]
pub mod bus;
pub mod checkpoint;
pub mod lease;
pub mod parked;
pub mod projection_scenario;
pub mod runner;
pub mod saga;
pub mod source;

pub mod prelude {
    //! The subscription side: the projection runner, the store-source
    //! adapter, the checkpoint port, and the saga runner.

    #[cfg(feature = "bus")]
    pub use crate::bus::{EventBus, Subscription};
    pub use crate::checkpoint::{CheckpointStore, InMemoryCheckpointStore};
    pub use crate::lease::{InMemoryLeaseStore, LeaseError, LeasePolicy, ProjectorLease};
    pub use crate::parked::{InMemoryParkedStore, NoParking, ParkedEvent, ParkedStore};
    pub use crate::projection_scenario::{ProjectionOutcome, ProjectionScenario};
    pub use crate::runner::{
        Catch, DriverPorts, Fanout, LeasedProjector, NoCatch, Projection, Projector, RunError,
        SkipRedelivered, drive_projector, drive_projector_blocking, drive_projector_leased,
    };
    pub use crate::saga::{SagaProjection, drive_saga};
    pub use crate::source::{FilteredSubscription, StoreSubscription, SubscriptionSource};
    pub use eventyr_core::saga::{
        Saga, SagaAction, SagaCommand, SagaInput, SagaMachine, SagaOutcome,
    };
    pub use eventyr_core::subscription_machine::{
        Batch, Checkpoint, FailurePolicy, SubscriptionAction, SubscriptionInput,
        SubscriptionMachine, SubscriptionOutcome, SubscriptionPolicy,
    };
    pub use eventyr_store::notify::{CommitListener, CommitSignal, LocalCommitSignal, NoSignal};
    pub use eventyr_store::store::EventFilter;

    #[cfg(feature = "testing")]
    pub use crate::checkpoint::checkpoint_store_contract;
    #[cfg(feature = "testing")]
    pub use crate::lease::lease_store_contract;
    #[cfg(feature = "testing")]
    pub use crate::parked::parked_store_contract;
}
