//! # eventyr
//!
//! Event sourcing for Rust — pure machines, thin drivers.
//!
//! This umbrella crate re-exports [`eventyr_core`] (the `Aggregate` trait,
//! the protocol vocabulary, and the `WriteMachine`) with the `time` and
//! `macros` features enabled — the derives arrive through the prelude.
//! With the `store` feature (default) it also re-exports the store side
//! as [`store`]: the [`EventStore`](store::prelude::EventStore) and
//! [`StreamsAll`](store::prelude::StreamsAll) ports, the
//! [`InMemoryStore`](store::prelude::InMemoryStore), the
//! [`drive_write`](store::prelude::drive_write) driver, and the
//! [`AggregateRepository`](store::prelude::AggregateRepository). The
//! `subscription` feature re-exports the catch-up subscription side as
//! [`subscription`]; the `bus` feature adds the `EventBus` live-push
//! trait on top of it; the `projection` feature re-exports the read-path
//! layer (upcaster chains, raw→typed sources, schema-versioned
//! rebuilds) as `projection`. The workspace ships
//! `eventyr-store-postgres` (the durable `EventStore`/`StreamsAll` over
//! Postgres) as a standalone crate; the umbrella pulls it in as its own
//! feature over time — see the design document for the roadmap.
//!
//! ## A taste
//!
#![doc = include_str!("../../README.md")]
#![doc = ""]
//! ```
//! use eventyr::prelude::*;
//!
//! let policy = RetryPolicy::default();
//! assert_eq!(policy.max_retries, 3);
//! ```

pub use eventyr_core::*;

/// The store side: ports, the in-memory store, the async driver, and
/// the repository — re-exported from `eventyr-store` behind the
/// `store` feature.
#[cfg(feature = "store")]
pub use eventyr_store as store;

/// The subscription side: the projector runner, the checkpoint store,
/// and the store-source adapter — re-exported from
/// `eventyr-subscription` behind the `subscription` feature.
#[cfg(feature = "subscription")]
pub use eventyr_subscription as subscription;

/// The read-path layer: upcaster chains, the raw→typed source adapter,
/// and schema-versioned rebuilds — re-exported from
/// `eventyr-projection` behind the `projection` feature.
#[cfg(feature = "projection")]
pub use eventyr_projection as projection;

/// The Postgres store: the durable `EventStore`/`StreamsAll` — and,
/// behind its `snapshots` feature (pulled in by this crate's
/// `snapshots` feature), the `SnapshotStore` — over Postgres.
#[cfg(feature = "postgres")]
pub use eventyr_store_postgres as postgres;

/// The embedded store: a durable `EventStore`/`StreamsAll` over
/// [fjall](https://crates.io/crates/fjall) — no server, and no async
/// runtime needed to drive it (the futures resolve immediately; drive
/// it through `store::prelude::drive_write_blocking`).
#[cfg(feature = "fjall")]
pub use eventyr_store_fjall as fjall;
