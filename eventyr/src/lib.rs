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
//! remaining store-side crates — the Postgres store, projections,
//! subscriptions — arrive with 0.2; see the design document for the
//! roadmap.
//!
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
