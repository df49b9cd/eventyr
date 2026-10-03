//! # eventyr
//!
//! Event sourcing for Rust — pure machines, thin drivers.
//!
//! This umbrella crate re-exports [`eventyr_core`] (the `Aggregate` trait,
//! the protocol vocabulary, and the `WriteMachine`) with the `time` feature
//! enabled. The store-side crates — `EventStore` traits, drivers, the
//! in-memory and Postgres stores, projections, subscriptions — arrive with
//! 0.1–0.2; see the design document for the roadmap.
//!
//! ```
//! use eventyr::prelude::*;
//!
//! let policy = RetryPolicy::default();
//! assert_eq!(policy.max_retries, 3);
//! ```

pub use eventyr_core::*;
