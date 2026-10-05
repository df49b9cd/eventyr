//! # eventyr-store-testing
//!
//! The **store contract**: a suite of checks every
//! [`EventStore`](eventyr_store::store::EventStore) /
//! [`StreamsAll`](eventyr_store::store::StreamsAll) (and, with the
//! opt-in [`snapshot_contract`], every
//! [`SnapshotStore`](eventyr_store::snapshot_store::SnapshotStore), and
//! with the opt-in [`query_append_contract`] every
//! [`QueryAppend`](eventyr_store::store::QueryAppend), and with
//! [`lifecycle_contract`] / [`lifecycle_query_append_contract`] every
//! [`StreamLifecycle`](eventyr_store::store::StreamLifecycle))
//! implementation must pass to claim compatibility with the eventyr
//! protocol — the eventcore-testing idea, applied to eventyr's ports.
//!
//! A store proves itself by calling [`event_store_contract`], and —
//! when it also streams globally — [`streams_all_contract`], each from
//! one `#[test]` that hands the suite a fresh, empty store:
//!
//! ````text
//! use eventyr_store::prelude::InMemoryStore;
//!
//! #[test]
//! fn in_memory_store_passes_the_contract() {
//!     eventyr_store_testing::event_store_contract::<u64, _>(InMemoryStore::new);
//!     eventyr_store_testing::streams_all_contract::<u64, _>(InMemoryStore::new);
//! }
//! ````
//!
//! Everything runs without an async runtime: a store whose futures
//! block on real I/O must do that parking inside its own methods (this
//! suite drives them to `Poll::Ready` one at a time per check), so the
//! checks run on CI with no database and under miri.
//!
//! [`PayloadEvent`] and [`ParityEvent`] are ready-made event types for
//! the suites; the `serde` feature derives their serialization, for
//! stores that persist events.
//!
//! The checks exercise the machine-visible protocol only — versions,
//! sequences, expectations, ordering — because that is the contract: a
//! machine-driven driver must be unable to tell a conforming store
//! from the in-memory one.

mod batch_store;
mod commit_signal;
mod event_store;
mod filtered;
mod fixtures;
mod lifecycle;
mod query_append;
mod snapshot_store;
mod streams_all;

pub use batch_store::event_store_batch_contract;
pub use commit_signal::commit_signal_contract;
pub use event_store::{ContractEvent, event_store_contract};
pub use filtered::filtered_read_contract;
pub use fixtures::{ParityEvent, PayloadEvent};
pub use lifecycle::{lifecycle_contract, lifecycle_query_append_contract};
pub use query_append::query_append_contract;
pub use snapshot_store::snapshot_contract;
pub use streams_all::streams_all_contract;
