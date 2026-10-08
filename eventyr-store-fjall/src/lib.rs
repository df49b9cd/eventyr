//! # eventyr-store-fjall
//!
//! The embedded event store: a durable [`EventStore`] /
//! [`StreamsAll`] over [fjall](https://crates.io/crates/fjall), an
//! LSM-tree key-value engine — no server, and no async runtime needed
//! to drive it (fjall is synchronous; this store's futures resolve
//! immediately, so [`drive_write_blocking`] parks nothing).
//!
//! [`open`](crate::FjallStore::open) takes the database *directory*
//! (created if absent) and owns the whole keyspace; a store sharing a
//! database with other data uses [`from_keyspace`](crate::FjallStore::from_keyspace)
//! instead. fjall is single-process: a second process must not open
//! the same directory. Snapshots (the `snapshots` feature) go through
//! [`FjallSnapshotStore::open`](snapshots::FjallSnapshotStore::open)
//! on the same database handle — see the store's test suite for the
//! wiring.
//!
//! One database, five keyspaces (fjall 3's name for fjall 2's
//! "partitions"):
//!
//! - **`streams`** — `"{stream_id}\0{version:016}"` → the serialized
//!   event row; the zero separator keeps stream ids prefix-safe, and
//!   the zero-padded version keeps one stream's keys in version order,
//!   so a `stream` read is a range scan.
//! - **`heads`** — `stream_id` → `"{version:016}"`: the current stream
//!   version, checked inside the append transaction to enforce
//!   [`ExpectedVersion`] ([`Any`] never reads it, [`Empty`] requires
//!   its absence, [`Exact`] requires equality).
//! - **`global`** — `"{sequence:016}"` → `"{stream_id}\0{version:016}"`:
//!   the pointer resolving `stream_all`'s sequence scan.
//! - **`meta`** — `"next"` → the global sequence counter, incremented
//!   inside the same transaction.
//! - **`lifecycle`** — `stream_id` → the close/truncate markers
//!   (roadmap 0.7.6), only for streams that were closed or truncated.
//!
//! All of it is transaction-atomic: one `WriteTransaction` checks the
//! head, writes every event into both keyspaces, advances the head,
//! and commits — so appends are all-or-nothing exactly as
//! [`EventStore::append`](eventyr_store::store::EventStore::append) promises, and `stream_all` never observes a
//! partial batch.
//!
//! ## Cargo features
//!
//! - `snapshots` — the `SnapshotStore` implementation over a third
//!   partition; off by default: stores that only want the event log
//!   pay nothing.
//!
//! [`EventStore`]: eventyr_store::store::EventStore
//! [`StreamsAll`]: eventyr_store::store::StreamsAll
//! [`drive_write_blocking`]: eventyr_store::driver::drive_write_blocking
//! [`ExpectedVersion`]: eventyr_core::vocabulary::ExpectedVersion
//! [`Any`]: eventyr_core::vocabulary::ExpectedVersion::Any
//! [`Empty`]: eventyr_core::vocabulary::ExpectedVersion::Empty
//! [`Exact`]: eventyr_core::vocabulary::ExpectedVersion::Exact

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

#[cfg(feature = "snapshots")]
pub mod snapshots;
mod store;

pub use store::FjallStore;

/// The error type the store maps into
/// [`StoreError::Other`](eventyr_core::error::StoreError::Other) at the
/// port boundary — exposed so callers correlating logs can recover the
/// fjall error and its `source` chain.
#[derive(Debug, thiserror::Error)]
pub enum FjallStoreError {
    /// An engine error (I/O, corruption, poisoned journal).
    #[error(transparent)]
    Engine(#[from] fjall::Error),
    /// A stored row did not decode (schema drift, corruption).
    #[error("a stored row failed to decode: {0}")]
    CorruptRow(String),
}
