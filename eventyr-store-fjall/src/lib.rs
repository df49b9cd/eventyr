//! # eventyr-store-fjall
//!
//! The embedded event store: a durable [`EventStore`] /
//! [`StreamsAll`] over [fjall](https://crates.io/crates/fjall), an
//! LSM-tree key-value engine — no server, and no async runtime needed
//! to drive it (fjall is synchronous; this store's futures resolve
//! immediately, so [`drive_write_blocking`] parks nothing).
//!
//! One database, three keyspaces (fjall 3's name for fjall 2's
//! "partitions"):
//!
//! - **`streams`** — `"{stream_id}\0{version:016}"` → the serialized
//!   event row; the zero separator keeps stream ids prefix-safe.
//!   The `016` zero-padding keeps keys in stream-version order, so a
//!   `stream` read is a range scan.
//! - **`heads`** — `stream_id` → `"{version:016}"`: the current stream
//!   version, checked inside the append transaction to enforce
//!   [`ExpectedVersion`] ([`Any`] never reads it, [`Empty`] requires
//!   its absence, [`Exact`] requires equality).
//! - **`global`** — a single `"next"` counter, incremented inside the
//!   same transaction; each appended event's key stores its global
//!   sequence in the value, and a `from`-range is impossible without a
//!   sequence→key index — so stream iteration carries the global order
//!   per keyspace, and the `global` keyspace also keeps
//!   `stream_id\0version` keys of every event under `"{seq:016}\0"`
//!   prefixes so a `stream_all` read is one range scan in sequence
//!   order.
//!
//! All of it is transaction-atomic: one `WriteTransaction` checks the
//! head, writes every event into both keyspaces, advances the head,
//! and commits — so appends are all-or-nothing exactly as
//! [`EventStore::append`] promises, and `stream_all` never observes a
//! partial batch.
//!
//! [`EventStore`]: eventyr_store::store::EventStore
//! [`StreamsAll`]: eventyr_store::store::StreamsAll
//! [`drive_write_blocking`]: eventyr_store::driver::drive_write_blocking
//! [`ExpectedVersion`]: eventyr_core::vocabulary::ExpectedVersion
//! [`Any`]: eventyr_core::vocabulary::ExpectedVersion::Any
//! [`Empty`]: eventyr_core::vocabulary::ExpectedVersion::Empty
//! [`Exact`]: eventyr_core::vocabulary::ExpectedVersion::Exact

#![warn(missing_docs)]

pub mod codec;
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
