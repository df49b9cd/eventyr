//! Event sourcing for Rust — pure machines, thin drivers.
//!
//! This umbrella crate re-exports [`eventyr_core`] (the `Aggregate` trait,
//! the protocol vocabulary, and the machines) with the `time` and
//! `macros` features enabled — the derives arrive through the prelude,
//! and target this crate automatically; a *renamed* dependency is the
//! only case that needs `#[eventyr(crate = "...")]`. With the `store`
//! feature (default) it also re-exports the store side as [`store`]:
//! the [`EventStore`](store::prelude::EventStore) and
//! [`StreamsAll`](store::prelude::StreamsAll) ports, the
//! [`InMemoryStore`](store::prelude::InMemoryStore), the
//! [`drive_write`](store::prelude::drive_write) driver, and the
//! [`AggregateRepository`](store::prelude::AggregateRepository). The
//! `subscription` feature re-exports the catch-up subscription side as
//! [`subscription`]; the `bus` feature adds the `EventBus` seam on top
//! of it; the `projection` feature re-exports the read-path layer
//! (upcaster chains, raw→typed sources, schema-versioned rebuilds) as
//! `projection`; the `postgres`, `sqlite`, and `fjall` features pull in
//! the durable stores. Everything else below is the workspace README.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub use eventyr_core::*;

/// `Vec`, for the derive macro's generated `decide`
/// signatures — the same re-export `eventyr_core::__private` provides
/// there (the serde `__private` pattern), so generated code targets one
/// shape in either crate.
pub mod __private {
    pub use std::vec::Vec;
}

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

/// The embedded store over SQLite (`rusqlite`, bundled) — the
/// contract-suite reference port, behind `sqlite` (plus
/// `sqlite_snapshots` for its snapshot store).
#[cfg(feature = "sqlite")]
pub use eventyr_store_sqlite as sqlite;

/// Crypto-shredding (roadmap 0.7.6): personal fields encrypted per data subject
/// and erased by deleting the subject's key — behind `shred`.
#[cfg(feature = "shred")]
pub use eventyr_shred as shred;

/// The AES-256-GCM cipher adapter for [`shred`], behind `shred_aes_gcm`.
#[cfg(feature = "shred_aes_gcm")]
pub use eventyr_shred_aes_gcm as shred_aes_gcm;

/// The XChaCha20-Poly1305 cipher adapter for [`shred`], behind
/// `shred_chacha`.
#[cfg(feature = "shred_chacha")]
pub use eventyr_shred_chacha as shred_chacha;
