//! # eventyr-sample — a miniature bank, end to end
//!
//! A deployable CQRS/event-sourcing sample on eventyr and
//! eventyr-mediator: commands through the mediator, events in the
//! store, views from a projection, failover via leases. One account
//! aggregate, one read model, one cross-aggregate rule (the transfer's
//! atomic batch), and two interchangeable backends — the in-memory
//! store and the compose-managed Postgres.
//!
//! ## The shape
//!
//! ```text
//!            ┌────────────────────────────────────────────┐
//!            │                the wiring                   │
//!            │  HTTP edge → mediator → Execute / batch      │
//!            │                 │            │              │
//!            │            projection ← store subscription  │
//!            └────────────────────────────────────────────┘
//!                 everything the slices name is inside the box
//! ```
//!
//! The **slices** ([`features`]) are vertical: one request, one
//! handler, one outcome. They send [`Execute`](eventyr_mediator::store::Execute)
//! and [`Transfer`](features::transfer::Transfer) through the mediator and
//! never name a store — the composition root ([`wiring`]) decides
//! which store, which repositories, and which view store. The
//! **domain** ([`domain`]) is one aggregate with one rule: no
//! overdraft. The **wiring** ([`wiring`]) is the only place that names
//! a store, and the backends each build every field of the same
//! mediator type.
//!
//! ## Running it
//!
//! ```sh
//! cargo run -p eventyr-sample                  # in memory, out of the box
//! docker compose up --build --scale api=3      # the compose-managed cluster
//! ```
//!
//! See the README for the full story: replicas, failover, the
//! clustered database, and the read side.
//!
//! ## What the sample demonstrates
//!
//! - The mediator composing an event-sourced write side: the slices
//!   send store-shaped requests and never name a repository.
//! - The store-behind-the-mediator surface: `Execute<Account>` routes
//!   to the aggregate repository, `StampMetadata` fills the
//!   interaction's ids, and `saga_dispatcher` runs the fee saga.
//! - The read side: a projection maintains the `balances` view; the
//!   balance query serves from the view store without touching the
//!   event log.
//! - Two interchangeable backends — in-memory and the compose-managed
//!   Postgres — behind one `compose`, one mediator type, and one edge.

pub mod domain;
pub mod features;
pub mod wiring;

pub use wiring::{Backend, Composed, Sample, compose};
