//! The metrics port: the seam between the drivers and an operations
//! backend.
//!
//! Per the §2 rule (a framework takes over nothing it can leave to the
//! caller), instrumenting a store or projector is an *option*, not a
//! takeover: a driver accepts any [`Metrics`] at constructor-call time
//! ([`NoopMetrics`] the default) and calls it at the protocol
//! boundaries the driver's loop already owns. The port is deliberately
//! small — the three instruments operations reach for, one type each.
//!
//! Keeping this in the *store* crate, not the core, is on purpose: the
//! machines are pure and never sleep or observe clocks; timing spans are
//! the driver's measurement of the I/O it performs, reported here. The
//! runner in `eventyr-subscription` wraps its loop the same way.
//!
//! [`tracing`](https://docs.rs/tracing) is the reference backend:
//! behind the `tracing` feature, [`TracingMetrics`] maps each counter/
//! gauge/histogram call onto a `tracing` event, so the crate's
//! zero-dependency build keeps its shape and the runtime cost of an
//! unused port is one atomics-free `&dyn` dispatch. With `NoopMetrics`
//! the calls monomorphize to nothing.

use std::time::Duration;

/// The port the drivers report to: one method per instrument kind.
///
/// Implementations must be `Send + Sync` because drivers hold them
/// across `.await`; the methods take `&self` and return nothing —
/// reporting failure is swallowed by the backend, never surfaced to
/// the store protocol.
pub trait Metrics: Send + Sync {
    /// Increment a counter (monotone upward) by `by`.
    fn counter(&self, name: &'static str, by: u64);
    /// Set a gauge (a value that can go up and down) to `value`.
    fn gauge(&self, name: &'static str, value: u64);
    /// Record one observation into a histogram (e.g. a latency).
    fn histogram(&self, name: &'static str, value: Duration);
}

/// The default [`Metrics`]: every call is a no-op. The driver compiles
/// it away; `dyn Metrics` over it is one branch on an unused path.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn counter(&self, _: &'static str, _: u64) {}
    fn gauge(&self, _: &'static str, _: u64) {}
    fn histogram(&self, _: &'static str, _: Duration) {}
}

/// The instruments, by protocol boundary — the names a driver reports
/// with, so a dashboard keys off the same strings whichever store runs.
pub mod names {
    /// Appends against the store (`drive_write`, `drive_write_batch`).
    pub const APPENDS: &str = "eventyr_appends_total";
    /// Optimistic-concurrency conflicts the write machine retried.
    pub const CONFLICTS: &str = "eventyr_conflicts_total";
    /// Snapshot read + write through the snapshot store.
    pub const SNAPSHOTS: &str = "eventyr_snapshots_total";
    /// Events the projector applied (the read path's forward progress).
    pub const PROJECTED_EVENTS: &str = "eventyr_projected_events_total";
    /// One append's latency (histogram).
    pub const APPEND_LATENCY: &str = "eventyr_append_seconds";
    /// One projector batch's latency (histogram).
    pub const PROJECT_BATCH_LATENCY: &str = "eventyr_project_batch_seconds";
}

/// The tracing backend: each instrument call becomes a `tracing` event.
///
/// `Metrics` is a trait so libraries don't lock the caller to one
/// stack; `TracingMetrics` is the shipped impl for the application that
/// already runs on tracing. An application with a metrics library of its
/// own implements the three methods over its counters.
#[cfg(feature = "tracing")]
#[derive(Clone, Copy, Debug, Default)]
pub struct TracingMetrics;

#[cfg(feature = "tracing")]
impl Metrics for TracingMetrics {
    fn counter(&self, name: &'static str, by: u64) {
        tracing::debug!(target: "eventyr.metrics", metric = name, value = by, "counter");
    }

    fn gauge(&self, name: &'static str, value: u64) {
        tracing::debug!(target: "eventyr.metrics", metric = name, value, "gauge");
    }

    fn histogram(&self, name: &'static str, value: Duration) {
        tracing::debug!(
            target: "eventyr.metrics",
            metric = name,
            value_ms = value.as_secs_f64() * 1_000.0,
            "histogram"
        );
    }
}
