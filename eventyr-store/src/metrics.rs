//! The metrics port: the seam between the drivers and an operations
//! backend.
//!
//! The port itself — [`Metrics`], [`NoopMetrics`], and the stable
//! instrument [`names`] — lives in [`eventyr_core::metrics`], where the
//! protocol boundaries it reports on are defined; this module re-exports
//! it unchanged, so `eventyr_store::metrics` paths keep working.
//!
//! What this crate adds is the reference backend: behind the `tracing`
//! feature, [`TracingMetrics`] maps each counter/gauge/histogram call
//! onto a `tracing` event, so the zero-dependency build keeps its shape
//! and the runtime cost of an unused port is one atomics-free `&dyn`
//! dispatch. With [`NoopMetrics`] the calls monomorphize to nothing.

pub use eventyr_core::metrics::{Metrics, NoopMetrics};

/// The instruments, by protocol boundary: core's
/// [`names`](eventyr_core::metrics::names), plus the projector driver's
/// failure counters.
pub mod names {
    pub use eventyr_core::metrics::names::*;

    /// Parks the parked store refused (roadmap 0.7.7): the event was not
    /// recorded, so the projector backs off and redelivers it instead of
    /// moving on.
    ///
    /// Rising steadily means parking is broken — a store
    /// that is down, or a projector told to park with no parked store —
    /// and the projection is stalled on the event: alert on it.
    pub const PARK_FAILURES: &str = "eventyr_park_failures_total";
    /// Checkpoint writes that failed: the batch is redelivered after a
    /// backoff. Occasional ones are harmless (at-least-once); a steady
    /// rate means the projection is not making durable progress.
    pub const ACK_FAILURES: &str = "eventyr_ack_failures_total";

    /// Lease renewals a projector driver attempted (roadmap 0.7.9): the rhythm
    /// at which a held lease stays held.
    ///
    /// A drop while the projector is
    /// running is a sign the driver has stalled between renewals.
    pub const LEASE_RENEWALS: &str = "eventyr_lease_renewals_total";
    /// Renewals the lease store failed: one is a flaky store, a run of
    /// them means the projector is about to report
    /// [`LeaseLost`](eventyr_core::subscription_machine::SubscriptionOutcome)-side
    /// loss and stop.
    pub const LEASE_RENEW_FAILURES: &str = "eventyr_lease_renew_failures_total";
    /// Leases a driver run ended without: another holder owns the name
    /// (an entry refusal or a takeover), or the holder can no longer
    /// renew.
    ///
    /// Each one ends a driver run; a repeating one means two
    /// projectors are configured on one name.
    pub const LEASE_LOST: &str = "eventyr_lease_lost_total";
    /// Releases of a completed run's lease that failed: the run's
    /// outcome is unaffected — the name frees at its grace bound
    /// instead of at once.
    ///
    /// A steady rate means the lease store is
    /// flaky at exactly the moment runs end.
    pub const LEASE_RELEASE_FAILURES: &str = "eventyr_lease_release_failures_total";
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

    fn histogram(&self, name: &'static str, value: std::time::Duration) {
        tracing::debug!(
            target: "eventyr.metrics",
            metric = name,
            value_ms = value.as_secs_f64() * 1_000.0,
            "histogram"
        );
    }
}
