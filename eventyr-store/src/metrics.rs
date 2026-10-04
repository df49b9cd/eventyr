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

pub use eventyr_core::metrics::{Metrics, NoopMetrics, names};

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
