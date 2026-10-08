//! `EventBus`: the pub/sub port for committed envelopes, per §2's
//! non-goal "EventBus as a trait users implement" — defined here,
//! with no transport baked in.
//!
//! This is the 0.3 seam, behind `cfg(feature = "bus")`: a live push of
//! the same envelopes the catch-up subscription polls. Spec tension,
//! reconciled: §6's poll/ack `Subscription` shape is pull-based (the
//! runner talks to [`SubscriptionSource`](crate::source::SubscriptionSource)),
//! while this trait is push; both deliver [`EventEnvelope`]s both
//! times, and a bridging adapter is where the two meet once a
//! transport ships. Until 0.3 the trait exists so user projections can
//! already target the spec's signature.

use core::future::Future;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::subscription_machine::Checkpoint;

/// Live push of committed envelopes: "new events are arriving" as a
/// subscription, so a projector can skip the idle poll when the store
/// (or something in front of it) already knows.
///
/// Publish only persists-to-committed envelopes, and on best-effort
/// terms: the projector's checkpoint poll stays authoritative — a lost
/// notification only costs an idle sleep.
pub trait EventBus: Send + Sync {
    /// The domain event type published.
    type Event: Send;

    /// Deliver `envelope` to subscribers. Implementations return
    /// immediately (fan-out itself is async where the transport wants
    /// it) — this method's `Future` is for the envelope's own
    /// transmit-if-possible.
    fn publish(
        &self,
        envelope: &EventEnvelope<Self::Event>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// §6 verbatim: a checkpoint-preserving push subscription.
///
/// `poll` answers "what arrived since `checkpoint`", at most until
/// caught up; `ack` persists the answer's upper bound. This is the
/// shape a live-bus-backed subscription exposes; the catch-up runner
/// prefers the pull shape ([`SubscriptionSource`](crate::source::SubscriptionSource))
/// because it owns polling cadence itself.
pub trait Subscription: Send + Sync {
    /// The domain event type the subscription delivers.
    type Event: Send;

    /// Poll for events after `checkpoint`.
    fn poll(
        &mut self,
        checkpoint: Checkpoint,
    ) -> impl Future<
        Output = Result<eventyr_core::subscription_machine::Batch<Self::Event>, StoreError>,
    > + Send;

    /// Persist that every event through `checkpoint` applied.
    fn ack(
        &mut self,
        checkpoint: Checkpoint,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}
