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
//! transport ships. No transport ships in this workspace — the trait
//! is the seam a user's own transport targets.

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
///
/// # Examples
///
/// A channel-based bus, the shape a transport sits behind: publish one
/// envelope, a subscribed receiver sees it. Push here is only a hint —
/// a projection consuming this bus still checkpoints through its
/// [`SubscriptionSource`](crate::source::SubscriptionSource).
///
/// ```
/// # fn main() {
/// use eventyr_core::envelope::{EventEnvelope, Metadata};
/// use eventyr_core::vocabulary::{Sequence, StreamId, Version};
/// use eventyr_subscription::bus::EventBus;
/// use futures::executor::block_on;
/// use std::sync::mpsc;
///
/// struct ChannelBus {
///     senders: Vec<mpsc::Sender<EventEnvelope<u64>>>,
/// }
/// impl EventBus for ChannelBus {
///     type Event = u64;
///     async fn publish(&self, envelope: &EventEnvelope<u64>)
///         -> Result<(), eventyr_core::error::StoreError> {
///         for sender in &self.senders {
///             // Best-effort: a gone receiver costs the hint, nothing else.
///             let _ = sender.send(envelope.clone());
///         }
///         Ok(())
///     }
/// }
///
/// let (tx, rx) = mpsc::channel();
/// let bus = ChannelBus { senders: vec![tx] };
/// let envelope = EventEnvelope {
///     sequence: Sequence::new(1),
///     stream_id: StreamId::from("account-1"),
///     version: Version::new(1),
///     event: 50,
///     metadata: Metadata::default(),
/// };
/// block_on(bus.publish(&envelope)).expect("publish");
/// let received = rx.recv().expect("the subscriber got the envelope");
/// assert_eq!(received.event, 50);
/// assert_eq!(received.stream_id.as_str(), "account-1");
/// # }
/// ```
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
