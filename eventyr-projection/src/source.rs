//! The raw→typed source adapter: upcast at fetch time.
//!
//! [`UpcastingSource`] adapts any
//! [`SubscriptionSource<Event = RawEvent>`](SubscriptionSource) — a
//! store that keeps `event_type` and payload raw, as the Postgres store
//! does — into a typed source by running every fetched envelope's event
//! through an [`UpcasterChain`] inside the existing `Fetch` action.
//!
//! Failure is loud by design. An upcast failure maps to
//! [`StoreError::other`], reaching the machine as
//! [`Failed`](eventyr_core::subscription::SubscriptionInput::Failed) —
//! so one poison event wedges the projection, the checkpoint never
//! moves past it, and the operator fixes the upcaster or the data and
//! rebuilds (see [`rebuild`](crate::rebuild)). A skip or dead-letter
//! mode is deliberately absent: silently dropping a stored fact is data
//! loss, and `eventyr-core`'s [`Upcaster`] contract forbids it.

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::subscription::{Batch, Checkpoint};
use eventyr_core::upcast::RawEvent;
use eventyr_subscription::source::SubscriptionSource;

use crate::chain::UpcasterChain;

/// A [`SubscriptionSource`] adapter that upcasts each fetched envelope's
/// raw event through a chain at fetch time.
///
/// No new machine, no driver changes: the upcast happens inside the
/// `Fetch` action the runner already drives, and a bad payload becomes
/// the `Failed` input the machine already handles.
pub struct UpcastingSource<S, E> {
    inner: S,
    chain: UpcasterChain<E>,
}

impl<S, E> UpcastingSource<S, E> {
    /// Wrap the raw `inner` source, upcasting through `chain`.
    pub fn new(inner: S, chain: UpcasterChain<E>) -> Self {
        Self { inner, chain }
    }

    /// The wrapped raw source.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// The chain this source upcasts through.
    pub fn chain(&self) -> &UpcasterChain<E> {
        &self.chain
    }

    /// Unwrap the raw source.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, E> UpcastingSource<S, E>
where
    S: SubscriptionSource<Event = RawEvent>,
    E: Send,
{
    /// Fetch from the raw source and map each envelope's event through
    /// the chain — the whole body of the port impl, factored out so the
    /// reference/`Arc` delegations share it instead of duplicating it.
    async fn fetch_upcasting(
        &self,
        from: Checkpoint,
        max: usize,
    ) -> Result<Batch<E>, StoreError> {
        let batch = self.inner.fetch(from, max).await?;
        let mut events = Vec::with_capacity(batch.events.len());
        for envelope in batch.events {
            // Loud, never skip (see the module docs): a poison payload
            // aborts the fetch, the batch is never applied, and the
            // checkpoint never advances past the offending event.
            let event = self.chain.upcast(envelope.event).map_err(|error| {
                StoreError::other(format!(
                    "upcasting sequence {}: {error}",
                    envelope.sequence.as_u64(),
                ))
            })?;
            events.push(EventEnvelope {
                sequence: envelope.sequence,
                stream_id: envelope.stream_id,
                version: envelope.version,
                event,
                metadata: envelope.metadata,
            });
        }
        Ok(Batch::new(events, batch.upper))
    }
}

impl<S, E> SubscriptionSource for UpcastingSource<S, E>
where
    S: SubscriptionSource<Event = RawEvent>,
    E: Send,
{
    type Event = E;

    async fn fetch(
        &self,
        from: Checkpoint,
        max: usize,
    ) -> Result<Batch<E>, StoreError> {
        self.fetch_upcasting(from, max).await
    }
}

// Sources are shared like every other port: `&UpcastingSource` and
// `Arc<UpcastingSource>` implement `SubscriptionSource` through
// `eventyr-subscription`'s own blanket delegation impls for `&S` and
// `Arc<S>` — no per-adapter delegation needed here.