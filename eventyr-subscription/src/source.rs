//! The `SubscriptionSource` port and the §6 adapter over any
//! [`StreamsAll`] store.
//!
//! [`SubscriptionSource`] is the object-safe, `Future`-returning
//! counterpart to [`StreamsAll`]'s stream: it answers a single bounded
//! poll, which is exactly the machine's
//! [`Fetch`](eventyr_core::subscription::SubscriptionAction::Fetch)
//! action. A [`StoreSubscription`] adapts any [`StreamsAll`] store,
//! truncating `stream_all` after `max` events.

use core::future::Future;

use futures::{StreamExt, TryStreamExt};

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::subscription::{Batch, Checkpoint};
use eventyr_store::store::{EventFilter, StreamsAll};

/// A pollable event source: the store-facing half of a subscription.
///
/// Answers one bounded poll — "give me up to `max` events after
/// `from`" — instead of conveying a live stream, because the machine
/// treats a poll's answer as data ([`Fetched`](eventyr_core::subscription::SubscriptionInput::Fetched))
/// and owns its own idle/backoff timing. A push-capable source (a bus)
/// implements the same port by buffering.
pub trait SubscriptionSource: Send + Sync {
    /// The domain event type the source delivers.
    type Event: Send;

    /// Read up to `max` events with sequence > `from`, in global
    /// sequence order, as a [`Batch`]. Fewer than `max` events — down
    /// to zero — means "caught up". Contiguity is not required (see
    /// [`Batch`]): identity-column sequences may carry permanent gaps,
    /// and a poll must return what is durably visible now — the machine
    /// skips gaps and acks only to the last delivered sequence. What is
    /// required is strictly increasing sequences strictly after `from`,
    /// and that `upper` is exactly the last delivered sequence.
    fn fetch(
        &self,
        from: Checkpoint,
        max: usize,
    ) -> impl Future<Output = Result<Batch<Self::Event>, StoreError>> + Send;
}

/// The §6 adapter: subscriptions over any [`StreamsAll`] store.
///
/// Polls run `stream_all(from)..take(max)`, so catch-up over the in-memory
/// store, the Postgres store, or any future `StreamsAll` implementation
/// needs no store-specific code.
pub struct StoreSubscription<S> {
    store: S,
}

impl<S> StoreSubscription<S> {
    /// Wrap a store as a subscription source.
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// The wrapped store.
    pub fn into_inner(self) -> S {
        self.store
    }
}

/// Errors streaming past the returned batch are dropped with it: the
/// next poll re-reads from the last ack, under the at-least-once
/// contract. Gaps in the upstream stream pass through untouched — the
/// `StreamsAll` contract (see its docs) makes a gap permanently absent,
/// and the machine skips it; only ordering violations break the
/// protocol.
impl<S: StreamsAll> SubscriptionSource for StoreSubscription<S>
where
    S: Send + Sync,
    S::Event: Send,
{
    type Event = S::Event;

    async fn fetch(&self, from: Checkpoint, max: usize) -> Result<Batch<Self::Event>, StoreError> {
        let events: Vec<EventEnvelope<Self::Event>> = self
            .store
            .stream_all(from.as_sequence())
            .take(max)
            .try_collect()
            .await?;
        let upper = events.last().map(|e| Checkpoint::new(e.sequence));
        Ok(Batch::new(events, upper))
    }
}

/// A filtered subscription source (0.7.4): only the events `filter`
/// selects, with the read's scan progress reported so the checkpoint
/// moves past events the filter skipped.
///
/// Without the scan bound a projection over a sparse filter — one
/// account's events in a busy log — checkpoints only at its own
/// matches, and every restart re-reads everything since the last one.
/// With it, each poll acks how far it looked.
pub struct FilteredSubscription<S> {
    store: S,
    filter: EventFilter,
    scan_limit: usize,
}

impl<S> FilteredSubscription<S> {
    /// How many events one poll looks at, at most, by default.
    pub const DEFAULT_SCAN_LIMIT: usize = 4096;

    /// A source over `store` delivering what `filter` selects.
    pub fn new(store: S, filter: EventFilter) -> Self {
        Self {
            store,
            filter,
            scan_limit: Self::DEFAULT_SCAN_LIMIT,
        }
    }

    /// Builder-style: look at no more than `scan_limit` events per
    /// poll (at least 1). A poll that finds nothing still reports its
    /// progress, so the bound trades poll cost against polls per
    /// unmatched run.
    pub fn scan_limit(mut self, scan_limit: usize) -> Self {
        self.scan_limit = scan_limit.max(1);
        self
    }

    /// The wrapped store.
    pub fn into_inner(self) -> S {
        self.store
    }
}

impl<S> SubscriptionSource for FilteredSubscription<S>
where
    S: StreamsAll + Send + Sync,
    S::Event: Send + EventName,
{
    type Event = S::Event;

    async fn fetch(&self, from: Checkpoint, max: usize) -> Result<Batch<Self::Event>, StoreError> {
        let read = self
            .store
            .stream_all_filtered(from.as_sequence(), &self.filter, max, self.scan_limit)
            .await?;
        let upper = read.events.last().map(|e| Checkpoint::new(e.sequence));
        Ok(Batch::new(read.events, upper).scanned_to(Checkpoint::new(read.scanned)))
    }
}

// Blanket impls: sources are shared (references, `Arc`), and the ports
// must work through the smart pointer the caller chose — the same
// delegation pattern the store ports use.

macro_rules! impl_source_delegation {
    ($pointer:ty) => {
        impl<S: SubscriptionSource + ?Sized> SubscriptionSource for $pointer {
            type Event = S::Event;

            fn fetch(
                &self,
                from: Checkpoint,
                max: usize,
            ) -> impl Future<Output = Result<Batch<Self::Event>, StoreError>> + Send {
                (**self).fetch(from, max)
            }
        }
    };
}

impl_source_delegation!(&S);
impl_source_delegation!(std::sync::Arc<S>);
