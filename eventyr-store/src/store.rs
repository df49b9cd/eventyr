//! The persistence ports.
//!
//! [`EventStore`] is the aggregate-persistence port every store implements;
//! [`StreamsAll`] adds the global ordered stream that projections and
//! subscriptions require. A store that cannot provide a global stream can
//! still implement `EventStore` — the split keeps honesty.

use core::future::Future;

use futures::Stream;

use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};

/// An append-only, ordered event log, readable per stream.
///
/// Implementations are shared (`&self`, internal synchronization is the
/// store's concern) and may serve concurrent appends to different
/// streams. [`append`](EventStore::append) is transactional: all events
/// or none, guarded by the [`ExpectedVersion`] optimistic-concurrency
/// expectation — a violation is reported as
/// [`StoreError::Conflict`].
pub trait EventStore {
    /// The domain event type this store persists.
    type Event: Send;

    /// Append `events` to the stream, guarded by `expected`.
    ///
    /// Returns the committed envelopes as the store recorded them —
    /// with sequence, stream id, and version assigned.
    fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<Self::Event>>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<Self::Event>>, StoreError>> + Send;

    /// Stream the events of one stream, from `from` (exclusive) onward.
    ///
    /// The stream is the async boundary: the method itself is
    /// synchronous (it only constructs the stream), and the store does
    /// its I/O as the stream is polled. An unknown stream yields an
    /// empty stream, not an error.
    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send;
}

/// A store that can also read the global, ordered event stream — the
/// projection and subscription backbone.
pub trait StreamsAll: EventStore {
    /// Stream all events across streams, ordered by global sequence,
    /// from `from` (exclusive) onward.
    ///
    /// Sequences are strictly increasing but need not be contiguous:
    /// stores backing the global sequence with an identity column
    /// (e.g. Postgres `BIGSERIAL`) burn values on rolled-back appends,
    /// so gaps are permanent and consumers (the subscription machine)
    /// skip them. A store *must not* emit a later sequence before an
    /// earlier one it will eventually deliver is durably visible — a
    /// skipped gap must be a gap forever — so reads that race in-flight
    /// appends must exclude uncommitted rows (the default
    /// read-committed snapshot does).
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send;
}

// Blanket impls: stores are shared (`Arc`, references), and the ports
// must work through the smart pointer the caller chose. One macro
// generates the delegation for each pointer type.

macro_rules! impl_port_delegation {
    ($pointer:ty) => {
        impl<S: EventStore + ?Sized> EventStore for $pointer {
            type Event = S::Event;

            fn append(
                &self,
                stream_id: &StreamId,
                expected: ExpectedVersion,
                events: Vec<NewEvent<Self::Event>>,
            ) -> impl Future<Output = Result<Vec<EventEnvelope<Self::Event>>, StoreError>> + Send
            {
                (**self).append(stream_id, expected, events)
            }

            fn stream(
                &self,
                stream_id: &StreamId,
                from: Version,
            ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
                (**self).stream(stream_id, from)
            }
        }

        impl<S: StreamsAll + ?Sized> StreamsAll for $pointer {
            fn stream_all(
                &self,
                from: Sequence,
            ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
                (**self).stream_all(from)
            }
        }
    };
}

impl_port_delegation!(&S);
impl_port_delegation!(std::sync::Arc<S>);
