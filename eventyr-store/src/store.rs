//! The persistence ports.
//!
//! [`EventStore`] is the aggregate-persistence port every store implements;
//! [`StreamsAll`] adds the global ordered stream that projections and
//! subscriptions require. A store that cannot provide a global stream can
//! still implement `EventStore` — the split keeps honesty.
//! [`QueryAppend`] is the opt-in port for dynamic consistency
//! boundaries (0.7.1): read by query, append under a query condition.

use core::future::Future;
use std::vec::Vec;

use futures::Stream;

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
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

    /// Append a multi-stream batch atomically: every append or none,
    /// each guarded by its own
    /// [`ExpectedVersion`]
    /// expectation (a violation on any stream is reported as
    /// [`StoreError::Conflict`] carrying that stream's id and version).
    ///
    /// A stream appears at most once per batch: every expectation is
    /// checked against the state before the batch, so a second entry for
    /// the same stream has no well-defined base version. A batch naming a
    /// stream twice fails [`StoreError::Other`] before anything is
    /// written ([`validate_batch`], which every store calls first); merge
    /// the stream's events into one [`StreamAppend`] instead. The same
    /// rule holds for [`QueryAppend::append_if`].
    ///
    /// This is the port the [`BatchMachine`](eventyr_core::batch::BatchMachine)
    /// drives — the write machine's one-stream [`append`](EventStore::append)
    /// generalized to a fixed set. Stores that cannot commit atomically
    /// across streams implement it with
    /// [`append_batch_fallback`], which handles the degenerate cases
    /// (an empty batch commits nothing; a single-stream batch delegates
    /// to [`append`](EventStore::append)) and refuses the rest with
    /// [`StoreError::Other`] — a store that cannot commit atomically
    /// across streams says so rather than pretending.
    fn append_batch(
        &self,
        appends: Vec<StreamAppend<Self::Event>>,
    ) -> impl Future<Output = Result<Vec<CommittedStream<Self::Event>>, StoreError>> + Send;

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

    /// Read up to `max` events after `from` that `filter` selects, and
    /// report how far the read scanned (0.7.4).
    ///
    /// The [`FilteredRead`]'s `scanned` is the highest sequence the read
    /// looked at — delivered, filtered out, or skipped as a gap — so a
    /// subscription over a sparse filter can checkpoint past long
    /// unmatched runs. The ordering and visibility rules of
    /// [`stream_all`](Self::stream_all) apply to it too: nothing below
    /// `scanned` may become visible later.
    ///
    /// The default filters on the client, over
    /// [`stream_all`](Self::stream_all), looking at no more than
    /// `scan_limit` events, so a read over a sparse filter returns
    /// progress instead of walking the whole log. Stores that can
    /// filter in the database override it.
    fn stream_all_filtered(
        &self,
        from: Sequence,
        filter: &EventFilter,
        max: usize,
        scan_limit: usize,
    ) -> impl Future<Output = Result<FilteredRead<Self::Event>, StoreError>> + Send
    where
        Self::Event: EventName,
    {
        let filter = filter.clone();
        let all = self.stream_all(from);
        async move {
            futures::pin_mut!(all);
            let mut events = Vec::new();
            let mut scanned = from;
            let mut looked = 0usize;
            while events.len() < max && looked < scan_limit {
                let Some(envelope) = futures::StreamExt::next(&mut all).await else {
                    break;
                };
                let envelope = envelope?;
                looked += 1;
                scanned = envelope.sequence;
                if filter.selects(&envelope) {
                    events.push(envelope);
                }
            }
            Ok(FilteredRead { events, scanned })
        }
    }
}

/// Which events a filtered global read delivers (0.7.4): those whose
/// stream id starts with one of `stream_prefixes` (any stream when
/// empty) *and* whose stored name is one of `event_types` (any type
/// when empty).
///
/// The two server-side filters KurrentDB offers, minus regular
/// expressions: prefixes and names index well, and a projection that
/// needs more selects on the client.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventFilter {
    /// Accepted stream-id prefixes; empty accepts every stream.
    pub stream_prefixes: Vec<String>,
    /// Accepted stored event names ([`EventName`]); empty accepts every
    /// type.
    pub event_types: Vec<String>,
}

impl EventFilter {
    /// The filter that selects every event.
    pub fn all() -> Self {
        Self::default()
    }

    /// Builder-style: accept streams starting with `prefix` (adding to
    /// any prefixes already accepted).
    pub fn stream_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.stream_prefixes.push(prefix.into());
        self
    }

    /// Builder-style: accept events with these stored names (adding to
    /// any already accepted).
    pub fn event_types<I, T>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.event_types.extend(types.into_iter().map(Into::into));
        self
    }

    /// Whether the filter accepts every event.
    pub fn is_all(&self) -> bool {
        self.stream_prefixes.is_empty() && self.event_types.is_empty()
    }

    /// Whether an event of `event_type` on `stream_id` passes.
    pub fn matches(&self, stream_id: &str, event_type: &str) -> bool {
        (self.stream_prefixes.is_empty()
            || self
                .stream_prefixes
                .iter()
                .any(|prefix| stream_id.starts_with(prefix.as_str())))
            && (self.event_types.is_empty()
                || self.event_types.iter().any(|name| name == event_type))
    }

    /// Whether `envelope` passes.
    pub fn selects<E: EventName>(&self, envelope: &EventEnvelope<E>) -> bool {
        self.matches(envelope.stream_id.as_str(), envelope.event.event_name())
    }
}

/// The answer to [`StreamsAll::stream_all_filtered`]: the selected
/// events, and how far the read scanned.
#[derive(Clone, Debug)]
pub struct FilteredRead<E> {
    /// The selected events, in global order.
    pub events: Vec<EventEnvelope<E>>,
    /// The highest sequence the read looked at (the read's `from` when
    /// it looked at nothing). At least the last event's sequence.
    pub scanned: Sequence,
}

/// A store that can end a stream's life (0.7.6): close it to further
/// appends, or drop the oldest part of its history.
///
/// Neither operation lets a decision see the wrong state. The write
/// protocol decides against the stream's whole folded history, so:
///
/// - [`close_stream`](Self::close_stream) is a tombstone, not a soft
///   delete. A deleted stream that later accepted appends would fold an
///   empty history and then append at version *N + 1*, a decision taken
///   against state it never saw. A closed stream refuses appends with
///   [`StoreError::StreamClosed`] instead, and its history stays
///   readable for projections and audits.
/// - [`truncate_before`](Self::truncate_before) drops events below a
///   version, and a read that starts before the cut fails with
///   [`StoreError::Truncated`] rather than folding a partial history.
///   Truncate only below a snapshot the readers start from, and only
///   after every projection that needs the dropped events has passed
///   them.
///
/// Both are permanent. Both reach the global stream as an ordinary
/// event the caller appends first (a `Closed` or `Archived` variant of
/// the domain's own enum), so projections see a marker rather than a
/// silent gap; the port does not invent one, since a store cannot
/// construct a domain event.
pub trait StreamLifecycle: EventStore {
    /// Refuse every later append to `stream_id`. Idempotent. Closing a
    /// stream that has no events is allowed and reserves the name.
    fn close_stream(
        &self,
        stream_id: &StreamId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Delete the events of `stream_id` below `version`, keeping
    /// `version` and everything after it. A later read whose exclusive
    /// lower bound lies below `version - 1` fails with
    /// [`StoreError::Truncated`]. Truncating to a version at or below
    /// the current cut is a no-op; past the stream's end it is an error.
    /// The global stream loses the same events.
    fn truncate_before(
        &self,
        stream_id: &StreamId,
        version: Version,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// A store that can serve dynamic consistency boundaries (0.7.1): read
/// the events a [`Query`] selects, and append guarded by an
/// [`AppendCondition`] instead of (only) a stream version.
///
/// Opt-in, like [`StreamsAll`]: the store must index each event's
/// stored name ([`EventName`]) and tags ([`Tagged`]) at append time —
/// both are pure functions of the payload, so nothing new travels on
/// the envelope.
///
/// The condition check and the write are one atomic step. Two appends
/// whose conditions overlap must serialize: whichever commits second
/// sees the first's events and fails. A store that cannot make that
/// guarantee must not implement this trait.
pub trait QueryAppend: StreamsAll
where
    Self::Event: EventName + Tagged,
{
    /// Stream every event `query` selects, committed after `after`
    /// (exclusive), in global sequence order.
    ///
    /// The ordering and visibility rules of
    /// [`stream_all`](StreamsAll::stream_all) apply.
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send;

    /// Append the per-stream batches atomically if no event selected by
    /// `condition.query` was committed after `condition.after`.
    ///
    /// A failed condition is [`StoreError::QueryConflict`] carrying the
    /// highest matching sequence, and nothing is written. Each
    /// [`StreamAppend`]'s own expectation is still checked
    /// ([`StoreError::Conflict`] on violation) — the boundary machine
    /// emits [`Any`](ExpectedVersion::Any), but the port does not
    /// assume it. As for [`append_batch`](EventStore::append_batch), a
    /// stream appears at most once ([`validate_batch`]).
    fn append_if(
        &self,
        appends: Vec<StreamAppend<Self::Event>>,
        condition: AppendCondition,
    ) -> impl Future<Output = Result<Vec<CommittedStream<Self::Event>>, StoreError>> + Send;
}

/// The optimistic-concurrency check, once: whether a stream at
/// `current` (0 = absent) satisfies the [`ExpectedVersion`]. Every store
/// answers the same question — the port ships the rule so no store
/// re-derives it.
pub fn expected_version_matches(expected: ExpectedVersion, current: u64) -> bool {
    match expected {
        ExpectedVersion::Any => true,
        ExpectedVersion::Empty => current == 0,
        ExpectedVersion::Exact(version) => current == version.as_u64(),
    }
}

/// Refuse a batch that names a stream more than once — the rule
/// [`EventStore::append_batch`] and [`QueryAppend::append_if`] document.
///
/// Every store calls it before touching storage, so the refusal is the
/// same everywhere and nothing is written. An empty batch and a batch of
/// distinct streams pass.
pub fn validate_batch<E>(appends: &[StreamAppend<E>]) -> Result<(), StoreError> {
    let mut seen: Vec<&StreamId> = appends.iter().map(|append| &append.stream_id).collect();
    seen.sort_unstable();
    match seen.windows(2).find(|pair| pair[0] == pair[1]) {
        Some(pair) => Err(StoreError::other(format!(
            "stream {} appears more than once in one batch; merge its events into one append",
            pair[0]
        ))),
        None => Ok(()),
    }
}

/// The truncated-read rule (0.7.6), once: a read of `stream_id` from
/// `from` (exclusive) fails [`StoreError::Truncated`] when it would start
/// before `first_kept`, the stream's first remaining version (1 for a
/// stream never truncated). A read that starts at the cut or later is
/// served.
pub fn read_starts_before_cut(
    stream_id: &StreamId,
    from: Version,
    first_kept: u64,
) -> Result<(), StoreError> {
    if from.as_u64().saturating_add(1) < first_kept {
        return Err(StoreError::Truncated {
            stream_id: stream_id.clone(),
            first: Version::new(first_kept),
        });
    }
    Ok(())
}

/// What [`StreamLifecycle::truncate_before`] must do, as decided by
/// [`plan_truncate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TruncatePlan {
    /// The cut is at or below the current one: nothing to do.
    Noop,
    /// Delete the events below this version and record it as the
    /// stream's first kept version.
    Cut(u64),
}

/// The truncation bounds (0.7.6), once: for a stream at `head` whose
/// first kept version is `first_kept` (1 if never truncated), truncating
/// before `cut` is
///
/// - an error past `head + 1` — there is nothing there to keep;
/// - a no-op at or below `first_kept` — truncation never un-truncates;
/// - otherwise a cut at `cut`.
pub fn plan_truncate(
    stream_id: &StreamId,
    head: u64,
    first_kept: u64,
    cut: Version,
) -> Result<TruncatePlan, StoreError> {
    let at = cut.as_u64();
    if at > head.saturating_add(1) {
        return Err(StoreError::other(format!(
            "cannot truncate {stream_id} before {cut}: it ends at {head}"
        )));
    }
    if at <= first_kept {
        return Ok(TruncatePlan::Noop);
    }
    Ok(TruncatePlan::Cut(at))
}

/// A version or sequence as a SQL `BIGINT`, saturating at `i64::MAX`.
///
/// Positions are `u64` in the protocol and signed 64-bit in SQL. A real
/// log never gets past `i64::MAX`, so the only values that saturate are
/// bounds a caller passed in (`stream_all(Sequence::new(u64::MAX))`),
/// and saturating keeps them meaning "after everything" where a wrapping
/// cast would turn them into -1, "before everything".
pub fn sql_position(position: u64) -> i64 {
    i64::try_from(position).unwrap_or(i64::MAX)
}

/// Keep what `query` selects (and every error, so a corrupt row is never
/// silently skipped). Tags are a pure function of the payload, so a tag
/// column written at append time would be wrong for every event stored
/// before it existed; matching the decoded event is correct on any
/// history. The stores that prefilter by event type call this on the
/// rows the prefilter let through.
pub fn selected<E: EventName + Tagged>(
    query: &Query,
    result: &Result<EventEnvelope<E>, StoreError>,
) -> bool {
    match result {
        Ok(envelope) => query.selects(&envelope.event),
        Err(_) => true,
    }
}

/// The events of every append, in commit order, for the inline views.
pub fn all_events<E: Clone>(committed: &[CommittedStream<E>]) -> Vec<EventEnvelope<E>> {
    committed
        .iter()
        .flat_map(|stream| stream.events.iter().cloned())
        .collect()
}

/// The fallback [`EventStore::append_batch`] for stores that cannot
/// commit atomically across streams: an empty batch commits nothing, a
/// single-stream batch delegates to [`append`](EventStore::append), and
/// a multi-stream batch fails [`StoreError::Other`] — a store that
/// cannot commit atomically across streams says so rather than
/// pretending.
pub async fn append_batch_fallback<S: EventStore + ?Sized>(
    store: &S,
    appends: Vec<StreamAppend<S::Event>>,
) -> Result<Vec<CommittedStream<S::Event>>, StoreError> {
    validate_batch(&appends)?;
    let mut appends = appends;
    match appends.len() {
        0 => Ok(Vec::new()),
        1 => {
            let append = appends.remove(0);
            let events = store
                .append(&append.stream_id, append.expected, append.events)
                .await?;
            Ok(Vec::from([CommittedStream {
                stream_id: append.stream_id,
                events,
            }]))
        }
        _ => Err(StoreError::other(
            "this store cannot commit atomically across multiple streams",
        )),
    }
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

            fn append_batch(
                &self,
                appends: Vec<StreamAppend<Self::Event>>,
            ) -> impl Future<Output = Result<Vec<CommittedStream<Self::Event>>, StoreError>> + Send
            {
                (**self).append_batch(appends)
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

            fn stream_all_filtered(
                &self,
                from: Sequence,
                filter: &EventFilter,
                max: usize,
                scan_limit: usize,
            ) -> impl Future<Output = Result<FilteredRead<Self::Event>, StoreError>> + Send
            where
                Self::Event: EventName,
            {
                (**self).stream_all_filtered(from, filter, max, scan_limit)
            }
        }
    };
}

macro_rules! impl_lifecycle_delegation {
    ($pointer:ty) => {
        impl<S: StreamLifecycle + ?Sized> StreamLifecycle for $pointer {
            fn close_stream(
                &self,
                stream_id: &StreamId,
            ) -> impl Future<Output = Result<(), StoreError>> + Send {
                (**self).close_stream(stream_id)
            }

            fn truncate_before(
                &self,
                stream_id: &StreamId,
                version: Version,
            ) -> impl Future<Output = Result<(), StoreError>> + Send {
                (**self).truncate_before(stream_id, version)
            }
        }
    };
}

impl_lifecycle_delegation!(&S);
impl_lifecycle_delegation!(std::sync::Arc<S>);

macro_rules! impl_query_delegation {
    ($pointer:ty) => {
        impl<S: QueryAppend + ?Sized> QueryAppend for $pointer
        where
            S::Event: EventName + Tagged,
        {
            fn read(
                &self,
                query: &Query,
                after: Sequence,
            ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
                (**self).read(query, after)
            }

            fn append_if(
                &self,
                appends: Vec<StreamAppend<Self::Event>>,
                condition: AppendCondition,
            ) -> impl Future<Output = Result<Vec<CommittedStream<Self::Event>>, StoreError>> + Send
            {
                (**self).append_if(appends, condition)
            }
        }
    };
}

impl_query_delegation!(&S);
impl_query_delegation!(std::sync::Arc<S>);

impl_port_delegation!(&S);
impl_port_delegation!(std::sync::Arc<S>);
