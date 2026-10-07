//! The store wrapper: seal on append, open on read.

use core::future::Future;
use std::sync::Arc;

use futures::{Stream, StreamExt};

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{
    EventFilter, EventStore, FilteredRead, QueryAppend, StreamLifecycle, StreamsAll,
};

use crate::cipher::Cipher;
use crate::keys::KeyStore;
use crate::shredder::Shredder;

/// Any store, with every [`Sensitive`](crate::Sensitive) field sealed
/// before it is written and opened after it is read.
///
/// Everything above it — the write machine, the repository, projectors,
/// sagas — sees plain fields, or [`Shredded`](crate::Sensitive::Shredded)
/// ones once a subject is erased; only sealed fields reach the inner
/// store. Lifecycle operations and the commit signal pass straight
/// through.
///
/// Queries ([`QueryAppend`]) select on the stored form, so an event's
/// [`Tagged::tags`] and [`EventName`] must not depend on a sensitive
/// field — tags are identifiers, not personal data.
pub struct ShreddingStore<S, C, K> {
    inner: S,
    shredder: Arc<Shredder<C, K>>,
}

impl<S: Clone, C, K> Clone for ShreddingStore<S, C, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shredder: Arc::clone(&self.shredder),
        }
    }
}

impl<S, C: Cipher, K: KeyStore> ShreddingStore<S, C, K> {
    /// Wrap `inner`, sealing and opening through `shredder`.
    pub fn new(inner: S, shredder: Arc<Shredder<C, K>>) -> Self {
        Self { inner, shredder }
    }

    /// The wrapped store. Reading from it directly yields sealed fields.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// The shredder, for [`erase`](Shredder::erase).
    pub fn shredder(&self) -> &Shredder<C, K> {
        &self.shredder
    }
}

/// The events of `events`, sealed.
async fn seal_all<E, C, K>(
    shredder: &Shredder<C, K>,
    events: Vec<NewEvent<E>>,
) -> Result<Vec<NewEvent<E>>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned + Sync,
    C: Cipher,
    K: KeyStore,
{
    let mut sealed = Vec::with_capacity(events.len());
    for event in events {
        sealed.push(NewEvent {
            event: shredder.seal(event.event).await?,
            metadata: event.metadata,
        });
    }
    Ok(sealed)
}

async fn open_envelope<E, C, K>(
    shredder: &Shredder<C, K>,
    envelope: EventEnvelope<E>,
) -> Result<EventEnvelope<E>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned,
    C: Cipher,
    K: KeyStore,
{
    let EventEnvelope {
        sequence,
        stream_id,
        version,
        event,
        metadata,
    } = envelope;
    Ok(EventEnvelope {
        sequence,
        stream_id,
        version,
        event: shredder.open(event).await?,
        metadata,
    })
}

async fn open_committed<E, C, K>(
    shredder: &Shredder<C, K>,
    committed: Vec<CommittedStream<E>>,
) -> Result<Vec<CommittedStream<E>>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned,
    C: Cipher,
    K: KeyStore,
{
    let mut opened = Vec::with_capacity(committed.len());
    for stream in committed {
        let mut events = Vec::with_capacity(stream.events.len());
        for envelope in stream.events {
            events.push(open_envelope(shredder, envelope).await?);
        }
        opened.push(CommittedStream {
            stream_id: stream.stream_id,
            events,
        });
    }
    Ok(opened)
}

async fn seal_appends<E, C, K>(
    shredder: &Shredder<C, K>,
    appends: Vec<StreamAppend<E>>,
) -> Result<Vec<StreamAppend<E>>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned + Sync,
    C: Cipher,
    K: KeyStore,
{
    let mut sealed = Vec::with_capacity(appends.len());
    for append in appends {
        sealed.push(StreamAppend {
            stream_id: append.stream_id,
            expected: append.expected,
            events: seal_all(shredder, append.events).await?,
        });
    }
    Ok(sealed)
}

impl<S, C, K> EventStore for ShreddingStore<S, C, K>
where
    S: EventStore + Sync,
    S::Event: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    C: Cipher + 'static,
    K: KeyStore + 'static,
{
    type Event = S::Event;

    async fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<Self::Event>>,
    ) -> Result<Vec<EventEnvelope<Self::Event>>, StoreError> {
        let sealed = seal_all(&self.shredder, events).await?;
        let committed = self.inner.append(stream_id, expected, sealed).await?;
        let mut opened = Vec::with_capacity(committed.len());
        for envelope in committed {
            opened.push(open_envelope(&self.shredder, envelope).await?);
        }
        Ok(opened)
    }

    async fn append_batch(
        &self,
        appends: Vec<StreamAppend<Self::Event>>,
    ) -> Result<Vec<CommittedStream<Self::Event>>, StoreError> {
        let sealed = seal_appends(&self.shredder, appends).await?;
        let committed = self.inner.append_batch(sealed).await?;
        open_committed(&self.shredder, committed).await
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
        let shredder = Arc::clone(&self.shredder);
        self.inner.stream(stream_id, from).then(move |read| {
            let shredder = Arc::clone(&shredder);
            async move { open_envelope(&shredder, read?).await }
        })
    }
}

impl<S, C, K> StreamsAll for ShreddingStore<S, C, K>
where
    S: StreamsAll + Sync,
    S::Event: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    C: Cipher + 'static,
    K: KeyStore + 'static,
{
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
        let shredder = Arc::clone(&self.shredder);
        self.inner.stream_all(from).then(move |read| {
            let shredder = Arc::clone(&shredder);
            async move { open_envelope(&shredder, read?).await }
        })
    }

    /// Filter *before* opening (0.7.4): the stream id and the stored
    /// event name are non-sensitive by this crate's rule — neither
    /// `EventName` nor the tags may depend on a `Sensitive` field — so
    /// the filter runs on sealed envelopes and only the selected ones
    /// pay the decrypt. Without this override the default filter would
    /// open every scanned event (up to `scan_limit` per poll) to
    /// discard most of them.
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
        let shredder = Arc::clone(&self.shredder);
        let filter = filter.clone();
        async move {
            let sealed = self
                .inner
                .stream_all_filtered(from, &filter, max, scan_limit)
                .await?;
            let mut events = Vec::with_capacity(sealed.events.len());
            for envelope in sealed.events {
                events.push(open_envelope(&shredder, envelope).await?);
            }
            Ok(FilteredRead {
                events,
                scanned: sealed.scanned,
            })
        }
    }
}

impl<S, C, K> QueryAppend for ShreddingStore<S, C, K>
where
    S: QueryAppend + Sync,
    S::Event: serde::Serialize + serde::de::DeserializeOwned + EventName + Tagged + Send + Sync,
    C: Cipher + 'static,
    K: KeyStore + 'static,
{
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
        let shredder = Arc::clone(&self.shredder);
        self.inner.read(query, after).then(move |read| {
            let shredder = Arc::clone(&shredder);
            async move { open_envelope(&shredder, read?).await }
        })
    }

    async fn append_if(
        &self,
        appends: Vec<StreamAppend<Self::Event>>,
        condition: AppendCondition,
    ) -> Result<Vec<CommittedStream<Self::Event>>, StoreError> {
        let sealed = seal_appends(&self.shredder, appends).await?;
        let committed = self.inner.append_if(sealed, condition).await?;
        open_committed(&self.shredder, committed).await
    }
}

impl<S, C, K> StreamLifecycle for ShreddingStore<S, C, K>
where
    S: StreamLifecycle + Sync,
    S::Event: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    C: Cipher + 'static,
    K: KeyStore + 'static,
{
    fn close_stream(
        &self,
        stream_id: &StreamId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.inner.close_stream(stream_id)
    }

    fn truncate_before(
        &self,
        stream_id: &StreamId,
        version: Version,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.inner.truncate_before(stream_id, version)
    }
}

impl<S, C, K> eventyr_store::notify::CommitSignal for ShreddingStore<S, C, K>
where
    S: eventyr_store::notify::CommitSignal,
    C: Send + Sync,
    K: Send + Sync,
{
    type Listener = S::Listener;

    fn subscribe(&self) -> impl Future<Output = Result<Self::Listener, StoreError>> + Send {
        self.inner.subscribe()
    }
}
