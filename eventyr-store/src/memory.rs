//! The in-memory event store: for tests, examples, and throwaway
//! prototypes.
//!
//! [`InMemoryStore`] keeps every envelope twice — per stream and in
//! global order — behind one lock. Appends are synchronous; reads clone
//! the envelopes out. It implements both [`EventStore`] and
//! [`StreamsAll`].

use std::collections::HashMap;
use std::sync::Mutex;

use futures::Stream;
use futures::stream::iter;

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};

use crate::store::{EventStore, StreamsAll};

struct Inner<E> {
    /// Envelopes per stream, in stream order.
    streams: HashMap<StreamId, Vec<EventEnvelope<E>>>,
    /// Every envelope, in global (append) order.
    global: Vec<EventEnvelope<E>>,
}

// Manual impl: the derive would impose an undesired `E: Default` bound.
impl<E> Default for Inner<E> {
    fn default() -> Self {
        Self {
            streams: HashMap::new(),
            global: Vec::new(),
        }
    }
}

impl<E: Clone> Inner<E> {
    fn current_version(&self, stream_id: &StreamId) -> u64 {
        self.streams
            .get(stream_id)
            .map_or(0, |stream| stream.len() as u64)
    }

    /// Check the expectation against the stream and write the batch,
    /// assigning stream and global positions. Caller holds the lock;
    /// conflicts short-circuit before anything is written.
    fn append_one(
        &mut self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<E>>,
    ) -> Result<Vec<EventEnvelope<E>>, StoreError> {
        let current = self.current_version(stream_id);
        if !matches_expected(expected, current) {
            return Err(StoreError::Conflict {
                stream_id: Some(stream_id.clone()),
                current: Version::new(current),
            });
        }

        let base_sequence = self.global.len() as u64;
        let mut committed = Vec::with_capacity(events.len());
        // Split the fields so the stream vec and the global vec can be
        // written in the same loop.
        let Inner { streams, global } = self;
        let stream = streams.entry(stream_id.clone()).or_default();
        for (index, new_event) in events.into_iter().enumerate() {
            let envelope = EventEnvelope {
                sequence: Sequence::new(base_sequence + index as u64 + 1),
                stream_id: stream_id.clone(),
                version: Version::new(current + index as u64 + 1),
                event: new_event.event,
                metadata: new_event.metadata,
            };
            stream.push(envelope.clone());
            global.push(envelope.clone());
            committed.push(envelope);
        }
        Ok(committed)
    }
}

fn matches_expected(expected: ExpectedVersion, current: u64) -> bool {
    match expected {
        ExpectedVersion::Any => true,
        ExpectedVersion::Empty => current == 0,
        ExpectedVersion::Exact(version) => current == version.as_u64(),
    }
}

/// An [`EventStore`] and [`StreamsAll`] kept in process memory, behind a
/// single lock.
///
/// Optimistic concurrency works exactly as in a real store: appends are
/// checked against the stream's current version and conflict with
/// [`StoreError::Conflict`] on mismatch. Sequences are assigned in
/// append order across all streams.
///
/// The lock is only held during appends and while cloning reads out —
/// never across an await (there are none).
pub struct InMemoryStore<E> {
    inner: Mutex<Inner<E>>,
}

impl<E> InMemoryStore<E> {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl<E> Default for InMemoryStore<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> InMemoryStore<E> {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<E>> {
        // No user code runs under this lock beyond `Clone`, and a panic
        // there leaves the store consistent enough to continue.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<E> EventStore for InMemoryStore<E>
where
    E: Clone + Send,
{
    type Event = E;

    async fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<E>>,
    ) -> Result<Vec<EventEnvelope<E>>, StoreError> {
        let mut inner = self.lock();
        let stream_id = stream_id.clone();
        let mut committed = inner.append_one(&stream_id, expected, events)?;
        Ok(committed.split_off(0))
    }

    /// Append the whole batch under the one lock — trivially atomic:
    /// pass 1 checks every expectation, pass 2 writes. The global
    /// sequence is assigned in input order across the batch.
    async fn append_batch(
        &self,
        appends: Vec<StreamAppend<E>>,
    ) -> Result<Vec<CommittedStream<E>>, StoreError> {
        let mut inner = self.lock();
        // Pass 1: every expectation, before anything is written.
        for append in &appends {
            let current = inner.current_version(&append.stream_id);
            if !matches_expected(append.expected, current) {
                return Err(StoreError::Conflict {
                    stream_id: Some(append.stream_id.clone()),
                    current: Version::new(current),
                });
            }
        }
        // Pass 2: write, knowing the whole batch's expectations held.
        let mut committed = Vec::with_capacity(appends.len());
        for append in appends {
            let stream_id = append.stream_id.clone();
            let events = inner.append_one(&stream_id, append.expected, append.events)?;
            committed.push(CommittedStream { stream_id, events });
        }
        Ok(committed)
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let inner = self.lock();
        let events: Vec<EventEnvelope<E>> = inner
            .streams
            .get(stream_id)
            .map(|stream| {
                stream
                    .iter()
                    .filter(|envelope| envelope.version > from)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        iter(events.into_iter().map(Ok))
    }
}

impl<E> StreamsAll for InMemoryStore<E>
where
    E: Clone + Send,
{
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let inner = self.lock();
        let events: Vec<EventEnvelope<E>> = inner
            .global
            .iter()
            .filter(|envelope| envelope.sequence > from)
            .cloned()
            .collect();
        iter(events.into_iter().map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_core::envelope::Metadata;
    use futures::TryStreamExt;

    const ANY_STREAM: &str = "account-1";

    fn sid() -> StreamId {
        StreamId::from(ANY_STREAM)
    }

    fn new_event(event: u64) -> NewEvent<u64> {
        NewEvent::new(event)
    }

    async fn append(
        store: &InMemoryStore<u64>,
        expected: ExpectedVersion,
        events: Vec<NewEvent<u64>>,
    ) -> Result<Vec<EventEnvelope<u64>>, StoreError> {
        store.append(&sid(), expected, events).await
    }

    #[tokio::test]
    async fn append_to_empty_stream_commits_with_positions() {
        let store = InMemoryStore::new();
        let committed = append(&store, ExpectedVersion::Empty, vec![new_event(1)]).await;

        let committed = committed.expect("empty expectation must match an empty stream");
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].version, Version::new(1));
        assert_eq!(committed[0].sequence, Sequence::new(1));
        assert_eq!(committed[0].stream_id, sid());
        assert_eq!(committed[0].event, 1);
    }

    #[tokio::test]
    async fn append_violating_the_expectation_conflicts() {
        let store = InMemoryStore::new();
        append(&store, ExpectedVersion::Empty, vec![new_event(1)])
            .await
            .expect("first append succeeds");

        // `Empty` on a stream that now exists.
        let error = append(&store, ExpectedVersion::Empty, vec![new_event(2)])
            .await
            .expect_err("stream is no longer empty");
        assert!(matches!(
            error,
            StoreError::Conflict { current, .. } if current == Version::new(1)
        ));

        // `Exact` with the wrong version.
        let error = append(
            &store,
            ExpectedVersion::Exact(Version::new(5)),
            vec![new_event(2)],
        )
        .await
        .expect_err("version does not match");
        assert!(matches!(
            error,
            StoreError::Conflict { current, .. } if current == Version::new(1)
        ));

        // `Exact` with the right version, and `Any`, both succeed.
        append(
            &store,
            ExpectedVersion::Exact(Version::new(1)),
            vec![new_event(2)],
        )
        .await
        .expect("exact expectation matches");
        append(&store, ExpectedVersion::Any, vec![new_event(3)])
            .await
            .expect("any expectation always matches");
    }

    #[tokio::test]
    async fn append_is_all_or_nothing_on_conflict() {
        let store = InMemoryStore::new();
        append(&store, ExpectedVersion::Empty, vec![new_event(1)])
            .await
            .expect("first append succeeds");

        // A conflicting batch of two must not commit anything.
        append(
            &store,
            ExpectedVersion::Empty,
            vec![new_event(2), new_event(3)],
        )
        .await
        .expect_err("conflict");

        let events: Vec<_> = store
            .stream(&sid(), Version::EMPTY)
            .try_collect()
            .await
            .expect("stream read");
        assert_eq!(events.len(), 1, "no partial batch may be committed");
    }

    #[tokio::test]
    async fn stream_reads_from_exclusive_bound() {
        let store = InMemoryStore::new();
        append(
            &store,
            ExpectedVersion::Any,
            vec![new_event(1), new_event(2), new_event(3)],
        )
        .await
        .expect("append");

        let all: Vec<_> = store
            .stream(&sid(), Version::EMPTY)
            .try_collect()
            .await
            .expect("from the beginning");
        assert_eq!(all.len(), 3);

        let tail: Vec<_> = store
            .stream(&sid(), Version::new(1))
            .try_collect()
            .await
            .expect("from after version 1");
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].version, Version::new(2));
    }

    #[tokio::test]
    async fn stream_of_unknown_stream_is_empty() {
        let store: InMemoryStore<u64> = InMemoryStore::new();
        let events: Vec<EventEnvelope<u64>> = store
            .stream(&StreamId::from("nobody-1"), Version::EMPTY)
            .try_collect()
            .await
            .expect("unknown streams are empty, not errors");
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn stream_all_orders_globally_across_streams() {
        let store = InMemoryStore::new();
        let a = StreamId::from("a-1");
        let b = StreamId::from("b-1");

        store
            .append(&a, ExpectedVersion::Empty, vec![new_event(1)])
            .await
            .unwrap();
        store
            .append(&b, ExpectedVersion::Empty, vec![new_event(2)])
            .await
            .unwrap();
        store
            .append(
                &a,
                ExpectedVersion::Exact(Version::new(1)),
                vec![new_event(3)],
            )
            .await
            .unwrap();

        let all: Vec<_> = store
            .stream_all(Sequence::START)
            .try_collect()
            .await
            .expect("global stream");
        assert_eq!(
            all.iter()
                .map(|e| (e.sequence, e.event))
                .collect::<Vec<_>>(),
            vec![
                (Sequence::new(1), 1),
                (Sequence::new(2), 2),
                (Sequence::new(3), 3)
            ]
        );

        let tail: Vec<_> = store
            .stream_all(Sequence::new(1))
            .try_collect()
            .await
            .expect("from after sequence 1");
        assert_eq!(tail.len(), 2);
    }

    #[tokio::test]
    async fn metadata_travels_from_new_event_to_envelope() {
        let store = InMemoryStore::new();
        let mut event = NewEvent::new(1);
        event.metadata = Metadata {
            causation_id: Some("cmd-42".into()),
            correlation_id: Some("corr-7".into()),
            ..Default::default()
        };

        store
            .append(&sid(), ExpectedVersion::Empty, vec![event])
            .await
            .expect("append");

        let events: Vec<_> = store
            .stream(&sid(), Version::EMPTY)
            .try_collect()
            .await
            .expect("stream read");
        assert_eq!(events[0].metadata.causation_id.as_deref(), Some("cmd-42"));
        assert_eq!(events[0].metadata.correlation_id.as_deref(), Some("corr-7"));
    }
}
