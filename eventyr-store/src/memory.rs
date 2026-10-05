//! The in-memory event store: for tests, examples, and throwaway
//! prototypes.
//!
//! [`InMemoryStore`] keeps every envelope twice — per stream and in
//! global order — behind one lock. Appends are synchronous; reads clone
//! the envelopes out. It implements [`EventStore`], [`StreamsAll`],
//! and — for events that are [`EventName`] + [`Tagged`] —
//! [`QueryAppend`], matching queries by scanning the global log under
//! the same lock that serializes appends.

use std::collections::HashMap;
use std::sync::Mutex;

use futures::Stream;
use futures::stream::iter;

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};

use crate::notify::{CommitSignal, LocalCommitListener, LocalCommitSignal};
use crate::store::{
    EventStore, QueryAppend, StreamLifecycle, StreamsAll, TruncatePlan, plan_truncate,
    read_starts_before_cut, validate_batch,
};

struct Inner<E> {
    /// Envelopes per stream, in stream order. After a truncation the
    /// vec starts at the cut, so a stream's version is its head, not
    /// the vec's length.
    streams: HashMap<StreamId, Vec<EventEnvelope<E>>>,
    /// Every envelope, in global (append) order.
    global: Vec<EventEnvelope<E>>,
    /// The highest sequence ever assigned. Not `global.len()`: a
    /// truncation removes envelopes but never reuses their sequences.
    next_sequence: u64,
    /// Each stream's version — its last event's, kept across truncation.
    heads: HashMap<StreamId, u64>,
    /// Streams closed to appends (0.7.6).
    closed: std::collections::HashSet<StreamId>,
    /// The first kept version of each truncated stream (0.7.6).
    cuts: HashMap<StreamId, u64>,
}

// Manual impl: the derive would impose an undesired `E: Default` bound.
impl<E> Default for Inner<E> {
    fn default() -> Self {
        Self {
            streams: HashMap::new(),
            global: Vec::new(),
            next_sequence: 0,
            heads: HashMap::new(),
            closed: std::collections::HashSet::new(),
            cuts: HashMap::new(),
        }
    }
}

impl<E: Clone> Inner<E> {
    fn current_version(&self, stream_id: &StreamId) -> u64 {
        self.heads.get(stream_id).copied().unwrap_or(0)
    }

    fn check_open(&self, stream_id: &StreamId) -> Result<(), StoreError> {
        if self.closed.contains(stream_id) {
            return Err(StoreError::StreamClosed {
                stream_id: stream_id.clone(),
            });
        }
        Ok(())
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
        self.check_open(stream_id)?;
        let current = self.current_version(stream_id);
        if !crate::store::expected_version_matches(expected, current) {
            return Err(StoreError::Conflict {
                stream_id: Some(stream_id.clone()),
                current: Version::new(current),
            });
        }

        let base_sequence = self.next_sequence;
        self.next_sequence += events.len() as u64;
        if !events.is_empty() {
            self.heads
                .insert(stream_id.clone(), current + events.len() as u64);
        }
        let mut committed = Vec::with_capacity(events.len());
        // Split the fields so the stream vec and the global vec can be
        // written in the same loop.
        let Inner {
            streams, global, ..
        } = self;
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

    /// Check every stream expectation, then write: the body of
    /// `append_batch`, shared with `append_if`. Caller holds the lock and
    /// has already refused a batch naming a stream twice.
    fn append_all(
        &mut self,
        appends: Vec<StreamAppend<E>>,
    ) -> Result<Vec<CommittedStream<E>>, StoreError> {
        // Pass 1: every expectation, before anything is written.
        for append in &appends {
            self.check_open(&append.stream_id)?;
            let current = self.current_version(&append.stream_id);
            if !crate::store::expected_version_matches(append.expected, current) {
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
            let events = self.append_one(&stream_id, append.expected, append.events)?;
            committed.push(CommittedStream { stream_id, events });
        }
        Ok(committed)
    }
}

impl<E: EventName + Tagged> Inner<E> {
    fn matching<'a>(
        &'a self,
        query: &'a Query,
        after: Sequence,
    ) -> impl Iterator<Item = &'a EventEnvelope<E>> + 'a {
        self.global
            .iter()
            .filter(move |envelope| envelope.sequence > after && query.selects(&envelope.event))
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
    /// Raised after every commit (0.7.2).
    signal: LocalCommitSignal,
}

impl<E> InMemoryStore<E> {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            signal: LocalCommitSignal::new(),
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

    /// Raise the commit signal after a successful, non-empty write —
    /// outside the lock, once the write is visible to reads.
    fn committed<T>(&self, result: Result<T, StoreError>, wrote: bool) -> Result<T, StoreError> {
        if result.is_ok() && wrote {
            self.signal.notify();
        }
        result
    }
}

impl<E> CommitSignal for InMemoryStore<E>
where
    E: Send,
{
    type Listener = LocalCommitListener;

    fn subscribe(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Listener, StoreError>> + Send {
        self.signal.subscribe()
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
        let wrote = !events.is_empty();
        let result = self.lock().append_one(&stream_id.clone(), expected, events);
        self.committed(result, wrote)
    }

    /// Append the whole batch under the one lock — trivially atomic:
    /// pass 1 checks every expectation, pass 2 writes. The global
    /// sequence is assigned in input order across the batch.
    async fn append_batch(
        &self,
        appends: Vec<StreamAppend<E>>,
    ) -> Result<Vec<CommittedStream<E>>, StoreError> {
        // A repeated stream would pass pass 1 against the old head and
        // then conflict in pass 2, after the first half was written.
        validate_batch(&appends)?;
        let wrote = appends.iter().any(|append| !append.events.is_empty());
        let result = self.lock().append_all(appends);
        self.committed(result, wrote)
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let inner = self.lock();
        let first_kept = inner.cuts.get(stream_id).copied().unwrap_or(1);
        let read: Vec<Result<EventEnvelope<E>, StoreError>> =
            match read_starts_before_cut(stream_id, from, first_kept) {
                Err(truncated) => vec![Err(truncated)],
                Ok(()) => inner
                    .streams
                    .get(stream_id)
                    .map(|stream| {
                        stream
                            .iter()
                            .filter(|envelope| envelope.version > from)
                            .cloned()
                            .map(Ok)
                            .collect()
                    })
                    .unwrap_or_default(),
            };
        iter(read)
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

impl<E> StreamLifecycle for InMemoryStore<E>
where
    E: Clone + Send,
{
    async fn close_stream(&self, stream_id: &StreamId) -> Result<(), StoreError> {
        self.lock().closed.insert(stream_id.clone());
        Ok(())
    }

    async fn truncate_before(
        &self,
        stream_id: &StreamId,
        version: Version,
    ) -> Result<(), StoreError> {
        let mut inner = self.lock();
        let head = inner.current_version(stream_id);
        let first = inner.cuts.get(stream_id).copied().unwrap_or(1);
        let TruncatePlan::Cut(cut) = plan_truncate(stream_id, head, first, version)? else {
            return Ok(());
        };
        inner.cuts.insert(stream_id.clone(), cut);
        if let Some(stream) = inner.streams.get_mut(stream_id) {
            stream.retain(|envelope| envelope.version.as_u64() >= cut);
        }
        inner.global.retain(|envelope| {
            &envelope.stream_id != stream_id || envelope.version.as_u64() >= cut
        });
        Ok(())
    }
}

impl<E> QueryAppend for InMemoryStore<E>
where
    E: Clone + Send + EventName + Tagged,
{
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let inner = self.lock();
        let events: Vec<EventEnvelope<E>> = inner.matching(query, after).cloned().collect();
        iter(events.into_iter().map(Ok))
    }

    /// Check the condition and append under the one lock that
    /// serializes every append — overlapping conditions cannot
    /// interleave.
    async fn append_if(
        &self,
        appends: Vec<StreamAppend<E>>,
        condition: AppendCondition,
    ) -> Result<Vec<CommittedStream<E>>, StoreError> {
        validate_batch(&appends)?;
        let wrote = appends.iter().any(|append| !append.events.is_empty());
        let result = {
            let mut inner = self.lock();
            match inner
                .matching(&condition.query, condition.after)
                .map(|envelope| envelope.sequence)
                .last()
            {
                Some(latest) => Err(StoreError::QueryConflict { sequence: latest }),
                None => inner.append_all(appends),
            }
        };
        self.committed(result, wrote)
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
        event.metadata = Metadata::of_ids(Some("cmd-42".into()), Some("corr-7".into()));

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
