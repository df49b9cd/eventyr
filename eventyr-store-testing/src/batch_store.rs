//! The [`EventStore::append_batch`] contract checks.
//!
//! A store that can commit atomically across streams proves it here:
//! multi-stream appends are atomic, per-stream-guarded, and globally
//! ordered like single-stream appends.

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::envelope::NewEvent;
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{EventStore, StreamsAll};
use futures::TryStreamExt;
use futures::executor::block_on;

use crate::event_store::ContractEvent;

async fn append_batch<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
    appends: Vec<StreamAppend<E>>,
) -> Result<Vec<CommittedStream<E>>, StoreError> {
    store.append_batch(appends).await
}

fn append<E: ContractEvent>(
    stream: &str,
    expected: ExpectedVersion,
    events: &[u64],
) -> StreamAppend<E> {
    StreamAppend {
        stream_id: StreamId::from(stream),
        expected,
        events: events
            .iter()
            .copied()
            .map(|v| NewEvent::new(E::from(v)))
            .collect(),
    }
}

/// Run the `append_batch` contract against `make_store`'s fresh stores.
pub fn event_store_batch_contract<E, S>(make_store: impl Fn() -> S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + StreamsAll<Event = E>,
{
    a_single_stream_batch_delegates_to_append::<E, _>(&make_store());
    a_multi_stream_batch_commits_atomically::<E, _>(&make_store());
    a_multi_stream_conflict_commits_nothing::<E, _>(&make_store());
    a_multi_stream_conflict_names_the_stream::<E, _>(&make_store());
    batch_events_are_globally_ordered::<E, _>(&make_store());
}

fn a_single_stream_batch_delegates_to_append<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
) {
    let committed = block_on(append_batch(
        store,
        vec![append("batch-single", ExpectedVersion::Empty, &[1, 2])],
    ))
    .expect("a single-stream batch commits");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].stream_id.as_str(), "batch-single");
    assert_eq!(committed[0].events.len(), 2);
}

fn a_multi_stream_batch_commits_atomically<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E>,
{
    let committed = block_on(append_batch(
        store,
        vec![
            append("batch-a", ExpectedVersion::Empty, &[1]),
            append("batch-b", ExpectedVersion::Empty, &[2]),
        ],
    ))
    .expect("a two-stream batch commits");
    assert_eq!(committed.len(), 2);
    // Each stream got exactly its own event, versioned from 1.
    for expected_stream in ["batch-a", "batch-b"] {
        let events: Vec<_> = block_on(
            store
                .stream(&StreamId::from(expected_stream), Version::EMPTY)
                .try_collect::<Vec<_>>(),
        )
        .expect("stream read");
        assert_eq!(events.len(), 1, "stream {expected_stream}");
        assert_eq!(events[0].version, Version::new(1));
    }
}

fn a_multi_stream_conflict_commits_nothing<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E>,
{
    block_on(append_batch(
        store,
        vec![append::<E>("batch-x", ExpectedVersion::Empty, &[1])],
    ))
    .expect("seed batch-x");
    // `batch-x` is at version 1; the batch asks for Empty there and
    // Empty on the fresh `batch-y`. The whole batch must roll back.
    block_on(append_batch::<E, S>(
        store,
        vec![
            append("batch-x", ExpectedVersion::Empty, &[2]),
            append("batch-y", ExpectedVersion::Empty, &[3]),
        ],
    ))
    .expect_err("the batch must conflict");
    let events: Vec<_> = block_on(
        store
            .stream(&StreamId::from("batch-x"), Version::EMPTY)
            .try_collect(),
    )
    .expect("read");
    assert_eq!(events.len(), 1, "the conflict rolled the batch back");
    let events: Vec<_> = block_on(
        store
            .stream(&StreamId::from("batch-y"), Version::EMPTY)
            .try_collect(),
    )
    .expect("read");
    assert!(events.is_empty(), "the other stream got nothing");
}

fn a_multi_stream_conflict_names_the_stream<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E>,
{
    block_on(append_batch(
        store,
        vec![append::<E>("batch-named", ExpectedVersion::Empty, &[1])],
    ))
    .expect("seed batch-named");
    let error = block_on(append_batch::<E, S>(
        store,
        vec![
            append("batch-named", ExpectedVersion::Empty, &[2]),
            append("batch-other", ExpectedVersion::Empty, &[3]),
        ],
    ))
    .expect_err("the batch must conflict");
    assert!(
        matches!(
            error,
            StoreError::Conflict {
                stream_id: Some(ref stream_id),
                current,
            } if stream_id.as_str() == "batch-named" && current == Version::new(1)
        ),
        "the conflict names the stream, got {error:?}"
    );
}

fn batch_events_are_globally_ordered<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + StreamsAll<Event = E>,
{
    block_on(append_batch(
        store,
        vec![
            append("batch-g1", ExpectedVersion::Empty, &[10, 11]),
            append("batch-g2", ExpectedVersion::Empty, &[12]),
        ],
    ))
    .expect("batch commits");
    let all: Vec<_> =
        block_on(store.stream_all(Sequence::new(0)).try_collect::<Vec<_>>()).expect("global read");
    // The batch's events land contiguously, in input order.
    assert_eq!(
        all.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![Sequence::new(1), Sequence::new(2), Sequence::new(3)]
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_store::memory::InMemoryStore;

    #[test]
    fn in_memory_store_passes() {
        event_store_batch_contract::<u64, _>(InMemoryStore::<u64>::new);
    }
}
