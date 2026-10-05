//! The [`StreamLifecycle`] contract checks (0.7.6).
//!
//! A store that can close and truncate streams proves it here: a closed
//! stream refuses every append (single, batch) and keeps its history
//! readable, closing is idempotent and works on an empty stream, and a
//! truncated stream refuses reads that would start before the cut while
//! serving those that start at or after it — in the stream and in the
//! global log alike.

use eventyr_core::batch::StreamAppend;
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{StreamLifecycle, StreamsAll};
use futures::TryStreamExt;
use futures::executor::block_on;

use crate::event_store::ContractEvent;

/// Run the [`StreamLifecycle`] contract against `make_store`'s fresh
/// stores.
pub fn lifecycle_contract<E, S>(make_store: impl Fn() -> S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    a_closed_stream_refuses_appends_and_keeps_its_history::<E, _>(&make_store());
    a_closed_stream_refuses_batch_appends_atomically::<E, _>(&make_store());
    closing_is_idempotent_and_works_on_an_empty_stream::<E, _>(&make_store());
    a_truncated_stream_refuses_reads_from_before_the_cut::<E, _>(&make_store());
    truncation_reaches_the_global_stream::<E, _>(&make_store());
    truncation_is_monotone_and_bounded::<E, _>(&make_store());
    a_truncated_stream_still_accepts_appends::<E, _>(&make_store());
}

fn sid(name: &str) -> StreamId {
    StreamId::from(name)
}

fn append<E: ContractEvent, S: StreamsAll<Event = E>>(
    store: &S,
    stream: &str,
    expected: ExpectedVersion,
    values: &[u64],
) -> Result<Vec<EventEnvelope<E>>, StoreError> {
    block_on(store.append(
        &sid(stream),
        expected,
        values.iter().map(|&v| NewEvent::new(E::from(v))).collect(),
    ))
}

fn read<E: ContractEvent, S: StreamsAll<Event = E>>(
    store: &S,
    stream: &str,
    from: u64,
) -> Result<Vec<E>, StoreError> {
    block_on(
        store
            .stream(&sid(stream), Version::new(from))
            .map_ok(|e| e.event)
            .try_collect(),
    )
}

fn global<E: ContractEvent, S: StreamsAll<Event = E>>(store: &S) -> Vec<(String, u64)> {
    block_on(store.stream_all(Sequence::START).try_collect::<Vec<_>>())
        .expect("global read")
        .into_iter()
        .map(|e| (e.stream_id.as_str().to_owned(), e.version.as_u64()))
        .collect()
}

fn closed(error: &StoreError, stream: &str) -> bool {
    matches!(error, StoreError::StreamClosed { stream_id } if stream_id.as_str() == stream)
}

fn a_closed_stream_refuses_appends_and_keeps_its_history<E, S>(store: &S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    append(store, "life-a", ExpectedVersion::Empty, &[1, 2]).expect("append");
    block_on(store.close_stream(&sid("life-a"))).expect("close");

    let error = append(
        store,
        "life-a",
        ExpectedVersion::Exact(Version::new(2)),
        &[3],
    )
    .expect_err("a closed stream refuses appends");
    assert!(closed(&error, "life-a"), "{error:?}");
    let error =
        append(store, "life-a", ExpectedVersion::Any, &[3]).expect_err("even unconditional ones");
    assert!(closed(&error, "life-a"), "{error:?}");

    assert_eq!(
        read(store, "life-a", 0).expect("the history stays readable"),
        vec![E::from(1), E::from(2)]
    );
    // Other streams are untouched.
    append(store, "life-b", ExpectedVersion::Empty, &[9]).expect("another stream");
}

fn a_closed_stream_refuses_batch_appends_atomically<E, S>(store: &S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    append(store, "life-a", ExpectedVersion::Empty, &[1]).expect("append");
    block_on(store.close_stream(&sid("life-a"))).expect("close");
    let before = global(store);

    let error = block_on(store.append_batch(vec![
        StreamAppend {
            stream_id: sid("life-b"),
            expected: ExpectedVersion::Empty,
            events: vec![NewEvent::new(E::from(2))],
        },
        StreamAppend {
            stream_id: sid("life-a"),
            expected: ExpectedVersion::Any,
            events: vec![NewEvent::new(E::from(3))],
        },
    ]))
    .expect_err("a batch touching a closed stream fails");
    assert!(closed(&error, "life-a"), "{error:?}");
    assert_eq!(global(store), before, "nothing in the batch was written");
}

fn closing_is_idempotent_and_works_on_an_empty_stream<E, S>(store: &S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    block_on(store.close_stream(&sid("life-empty"))).expect("close an empty stream");
    block_on(store.close_stream(&sid("life-empty"))).expect("close it again");
    let error = append(store, "life-empty", ExpectedVersion::Empty, &[1])
        .expect_err("the closed name is reserved");
    assert!(closed(&error, "life-empty"), "{error:?}");
    assert!(read(store, "life-empty", 0).expect("read").is_empty());
}

fn a_truncated_stream_refuses_reads_from_before_the_cut<E, S>(store: &S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    append(store, "life-a", ExpectedVersion::Empty, &[1, 2, 3, 4, 5]).expect("append");
    block_on(store.truncate_before(&sid("life-a"), Version::new(4))).expect("truncate");

    for from in [0, 1, 2] {
        match read(store, "life-a", from) {
            Err(StoreError::Truncated { stream_id, first }) => {
                assert_eq!(stream_id.as_str(), "life-a");
                assert_eq!(first, Version::new(4));
            }
            other => panic!("a read from {from} spans the cut: {other:?}"),
        }
    }
    // From 3 (exclusive) the read starts at the first kept event.
    assert_eq!(
        read(store, "life-a", 3).expect("a read at the cut"),
        vec![E::from(4), E::from(5)]
    );
    assert_eq!(
        read(store, "life-a", 4).expect("past the cut"),
        vec![E::from(5)]
    );
}

fn truncation_reaches_the_global_stream<E, S>(store: &S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    append(store, "life-a", ExpectedVersion::Empty, &[1, 2, 3]).expect("append");
    append(store, "life-b", ExpectedVersion::Empty, &[4]).expect("append");
    block_on(store.truncate_before(&sid("life-a"), Version::new(3))).expect("truncate");
    assert_eq!(
        global(store),
        vec![("life-a".to_owned(), 3), ("life-b".to_owned(), 1)],
        "the dropped events are gone from the global stream too"
    );
}

fn truncation_is_monotone_and_bounded<E, S>(store: &S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    append(store, "life-a", ExpectedVersion::Empty, &[1, 2, 3, 4]).expect("append");
    block_on(store.truncate_before(&sid("life-a"), Version::new(3))).expect("truncate");
    // An earlier cut is a no-op, not an un-truncate.
    block_on(store.truncate_before(&sid("life-a"), Version::new(2))).expect("no-op");
    assert!(matches!(
        read(store, "life-a", 1),
        Err(StoreError::Truncated { first, .. }) if first == Version::new(3)
    ));
    // Truncating to just past the end empties the stream's history but
    // keeps its version.
    block_on(store.truncate_before(&sid("life-a"), Version::new(5))).expect("to the end");
    assert!(read(store, "life-a", 4).expect("at the end").is_empty());
    // Past that is an error: there is nothing there to keep.
    assert!(
        block_on(store.truncate_before(&sid("life-a"), Version::new(7))).is_err(),
        "truncating past the stream's end"
    );
}

fn a_truncated_stream_still_accepts_appends<E, S>(store: &S)
where
    E: ContractEvent,
    S: StreamLifecycle<Event = E> + StreamsAll<Event = E>,
{
    append(store, "life-a", ExpectedVersion::Empty, &[1, 2, 3]).expect("append");
    block_on(store.truncate_before(&sid("life-a"), Version::new(4))).expect("truncate all");
    // The version survives the truncation: the next append is 4, and
    // `Empty` no longer matches.
    assert!(append(store, "life-a", ExpectedVersion::Empty, &[9]).is_err());
    let committed = append(
        store,
        "life-a",
        ExpectedVersion::Exact(Version::new(3)),
        &[4],
    )
    .expect("append after truncation");
    assert_eq!(committed[0].version, Version::new(4));
    assert_eq!(read(store, "life-a", 3).expect("read"), vec![E::from(4)]);
}

#[cfg(test)]
mod tests {
    use super::lifecycle_contract;
    use eventyr_store::memory::InMemoryStore;

    #[test]
    fn the_in_memory_store_passes_the_lifecycle_contract() {
        lifecycle_contract::<u64, _>(InMemoryStore::new);
    }
}
