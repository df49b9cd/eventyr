//! The [`EventStore`] contract checks.

use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::{ExpectedVersion, StreamId, Version};
use eventyr_store::store::EventStore;
use futures::TryStreamExt;
use futures::executor::block_on;

/// The event payloads the suite needs from `E`: a payload per `u64`,
/// plus the equality and debuggability the assertions need. The
/// reflexive `From<u64> for u64` makes plain `u64` the natural choice
/// for generic stores.
pub trait ContractEvent: Clone + Send + PartialEq + core::fmt::Debug + From<u64> {}

impl<T> ContractEvent for T where T: Clone + Send + PartialEq + core::fmt::Debug + From<u64> {}

async fn append<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
    stream_id: &StreamId,
    expected: ExpectedVersion,
    events: &[u64],
) -> Result<Vec<EventEnvelope<E>>, StoreError> {
    let events = events
        .iter()
        .copied()
        .map(|value| NewEvent::new(E::from(value)))
        .collect();
    store.append(stream_id, expected, events).await
}

async fn stream_of<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
    stream_id: &StreamId,
    from: Version,
) -> Result<Vec<EventEnvelope<E>>, StoreError> {
    store.stream(stream_id, from).try_collect().await
}

/// Run the [`EventStore`] contract against `make_store`'s fresh stores.
///
/// Each check gets a fresh store (`make_store()`), so stores with
/// setup cost (a test-database schema per case) can charge it per
/// check.
pub fn event_store_contract<E, S>(make_store: impl Fn() -> S)
where
    E: ContractEvent,
    S: EventStore<Event = E>,
{
    appends_are_versioned_positioned_and_typed::<E, _>(&make_store());
    unknown_streams_read_empty::<E, _>(&make_store());
    expectation_violations_conflict_with_the_current_version::<E, _>(&make_store());
    any_expectation_always_appends::<E, _>(&make_store());
    append_is_transactional_on_conflict::<E, _>(&make_store());
    stream_reads_start_from_the_exclusive_bound::<E, _>(&make_store());
    appends_are_visible_to_reads::<E, _>(&make_store());
    metadata_round_trips::<E, _>(&make_store());
    an_empty_append_still_checks_its_expectation::<E, _>(&make_store());
}

fn appends_are_versioned_positioned_and_typed<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
) {
    let stream = StreamId::from("contract-versioning");
    let committed = block_on(append(store, &stream, ExpectedVersion::Empty, &[10, 20]))
        .expect("an empty-stream append to an unknown stream commits");

    assert_eq!(committed.len(), 2);
    assert_eq!(
        committed
            .iter()
            .map(|envelope| (envelope.version, envelope.event.clone()))
            .collect::<Vec<_>>(),
        vec![
            (Version::new(1), E::from(10)),
            (Version::new(2), E::from(20))
        ],
        "versions are 1-based and contiguous within the batch"
    );
    assert!(
        committed
            .iter()
            .all(|envelope| envelope.stream_id == stream),
        "every committed envelope names its stream"
    );
    assert!(
        committed[0].sequence < committed[1].sequence,
        "sequences increase within the batch: {:?}",
        committed
            .iter()
            .map(|envelope| envelope.sequence)
            .collect::<Vec<_>>()
    );
}

fn unknown_streams_read_empty<E: ContractEvent, S: EventStore<Event = E>>(store: &S) {
    let events: Vec<EventEnvelope<E>> = block_on(stream_of(
        store,
        &StreamId::from("contract-unknown"),
        Version::EMPTY,
    ))
    .expect("an unknown stream reads as empty, not as an error");
    assert!(events.is_empty());
}

fn expectation_violations_conflict_with_the_current_version<
    E: ContractEvent,
    S: EventStore<Event = E>,
>(
    store: &S,
) {
    let stream = StreamId::from("contract-expectations");
    block_on(append(store, &stream, ExpectedVersion::Empty, &[1])).expect("first append commits");

    // `Empty` on a stream that now exists.
    let error = block_on(append(store, &stream, ExpectedVersion::Empty, &[2]))
        .expect_err("Empty on a non-empty stream must conflict");
    assert!(
        matches!(error, StoreError::Conflict { current, .. } if current == Version::new(1)),
        "the conflict reports the current version, got {error:?}"
    );

    // `Exact` with the wrong version.
    let error = block_on(append(
        store,
        &stream,
        ExpectedVersion::Exact(Version::new(9)),
        &[2],
    ))
    .expect_err("a wrong Exact version must conflict");
    assert!(
        matches!(error, StoreError::Conflict { current, .. } if current == Version::new(1)),
        "the conflict reports the current version, got {error:?}"
    );

    // `Exact` with the right version commits.
    block_on(append(
        store,
        &stream,
        ExpectedVersion::Exact(Version::new(1)),
        &[2],
    ))
    .expect("the matching Exact version commits");
}

fn any_expectation_always_appends<E: ContractEvent, S: EventStore<Event = E>>(store: &S) {
    let stream = StreamId::from("contract-any");
    block_on(append(store, &stream, ExpectedVersion::Any, &[1]))
        .expect("Any commits to an unknown stream");
    block_on(append(store, &stream, ExpectedVersion::Any, &[2]))
        .expect("Any commits over existing events");
}

fn append_is_transactional_on_conflict<E: ContractEvent, S: EventStore<Event = E>>(store: &S) {
    let stream = StreamId::from("contract-transactional");
    block_on(append(store, &stream, ExpectedVersion::Empty, &[1])).expect("first append commits");

    // A conflicting batch of several events must not commit anything.
    block_on(append(store, &stream, ExpectedVersion::Empty, &[2, 3, 4]))
        .expect_err("a batch violating its expectation conflicts as a whole");

    let events = block_on(stream_of(store, &stream, Version::EMPTY)).expect("stream read");
    assert_eq!(
        events.len(),
        1,
        "no partial batch may be committed: found {:?}",
        events
            .iter()
            .map(|envelope| envelope.event.clone())
            .collect::<Vec<_>>()
    );
}

fn stream_reads_start_from_the_exclusive_bound<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
) {
    let stream = StreamId::from("contract-exclusive");
    block_on(append(store, &stream, ExpectedVersion::Any, &[1, 2, 3])).expect("append");

    let all = block_on(stream_of(store, &stream, Version::EMPTY)).expect("from the start");
    assert_eq!(
        all.iter()
            .map(|envelope| envelope.event.clone())
            .collect::<Vec<_>>(),
        vec![E::from(1), E::from(2), E::from(3)]
    );

    let tail = block_on(stream_of(store, &stream, Version::new(1))).expect("from after version 1");
    assert_eq!(
        tail.iter()
            .map(|envelope| envelope.version)
            .collect::<Vec<_>>(),
        vec![Version::new(2), Version::new(3)],
        "the bound is exclusive and order is stream order"
    );
}

fn appends_are_visible_to_reads<E: ContractEvent, S: EventStore<Event = E>>(store: &S) {
    let first = StreamId::from("contract-visible-a");
    let second = StreamId::from("contract-visible-b");
    block_on(append(store, &first, ExpectedVersion::Empty, &[1])).expect("append a");
    block_on(append(store, &second, ExpectedVersion::Empty, &[2])).expect("append b");

    // Streams are independent: reading one never returns the other's.
    let events = block_on(stream_of(store, &first, Version::EMPTY)).expect("stream read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, E::from(1));
}

fn metadata_round_trips<E: ContractEvent, S: EventStore<Event = E>>(store: &S) {
    let stream = StreamId::from("contract-metadata");
    let mut event = NewEvent::new(E::from(1));
    event.metadata =
        Metadata::of_ids(Some("cmd-1".into()), Some("corr-1".into())).with_idempotency_key("key-1");
    let plain = NewEvent::new(E::from(2));

    let committed = block_on(store.append(&stream, ExpectedVersion::Empty, vec![event, plain]))
        .expect("append with metadata");
    assert_eq!(
        committed[0].metadata.idempotency_key.as_deref(),
        Some("key-1"),
        "the committed envelopes carry the key"
    );

    let events = block_on(stream_of(store, &stream, Version::EMPTY)).expect("stream read");
    assert_eq!(events[0].metadata.causation_id.as_deref(), Some("cmd-1"));
    assert_eq!(events[0].metadata.correlation_id.as_deref(), Some("corr-1"));
    assert_eq!(
        events[0].metadata.idempotency_key.as_deref(),
        Some("key-1"),
        "the idempotency key round-trips (0.7.5)"
    );
    assert_eq!(events[1].metadata.idempotency_key, None);
}

fn an_empty_append_still_checks_its_expectation<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
) {
    let stream = StreamId::from("contract-empty");
    block_on(append(store, &stream, ExpectedVersion::Empty, &[1])).expect("first append commits");

    // Appending nothing writes nothing, but it is still an append: a
    // caller using it to assert a version must hear about a conflict.
    let error = block_on(append(store, &stream, ExpectedVersion::Empty, &[]))
        .expect_err("an empty append with a violated expectation conflicts");
    assert!(
        matches!(error, StoreError::Conflict { current, .. } if current == Version::new(1)),
        "the conflict reports the current version, got {error:?}"
    );
    let error = block_on(append(
        store,
        &stream,
        ExpectedVersion::Exact(Version::new(4)),
        &[],
    ))
    .expect_err("a wrong Exact version conflicts even with nothing to write");
    assert!(matches!(error, StoreError::Conflict { .. }), "{error:?}");

    let committed = block_on(append(
        store,
        &stream,
        ExpectedVersion::Exact(Version::new(1)),
        &[],
    ))
    .expect("an empty append with a matching expectation succeeds");
    assert!(committed.is_empty());
    let events = block_on(stream_of(store, &stream, Version::EMPTY)).expect("stream read");
    assert_eq!(events.len(), 1, "an empty append writes nothing");
}

// The contract is self-testing: the in-memory store ships it, so the
// suite must pass against the store the workspace considers honest.
#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_store::memory::InMemoryStore;

    #[test]
    fn in_memory_store_passes() {
        event_store_contract::<u64, _>(InMemoryStore::<u64>::new);
    }
}
