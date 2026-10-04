//! The [`StreamsAll`] contract checks: the global, ordered stream.

use eventyr_core::envelope::NewEvent;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
use eventyr_store::store::StreamsAll;
use futures::TryStreamExt;
use futures::executor::block_on;

use crate::event_store::ContractEvent;

/// Run the [`StreamsAll`] contract against `make_store`'s fresh stores.
///
/// The global stream is what projections and subscriptions rebuild
/// from; these checks pin the ordering and the exclusive-bound
/// semantics the subscription machine relies on.
pub fn streams_all_contract<E, S>(make_store: impl Fn() -> S)
where
    E: ContractEvent,
    S: StreamsAll<Event = E>,
{
    the_global_stream_orders_across_streams_by_append_order::<E, _>(&make_store());
    the_global_bound_is_exclusive::<E, _>(&make_store());
    sequences_are_strictly_increasing::<E, _>(&make_store());
}

fn append_all<E: ContractEvent, S: StreamsAll<Event = E>>(store: &S, batches: &[(&str, &[u64])]) {
    for (stream, events) in batches {
        let events = events
            .iter()
            .copied()
            .map(|value| NewEvent::new(E::from(value)))
            .collect();
        block_on(store.append(&StreamId::from(*stream), ExpectedVersion::Any, events))
            .expect("append commits");
    }
}

fn the_global_stream_orders_across_streams_by_append_order<
    E: ContractEvent,
    S: StreamsAll<Event = E>,
>(
    store: &S,
) {
    append_all(
        store,
        &[("global-a", &[1]), ("global-b", &[2]), ("global-a", &[3])],
    );

    let all: Vec<_> =
        block_on(store.stream_all(Sequence::START).try_collect()).expect("global stream read");
    assert_eq!(
        all.iter()
            .map(|envelope| envelope.event.clone())
            .collect::<Vec<_>>(),
        vec![E::from(1), E::from(2), E::from(3)],
        "the global stream is ordered by append, interleaving streams"
    );
    // And per-event stream attribution survives the merge.
    assert_eq!(
        all.iter()
            .map(|envelope| envelope.stream_id.as_str())
            .collect::<Vec<_>>(),
        vec!["global-a", "global-b", "global-a"]
    );
}

fn the_global_bound_is_exclusive<E: ContractEvent, S: StreamsAll<Event = E>>(store: &S) {
    append_all(store, &[("bound-a", &[1, 2]), ("bound-b", &[3])]);

    let all: Vec<_> =
        block_on(store.stream_all(Sequence::START).try_collect()).expect("global stream read");
    let tail: Vec<_> = block_on(store.stream_all(all[0].sequence).try_collect())
        .expect("read from the first sequence");

    assert_eq!(
        tail.iter()
            .map(|envelope| envelope.event.clone())
            .collect::<Vec<_>>(),
        vec![E::from(2), E::from(3)],
        "the bound itself is not redelivered"
    );
}

fn sequences_are_strictly_increasing<E: ContractEvent, S: StreamsAll<Event = E>>(store: &S) {
    append_all(
        store,
        &[("seq-a", &[1]), ("seq-b", &[2, 3]), ("seq-a", &[4])],
    );

    let all: Vec<_> =
        block_on(store.stream_all(Sequence::START).try_collect()).expect("global stream read");

    // Strictly increasing — gaps are allowed (a rolled-back append may
    // burn a value), a regression never is.
    for window in all.windows(2) {
        assert!(
            window[0].sequence < window[1].sequence,
            "sequences must strictly increase along the global stream: {:?}",
            all.iter()
                .map(|envelope| envelope.sequence)
                .collect::<Vec<_>>()
        );
    }
    // The whole stream is already proven globally ordered; a store that
    // delivered a later sequence before an earlier one fails that check.
    assert_eq!(all.len(), 4);
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_store::memory::InMemoryStore;

    #[test]
    fn in_memory_store_passes() {
        streams_all_contract::<u64, _>(InMemoryStore::new);
    }
}
