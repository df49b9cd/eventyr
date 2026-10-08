//! The [`StreamsAll::stream_all_filtered`] contract checks (roadmap 0.7.4).
//!
//! A store that overrides the filtered read proves it agrees with the
//! default: the same events selected by prefix and by name, in global
//! order, and a scan bound that is honest — at least the last delivered
//! event, never past what the store has, and moving through unmatched
//! runs so a subscription can checkpoint past them.

use eventyr_core::envelope::NewEvent;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
use eventyr_store::store::{EventFilter, FilteredRead, StreamsAll};
use futures::executor::block_on;

use crate::event_store::ContractEvent;

/// Run the filtered-read contract against `make_store`'s fresh stores.
///
/// `E::event_name` must return a name that depends on the payload:
/// even values `"Even"`, odd values `"Odd"` — the suite filters by it.
/// [`ParityEvent`](crate::ParityEvent) is that type, ready-made.
pub fn filtered_read_contract<E, S>(make_store: impl Fn() -> S)
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    selects_by_prefix_and_name_in_global_order::<E, _>(&make_store());
    respects_the_bound_and_max::<E, _>(&make_store());
    reports_progress_through_unmatched_runs::<E, _>(&make_store());
    an_empty_store_scans_nothing::<E, _>(&make_store());
    prefixes_match_literally::<E, _>(&make_store());
    a_bound_past_the_end_scans_nothing::<E, _>(&make_store());
}

fn seed<E: ContractEvent, S: StreamsAll<Event = E>>(store: &S, stream: &str, values: &[u64]) {
    block_on(store.append(
        &StreamId::from(stream),
        ExpectedVersion::Any,
        values.iter().map(|&v| NewEvent::new(E::from(v))).collect(),
    ))
    .expect("seeding appends");
}

fn read<E, S>(
    store: &S,
    from: u64,
    filter: &EventFilter,
    max: usize,
    scan: usize,
) -> FilteredRead<E>
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    block_on(store.stream_all_filtered(Sequence::new(from), filter, max, scan)).expect("read")
}

fn values<E: ContractEvent>(read: &FilteredRead<E>) -> Vec<E> {
    read.events.iter().map(|e| e.event.clone()).collect()
}

fn selects_by_prefix_and_name_in_global_order<E, S>(store: &S)
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    seed(store, "account-1", &[1, 2]);
    seed(store, "order-1", &[3, 4]);
    seed(store, "account-2", &[5, 6]);

    let accounts = EventFilter::all().stream_prefix("account-");
    let got = read(store, 0, &accounts, 100, 100);
    assert_eq!(values(&got), [1, 2, 5, 6].map(E::from).to_vec());
    assert!(got.events.windows(2).all(|w| w[0].sequence < w[1].sequence));

    let odd_accounts = accounts.clone().event_types(["Odd"]);
    assert_eq!(
        values(&read(store, 0, &odd_accounts, 100, 100)),
        [1, 5].map(E::from).to_vec()
    );

    let two_prefixes = EventFilter::all()
        .stream_prefix("order-")
        .stream_prefix("account-2");
    assert_eq!(
        values(&read(store, 0, &two_prefixes, 100, 100)),
        [3, 4, 5, 6].map(E::from).to_vec()
    );

    assert_eq!(
        read(store, 0, &EventFilter::all(), 100, 100).events.len(),
        6
    );
}

fn respects_the_bound_and_max<E, S>(store: &S)
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    seed(store, "account-1", &[1, 2, 3, 4, 5]);
    let all = read(store, 0, &EventFilter::all(), 100, 100);
    let second = all.events[1].sequence.as_u64();

    let after = read(store, second, &EventFilter::all(), 100, 100);
    assert_eq!(values(&after), [3, 4, 5].map(E::from).to_vec());

    let two = read(store, 0, &EventFilter::all(), 2, 100);
    assert_eq!(two.events.len(), 2);
    assert!(
        two.scanned >= two.events[1].sequence,
        "the scan reaches at least the last delivered event"
    );
    assert!(
        two.scanned < all.events[4].sequence,
        "a read that stopped at `max` did not scan the rest"
    );
}

fn reports_progress_through_unmatched_runs<E, S>(store: &S)
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    seed(store, "order-1", &[1, 2, 3, 4, 5, 6, 7, 8]);
    seed(store, "account-1", &[9]);
    let all = read(store, 0, &EventFilter::all(), 100, 100);
    let head = all.events.last().expect("seeded").sequence;
    let accounts = EventFilter::all().stream_prefix("account-");

    // A small scan budget: no match, but progress.
    let first = read(store, 0, &accounts, 100, 3);
    assert!(first.events.is_empty());
    assert!(
        first.scanned > Sequence::START,
        "the unmatched run moved the scan"
    );
    assert!(first.scanned < head);

    // Polling on from the scan bound reaches the match.
    let mut from = first.scanned;
    let mut found = Vec::new();
    for _ in 0..10 {
        let next = read(store, from.as_u64(), &accounts, 100, 3);
        assert!(next.scanned >= from, "the scan never moves back");
        found.extend(values(&next));
        if next.scanned == from {
            break;
        }
        from = next.scanned;
    }
    assert_eq!(found, vec![E::from(9)]);
    assert_eq!(from, head, "the scan ends at the head, not past it");
}

fn an_empty_store_scans_nothing<E, S>(store: &S)
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    let empty = read(store, 0, &EventFilter::all().stream_prefix("x"), 10, 10);
    assert!(empty.events.is_empty());
    assert_eq!(empty.scanned, Sequence::START);
}

fn a_bound_past_the_end_scans_nothing<E, S>(store: &S)
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    seed(store, "account-1", &[1, 2]);
    // A bound no database integer holds: it must still mean "after
    // everything", not wrap around to "before everything".
    let past = read(store, u64::MAX, &EventFilter::all(), 10, 10);
    assert!(past.events.is_empty(), "{:?}", values(&past));
    assert_eq!(past.scanned, Sequence::new(u64::MAX));
}

fn prefixes_match_literally<E, S>(store: &S)
where
    E: ContractEvent + EventName,
    S: StreamsAll<Event = E>,
{
    seed(store, "a_b-1", &[1]);
    seed(store, "axb-1", &[2]);
    seed(store, "a%-1", &[3]);
    seed(store, "az-1", &[4]);
    // Pattern characters in a prefix are plain characters.
    assert_eq!(
        values(&read(
            store,
            0,
            &EventFilter::all().stream_prefix("a_b"),
            100,
            100
        )),
        vec![E::from(1)]
    );
    assert_eq!(
        values(&read(
            store,
            0,
            &EventFilter::all().stream_prefix("a%"),
            100,
            100
        )),
        vec![E::from(3)]
    );
    // Case-sensitive, byte for byte.
    assert!(
        read(store, 0, &EventFilter::all().stream_prefix("A"), 100, 100)
            .events
            .is_empty()
    );
}

#[cfg(test)]
mod tests {
    use super::filtered_read_contract;
    use crate::ParityEvent;
    use eventyr_store::memory::InMemoryStore;

    #[test]
    fn the_default_filtered_read_passes() {
        filtered_read_contract::<ParityEvent, _>(InMemoryStore::new);
    }
}
