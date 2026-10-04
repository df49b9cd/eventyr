//! The [`CommitSignal`] contract checks (0.7.2).
//!
//! A store that raises a commit signal proves it here: a commit
//! resolves an armed listener, a commit made before the wait is not
//! lost, a failed append and an empty one stay silent, and many
//! commits may coalesce into one wake-up but never into none.
//!
//! The checks poll futures without a runtime (`now_or_never`), so the
//! signal must have fired by the time the append returns — true for
//! the in-process signal. A store whose signal travels over the network
//! (Postgres's `NOTIFY`) proves itself with its own timed tests.

use eventyr_core::envelope::NewEvent;
use eventyr_core::vocabulary::{ExpectedVersion, StreamId};
use eventyr_store::notify::{CommitListener, CommitSignal};
use eventyr_store::store::EventStore;
use futures::FutureExt;
use futures::executor::block_on;

use crate::event_store::ContractEvent;

/// Run the [`CommitSignal`] contract against `make_store`'s fresh stores.
pub fn commit_signal_contract<E, S>(make_store: impl Fn() -> S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + CommitSignal,
{
    a_commit_resolves_an_armed_listener::<E, _>(&make_store());
    a_listener_armed_after_a_commit_does_not_see_it::<E, _>(&make_store());
    commits_coalesce_but_are_never_lost::<E, _>(&make_store());
    failed_and_empty_appends_stay_silent::<E, _>(&make_store());
    batch_appends_signal::<E, _>(&make_store());
}

fn append<E: ContractEvent, S: EventStore<Event = E>>(
    store: &S,
    stream: &str,
    expected: ExpectedVersion,
    values: &[u64],
) -> bool {
    block_on(store.append(
        &StreamId::from(stream),
        expected,
        values.iter().map(|&v| NewEvent::new(E::from(v))).collect(),
    ))
    .is_ok()
}

fn listen<S: CommitSignal>(store: &S) -> S::Listener {
    block_on(store.subscribe()).expect("the signal arms")
}

fn fired<L: CommitListener>(listener: &mut L) -> bool {
    matches!(listener.committed().now_or_never(), Some(Ok(())))
}

fn a_commit_resolves_an_armed_listener<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + CommitSignal,
{
    let mut listener = listen(store);
    assert!(!fired(&mut listener), "nothing committed yet");
    assert!(append(store, "signal-a", ExpectedVersion::Empty, &[1]));
    assert!(fired(&mut listener), "a commit resolves the listener");
    assert!(!fired(&mut listener), "and is consumed by it");
}

fn a_listener_armed_after_a_commit_does_not_see_it<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + CommitSignal,
{
    assert!(append(store, "signal-a", ExpectedVersion::Empty, &[1]));
    let mut listener = listen(store);
    assert!(!fired(&mut listener));
}

fn commits_coalesce_but_are_never_lost<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + CommitSignal,
{
    let mut listener = listen(store);
    // A wait started and abandoned before the commits.
    assert!(!fired(&mut listener));
    assert!(append(store, "signal-a", ExpectedVersion::Any, &[1]));
    assert!(append(store, "signal-b", ExpectedVersion::Any, &[2]));
    assert!(fired(&mut listener), "at least one wake for the commits");
}

fn failed_and_empty_appends_stay_silent<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + CommitSignal,
{
    assert!(append(store, "signal-a", ExpectedVersion::Empty, &[1]));
    let mut listener = listen(store);
    assert!(
        !append(store, "signal-a", ExpectedVersion::Empty, &[2]),
        "the conflicting append fails"
    );
    assert!(append(store, "signal-a", ExpectedVersion::Any, &[]));
    assert!(
        !fired(&mut listener),
        "neither a conflict nor an empty append is a commit"
    );
}

fn batch_appends_signal<E, S>(store: &S)
where
    E: ContractEvent,
    S: EventStore<Event = E> + CommitSignal,
{
    use eventyr_core::batch::StreamAppend;
    let mut listener = listen(store);
    let appended = block_on(store.append_batch(vec![StreamAppend {
        stream_id: StreamId::from("signal-batch"),
        expected: ExpectedVersion::Empty,
        events: vec![NewEvent::new(E::from(1))],
    }]));
    assert!(appended.is_ok());
    assert!(fired(&mut listener), "a batch commit signals too");
}

#[cfg(test)]
mod tests {
    use super::commit_signal_contract;
    use eventyr_store::memory::InMemoryStore;

    #[test]
    fn the_in_memory_store_passes_the_commit_signal_contract() {
        commit_signal_contract::<u64, _>(InMemoryStore::new);
    }
}
