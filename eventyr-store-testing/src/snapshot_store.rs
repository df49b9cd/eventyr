//! The [`SnapshotStore`] contract checks: newest-per-stream,
//! never-regressing.

use eventyr_core::snapshot::Snapshot;
use eventyr_core::vocabulary::{StreamId, Version};
use eventyr_store::snapshot_store::SnapshotStore;
use futures::executor::block_on;

/// Run the [`SnapshotStore`] contract against `make_store`'s fresh
/// stores.
///
/// Snapshots are read-side shortcuts; the contract is consequently
/// small — but the newest-wins rule is load-bearing (the write driver
/// saves fire-and-forget), so it gets the sharpest checks.
pub fn snapshot_contract<S, SS>(make_store: impl Fn() -> SS)
where
    SS: SnapshotStore<State = S>,
    S: Clone + Send + PartialEq + core::fmt::Debug + From<u64>,
{
    unknown_streams_load_as_none(&make_store());
    loads_return_the_newest_saved_snapshot(&make_store());
    a_stale_save_never_regresses_the_stored_version(&make_store());
}

fn snapshot<S: From<u64>>(stream_id: &StreamId, version: u64, state: u64) -> Snapshot<S> {
    Snapshot {
        stream_id: stream_id.clone(),
        version: Version::new(version),
        state: S::from(state),
    }
}

fn unknown_streams_load_as_none<S: Clone + Send + PartialEq + core::fmt::Debug + From<u64>, SS>(
    store: &SS,
) where
    SS: SnapshotStore<State = S>,
{
    let loaded = block_on(store.load(&StreamId::from("snap-unknown")));
    assert!(
        matches!(loaded, Ok(None)),
        "an unknown stream loads as None, not an error: {loaded:?}"
    );
}

fn loads_return_the_newest_saved_snapshot<
    S: Clone + Send + PartialEq + core::fmt::Debug + From<u64>,
    SS,
>(
    store: &SS,
) where
    SS: SnapshotStore<State = S>,
{
    let stream = StreamId::from("snap-newest");
    block_on(store.save(snapshot(&stream, 1, 100))).expect("first save");
    block_on(store.save(snapshot(&stream, 5, 500))).expect("newer save");

    let loaded = block_on(store.load(&stream))
        .expect("load")
        .expect("a snapshot is stored");
    assert_eq!(loaded.version, Version::new(5));
    assert_eq!(loaded.state, S::from(500));
}

fn a_stale_save_never_regresses_the_stored_version<
    S: Clone + Send + PartialEq + core::fmt::Debug + From<u64>,
    SS,
>(
    store: &SS,
) where
    SS: SnapshotStore<State = S>,
{
    let stream = StreamId::from("snap-regress");
    block_on(store.save(snapshot(&stream, 5, 500))).expect("newer save");
    // A fire-and-forget offer arriving late must be dropped, not stored.
    block_on(store.save(snapshot(&stream, 2, 200)))
        .expect("a stale save is still an Ok — dropper, not error");
    block_on(store.save(snapshot(&stream, 5, 555))).expect("an equal-version save is also dropped");

    let loaded = block_on(store.load(&stream))
        .expect("load")
        .expect("a snapshot is stored");
    assert_eq!(loaded.version, Version::new(5));
    assert_eq!(
        loaded.state,
        S::from(500),
        "the newest save wins; regressions are dropped"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_store::snapshot_store::InMemorySnapshotStore;

    #[test]
    fn in_memory_snapshot_store_passes() {
        snapshot_contract::<u64, _>(InMemorySnapshotStore::new);
    }
}
