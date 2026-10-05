//! Store-side snapshot coverage: the in-memory snapshot store, the
//! snapshot-off repository's unchanged protocol, and the snapshots-on
//! repository driving `LoadSnapshot` → delta → commit → fire-and-forget
//! offer.

use std::sync::Arc;

use eventyr_core::prelude::*;
use eventyr_core::testing::account::{
    Account, AccountCommand, AccountEvent, AccountId, AccountState,
};
use eventyr_store::prelude::*;
use futures::TryStreamExt;

use std::num::NonZeroU64;

fn repository() -> AggregateRepository<Account, Arc<InMemoryStore<AccountEvent>>> {
    AggregateRepository::new(Arc::new(InMemoryStore::new()), RetryPolicy::default())
}

fn policy(every: u64) -> SnapshotPolicy {
    SnapshotPolicy::new(NonZeroU64::new(every).unwrap())
}

// -- the snapshot store itself -------------------------------------------

#[tokio::test]
async fn in_memory_snapshot_store_roundtrips_the_newest() {
    let store = InMemorySnapshotStore::new();
    let stream = StreamId::from("account-1");

    assert!(
        store.load(&stream).await.expect("load").is_none(),
        "an unknown stream has no snapshot"
    );

    let snap = |version: u64, balance: u64| Snapshot {
        stream_id: stream.clone(),
        version: Version::new(version),
        state: AccountState {
            open: true,
            balance,
        },
    };

    store.save(snap(3, 30)).await.expect("save 3");
    let found = store.load(&stream).await.expect("load 3").expect("present");
    assert_eq!(found.version, Version::new(3));
    assert_eq!(found.state.balance, 30);

    // The store's "newest wins" rule: a newer save replaces; an older
    // one is dropped without touching the persisted snapshot — the
    // snapshot is a cache, and a stale snapshot is self-correcting via
    // the delta fold on the next load.
    store.save(snap(7, 70)).await.expect("save 7");
    let found = store.load(&stream).await.expect("load 7").expect("present");
    assert_eq!(found.version, Version::new(7));
}

#[tokio::test]
async fn in_memory_snapshot_store_never_regresses_the_version() {
    let store = InMemorySnapshotStore::new();
    let stream = StreamId::from("account-1");
    let snap = |version: u64, balance: u64| Snapshot {
        stream_id: stream.clone(),
        version: Version::new(version),
        state: AccountState {
            open: true,
            balance,
        },
    };

    store.save(snap(7, 70)).await.expect("save 7");

    // An out-of-order offer racing the persisted snapshot (two commits,
    // offers applied out of order) is dropped, not stored over it.
    store.save(snap(5, 50)).await.expect("save 5");
    let found = store.load(&stream).await.expect("load").expect("present");
    assert_eq!(
        found.version,
        Version::new(7),
        "an older offer cannot regress the row"
    );
    assert_eq!(found.state.balance, 70);

    // The same version does not count as newer either: the first of two
    // same-version offers stays (identical states by construction —
    // the machine snapshots the committed fold).
    store.save(snap(7, 70)).await.expect("save 7 again");
    let found = store.load(&stream).await.expect("load").expect("present");
    assert_eq!(found.version, Version::new(7));
}

// -- the snapshot-off repository is unchanged -----------------------------

#[tokio::test]
async fn snapshots_off_execute_matches_the_pre_snapshot_protocol() {
    let repo = repository();

    let outcome = repo
        .execute(AccountId(1), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");
    let ExecutionOutcome::Committed {
        committed,
        snapshot,
    } = outcome
    else {
        panic!("expected a commit")
    };
    assert_eq!(committed.len(), 1);
    assert!(snapshot.is_none(), "snapshots off means no offer");
}

// -- snapshots on: load from snapshot, delta fold, fire-and-forget save ---

#[tokio::test]
async fn snapshots_on_offers_and_persists_after_the_cadence() {
    let store = Arc::new(InMemoryStore::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default())
            .with_snapshots(Arc::clone(&snapshots), policy(2));

    let id = AccountId(7);
    let stream = StreamId::for_aggregate::<Account>(&id);

    // First commit: Open is one event — progress 1 < every(2), no offer.
    let outcome = repo
        .execute_with_snapshots(id.clone(), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");
    let ExecutionOutcome::Committed { snapshot, .. } = outcome else {
        panic!("expected a commit")
    };
    assert!(snapshot.is_none(), "one version of progress < cadence 2");
    assert!(snapshots.load(&stream).await.expect("load").is_none());

    // Second commit: the deposit moves the stream to v2 — progress 2
    // from base 0 → the policy fires, and the driver persists the offer.
    let outcome = repo
        .execute_with_snapshots(id.clone(), AccountCommand::Deposit { amount: 100 })
        .await
        .expect("deposit");
    let ExecutionOutcome::Committed { snapshot, .. } = outcome else {
        panic!("expected a commit")
    };
    let offer = snapshot.expect("cadence fired");
    assert_eq!(offer.0.version, Version::new(2));
    assert_eq!(offer.0.state.balance, 100);

    let persisted = snapshots.load(&stream).await.expect("load").expect("saved");
    assert_eq!(persisted.version, Version::new(2));
    assert_eq!(persisted.state.balance, 100);

    // A snapshots-*off* repository over the same store now reads the
    // persisted snapshot straight from the store: the write protocol
    // stays identical, only the load shortened. (Delta fold: deposit 50
    // on top of the snapshot's 100.)
    let repo_plain: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default())
            .with_snapshots(Arc::clone(&snapshots), policy(100));
    let outcome = repo_plain
        .execute_with_snapshots(id.clone(), AccountCommand::Deposit { amount: 50 })
        .await
        .expect("deposit from snapshot");
    let ExecutionOutcome::Committed {
        committed,
        snapshot,
    } = outcome
    else {
        panic!("expected a commit")
    };
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].version, Version::new(3));
    // Policy is every(100); one event of progress from the snapshot's
    // v2 is not enough, so no new snapshot is offered.
    assert!(snapshot.is_none());
}

#[tokio::test]
async fn snapshots_on_conflict_retry_still_converges() {
    let store = Arc::new(InMemoryStore::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default())
            .with_snapshots(Arc::clone(&snapshots), policy(2));

    let id = AccountId(9);
    repo.execute_with_snapshots(id.clone(), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");
    repo.execute_with_snapshots(id.clone(), AccountCommand::Deposit { amount: 100 })
        .await
        .expect("deposit"); // snapshot persisted at v2

    // Two concurrent commands on the same instance: one opens a machine
    // manually, the other goes through the repo. A conflicted retry must
    // reload only the delta past its folded point — and then converge.
    let mut machine = WriteMachine::<Account, AccountState>::with_snapshots(
        id.clone(),
        AccountCommand::Withdraw { amount: 30 },
        RetryPolicy::default(),
        policy(2),
    );
    let action = machine.start();
    let WriteAction::LoadSnapshot { stream_id } = action else {
        unreachable!("snapshots-on starts with a snapshot read")
    };
    let snap = snapshots.load(&stream_id).await.expect("load snapshot");
    let action = machine.handle(WriteInput::SnapshotLoaded { snapshot: snap });
    let WriteAction::LoadStream { from, .. } = action else {
        unreachable!("after the snapshot, the delta load")
    };
    assert_eq!(from, Version::new(2), "fold from the snapshot's version");

    // Read the delta *now* — it is empty, because the competitor has not
    // committed yet. The machine decides against the folded state
    // (snapshot v2, balance 100).
    let delta: Vec<_> = store
        .stream(&stream_id, from)
        .try_collect()
        .await
        .expect("delta read");
    let action = machine.handle(WriteInput::Loaded { events: delta });
    let WriteAction::Append {
        expected, events, ..
    } = action
    else {
        unreachable!("decide appends")
    };
    assert_eq!(expected, ExpectedVersion::Exact(Version::new(2)));

    // The competing writer commits v3 while we're between decide and
    // append — the append conflicts.
    repo.execute_with_snapshots(id.clone(), AccountCommand::Deposit { amount: 200 })
        .await
        .expect("the competitor commits");

    match store.append(&stream_id, expected, events).await {
        Err(StoreError::Conflict { current, .. }) => {
            assert_eq!(current, Version::new(3));
            let action = machine.handle(WriteInput::Conflict { current });
            // The retry reloads the delta from the folded version — the
            // snapshot was folded once, never re-snapshotted.
            assert!(
                matches!(action, WriteAction::LoadStream { from, .. } if from == Version::new(2))
            );
        }
        Ok(_) => panic!("the conflict must surface"),
        Err(other) => panic!("unexpected store failure: {other:?}"),
    }
}

// -- seeded loads (0.7.8) ----------------------------------------------------

/// A snapshots-on repository loads from the newest seed and folds only
/// the delta — a deliberately wrong planted snapshot proves the seed is
/// used (and a plain load over the same store ignores it).
#[tokio::test]
async fn a_seeded_load_starts_from_the_newest_snapshot() {
    let store = Arc::new(InMemoryStore::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default())
            .with_snapshots(Arc::clone(&snapshots), policy(100));
    let id = AccountId(7);
    let stream = StreamId::for_aggregate::<Account>(&id);

    repo.execute(id.clone(), AccountCommand::Open { owner: "a".into() })
        .await
        .expect("open");
    repo.execute(id.clone(), AccountCommand::Deposit { amount: 100 })
        .await
        .expect("deposit");

    // Plant a snapshot whose state the real fold never reached. A
    // seeded load must return the seed + delta, not recompute.
    snapshots
        .save(Snapshot {
            stream_id: stream.clone(),
            version: Version::new(2),
            state: AccountState {
                open: true,
                balance: 1_000,
            },
        })
        .await
        .expect("save");

    let seeded = repo.load_with_snapshots(id.clone()).await.expect("seeded");
    assert_eq!(seeded.version, Version::new(2));
    assert_eq!(seeded.state.balance, 1_000);

    // The plain load over the same store replays from the start.
    let plain: AggregateRepository<Account, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default());
    let full = plain.load(id.clone()).await.expect("plain");
    assert_eq!(full.state.balance, 100);
}

/// A real cadence snapshot is the same seed: the seeded load and the
/// full load agree.
#[tokio::test]
async fn a_stored_snapshot_and_the_full_fold_agree() {
    let store = Arc::new(InMemoryStore::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default())
            .with_snapshots(Arc::clone(&snapshots), policy(2));
    let id = AccountId(7);

    repo.execute(id.clone(), AccountCommand::Open { owner: "a".into() })
        .await
        .expect("open");
    for amount in [100, 50] {
        repo.execute_with_snapshots(id.clone(), AccountCommand::Deposit { amount })
            .await
            .expect("deposit");
    }

    let seeded = repo.load_with_snapshots(id.clone()).await.expect("seeded");
    let plain: AggregateRepository<Account, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default());
    let full = plain.load(id.clone()).await.expect("plain");
    assert_eq!(seeded.version, full.version);
    assert_eq!(seeded.state.balance, full.state.balance);
}

/// `load_at_with_snapshots` never seeds from past its own bound: a
/// snapshot past the asked version is ignored.
#[tokio::test]
async fn a_seeded_load_at_ignores_a_snapshot_past_its_bound() {
    let store = Arc::new(InMemoryStore::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default())
            .with_snapshots(Arc::clone(&snapshots), policy(100));
    let id = AccountId(7);
    let stream = StreamId::for_aggregate::<Account>(&id);

    repo.execute(id.clone(), AccountCommand::Open { owner: "a".into() })
        .await
        .expect("open");
    repo.execute(id.clone(), AccountCommand::Deposit { amount: 100 })
        .await
        .expect("deposit");
    repo.execute(id.clone(), AccountCommand::Deposit { amount: 50 })
        .await
        .expect("deposit");

    snapshots
        .save(Snapshot {
            stream_id: stream.clone(),
            version: Version::new(3),
            state: AccountState {
                open: true,
                balance: 1_000,
            },
        })
        .await
        .expect("save");

    // The bound is below the snapshot: the fold replays from the start,
    // stopping at version 2.
    let at_two = repo
        .load_at_with_snapshots(id.clone(), Version::new(2))
        .await
        .expect("at 2");
    assert_eq!(at_two.version, Version::new(2));
    assert_eq!(at_two.state.balance, 100);
}

/// Truncation below the seed is fine — the seeded load starts past it —
/// while the plain load must fail with `Truncated` (0.7.6's rule: cut
/// only below what the snapshot readers start from, now for loads too).
#[tokio::test]
async fn a_truncated_stream_loads_from_a_snapshot_but_not_a_full_fold() {
    let store = Arc::new(InMemoryStore::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default())
            .with_snapshots(Arc::clone(&snapshots), policy(2));
    let id = AccountId(7);
    let stream = StreamId::for_aggregate::<Account>(&id);

    repo.execute_with_snapshots(id.clone(), AccountCommand::Open { owner: "a".into() })
        .await
        .expect("open");
    repo.execute_with_snapshots(id.clone(), AccountCommand::Deposit { amount: 100 })
        .await
        .expect("deposit"); // snapshot at 2
    repo.execute_with_snapshots(id.clone(), AccountCommand::Deposit { amount: 50 })
        .await
        .expect("deposit");
    store
        .truncate_before(&stream, Version::new(3))
        .await
        .expect("truncate");

    let seeded = repo.load_with_snapshots(id.clone()).await.expect("seeded");
    assert_eq!(seeded.version, Version::new(3));
    assert_eq!(seeded.state.balance, 150);

    let plain: AggregateRepository<Account, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default());
    let error = plain.load(id).await.expect_err("truncated");
    assert!(matches!(error, StoreError::Truncated { .. }));
}
