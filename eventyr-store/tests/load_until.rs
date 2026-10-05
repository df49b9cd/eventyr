//! `AggregateRepository::load_until` (0.7.8): fold a stream to an
//! instant. The suite is gated on `time` — only the envelope's
//! `metadata.timestamp` exists to stop at, and only this store (via the
//! writer's `Metadata`) and Postgres populate it. The workspace
//! builds `time` without its `std` feature, so timestamps come from
//! `UNIX_EPOCH + Duration`, never `now_utc`.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use eventyr_core::prelude::*;
use eventyr_core::snapshot::{Snapshot, SnapshotPolicy};
use eventyr_core::testing::account::{
    Account, AccountCommand, AccountEvent, AccountId, AccountState,
};
use eventyr_store::prelude::*;
use time::OffsetDateTime;

fn repository() -> AggregateRepository<Account, Arc<InMemoryStore<AccountEvent>>> {
    AggregateRepository::new(Arc::new(InMemoryStore::new()), RetryPolicy::default())
}

fn at(seconds: u64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

async fn stamp<SS>(
    repo: &AggregateRepository<Account, Arc<InMemoryStore<AccountEvent>>, SS>,
    id: u64,
    command: AccountCommand,
    timestamp: OffsetDateTime,
) {
    repo.execute_with_metadata(
        AccountId(id),
        command,
        Metadata {
            timestamp: Some(timestamp),
            ..Default::default()
        },
    )
    .await
    .expect("append");
}

#[tokio::test]
async fn a_load_until_stops_at_the_instant_inclusively() {
    let repo = repository();
    let id = 1;
    stamp(&repo, id, AccountCommand::Open { owner: "a".into() }, at(0)).await;
    stamp(&repo, id, AccountCommand::Deposit { amount: 100 }, at(10)).await;
    stamp(&repo, id, AccountCommand::Deposit { amount: 50 }, at(20)).await;
    stamp(&repo, id, AccountCommand::Withdraw { amount: 30 }, at(30)).await;

    // Exactly at an event's instant: that event is the last folded.
    let at_20 = repo.load_until(AccountId(id), at(20)).await.expect("at 20");
    assert_eq!(at_20.version, Version::new(3));
    assert_eq!(at_20.state.balance, 150);

    // Just before it: the fold stops one earlier, never splitting a
    // commit's shared timestamp.
    let at_19 = repo.load_until(AccountId(id), at(19)).await.expect("at 19");
    assert_eq!(at_19.version, Version::new(2));
    assert_eq!(at_19.state.balance, 100);

    // Before the first event and past the last.
    let before = repo.load_until(AccountId(id), at(0)).await.expect("before");
    assert_eq!(before.version, Version::new(1)); // the open at t=0 is in
    let empty = repo
        .load_until(AccountId(id), at(0) - Duration::from_secs(1))
        .await
        .expect("empty");
    assert_eq!(empty.version, Version::EMPTY);
    let after = repo
        .load_until(AccountId(id), at(1_000))
        .await
        .expect("after");
    assert_eq!(after.version, Version::new(4));
}

/// A stream whose timestamps move out of order still folds a prefix:
/// the first event past the instant stops it, and a later one is never
/// skipped into the fold.
#[tokio::test]
async fn an_out_of_order_stream_still_folds_a_prefix() {
    let repo = repository();
    let id = 2;
    stamp(&repo, id, AccountCommand::Open { owner: "a".into() }, at(0)).await;
    stamp(&repo, id, AccountCommand::Deposit { amount: 100 }, at(30)).await;
    stamp(&repo, id, AccountCommand::Deposit { amount: 50 }, at(20)).await; // out of order

    // t=25: only the open counts (t=30 is later — the fold stops there,
    // never skipping to the t=20 event behind it). t=40: all three, in
    // stream order (the out-of-order instants reorder nothing).
    let at_25 = repo.load_until(AccountId(id), at(25)).await.expect("at 25");
    assert_eq!(at_25.version, Version::new(1));
    let at_40 = repo.load_until(AccountId(id), at(40)).await.expect("at 40");
    assert_eq!(at_40.version, Version::new(3));
}

/// An event with no timestamp fails the load, naming the stream — the
/// fold never guesses one.
#[tokio::test]
async fn an_event_without_a_timestamp_fails_the_load() {
    let repo = repository();
    let id = 3;
    repo.execute(AccountId(id), AccountCommand::Open { owner: "a".into() })
        .await
        .expect("open (no timestamp)");
    stamp(&repo, id, AccountCommand::Deposit { amount: 100 }, at(10)).await;

    let error = repo
        .load_until(AccountId(id), at(5))
        .await
        .expect_err("the first event has no timestamp");
    assert!(matches!(error, StoreError::Other(_)), "{error:?}");
    assert!(error.to_string().contains("account-3"), "{error}");
}

/// A truncated stream fails a load_until: nothing seeds a fold to a
/// deleted prefix.
#[tokio::test]
async fn a_truncated_stream_cannot_load_by_time() {
    let store = Arc::new(InMemoryStore::<AccountEvent>::new());
    let repo = AggregateRepository::new(Arc::clone(&store), RetryPolicy::default());
    let id = AccountId(4);
    let stream = StreamId::for_aggregate::<Account>(&id);

    stamp(&repo, 4, AccountCommand::Open { owner: "a".into() }, at(0)).await;
    stamp(&repo, 4, AccountCommand::Deposit { amount: 100 }, at(10)).await;
    store
        .truncate_before(&stream, Version::new(2))
        .await
        .expect("truncate");

    let error = repo
        .load_until(id, at(20))
        .await
        .expect_err("a truncated stream has no prefix to fold by time");
    assert!(matches!(error, StoreError::Truncated { .. }));
}

/// `load_until` never uses a snapshot: the fold records a *position*,
/// not an instant, so a planted wrong snapshot cannot seed it.
#[tokio::test]
async fn a_wrong_snapshot_never_seeds_a_time_fold() {
    let store = Arc::new(InMemoryStore::<AccountEvent>::new());
    let snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo: AggregateRepository<Account, _, _> =
        AggregateRepository::new(Arc::clone(&store), RetryPolicy::default()).with_snapshots(
            Arc::clone(&snapshots),
            SnapshotPolicy::new(NonZeroU64::new(100).unwrap()),
        );
    let id = AccountId(5);
    let stream = StreamId::for_aggregate::<Account>(&id);

    stamp(&repo, 5, AccountCommand::Open { owner: "a".into() }, at(0)).await;
    stamp(&repo, 5, AccountCommand::Deposit { amount: 100 }, at(10)).await;

    snapshots
        .save(Snapshot {
            stream_id: stream.clone(),
            version: Version::new(2),
            state: AccountState {
                open: true,
                balance: 9_999,
            },
        })
        .await
        .expect("save");

    let at_20 = repo.load_until(id, at(20)).await.expect("at 20");
    assert_eq!(
        at_20.state.balance, 100,
        "the fold uses the events, never the snapshot"
    );
}
