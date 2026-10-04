//! The blocking write driver: the same machine protocol, driven to its
//! outcome without an async runtime.

use std::sync::Arc;

use eventyr_core::prelude::*;
use eventyr_core::testing::account::{Account, AccountCommand, AccountEvent, AccountId};
use eventyr_store::driver;
use eventyr_store::prelude::{EventStore, InMemoryStore};
use futures::TryStreamExt;

#[test]
fn blocking_driver_commits_then_conflicts_like_the_async_one() {
    let store = Arc::new(InMemoryStore::<AccountEvent>::new());
    let id = AccountId(7);

    // Interaction 1: empty stream, `Open` commits.
    let mut machine = WriteMachine::<Account>::new(
        id.clone(),
        AccountCommand::Open { owner: "me".into() },
        RetryPolicy::default(),
    );
    let outcome = driver::drive_write_blocking(&mut machine, &store);
    let WriteOutcome::Committed { committed, .. } = outcome else {
        panic!("expected a commit, not {outcome:?}")
    };
    assert_eq!(committed.len(), 1);

    // Interaction 2: the same id folds the committed event, so `Open`
    // decides a domain error — the conflict path is the machine's, and
    // a blocking driver cannot change it.
    let mut machine = WriteMachine::<Account>::new(
        id.clone(),
        AccountCommand::Open { owner: "me".into() },
        RetryPolicy::default(),
    );
    let outcome = driver::drive_write_blocking(&mut machine, &store);
    assert!(
        matches!(outcome, WriteOutcome::Rejected(_)),
        "a second Open must reject, not {outcome:?}"
    );

    // And the blocking driver's writes are visible through the async
    // stream: one runtime never hides from the other.
    let machine_id = id.clone();
    let stream_id = StreamId::for_aggregate::<Account>(&machine_id);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let events: Vec<_> = store
                .stream(&stream_id, Version::EMPTY)
                .try_collect()
                .await
                .expect("stream read");
            assert_eq!(events.len(), 1);
        });
}

#[test]
fn blocking_driver_accepts_a_domain_rejection_unchanged() {
    let store = Arc::new(InMemoryStore::<AccountEvent>::new());
    let id = AccountId(1);

    // Withdraw before open: the machine rejects on the empty fold, the
    // driver reports it, nothing is appended.
    let mut machine = WriteMachine::<Account>::new(
        id.clone(),
        AccountCommand::Withdraw { amount: 10 },
        RetryPolicy::default(),
    );
    let outcome = driver::drive_write_blocking(&mut machine, &store);
    assert!(
        matches!(outcome, WriteOutcome::Rejected(_)),
        "a withdraw before open must reject, not {outcome:?}"
    );
}
