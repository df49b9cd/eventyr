//! End-to-end tests: repository → driver → machine → in-memory store.

use std::sync::Arc;

use eventyr_core::prelude::*;
use eventyr_core::testing::account::{
    Account, AccountCommand, AccountError, AccountEvent, AccountId,
};
use eventyr_store::prelude::*;
use futures::{Stream, TryStreamExt};

// -- helpers -------------------------------------------------------------

fn repository() -> AggregateRepository<Account, Arc<InMemoryStore<AccountEvent>>> {
    AggregateRepository::new(Arc::new(InMemoryStore::new()), RetryPolicy::default())
}

async fn stream_of(
    store: &InMemoryStore<AccountEvent>,
    id: u64,
) -> Vec<EventEnvelope<AccountEvent>> {
    store
        .stream(
            &StreamId::for_aggregate::<Account>(&AccountId(id)),
            Version::EMPTY,
        )
        .try_collect()
        .await
        .expect("stream read")
}

// -- the loop ------------------------------------------------------------

#[tokio::test]
async fn open_deposit_withdraw_roundtrip() {
    let repo = repository();
    let id = AccountId(1);

    // Open: empty stream, expects `Empty`.
    repo.execute(id.clone(), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");

    // Deposit twice: state folds across interactions.
    repo.execute(id.clone(), AccountCommand::Deposit { amount: 100 })
        .await
        .expect("deposit 1");
    repo.execute(id.clone(), AccountCommand::Deposit { amount: 50 })
        .await
        .expect("deposit 2");

    // Withdraw: decided against the folded balance of 150.
    let outcome = repo
        .execute(id.clone(), AccountCommand::Withdraw { amount: 120 })
        .await
        .expect("withdraw");
    let ExecutionOutcome::Committed {
        committed: events, ..
    } = outcome
    else {
        panic!("expected a commit")
    };
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].version, Version::new(4));

    // The domain rejects what the folded state forbids.
    let error = repo
        .execute(id, AccountCommand::Withdraw { amount: 100 })
        .await
        .expect_err("only 30 left");
    assert!(matches!(
        error,
        ExecutionError::Domain(AccountError::InsufficientFunds)
    ));
}

#[tokio::test]
async fn domain_rejection_leaves_the_stream_untouched() {
    let repo = repository();
    let id = AccountId(2);

    // Withdraw on a nonexistent account: rejected before any append.
    let error = repo
        .execute(id.clone(), AccountCommand::Withdraw { amount: 10 })
        .await
        .expect_err("not open");
    assert!(matches!(
        error,
        ExecutionError::Domain(AccountError::NotOpen)
    ));

    let events = stream_of(repo.store(), id.0).await;
    assert!(events.is_empty(), "a rejection must not append");
}

#[tokio::test]
async fn interleaved_executions_all_land_contiguously() {
    let repo = repository();
    let id = AccountId(3);

    repo.execute(id.clone(), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");

    // Three deposits racing on one task: the in-memory store never
    // yields, so these interleave at await points only — but whatever
    // the interleaving, all three must land with contiguous versions
    // and no lost updates.
    let store = Arc::clone(repo.store());
    let (a, b, c) = tokio::join!(
        repo.execute(id.clone(), AccountCommand::Deposit { amount: 10 }),
        repo.execute(id.clone(), AccountCommand::Deposit { amount: 20 }),
        repo.execute(id, AccountCommand::Deposit { amount: 30 }),
    );
    a.expect("deposit a");
    b.expect("deposit b");
    c.expect("deposit c");

    let events = stream_of(&store, 3).await;
    assert_eq!(events.len(), 4);
    let versions: Vec<_> = events.iter().map(|e| e.version).collect();
    assert_eq!(
        versions,
        vec![
            Version::new(1),
            Version::new(2),
            Version::new(3),
            Version::new(4)
        ]
    );
}

/// Drives a machine by hand to its `Append` action — the deterministic
/// way to put a writer in the window between load and append.
async fn drive_to_append(
    store: &InMemoryStore<AccountEvent>,
    id: u64,
    amount: u64,
    retry_policy: RetryPolicy,
) -> (
    WriteMachine<Account>,
    StreamId,
    ExpectedVersion,
    Vec<NewEvent<AccountEvent>>,
) {
    let mut machine = WriteMachine::<Account>::new(
        AccountId(id),
        AccountCommand::Deposit { amount },
        retry_policy,
    );
    let WriteAction::LoadStream { stream_id, from } = machine.start() else {
        unreachable!("start always loads")
    };
    let events: Vec<_> = store
        .stream(&stream_id, from)
        .try_collect()
        .await
        .expect("load");
    let WriteAction::Append {
        stream_id,
        expected,
        events,
    } = machine.handle(WriteInput::Loaded { events })
    else {
        unreachable!("a deposit on an open account always appends")
    };
    (machine, stream_id, expected, events)
}

#[tokio::test]
async fn conflict_retry_commits_against_fresh_state() {
    let store = Arc::new(InMemoryStore::new());
    let repo = AggregateRepository::<Account, _>::new(Arc::clone(&store), RetryPolicy::default());
    let id = AccountId(4);

    repo.execute(id.clone(), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");

    // Writer A reaches its append (expecting v1)...
    let (mut machine, stream_id, expected, events) =
        drive_to_append(&store, 4, 10, RetryPolicy::default()).await;
    assert_eq!(expected, ExpectedVersion::Exact(Version::new(1)));

    // ...but a competing writer commits v2 first.
    store
        .append(
            &stream_id,
            expected,
            vec![NewEvent::new(AccountEvent::Deposited { amount: 99 })],
        )
        .await
        .expect("competing append wins the race");

    // A's append now conflicts; the machine must reload the delta and
    // re-decide against the fresh balance.
    let Err(StoreError::Conflict { current, .. }) =
        store.append(&stream_id, expected, events).await
    else {
        unreachable!("the expectation no longer matches")
    };
    let WriteAction::LoadStream { from, .. } = machine.handle(WriteInput::Conflict { current })
    else {
        unreachable!("a conflict with retries left reloads")
    };
    assert_eq!(from, Version::new(1), "only the delta since the fold");

    let delta: Vec<_> = store
        .stream(&stream_id, from)
        .try_collect()
        .await
        .expect("delta read");
    let WriteAction::Append {
        expected,
        events: retry_events,
        ..
    } = machine.handle(WriteInput::Loaded { events: delta })
    else {
        unreachable!("re-decide appends")
    };
    assert_eq!(expected, ExpectedVersion::Exact(Version::new(2)));

    let committed = store
        .append(&stream_id, expected, retry_events)
        .await
        .expect("the retried append commits");
    let action = machine.handle(WriteInput::Appended { committed });
    assert!(matches!(
        action,
        WriteAction::Done(WriteOutcome::Committed { .. })
    ));

    // Both deposits are on the stream: v2 (the competitor) and v3 (ours).
    let events = stream_of(&store, 4).await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[2].version, Version::new(3));
}

#[tokio::test]
async fn conflict_beyond_the_retry_budget_fails() {
    let store = Arc::new(InMemoryStore::new());
    let repo = AggregateRepository::<Account, _>::new(Arc::clone(&store), RetryPolicy::NEVER);
    let id = AccountId(5);

    repo.execute(id.clone(), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");

    // Writer A reaches its append with no retry budget...
    let (mut machine, stream_id, expected, events) =
        drive_to_append(&store, 5, 10, RetryPolicy::NEVER).await;

    // ...the competing writer wins...
    store
        .append(
            &stream_id,
            expected,
            vec![NewEvent::new(AccountEvent::Deposited { amount: 99 })],
        )
        .await
        .expect("competing append wins the race");

    // ...and A's conflict is terminal: NEVER means no reload.
    let Err(StoreError::Conflict { current, .. }) =
        store.append(&stream_id, expected, events).await
    else {
        unreachable!("the expectation no longer matches")
    };
    let action = machine.handle(WriteInput::Conflict { current });
    assert!(matches!(
        action,
        WriteAction::Done(WriteOutcome::Failed(StoreError::Conflict { .. }))
    ));
}

// -- store access from the repository -------------------------------------

/// The repository exposes its store for reads (projections, queries).
#[tokio::test]
async fn repository_exposes_the_store_for_reads() {
    let repo = repository();
    repo.execute(AccountId(5), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");

    let events = stream_of(repo.store(), 5).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, AccountEvent::Opened { owner: "me".into() });
}

// -- failure routing -------------------------------------------------------

/// A store whose reads fail: proves load errors travel through the
/// machine as `Failed` — the same shape as append errors — instead of
/// bypassing the protocol.
struct LoadFails;

impl EventStore for LoadFails {
    type Event = AccountEvent;

    async fn append(
        &self,
        _stream_id: &StreamId,
        _expected: ExpectedVersion,
        events: Vec<NewEvent<AccountEvent>>,
    ) -> Result<Vec<EventEnvelope<AccountEvent>>, StoreError> {
        Ok(events
            .into_iter()
            .map(|_| EventEnvelope {
                sequence: Sequence::new(1),
                stream_id: StreamId::from("account-6"),
                version: Version::new(1),
                event: AccountEvent::Deposited { amount: 0 },
                metadata: Metadata::default(),
            })
            .collect())
    }

    fn stream(
        &self,
        _stream_id: &StreamId,
        _from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<AccountEvent>, StoreError>> + Send {
        futures::stream::iter(vec![Err(StoreError::Unavailable)])
    }

    // The batch path uses the same failing reads as the single one.
    async fn append_batch(
        &self,
        appends: Vec<eventyr_core::batch::StreamAppend<AccountEvent>>,
    ) -> Result<Vec<eventyr_core::batch::CommittedStream<AccountEvent>>, StoreError> {
        eventyr_store::store::append_batch_fallback(self, appends).await
    }
}

#[tokio::test]
async fn a_load_failure_surfaces_as_the_failed_outcome() {
    let repo = AggregateRepository::<Account, LoadFails>::new(LoadFails, RetryPolicy::default());

    let error = repo
        .execute(AccountId(6), AccountCommand::Deposit { amount: 10 })
        .await
        .expect_err("the load fails");
    // The machine saw the failure and ended the interaction itself —
    // the driver never bypassed the protocol.
    assert!(matches!(
        error,
        ExecutionError::Store(StoreError::Unavailable)
    ));
}

#[tokio::test]
async fn execute_with_metadata_stamps_every_committed_event() {
    let store = Arc::new(InMemoryStore::new());
    let repo = AggregateRepository::<Account, _>::new(store.clone(), RetryPolicy::default());

    // Open the account, then deposit with a request-scoped metadata set.
    repo.execute(AccountId(9), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");
    let metadata =
        eventyr_core::envelope::Metadata::of_ids(Some("cmd-42".into()), Some("request-7".into()));
    repo.execute_with_metadata(
        AccountId(9),
        AccountCommand::Deposit { amount: 5 },
        metadata.clone(),
    )
    .await
    .expect("deposit");

    let events: Vec<_> = store
        .stream(
            &StreamId::for_aggregate::<Account>(&AccountId(9)),
            Version::EMPTY,
        )
        .try_collect()
        .await
        .expect("read");
    // The second commit carried the metadata; the first (plain `execute`)
    // carried none.
    assert!(events[0].metadata.causation_id.is_none());
    assert_eq!(events[1].metadata.causation_id.as_deref(), Some("cmd-42"));
    assert_eq!(
        events[1].metadata.correlation_id.as_deref(),
        Some("request-7")
    );
}

#[tokio::test]
async fn a_driver_reports_append_metrics() {
    use std::sync::Mutex;

    // A tiny in-memory Metrics: counts, in order.
    struct Spy(Mutex<Vec<u64>>);
    impl eventyr_store::metrics::Metrics for Spy {
        fn counter(&self, name: &'static str, by: u64) {
            if name == eventyr_store::metrics::names::APPENDS {
                self.0.lock().unwrap().push(by);
            }
        }
        fn gauge(&self, _: &'static str, _: u64) {}
        fn histogram(&self, _: &'static str, _: std::time::Duration) {}
    }

    let store = Arc::new(InMemoryStore::new());
    let repo = AggregateRepository::<Account, _>::new(store.clone(), RetryPolicy::default());
    let metrics = Spy(Mutex::new(Vec::new()));

    // Seed: open, then a machine driven with metrics on.
    repo.execute(AccountId(11), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");
    let mut machine = WriteMachine::<Account>::new(
        AccountId(11),
        AccountCommand::Deposit { amount: 3 },
        RetryPolicy::default(),
    );
    eventyr_store::driver::drive_write_with_metrics(&mut machine, &*store, &metrics).await;

    // The deposit committed: the driver counted the one event it appended.
    assert_eq!(metrics.0.lock().unwrap().as_slice(), &[1]);
}

// -- idempotency keys (0.7.5) ---------------------------------------------

fn key(key: &str) -> eventyr_core::envelope::Metadata {
    eventyr_core::envelope::Metadata::default().with_idempotency_key(key)
}

#[tokio::test]
async fn a_replayed_key_commits_once_and_returns_the_earlier_commit() {
    let repo = repository();
    let id = AccountId(1);
    repo.execute(id.clone(), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");

    let first = repo
        .execute_with_metadata(
            id.clone(),
            AccountCommand::Deposit { amount: 10 },
            key("pay-1"),
        )
        .await
        .expect("first");
    let ExecutionOutcome::Committed { committed, .. } = first else {
        panic!("the first run commits");
    };

    let again = repo
        .execute_with_metadata(
            id.clone(),
            AccountCommand::Deposit { amount: 10 },
            key("pay-1"),
        )
        .await
        .expect("replay");
    let ExecutionOutcome::AlreadyCommitted { committed: earlier } = again else {
        panic!("the replay returns the earlier commit");
    };
    assert_eq!(earlier, committed);
    assert_eq!(
        stream_of(repo.store(), 1).await.len(),
        2,
        "open + one deposit"
    );

    // Another key is another command.
    repo.execute_with_metadata(id, AccountCommand::Deposit { amount: 10 }, key("pay-2"))
        .await
        .expect("second payment");
    assert_eq!(stream_of(repo.store(), 1).await.len(), 3);
}

/// Racing duplicates of one keyed command: exactly one commits, every
/// other run is caught — on its first read or on its conflict retry —
/// and returns that commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_of_a_keyed_command_commit_once() {
    let repo = Arc::new(AggregateRepository::<Account, _>::new(
        Arc::new(InMemoryStore::new()),
        RetryPolicy::new(16),
    ));
    repo.execute(AccountId(1), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");

    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let repo = Arc::clone(&repo);
            tokio::spawn(async move {
                repo.execute_with_metadata(
                    AccountId(1),
                    AccountCommand::Deposit { amount: 10 },
                    key("pay-1"),
                )
                .await
            })
        })
        .collect();
    let mut committed = 0;
    for task in tasks {
        match task.await.expect("task").expect("execute") {
            ExecutionOutcome::Committed { .. } => committed += 1,
            ExecutionOutcome::AlreadyCommitted { .. } => {}
            ExecutionOutcome::Noop => panic!("a deposit decides an event"),
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(
        stream_of(repo.store(), 1).await.len(),
        2,
        "open + one deposit"
    );
}

// -- stream lifecycle (0.7.6) ----------------------------------------------

#[tokio::test]
async fn a_command_against_a_closed_stream_fails_with_stream_closed() {
    let repo = repository();
    repo.execute(AccountId(1), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");
    repo.store()
        .close_stream(&StreamId::for_aggregate::<Account>(&AccountId(1)))
        .await
        .expect("close");
    let error = repo
        .execute(AccountId(1), AccountCommand::Deposit { amount: 5 })
        .await
        .expect_err("a closed stream refuses the append");
    assert!(
        matches!(
            error,
            ExecutionError::Store(StoreError::StreamClosed { .. })
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_command_against_a_truncated_stream_fails_rather_than_folding_a_partial_history() {
    let repo = repository();
    repo.execute(AccountId(1), AccountCommand::Open { owner: "me".into() })
        .await
        .expect("open");
    repo.execute(AccountId(1), AccountCommand::Deposit { amount: 5 })
        .await
        .expect("deposit");
    repo.store()
        .truncate_before(
            &StreamId::for_aggregate::<Account>(&AccountId(1)),
            Version::new(2),
        )
        .await
        .expect("truncate");
    // Without the Opened event the fold would say "not open" and reject
    // a deposit that is valid: the read must fail instead.
    let error = repo
        .execute(AccountId(1), AccountCommand::Deposit { amount: 5 })
        .await
        .expect_err("the full history is gone");
    assert!(
        matches!(error, ExecutionError::Store(StoreError::Truncated { .. })),
        "{error:?}"
    );
}
