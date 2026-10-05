//! Driver tests: run a projector against the in-memory store and assert
//! on the at-least-once / catch-up behavior end-to-end, and scripted
//! driver tests for the failure paths a real store can't reach.

use futures::future;
use std::sync::{Arc, Mutex};

use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::subscription::{Batch, Checkpoint, SubscriptionOutcome, SubscriptionPolicy};
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::memory::InMemoryStore;
use eventyr_store::store::EventStore;
use eventyr_subscription::prelude::*;

/// A projection that remembers applied sequence numbers: idempotence
/// made checkable.
#[derive(Default)]
struct Seen(Arc<Mutex<Vec<u64>>>);

impl Projection for Seen {
    type Event = u64;
    type Error = core::convert::Infallible;

    async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), Self::Error> {
        self.0
            .lock()
            .expect("poisoned")
            .push(event.sequence.as_u64());
        Ok(())
    }
}

fn envelope(sequence: u64, event: u64) -> EventEnvelope<u64> {
    EventEnvelope {
        sequence: Sequence::new(sequence),
        stream_id: StreamId::from("account-1"),
        version: Version::new(sequence),
        event,
        metadata: Metadata::default(),
    }
}

async fn populate(store: &InMemoryStore<u64>, events: &[u64]) {
    let sid = StreamId::from("account-1");
    let new: Vec<_> = events.iter().copied().map(NewEvent::new).collect();
    store
        .append(&sid, ExpectedVersion::Empty, new)
        .await
        .expect("append");
}

#[tokio::test]
async fn a_fresh_projection_catches_up_and_stops_at_catch_up() {
    let store = InMemoryStore::new();
    populate(&store, &[10, 20, 30]).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let projector = Projector::new(
        "balance",
        StoreSubscription::new(store),
        InMemoryCheckpointStore::new(),
        Seen(seen.clone()),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up());

    let outcome = projector.run(|_| future::ready(())).await.expect("run");

    let checkpoint = match outcome {
        SubscriptionOutcome::CaughtUp { checkpoint } => checkpoint,
        other => panic!("expected CaughtUp, got {other:?}"),
    };
    assert_eq!(checkpoint, Checkpoint::new(Sequence::new(3)));
    assert_eq!(
        *seen.lock().expect("poisoned"),
        vec![1, 2, 3],
        "every event applied exactly once"
    );
}

#[tokio::test]
async fn a_restart_resumes_from_the_checkpoint() {
    let store = InMemoryStore::new();
    let checkpoints = InMemoryCheckpointStore::new();
    populate(&store, &[10, 20]).await;

    // First run: catch up from ORIGIN.
    let projector = Projector::new(
        "balance",
        StoreSubscription::new(&store),
        &checkpoints,
        Seen(Arc::new(Mutex::new(Vec::new()))),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up());
    projector.run(|_| future::ready(())).await.expect("first");
    assert_eq!(
        checkpoints.load("balance").await.expect("load"),
        Checkpoint::new(Sequence::new(2))
    );

    // Events arrive; second run: resumes at the checkpoint.
    store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Any,
            vec![NewEvent::new(30)],
        )
        .await
        .expect("append");

    let seen = Arc::new(Mutex::new(Vec::new()));
    let projector = Projector::new(
        "balance",
        StoreSubscription::new(&store),
        &checkpoints,
        Seen(seen.clone()),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up());
    let outcome = projector.run(|_| future::ready(())).await.expect("second");

    assert!(matches!(
        outcome,
        SubscriptionOutcome::CaughtUp { checkpoint }
            if checkpoint == Checkpoint::new(Sequence::new(3))
    ));
    // Only the new event re-applied; 1 and 2 are behind the checkpoint.
    assert_eq!(*seen.lock().expect("poisoned"), vec![3]);
}

#[tokio::test]
async fn names_are_scoped_across_projections() {
    let store = InMemoryStore::new();
    populate(&store, &[10, 20]).await;
    let checkpoints = InMemoryCheckpointStore::new();

    for name in ["one", "two"] {
        let projector = Projector::new(
            name,
            StoreSubscription::new(&store),
            &checkpoints,
            Seen(Arc::new(Mutex::new(Vec::new()))),
        )
        .with_policy(SubscriptionPolicy::default().stop_at_catch_up());
        projector.run(|_| future::ready(())).await.expect("run");
        assert_eq!(
            checkpoints.load(name).await.expect("load"),
            Checkpoint::new(Sequence::new(2)),
            "each projection resumed its own position"
        );
    }
}

// -- pagination & mid-batch errors through the real runner ----------------

/// A checkpoint store that records every ack, wrapping the in-memory one.
struct RecordingCheckpoints {
    inner: InMemoryCheckpointStore,
    acks: Arc<Mutex<Vec<Checkpoint>>>,
}

impl CheckpointStore for RecordingCheckpoints {
    async fn load(&self, name: &str) -> Result<Checkpoint, StoreError> {
        self.inner.load(name).await
    }

    async fn store(&self, name: &str, checkpoint: Checkpoint) -> Result<(), StoreError> {
        self.acks.lock().expect("poisoned").push(checkpoint);
        self.inner.store(name, checkpoint).await
    }
}

/// With `batch_size` 1 over three events, the runner must run a fetch →
/// apply → ack cycle per event, advancing the checkpoint one step each
/// time — this is the path upstream gaps and partial failures re-enter.
#[tokio::test]
async fn batch_size_one_cycles_fetch_ack_per_event() {
    let store = InMemoryStore::new();
    populate(&store, &[10, 20, 30]).await;
    let acks = Arc::new(Mutex::new(Vec::new()));
    let checkpoints = RecordingCheckpoints {
        inner: InMemoryCheckpointStore::new(),
        acks: acks.clone(),
    };
    let seen = Arc::new(Mutex::new(Vec::new()));

    let projector = Projector::new(
        "balance",
        StoreSubscription::new(&store),
        checkpoints,
        Seen(seen.clone()),
    )
    .with_policy(
        SubscriptionPolicy::new(1, Default::default(), Default::default()).stop_at_catch_up(),
    );
    let outcome = projector.run(|_| future::ready(())).await.expect("run");

    assert!(matches!(
        outcome,
        SubscriptionOutcome::CaughtUp { checkpoint }
            if checkpoint == Checkpoint::new(Sequence::new(3))
    ));
    // One ack per single-event batch: the checkpoint advances 1 → 2 → 3.
    assert_eq!(
        *acks.lock().expect("poisoned"),
        vec![
            Checkpoint::new(Sequence::new(1)),
            Checkpoint::new(Sequence::new(2)),
            Checkpoint::new(Sequence::new(3)),
        ],
        "a fetch→ack cycle per batch"
    );
    assert_eq!(*seen.lock().expect("poisoned"), vec![1, 2, 3]);
}

/// A source whose stream fails after delivering `fail_after` events: the
/// mid-batch error path a real store can't reach on demand.
struct FailAfter {
    events: Vec<EventEnvelope<u64>>,
    fail_after: usize,
}

impl SubscriptionSource for &FailAfter {
    type Event = u64;

    async fn fetch(&self, from: Checkpoint, max: usize) -> Result<Batch<u64>, StoreError> {
        let floor = from.as_sequence().as_u64();
        let rest: Vec<_> = self
            .events
            .iter()
            .filter(|e| e.sequence.as_u64() > floor)
            .take(max)
            .cloned()
            .collect();
        if rest.len() > self.fail_after {
            return Err(StoreError::other("stream broke mid-batch"));
        }
        let upper = rest.last().map(|e| Checkpoint::new(e.sequence));
        Ok(Batch::new(rest, upper))
    }
}

/// A fetch that errors part-way through a batch surfaces as a fatal
/// `Failed` — the machine does not ack a partial batch.
#[tokio::test]
async fn a_source_error_surfaces_as_a_failed_outcome() {
    let source = FailAfter {
        events: vec![envelope(1, 10), envelope(2, 20)],
        fail_after: 1,
    };
    let projector = Projector::new(
        "balance",
        &source,
        InMemoryCheckpointStore::new(),
        Seen(Arc::new(Mutex::new(Vec::new()))),
    )
    // Batch over both events so the stream fails mid-batch.
    .with_policy(SubscriptionPolicy {
        batch_size: 2,
        ..SubscriptionPolicy::default()
    });

    let outcome = projector.run(|_| future::ready(())).await.expect("run");

    assert!(
        matches!(outcome, SubscriptionOutcome::Failed(StoreError::Other(_))),
        "a mid-batch source error is fatal, got {outcome:?}"
    );
}

/// 0.7.2: a caught-up projector woken by the store's commit signal
/// applies a new event long before its idle sleep would end.
#[tokio::test]
async fn a_commit_wakes_an_idle_projector() {
    use std::time::Duration;

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[10]).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let projector = Projector::new(
        "woken",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Seen(seen.clone()),
    )
    // An idle sleep far longer than the test may take.
    .with_policy(SubscriptionPolicy::new(
        64,
        Duration::from_secs(3600),
        Duration::from_secs(1),
    ))
    .wake_on(Arc::clone(&store));
    let run = tokio::spawn(projector.run_woken(tokio::time::sleep));

    wait_until(&seen, 1).await;
    store
        .append(
            &StreamId::from("account-2"),
            ExpectedVersion::Empty,
            vec![NewEvent::new(20)],
        )
        .await
        .expect("append");
    tokio::time::timeout(Duration::from_secs(5), wait_until(&seen, 2))
        .await
        .expect("the commit woke the projector well before its idle sleep ended");
    assert_eq!(*seen.lock().expect("poisoned"), vec![1, 2]);
    run.abort();
}

/// Without a signal the same projector would sleep its full idle time:
/// the wake-up is what makes the difference above.
#[tokio::test]
async fn without_a_signal_an_idle_projector_waits_out_its_sleep() {
    use std::time::Duration;

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[10]).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let projector = Projector::new(
        "unwoken",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Seen(seen.clone()),
    )
    .with_policy(SubscriptionPolicy::new(
        64,
        Duration::from_secs(3600),
        Duration::from_secs(1),
    ));
    let run = tokio::spawn(projector.run(tokio::time::sleep));

    wait_until(&seen, 1).await;
    store
        .append(
            &StreamId::from("account-2"),
            ExpectedVersion::Empty,
            vec![NewEvent::new(20)],
        )
        .await
        .expect("append");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), wait_until(&seen, 2))
            .await
            .is_err(),
        "no signal: the projector is still asleep"
    );
    run.abort();
}

/// A backoff is not cut short by commits: a failing projection is
/// retried on its schedule, not hammered on every write. Paused time:
/// the clock only moves when every task is waiting, so "not within
/// 300 ms" is exact, and the hour-long backoff ends when the test waits
/// for it.
#[tokio::test(start_paused = true)]
async fn a_commit_does_not_cut_a_backoff_short() {
    use std::time::Duration;

    struct FailOnce(
        Arc<Mutex<Vec<u64>>>,
        Option<tokio::sync::oneshot::Sender<()>>,
    );
    impl Projection for FailOnce {
        type Event = u64;
        type Error = &'static str;
        async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), Self::Error> {
            if let Some(failed) = self.1.take() {
                let _ = failed.send(());
                return Err("first apply fails");
            }
            self.0
                .lock()
                .expect("poisoned")
                .push(event.sequence.as_u64());
            Ok(())
        }
    }

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[10]).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (failed, first_failure) = tokio::sync::oneshot::channel();
    let projector = Projector::new(
        "backoff",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        FailOnce(seen.clone(), Some(failed)),
    )
    .with_policy(SubscriptionPolicy::new(
        64,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
    ))
    .wake_on(Arc::clone(&store));
    let run = tokio::spawn(projector.run_woken(tokio::time::sleep));

    // The first apply failed; the projector backs off for an hour.
    first_failure.await.expect("the projection failed once");
    store
        .append(
            &StreamId::from("account-2"),
            ExpectedVersion::Empty,
            vec![NewEvent::new(20)],
        )
        .await
        .expect("append");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), wait_until(&seen, 1))
            .await
            .is_err(),
        "the backoff ran its course despite the commit"
    );
    // Once the hour is up the projector retries and catches up.
    wait_until(&seen, 2).await;
    assert_eq!(*seen.lock().expect("poisoned"), vec![1, 2]);
    run.abort();
}

async fn wait_until(seen: &Arc<Mutex<Vec<u64>>>, count: usize) {
    while seen.lock().expect("poisoned").len() < count {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// 0.7.4: a filtered projector checkpoints past events its filter
/// skipped. Without the scan bound its checkpoint would stay at its
/// last match, and every restart would re-read the unmatched tail.
#[tokio::test]
async fn a_filtered_projector_checkpoints_past_skipped_events() {
    #[derive(Clone, Copy, PartialEq, Debug)]
    struct Named(u64);
    impl eventyr_core::event_name::EventName for Named {
        fn event_name(&self) -> &'static str {
            "Named"
        }
    }

    struct Count(Arc<Mutex<Vec<u64>>>);
    impl Projection for Count {
        type Event = Named;
        type Error = core::convert::Infallible;
        async fn apply(&mut self, event: &EventEnvelope<Named>) -> Result<(), Self::Error> {
            self.0.lock().expect("poisoned").push(event.event.0);
            Ok(())
        }
    }

    let store = Arc::new(InMemoryStore::new());
    store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Empty,
            vec![NewEvent::new(Named(1))],
        )
        .await
        .expect("append");
    let noise: Vec<_> = (0..500).map(|n| NewEvent::new(Named(n))).collect();
    store
        .append(&StreamId::from("order-1"), ExpectedVersion::Empty, noise)
        .await
        .expect("append");

    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let outcome = Projector::new(
        "accounts",
        FilteredSubscription::new(
            Arc::clone(&store),
            EventFilter::all().stream_prefix("account-"),
        )
        .scan_limit(64),
        Arc::clone(&checkpoints),
        Count(seen.clone()),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .run(|_| future::ready(()))
    .await
    .expect("run");

    assert_eq!(*seen.lock().expect("poisoned"), vec![1]);
    let SubscriptionOutcome::CaughtUp { checkpoint } = outcome else {
        panic!("expected CaughtUp, got {outcome:?}");
    };
    assert_eq!(
        checkpoint,
        Checkpoint::new(Sequence::new(501)),
        "the checkpoint reached the head, not the last match"
    );
    assert_eq!(
        CheckpointStore::load(&*checkpoints, "accounts")
            .await
            .expect("load"),
        checkpoint,
        "and it was persisted"
    );
}

/// 0.7.5 end to end: a saga's event is redelivered after its commands
/// were dispatched but before the ack (the checkpoint write fails), so
/// the saga re-issues the command. The command's idempotency key makes
/// the second dispatch return the first commit: the account is debited
/// once.
#[tokio::test]
async fn a_redelivered_saga_event_dispatches_its_command_once() {
    use eventyr_core::saga::{Saga, SagaCommand};
    use eventyr_core::testing::account::{Account, AccountCommand, AccountEvent, AccountId};
    use eventyr_core::write::RetryPolicy;
    use eventyr_store::repository::{AggregateRepository, ExecutionOutcome};

    /// Every deposit on account 2 withdraws 1 from account 1.
    #[derive(Clone)]
    struct Fee;
    impl Saga for Fee {
        type Event = AccountEvent;
        type Command = AccountCommand;
        fn name(&self) -> &str {
            "fee"
        }
        fn react(&self, event: &EventEnvelope<AccountEvent>) -> Vec<(StreamId, AccountCommand)> {
            let fee_payer = StreamId::for_aggregate::<Account>(&AccountId(1));
            match event.event {
                AccountEvent::Deposited { .. } if event.stream_id != fee_payer => {
                    vec![(fee_payer, AccountCommand::Withdraw { amount: 1 })]
                }
                _ => vec![],
            }
        }
    }

    /// Fails the first `store` — the crash between dispatch and ack.
    struct FlakyCheckpoints {
        inner: InMemoryCheckpointStore,
        failed: std::sync::atomic::AtomicBool,
    }
    impl CheckpointStore for FlakyCheckpoints {
        async fn load(&self, name: &str) -> Result<Checkpoint, StoreError> {
            self.inner.load(name).await
        }
        async fn store(&self, name: &str, checkpoint: Checkpoint) -> Result<(), StoreError> {
            if !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Err(StoreError::Unavailable);
            }
            self.inner.store(name, checkpoint).await
        }
    }

    let store = Arc::new(InMemoryStore::<AccountEvent>::new());
    let repo = Arc::new(AggregateRepository::<Account, _>::new(
        Arc::clone(&store),
        RetryPolicy::default(),
    ));
    for id in [1, 2] {
        repo.execute(AccountId(id), AccountCommand::Open { owner: "me".into() })
            .await
            .expect("open");
    }
    repo.execute(AccountId(1), AccountCommand::Deposit { amount: 10 })
        .await
        .expect("fund the payer");
    repo.execute(AccountId(2), AccountCommand::Deposit { amount: 50 })
        .await
        .expect("the deposit that triggers the fee");
    let head = futures::TryStreamExt::try_collect::<Vec<_>>(
        eventyr_store::store::StreamsAll::stream_all(&*store, Sequence::START),
    )
    .await
    .expect("read")
    .len() as u64;

    let dispatched = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = {
        let repo = Arc::clone(&repo);
        let dispatched = Arc::clone(&dispatched);
        move |command: SagaCommand<AccountCommand>| {
            let repo = Arc::clone(&repo);
            let dispatched = Arc::clone(&dispatched);
            async move {
                let outcome = repo
                    .execute_with_metadata(AccountId(1), command.command, command.metadata)
                    .await
                    .map_err(|e| StoreError::other(e.to_string()))?;
                dispatched
                    .lock()
                    .expect("poisoned")
                    .push(matches!(outcome, ExecutionOutcome::Committed { .. }));
                Ok(())
            }
        }
    };

    let outcome = Projector::new(
        "fee",
        StoreSubscription::new(Arc::clone(&store)),
        FlakyCheckpoints {
            inner: InMemoryCheckpointStore::new(),
            failed: std::sync::atomic::AtomicBool::new(false),
        },
        SagaProjection::new(Fee, dispatcher),
    )
    .with_policy(
        SubscriptionPolicy::new(64, std::time::Duration::ZERO, std::time::Duration::ZERO)
            .stop_at_catch_up(),
    )
    .run(|_| future::ready(()))
    .await
    .expect("run");

    // The fee command was dispatched twice (the redelivery), but only
    // the first dispatch committed.
    assert_eq!(*dispatched.lock().expect("poisoned"), vec![true, false]);
    let payer: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(EventStore::stream(
        &*store,
        &StreamId::for_aggregate::<Account>(&AccountId(1)),
        Version::EMPTY,
    ))
    .await
    .expect("read");
    let withdrawals = payer
        .iter()
        .filter(|e| matches!(e.event, AccountEvent::Withdrawn { .. }))
        .count();
    assert_eq!(withdrawals, 1, "the fee was charged once");
    // Caught up through everything, including the fee's own withdrawal.
    assert!(matches!(
        outcome,
        SubscriptionOutcome::CaughtUp { checkpoint }
            if checkpoint == Checkpoint::new(Sequence::new(head + 1))
    ));
}

/// 0.7.7 end to end: one event the projection can never apply, among
/// good ones. With parking the projector records it, catches up past it,
/// and the event can be listed and — once the projection is fixed —
/// replayed and removed. Without parking (the default) it stalls.
#[tokio::test]
async fn a_poison_event_is_parked_listed_and_replayed() {
    /// Rejects the value 13 until `fixed` is set. Idempotent, as the
    /// at-least-once contract requires: a retry redelivers the batch
    /// from the last ack, so 11 and 12 arrive again with 13.
    struct Picky {
        seen: Arc<Mutex<Vec<u64>>>,
        fixed: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Projection for Picky {
        type Event = u64;
        type Error = String;
        async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), String> {
            if event.event == 13 && !self.fixed.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("thirteen is unlucky".into());
            }
            let mut seen = self.seen.lock().expect("poisoned");
            if !seen.contains(&event.event) {
                seen.push(event.event);
            }
            Ok(())
        }
    }

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[11, 12, 13, 14, 15]).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fixed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let parked = Arc::new(InMemoryParkedStore::new());

    let policy = SubscriptionPolicy::new(64, std::time::Duration::ZERO, std::time::Duration::ZERO)
        .stop_at_catch_up();
    let outcome = Projector::new(
        "picky",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Picky {
            seen: Arc::clone(&seen),
            fixed: Arc::clone(&fixed),
        },
    )
    .with_policy(policy)
    .park_into(Arc::clone(&parked), FailurePolicy::Park { retries: 2 })
    .run(|_| future::ready(()))
    .await
    .expect("run");

    assert!(matches!(
        outcome,
        SubscriptionOutcome::CaughtUp { checkpoint } if checkpoint == Checkpoint::new(Sequence::new(5))
    ));
    assert_eq!(
        *seen.lock().expect("poisoned"),
        vec![11, 12, 14, 15],
        "13 skipped"
    );

    let list = parked.list("picky").await.expect("list");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].envelope.event, 13);
    assert_eq!(list[0].attempts, 3, "the first try and two retries");
    assert!(list[0].error.contains("unlucky"), "{}", list[0].error);

    // Fix the projection, replay the parked event, remove it.
    fixed.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut projection = Picky {
        seen: Arc::clone(&seen),
        fixed: Arc::clone(&fixed),
    };
    for entry in list {
        projection.apply(&entry.envelope).await.expect("replay");
        parked
            .remove("picky", entry.envelope.sequence)
            .await
            .expect("remove");
    }
    assert!(parked.list("picky").await.expect("list").is_empty());
    assert!(seen.lock().expect("poisoned").contains(&13));
}

/// The combinator's point: without it the projection itself must
/// remember what it folded, or the redelivered batch counts the events
/// before the poison one twice. `SkipRedelivered` dedupes by
/// (stream, version), so only 13's retries reach the inner projection.
#[tokio::test(start_paused = true)]
async fn skip_redelivered_keeps_a_poison_event_from_recounting_its_batch() {
    use std::collections::BTreeMap;

    use eventyr_subscription::runner::SkipRedelivered;

    /// Counts every envelope it is handed — deliberately naive about
    /// redelivery, the mistake the combinator exists to forgive.
    struct Counting {
        folds: Arc<Mutex<BTreeMap<u64, usize>>>,
    }
    impl Projection for Counting {
        type Event = u64;
        type Error = String;
        async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), String> {
            if event.event == 13 {
                return Err("thirteen is unlucky".into());
            }
            *self
                .folds
                .lock()
                .expect("poisoned")
                .entry(event.event)
                .or_default() += 1;
            Ok(())
        }
    }

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[11, 12, 13, 14, 15]).await;
    let folds = Arc::new(Mutex::new(BTreeMap::<u64, usize>::new()));
    let parked = Arc::new(InMemoryParkedStore::new());

    let outcome = Projector::new(
        "counting",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        SkipRedelivered::new(Counting {
            folds: Arc::clone(&folds),
        }),
    )
    .with_policy(
        SubscriptionPolicy::new(64, std::time::Duration::ZERO, std::time::Duration::ZERO)
            .stop_at_catch_up(),
    )
    .park_into(Arc::clone(&parked), FailurePolicy::Park { retries: 2 })
    .run(|_| future::ready(()))
    .await
    .expect("run");

    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    let folds = folds.lock().expect("poisoned");
    assert_eq!(
        *folds,
        BTreeMap::from([(11, 1), (12, 1), (14, 1), (15, 1)]),
        "the redeliveries before 13 folded once, and 13 never"
    );
}

/// The default halts: the projector rejects 13 again and again and
/// its checkpoint never passes the event before it.
#[tokio::test(start_paused = true)]
async fn without_parking_a_poison_event_stalls_the_projector() {
    /// Rejects 13, reporting each rejection's sequence.
    struct Never(tokio::sync::mpsc::UnboundedSender<u64>);
    impl Projection for Never {
        type Event = u64;
        type Error = &'static str;
        async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), &'static str> {
            if event.event != 13 {
                return Ok(());
            }
            let _ = self.0.send(event.sequence.as_u64());
            Err("no")
        }
    }
    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[11, 13, 15]).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (rejected, mut rejections) = tokio::sync::mpsc::unbounded_channel();
    let run = tokio::spawn(
        Projector::new(
            "stalled",
            StoreSubscription::new(Arc::clone(&store)),
            Arc::clone(&checkpoints),
            Never(rejected),
        )
        .with_policy(
            SubscriptionPolicy::new(
                1,
                std::time::Duration::ZERO,
                std::time::Duration::from_millis(1),
            )
            .stop_at_catch_up(),
        )
        .run(tokio::time::sleep),
    );
    // Many redeliveries, every one of them the same event.
    for _ in 0..20 {
        assert_eq!(rejections.recv().await, Some(2), "13 is redelivered");
    }
    assert!(
        !run.is_finished(),
        "halting: the projector never gets past 13"
    );
    assert_eq!(
        CheckpointStore::load(&*checkpoints, "stalled")
            .await
            .expect("load"),
        Checkpoint::new(Sequence::new(1))
    );
    run.abort();
}

/// Counts every counter it is given, by name.
#[derive(Default)]
struct Counters(Mutex<std::collections::BTreeMap<&'static str, u64>>);

impl Counters {
    fn get(&self, name: &str) -> u64 {
        self.0
            .lock()
            .expect("poisoned")
            .get(name)
            .copied()
            .unwrap_or(0)
    }
}

impl eventyr_store::metrics::Metrics for Counters {
    fn counter(&self, name: &'static str, by: u64) {
        *self.0.lock().expect("poisoned").entry(name).or_default() += by;
    }
    fn gauge(&self, _: &'static str, _: u64) {}
    fn histogram(&self, _: &'static str, _: std::time::Duration) {}
}

impl eventyr_store::metrics::Metrics for &'static Counters {
    fn counter(&self, name: &'static str, by: u64) {
        (**self).counter(name, by);
    }
    fn gauge(&self, _: &'static str, _: u64) {}
    fn histogram(&self, _: &'static str, _: std::time::Duration) {}
}

/// Rejects every 13.
struct NoThirteen;
impl Projection for NoThirteen {
    type Event = u64;
    type Error = &'static str;
    async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), &'static str> {
        if event.event == 13 { Err("no") } else { Ok(()) }
    }
}

/// The projector reports through the metrics it is given, so the parked
/// counter — the one to alert on — fires through the bundled runner.
#[tokio::test]
async fn a_projector_reports_parks_through_its_metrics() {
    use eventyr_store::metrics::names::{PARKED_EVENTS, PROJECTED_EVENTS};

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[11, 13, 15]).await;
    let counters: &'static Counters = Box::leak(Box::default());
    let outcome = Projector::new(
        "counted",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        NoThirteen,
    )
    .with_policy(
        SubscriptionPolicy::new(64, std::time::Duration::ZERO, std::time::Duration::ZERO)
            .stop_at_catch_up(),
    )
    .park_into(
        InMemoryParkedStore::new(),
        FailurePolicy::Park { retries: 0 },
    )
    .with_metrics(counters)
    .run(|_| future::ready(()))
    .await
    .expect("run");

    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    assert_eq!(counters.get(PARKED_EVENTS), 1);
    assert_eq!(counters.get(PROJECTED_EVENTS), 2);
}

/// A parking policy with nowhere to park is refused up front, not
/// retried forever against `NoParking`.
#[tokio::test]
async fn a_parking_policy_without_a_parked_store_refuses_to_run() {
    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[11, 13]).await;
    let refused = Projector::new(
        "misconfigured",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        NoThirteen,
    )
    .with_policy(
        SubscriptionPolicy::default()
            .stop_at_catch_up()
            .on_failure(FailurePolicy::Park { retries: 0 }),
    )
    .run(|_| future::ready(()))
    .await;

    let Err(error) = refused else {
        panic!("expected a refusal, got {refused:?}");
    };
    assert!(error.to_string().contains("park_into"), "{error}");
}

/// Driven directly, a park the store refuses — here `NoParking` — is
/// counted, so a stalled projection shows up on a dashboard.
#[tokio::test(start_paused = true)]
async fn a_refused_park_is_counted() {
    use eventyr_core::subscription::SubscriptionMachine;
    use eventyr_store::metrics::names::{PARK_FAILURES, PARKED_EVENTS};

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[11, 13]).await;
    let counters: &'static Counters = Box::leak(Box::default());
    let mut machine = SubscriptionMachine::new(
        SubscriptionPolicy::new(
            64,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(1),
        )
        .on_failure(FailurePolicy::Park { retries: 0 }),
        Checkpoint::ORIGIN,
    );
    let source = StoreSubscription::new(Arc::clone(&store));
    let checkpoints = InMemoryCheckpointStore::new();
    let run = drive_projector(
        &mut machine,
        "refused",
        &source,
        &checkpoints,
        NoThirteen,
        tokio::time::sleep,
        DriverPorts::new().with_metrics(counters),
    );
    // Three backoffs' worth of virtual time, then give up on the run.
    let timed_out = tokio::time::timeout(std::time::Duration::from_millis(3_500), run).await;

    assert!(timed_out.is_err(), "the projector is stalled on 13");
    assert_eq!(counters.get(PARK_FAILURES), 4, "one refused park per try");
    assert_eq!(counters.get(PARKED_EVENTS), 0);
}

/// A failed checkpoint write is counted and the batch redelivered.
#[tokio::test]
async fn a_failed_ack_is_counted() {
    use eventyr_store::metrics::names::ACK_FAILURES;

    /// Fails the first `store`.
    struct FlakyOnce {
        inner: InMemoryCheckpointStore,
        failed: std::sync::atomic::AtomicBool,
    }
    impl CheckpointStore for FlakyOnce {
        async fn load(&self, name: &str) -> Result<Checkpoint, StoreError> {
            self.inner.load(name).await
        }
        async fn store(&self, name: &str, checkpoint: Checkpoint) -> Result<(), StoreError> {
            if !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Err(StoreError::Unavailable);
            }
            self.inner.store(name, checkpoint).await
        }
    }

    let store = Arc::new(InMemoryStore::new());
    populate(&store, &[10]).await;
    let counters: &'static Counters = Box::leak(Box::default());
    let outcome = Projector::new(
        "flaky",
        StoreSubscription::new(Arc::clone(&store)),
        FlakyOnce {
            inner: InMemoryCheckpointStore::new(),
            failed: std::sync::atomic::AtomicBool::new(false),
        },
        Seen::default(),
    )
    .with_policy(
        SubscriptionPolicy::new(64, std::time::Duration::ZERO, std::time::Duration::ZERO)
            .stop_at_catch_up(),
    )
    .with_metrics(counters)
    .run(|_| future::ready(()))
    .await
    .expect("run");

    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    assert_eq!(counters.get(ACK_FAILURES), 1);
}
