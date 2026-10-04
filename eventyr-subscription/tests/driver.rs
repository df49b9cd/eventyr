//! Driver tests: run a projector against the in-memory store and assert
//! on the at-least-once / catch-up behavior end-to-end, and scripted
//! driver tests for the failure paths a real store can't reach.

use futures::future;
use std::sync::{Arc, Mutex};

use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::subscription::{
    Batch, Checkpoint, SubscriptionAction, SubscriptionInput, SubscriptionMachine,
    SubscriptionOutcome, SubscriptionPolicy,
};
use eventyr_core::testing::projector_scripted;
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

// -- scripted driver: the failure paths a real store can't reach --------

type Scripted = Vec<SubscriptionAction<u64>>;

fn machine() -> SubscriptionMachine<u64> {
    SubscriptionMachine::new(SubscriptionPolicy::default(), Checkpoint::ORIGIN)
}

#[test]
fn apply_failed_redelivers_the_whole_batch() {
    let mut m = machine();
    let actions: Scripted = projector_scripted(
        &mut m,
        vec![
            SubscriptionInput::Fetched {
                batch: Batch::new(
                    vec![envelope(1, 10), envelope(2, 20)],
                    Some(Checkpoint::new(Sequence::new(2))),
                ),
            },
            SubscriptionInput::Applied, // applied 1
            SubscriptionInput::ApplyFailed {
                error: StoreError::other("boom"),
            }, // failed 2
            SubscriptionInput::Slept,
            SubscriptionInput::Fetched {
                batch: Batch::new(
                    vec![envelope(1, 10), envelope(2, 20)],
                    Some(Checkpoint::new(Sequence::new(2))),
                ),
            },
            SubscriptionInput::Applied, // re-applied 1
        ],
    );
    // Backoff evidence: after ApplyFailed the machine drops the pending
    // batch and sleeps `retry_sleep` before re-fetching. The sequence is
    // Fetch[0] → Apply(1)[1] → Apply(2)[2] → (ApplyFailed) Sleep[3].
    assert!(matches!(
        actions[3],
        SubscriptionAction::Sleep { for_ } if for_ == SubscriptionPolicy::default().retry_sleep
    ));
    assert!(matches!(
        actions.last(),
        Some(SubscriptionAction::Apply { envelope })
            if envelope.sequence == Sequence::new(2)
    ));
    // The checkpoint is still at the origin: only events ≤ it count as
    // durable, so both events redelivered.
    assert_eq!(m.checkpoint(), Checkpoint::ORIGIN);
}

#[test]
fn ack_failed_redelivers_the_whole_batch() {
    let mut m = machine();
    let actions: Scripted = projector_scripted(
        &mut m,
        vec![
            SubscriptionInput::Fetched {
                batch: Batch::new(
                    vec![envelope(1, 10)],
                    Some(Checkpoint::new(Sequence::new(1))),
                ),
            },
            SubscriptionInput::Applied,
            SubscriptionInput::AckFailed,
            SubscriptionInput::Slept,
            SubscriptionInput::Fetched {
                batch: Batch::new(
                    vec![envelope(1, 10)],
                    Some(Checkpoint::new(Sequence::new(1))),
                ),
            },
        ],
    );
    assert!(matches!(
        actions.last(),
        Some(SubscriptionAction::Apply { envelope })
            if envelope.event == 10
    ));
    assert_eq!(m.checkpoint(), Checkpoint::ORIGIN);
}

#[test]
fn an_idle_poll_stops_at_the_catch_up_policy() {
    let mut m = SubscriptionMachine::<u64>::new(
        SubscriptionPolicy::default().stop_at_catch_up(),
        Checkpoint::new(Sequence::new(5)),
    );
    let actions = projector_scripted(
        &mut m,
        vec![SubscriptionInput::Fetched {
            batch: Batch::empty(),
        }],
    );
    assert!(matches!(
        actions.last(),
        Some(SubscriptionAction::Done(SubscriptionOutcome::CaughtUp { checkpoint }))
            if *checkpoint == Checkpoint::new(Sequence::new(5))
    ));
}

#[test]
fn shutdown_during_idle_drains_a_final_poll() {
    let mut m = machine();
    let actions = projector_scripted(
        &mut m,
        vec![
            SubscriptionInput::Fetched {
                batch: Batch::new(
                    vec![envelope(1, 1)],
                    Some(Checkpoint::new(Sequence::new(1))),
                ),
            },
            SubscriptionInput::Applied,
            SubscriptionInput::Acked,
            SubscriptionInput::Fetched {
                batch: Batch::empty(),
            },
            SubscriptionInput::Shutdown,
            SubscriptionInput::Fetched {
                batch: Batch::empty(),
            },
        ],
    );
    assert!(matches!(
        actions.last(),
        Some(SubscriptionAction::Done(SubscriptionOutcome::Stopped { checkpoint }))
            if *checkpoint == Checkpoint::new(Sequence::new(1))
    ));
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
