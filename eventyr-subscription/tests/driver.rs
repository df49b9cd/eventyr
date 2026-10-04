//! Driver tests: run a projector against the in-memory store and assert
//! on the at-least-once / catch-up behavior end-to-end, and scripted
//! driver tests for the failure paths a real store can't reach.

use futures::future;
use std::sync::{Arc, Mutex};

use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
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
        self.0.lock().expect("poisoned").push(event.sequence.as_u64());
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
            SubscriptionInput::Applied,     // applied 1
            SubscriptionInput::ApplyFailed, // failed 2
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
