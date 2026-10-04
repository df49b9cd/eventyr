//! End-to-end tests for the read path: raw payloads in a real
//! `InMemoryStore`, upcast through a chain at fetch time, folded by a
//! projection, checkpointed and rebuilt — all through
//! `eventyr-subscription`'s shipped runner.

use std::sync::{Arc, Mutex};

use eventyr_core::error::{StoreError, UpcastError};
use eventyr_core::prelude::{
    Batch, Checkpoint, EventEnvelope, ExpectedVersion, NewEvent, RawEvent, StreamId,
    SubscriptionOutcome,
};
use eventyr_projection::prelude::*;
use eventyr_store::prelude::{EventStore, InMemoryStore};
use eventyr_subscription::prelude::{
    CheckpointStore, InMemoryCheckpointStore, Projection, Projector, StoreSubscription,
    SubscriptionPolicy, SubscriptionSource,
};

/// A scripted raw source over a `Vec`, for tests that must place a
/// poison payload exactly — the store round-trip needs everything the
/// store needs, and pinning the protocol integration does not.
struct ScriptedSource {
    events: Vec<RawEvent>,
}

impl SubscriptionSource for ScriptedSource {
    type Event = RawEvent;

    async fn fetch(
        &self,
        from: Checkpoint,
        max: usize,
    ) -> Result<Batch<Self::Event>, StoreError> {
        let events: Vec<_> = self
            .events
            .iter()
            .enumerate()
            .skip(from.as_sequence().as_u64() as usize)
            .take(max)
            .map(|(index, event)| EventEnvelope {
                sequence: eventyr_core::vocabulary::Sequence::new(index as u64 + 1),
                stream_id: StreamId::from("scripted"),
                version: eventyr_core::vocabulary::Version::new(index as u64 + 1),
                event: event.clone(),
                metadata: Default::default(),
            })
            .collect();
        let upper = events.last().map(|envelope| Checkpoint::new(envelope.sequence));
        Ok(Batch::new(events, upper))
    }
}

fn parse_amount_chain() -> UpcasterChain<u64> {
    UpcasterChain::new().with(
        "AmountV1",
        ClosureUpcaster::new(|raw: RawEvent| {
            std::str::from_utf8(&raw.payload)
                .ok()
                .and_then(|text| text.parse::<u64>().ok())
                .ok_or_else(|| UpcastError {
                    event_type: raw.event_type.clone(),
                    message: "payload is not a number".into(),
                })
        }),
    )
}

fn raw(event_type: &str, payload: &str) -> NewEvent<RawEvent> {
    NewEvent::new(RawEvent {
        event_type: event_type.into(),
        payload: payload.as_bytes().to_vec(),
    })
}

/// A shared read model: the documented state-struct + `impl Projection`
/// pattern, the state readable after the run through the `Arc`.
#[derive(Clone, Default)]
struct Total(Arc<Mutex<u64>>);

impl Projection for Total {
    type Event = u64;
    type Error = core::convert::Infallible;

    async fn apply(&mut self, envelope: &EventEnvelope<u64>) -> Result<(), Self::Error> {
        // Idempotent folding is the model's to keep; the test events are
        // applied exactly once per run here because every run is a full
        // rebuild from the origin with a fresh state.
        *self.0.lock().expect("lock") += envelope.event;
        Ok(())
    }
}

impl Total {
    fn get(&self) -> u64 {
        *self.0.lock().expect("lock")
    }
}

async fn no_sleep(_duration: core::time::Duration) {}

#[tokio::test]
async fn raw_payloads_upcast_on_the_read_path_end_to_end() {
    let store = InMemoryStore::<RawEvent>::new();
    store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Empty,
            vec![raw("AmountV1", "10"), raw("AmountV1", "20")],
        )
        .await
        .expect("append");

    let total = Total::default();
    let plan = RebuildPlan::new("balance", SchemaVersion(1), parse_amount_chain());
    let outcome = plan
        .projector(
            StoreSubscription::new(&store),
            InMemoryCheckpointStore::new(),
            total.clone(),
        )
        .run(no_sleep)
        .await
        .expect("drive");

    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    assert_eq!(total.get(), 30);
}

#[tokio::test]
async fn a_poison_payload_wedges_the_projection_and_the_checkpoint_stays_put() {
    // The second event cannot upcast; per the contract (loud, never
    // skip) the projection aborts and never acks past the poison event.
    let source = ScriptedSource {
        events: vec![
            RawEvent {
                event_type: "AmountV1".into(),
                payload: b"10".to_vec(),
            },
            RawEvent {
                event_type: "AmountV1".into(),
                payload: b"poison".to_vec(),
            },
        ],
    };
    let checkpoints = InMemoryCheckpointStore::new();

    let plan = RebuildPlan::new("balance", SchemaVersion(1), parse_amount_chain());
    let key = plan.checkpoint_key();
    let outcome = plan
        .projector(source, &checkpoints, Total::default())
        .run(no_sleep)
        .await
        .expect("drive");

    let SubscriptionOutcome::Failed(error) = outcome else {
        panic!("a poison event aborts loudly, got {outcome:?}");
    };
    assert!(
        error.to_string().contains("AmountV1"),
        "the error names the offending event type: {error}"
    );
    // The poison payload was in the first batch, which never acked.
    assert_eq!(
        checkpoints.load(&key).await.expect("load"),
        Checkpoint::ORIGIN
    );
}

#[tokio::test]
async fn a_schema_bump_rebuilds_from_the_origin_and_leaves_the_old_checkpoint() {
    let store = InMemoryStore::<RawEvent>::new();
    store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Empty,
            vec![raw("AmountV1", "10"), raw("AmountV1", "20")],
        )
        .await
        .expect("append");
    let checkpoints = InMemoryCheckpointStore::new();

    // v1 sums the amounts.
    let v1 = Total::default();
    let outcome = RebuildPlan::new("balance", SchemaVersion(1), parse_amount_chain())
        .projector(StoreSubscription::new(&store), &checkpoints, v1.clone())
        .run(no_sleep)
        .await
        .expect("drive v1");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    assert_eq!(v1.get(), 30);
    let v1_checkpoint = checkpoints
        .load("balance@v1")
        .await
        .expect("v1 checkpoint was stored");
    assert_ne!(v1_checkpoint, Checkpoint::ORIGIN);

    // The fold changes: v2 counts instead of summing. Rebuild from the
    // origin under the fresh versioned key.
    #[derive(Clone, Default)]
    struct Count(Arc<Mutex<u64>>);
    impl Projection for Count {
        type Event = u64;
        type Error = core::convert::Infallible;
        async fn apply(&mut self, _envelope: &EventEnvelope<u64>) -> Result<(), Self::Error> {
            *self.0.lock().expect("lock") += 1;
            Ok(())
        }
    }

    let v2 = Count::default();
    let outcome = RebuildPlan::new("balance", SchemaVersion(2), parse_amount_chain())
        .projector(StoreSubscription::new(&store), &checkpoints, v2.clone())
        .run(no_sleep)
        .await
        .expect("drive v2");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    // Fresh key: no resume, both events re-folded.
    assert_eq!(*v2.0.lock().expect("lock"), 2);
    // Name rotation proven: v1's checkpoint is untouched for rollback.
    assert_eq!(
        checkpoints.load("balance@v1").await.expect("load"),
        v1_checkpoint
    );
    assert_ne!(
        checkpoints.load("balance@v2").await.expect("load"),
        Checkpoint::ORIGIN
    );
}

#[tokio::test]
async fn a_planned_projector_runs_through_the_subscription_runner_unchanged() {
    // The composition seam: RebuildPlan builds a plain Projector, so
    // resuming a versioned key reuses every piece of the shipped
    // runner, including custom policies.
    let store = InMemoryStore::<RawEvent>::new();
    store
        .append(
            &StreamId::from("account-1"),
            ExpectedVersion::Empty,
            vec![raw("AmountV1", "5")],
        )
        .await
        .expect("append");

    let total = Total::default();
    let projector: Projector<_, InMemoryCheckpointStore, _> =
        RebuildPlan::new("balance", SchemaVersion(1), parse_amount_chain())
            .with_policy(
                SubscriptionPolicy::default().stop_at_catch_up(),
            )
            .projector(
                StoreSubscription::new(&store),
                InMemoryCheckpointStore::new(),
                total.clone(),
            );
    assert_eq!(projector.name(), "balance@v1");
    let outcome = projector.run(no_sleep).await.expect("drive");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    assert_eq!(total.get(), 5);
}
