//! The 0.7.9 lease: exclusivity for a named projector.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::time::Duration;

use eventyr_core::prelude::*;
use eventyr_store::prelude::*;
use eventyr_subscription::lease::{
    InMemoryLeaseStore, LeaseError, LeasePolicy, ProjectorLease, lease_store_contract,
};
use eventyr_subscription::prelude::{
    InMemoryCheckpointStore, Projector, RunError, StoreSubscription, SubscriptionOutcome,
    SubscriptionPolicy,
};
use futures::future;

// -- the contract -----------------------------------------------------------

#[test]
fn the_in_memory_lease_store_passes_the_contract() {
    lease_store_contract(InMemoryLeaseStore::new);
}

/// Holding the name blocks a second acquirer.
#[tokio::test]
async fn one_projector_holds_the_checkpoint_at_a_time() {
    let store = Arc::new(InMemoryStore::new());
    populate(&store, 5).await;
    let leases = Arc::new(InMemoryLeaseStore::new());
    let checkpoints = || InMemoryCheckpointStore::new();

    // The first projector owns "balance" for the run.
    let policy = || SubscriptionPolicy::new(64, Duration::ZERO, Duration::ZERO).stop_at_catch_up();
    let first = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        checkpoints(),
        Counter::default(),
    )
    .with_policy(policy())
    .lease_with(Arc::clone(&leases))
    .run_leased(|_| future::ready(()))
    .await
    .expect("the first run leases and finishes");

    // A second driver of the same name is refused before it touches
    // anything.
    let second = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        checkpoints(),
        Counter::default(),
    )
    .with_policy(policy())
    .lease_with(Arc::clone(&leases));
    // The first run released on completion, so the second acquires fine.
    let outcome = second
        .run_leased(|_| future::ready(()))
        .await
        .expect("released is claimable");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    assert!(matches!(first, SubscriptionOutcome::CaughtUp { .. }));
}

/// A lease never renewed expires under a zero TTL, and the next acquirer
/// takes the name.
#[tokio::test]
async fn a_lease_never_renewed_expires() {
    let leases = InMemoryLeaseStore::new();
    let policy = LeasePolicy {
        ttl: Duration::ZERO,
        grace: 1,
        max_grace: 1,
    };
    let mut lease = leases
        .acquire("ledger", policy.ttl, policy.grace, policy.max_grace)
        .await
        .expect("acquire");
    assert!(matches!(
        leases
            .renew(&mut lease, policy.ttl, policy.grace, policy.max_grace)
            .await,
        Err(LeaseError::Lost) // a zero-TTL lease is lost at the first renewal
    ));
    assert!(!leases.is_held("ledger"));
}

/// A lease renewed on schedule stays held past `grace` but dies at
/// `max_grace`.
#[tokio::test]
async fn a_renewed_lease_stays_held_then_dies_at_max_grace() {
    let leases = InMemoryLeaseStore::new();
    let policy = LeasePolicy {
        ttl: Duration::from_millis(20),
        grace: 2,
        max_grace: 2,
    };
    let mut lease = leases
        .acquire("ledger", policy.ttl, policy.grace, policy.max_grace)
        .await
        .expect("acquire");
    leases
        .renew(&mut lease, policy.ttl, policy.grace, policy.max_grace)
        .await
        .expect("renewed in time");
    assert!(matches!(
        leases
            .acquire("ledger", policy.ttl, policy.grace, policy.max_grace)
            .await,
        Err(LeaseError::Taken) // a renewed lease is still held
    ));
    // max_grace reached: the next renewal is lost.
    std::thread::sleep(policy.ttl * policy.max_grace + Duration::from_millis(5));
    assert!(matches!(
        leases
            .renew(&mut lease, policy.ttl, policy.grace, policy.max_grace)
            .await,
        Err(LeaseError::Lost)
    ));
}

/// A lease lost mid-run stops the driver at the last acked checkpoint.
/// The projection rejects one event, so the driver takes a renewal
/// round before it parks; the lease is zero-TTL, lost at that renewal.
#[tokio::test(start_paused = true)]
async fn a_lost_lease_stops_the_driver_at_the_last_ack() {
    use eventyr_core::subscription::FailurePolicy;
    use eventyr_subscription::prelude::{InMemoryParkedStore, Projection};

    struct Picky;
    impl Projection for Picky {
        type Event = u64;
        type Error = String;
        async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), String> {
            if event.event == 13 {
                return Err("thirteen is unlucky".into());
            }
            Ok(())
        }
    }

    let store = Arc::new(InMemoryStore::new());
    populate(&store, 20).await;
    let leases = Arc::new(InMemoryLeaseStore::new());
    let parked = Arc::new(InMemoryParkedStore::new());
    let policy = LeasePolicy {
        ttl: Duration::ZERO,
        grace: 1,
        max_grace: 1,
    };

    let run = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Picky,
    )
    .with_policy(SubscriptionPolicy::new(1, Duration::ZERO, Duration::ZERO).stop_at_catch_up())
    .park_into(Arc::clone(&parked), FailurePolicy::Park { retries: 1 })
    .lease_with(Arc::clone(&leases))
    .with_policy(policy)
    .run_leased(|_| future::ready(()));

    let outcome = run.await;
    assert!(
        matches!(outcome, Err(RunError::LeaseLost { .. })),
        "the driver stops on a lost lease: {outcome:?}"
    );
}

/// A failed acquire on a held name leaves the other driver's checkpoint
/// untouched.
#[tokio::test(start_paused = true)]
async fn a_failed_acquire_never_touches_the_checkpoint() {
    let store = Arc::new(InMemoryStore::new());
    populate(&store, 5).await;
    let leases = Arc::new(InMemoryLeaseStore::new());

    let holder = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Counter::default(),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .lease_with(Arc::clone(&leases))
    .run_leased(|_| future::ready(()))
    .await
    .expect("the holder runs");

    // The holder released on its normal stop; hold it again directly so
    // the contender cannot take it.
    let _held = leases
        .acquire(
            "balance",
            LeasePolicy::default().ttl,
            LeasePolicy::default().grace,
            LeasePolicy::default().max_grace,
        )
        .await
        .expect("the holder reclaims it");
    let contended = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Counter::default(),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .lease_with(Arc::clone(&leases))
    .run_leased(|_| future::ready(()))
    .await;
    assert!(
        matches!(contended, Err(RunError::Taken { .. })),
        "the name is held: {contended:?}"
    );
    assert!(matches!(holder, SubscriptionOutcome::CaughtUp { .. }));
}

// -- fixtures ---------------------------------------------------------------

/// A projection that counts what it applies — the success-path spy.
#[derive(Default)]
struct Counter {
    seen: usize,
}
impl Projection for Counter {
    type Event = u64;
    type Error = std::convert::Infallible;
    async fn apply(&mut self, _: &EventEnvelope<u64>) -> Result<(), Self::Error> {
        self.seen += 1;
        Ok(())
    }
}

use eventyr_subscription::prelude::Projection;

async fn populate(store: &InMemoryStore<u64>, count: u64) {
    let events: Vec<_> = (1..=count).map(NewEvent::new).collect();
    store
        .append(&StreamId::from("account-1"), ExpectedVersion::Empty, events)
        .await
        .expect("seed");
}
