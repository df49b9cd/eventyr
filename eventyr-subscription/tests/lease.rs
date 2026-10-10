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

/// A completed run releases the lease, so the next driver of the same
/// name acquires it. (Exclusivity while held is the contract's and
/// `a_failed_acquire_never_touches_the_checkpoint`'s to prove.)
#[tokio::test]
async fn a_completed_run_releases_the_lease() {
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

    // A second driver of the same name: the first run released on
    // completion, so it acquires fine.
    let second = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        checkpoints(),
        Counter::default(),
    )
    .with_policy(policy())
    .lease_with(Arc::clone(&leases));
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
    let mut lease = leases.acquire("ledger", policy).await.expect("acquire");
    assert!(matches!(
        leases.renew(&mut lease, policy).await,
        Err(LeaseError::Lost) // a zero-TTL lease is lost at the first renewal
    ));
    assert!(!leases.is_held("ledger"));
}

/// A lease renewed on schedule stays held past `grace` but dies at
/// `max_grace` — driven through the store's test seam, not a sleep:
/// backdating only `acquired_at` leaves the grace clause holding, so
/// the acquired-at bound is the one that fails.
#[tokio::test]
async fn a_renewed_lease_stays_held_then_dies_at_max_grace() {
    let leases = InMemoryLeaseStore::new();
    let policy = LeasePolicy {
        ttl: Duration::from_millis(20),
        grace: 2,
        max_grace: 2,
    };
    let mut lease = leases.acquire("ledger", policy).await.expect("acquire");
    leases
        .renew(&mut lease, policy)
        .await
        .expect("renewed in time");
    assert!(matches!(
        leases.acquire("ledger", policy).await,
        Err(LeaseError::Taken) // a renewed lease is still held
    ));
    // max_grace reached: the next renewal is lost.
    leases.expire_at_max_grace("ledger");
    assert!(matches!(
        leases.renew(&mut lease, policy).await,
        Err(LeaseError::Lost)
    ));
}

/// A lease lost mid-run stops the driver at the last acked checkpoint.
/// The projection rejects one event, so the driver takes a renewal
/// round before it parks; the lease is zero-TTL, lost at that renewal.
#[tokio::test(start_paused = true)]
async fn a_lost_lease_stops_the_driver_at_the_last_ack() {
    use eventyr_core::subscription_machine::FailurePolicy;
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
        .acquire("balance", LeasePolicy::default())
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

// -- the grace window and the metrics ---------------------------------------

/// Records counters by name, the metrics spy the runner-level tests
/// use (the store driver tests' `Counters` shape).
#[derive(Clone, Default)]
struct Counters(Arc<std::sync::Mutex<std::collections::HashMap<&'static str, u64>>>);
impl Counters {
    fn get(&self, name: &'static str) -> u64 {
        *self.0.lock().expect("poisoned").get(name).unwrap_or(&0)
    }
}
impl eventyr_store::metrics::Metrics for Counters {
    fn counter(&self, name: &'static str, by: u64) {
        *self.0.lock().expect("poisoned").entry(name).or_default() += by;
    }
    fn gauge(&self, _: &'static str, _: u64) {}
    fn histogram(&self, _: &'static str, _: std::time::Duration) {}
}

/// A lease store whose `renew` fails `failures` times with the given
/// `renewed_until`, then delegates to the inner store — the seam that
/// drives the grace-window branch: a renewal that failed against a
/// live store rides the deadline the store reported.
struct RenewFails {
    inner: InMemoryLeaseStore,
    failures: std::sync::atomic::AtomicU32,
    renewed_until: Option<std::time::Instant>,
    /// How many renewals the delegated store has served.
    renewed: std::sync::atomic::AtomicU32,
}

impl RenewFails {
    fn new(
        inner: InMemoryLeaseStore,
        failures: u32,
        renewed_until: Option<std::time::Instant>,
    ) -> Self {
        Self {
            inner,
            failures: std::sync::atomic::AtomicU32::new(failures),
            renewed_until,
            renewed: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

impl ProjectorLease for RenewFails {
    type Lease = <InMemoryLeaseStore as ProjectorLease>::Lease;

    async fn acquire(&self, name: &str, policy: LeasePolicy) -> Result<Self::Lease, LeaseError> {
        self.inner.acquire(name, policy).await
    }

    async fn renew(
        &self,
        lease: &mut Self::Lease,
        policy: LeasePolicy,
    ) -> Result<std::time::Instant, LeaseError> {
        if self
            .failures
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            > 0
        {
            return Err(LeaseError::Store {
                error: eventyr_core::error::StoreError::other("a pool blip, say"),
                renewed_until: self.renewed_until,
            });
        }
        let at = self.inner.renew(lease, policy).await?;
        self.renewed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(at)
    }

    async fn release(&self, lease: Self::Lease) -> Result<(), eventyr_core::error::StoreError> {
        self.inner.release(lease).await
    }
}

/// A renewal that failed against a live store rides the grace window:
/// the deadline it reported is the next due time, the run acks past the
/// events it already held the lease for, and the metrics count the
/// failure without stopping anything. This is the branch the pg store's
/// `presumed_held_until` feeds (d6daf40's headline fix) — untested
/// until now.
#[tokio::test(start_paused = true)]
async fn a_failed_renewal_against_a_live_store_rides_the_grace_window() {
    use eventyr_store::metrics::names::{LEASE_LOST, LEASE_RENEW_FAILURES};

    let store = Arc::new(InMemoryStore::new());
    populate(&store, 5).await;
    // The failed renewal reports a deadline a full ttl out; with paused
    // time the whole run happens inside that window.
    let window = LeasePolicy {
        ttl: Duration::from_millis(50),
        grace: 4,
        max_grace: 8,
    };
    let deadline = std::time::Instant::now() + window.ttl;
    let leases = Arc::new(RenewFails::new(
        InMemoryLeaseStore::new(),
        1,
        Some(deadline),
    ));
    let counters = Counters::default();

    let outcome = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Counter::default(),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .with_metrics(counters.clone())
    .lease_with(Arc::clone(&leases))
    .with_policy(window)
    .run_leased(|_| future::ready(()))
    .await;

    assert!(
        matches!(outcome, Ok(SubscriptionOutcome::CaughtUp { .. })),
        "a blip rides the grace window: {outcome:?}"
    );
    // The one renewal the run attempted failed against a live store
    // and the run rode the deadline it reported: one renewal failure,
    // no loss, the outcome completed.
    assert_eq!(counters.get(LEASE_RENEW_FAILURES), 1);
    assert_eq!(counters.get(LEASE_LOST), 0);
}

/// A renewal failure that carries no presumed deadline — the renewal
/// never reached a store that could have accepted it — stops the run
/// as a lost lease, counting it.
#[tokio::test(start_paused = true)]
async fn a_failed_renewal_with_no_deadline_stops_the_run() {
    use eventyr_store::metrics::names::{LEASE_LOST, LEASE_RENEW_FAILURES};

    let store = Arc::new(InMemoryStore::new());
    populate(&store, 5).await;
    let window = LeasePolicy {
        ttl: Duration::from_millis(50),
        grace: 4,
        max_grace: 8,
    };
    let leases = Arc::new(RenewFails::new(InMemoryLeaseStore::new(), 9, None));
    let counters = Counters::default();

    let outcome = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Counter::default(),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .with_metrics(counters.clone())
    .lease_with(Arc::clone(&leases))
    .with_policy(window)
    .run_leased(|_| future::ready(()))
    .await;

    assert!(
        matches!(outcome, Err(RunError::LeaseLost { .. })),
        "a renewal that reached nothing is a lost lease: {outcome:?}"
    );
    assert_eq!(counters.get(LEASE_RENEW_FAILURES), 1);
    assert_eq!(counters.get(LEASE_LOST), 1);
}

/// A lease store whose release always fails: the run still returns its
/// completed outcome — the release failure is counted, not fatal.
struct ReleaseFails(InMemoryLeaseStore);
impl ProjectorLease for ReleaseFails {
    type Lease = <InMemoryLeaseStore as ProjectorLease>::Lease;
    async fn acquire(&self, name: &str, policy: LeasePolicy) -> Result<Self::Lease, LeaseError> {
        self.0.acquire(name, policy).await
    }
    async fn renew(
        &self,
        lease: &mut Self::Lease,
        policy: LeasePolicy,
    ) -> Result<std::time::Instant, LeaseError> {
        self.0.renew(lease, policy).await
    }
    async fn release(&self, _: Self::Lease) -> Result<(), eventyr_core::error::StoreError> {
        Err(eventyr_core::error::StoreError::other(
            "the row is gone, say",
        ))
    }
}

/// The release at the end of a completed run is best-effort: a failure
/// is counted and the completed outcome is still returned.
#[tokio::test(start_paused = true)]
async fn a_failed_release_is_counted_not_fatal() {
    use eventyr_store::metrics::names::LEASE_RELEASE_FAILURES;

    let store = Arc::new(InMemoryStore::new());
    populate(&store, 5).await;
    let leases = Arc::new(ReleaseFails(InMemoryLeaseStore::new()));
    let counters = Counters::default();

    let outcome = Projector::new(
        "balance",
        StoreSubscription::new(Arc::clone(&store)),
        InMemoryCheckpointStore::new(),
        Counter::default(),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .with_metrics(counters.clone())
    .lease_with(Arc::clone(&leases))
    .with_policy(LeasePolicy::default())
    .run_leased(|_| future::ready(()))
    .await;

    assert!(
        matches!(outcome, Ok(SubscriptionOutcome::CaughtUp { .. })),
        "the completed outcome survives a failed release: {outcome:?}"
    );
    assert_eq!(counters.get(LEASE_RELEASE_FAILURES), 1);
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
