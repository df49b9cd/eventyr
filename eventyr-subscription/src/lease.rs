//! The `ProjectorLease` port: one driver per checkpoint name (roadmap 0.7.9).
//!
//! Two copies of a projector against one checkpoint corrupt the read
//! model the runner is otherwise careful about: both fetch, both apply,
//! and their acks interleave, so a later checkpoint can bury an earlier
//! one's unapplied batches. The runner cannot see this — exclusivity is
//! a *driver* property, not a machine transition.
//!
//! A lease is acquired before the driver reads the checkpoint it
//! reports as the resume point and before any checkpoint write,
//! renewed before each fetch that is due and before every ack (the
//! renewal is the guard on the checkpoint write), and released when the
//! driver stops. (`LeasedProjector` reads the machine's starting
//! checkpoint before acquiring; that read only seeds the fetch
//! position, and at-least-once delivery covers it.) Losing the lease
//! ends the run with the last *successfully acked* checkpoint as the
//! resume point.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use core::future::Future;

use eventyr_core::error::StoreError;
use uuid::Uuid;

/// Exclusivity for one named projector: one driver holds it at a time.
pub trait ProjectorLease: Send + Sync {
    /// The handle a successful [`acquire`](ProjectorLease::acquire)
    /// returns. Renewed between batches, released on stop.
    type Lease: Send;

    /// Take the lease for `name`: exclusive for `ttl * grace` past the
    /// last renewal, never past `ttl * max_grace` from acquire (a stuck
    /// renewer cannot jail the name forever). Returns
    /// [`LeaseError::Taken`] when another holder owns it.
    fn acquire(
        &self,
        name: &str,
        ttl: Duration,
        grace: u32,
        max_grace: u32,
    ) -> impl Future<Output = Result<Self::Lease, LeaseError>> + Send;

    /// Extend `lease`, due again `ttl` after this call. The returned
    /// `Instant` is the store's own renewal clock — the anchor the next
    /// due-ness is computed against, never the caller's wall clock.
    /// [`LeaseError::Lost`] means the lease is gone (expired or taken
    /// over): the run stops, and the checkpoint store is not written
    /// again.
    fn renew(
        &self,
        lease: &mut Self::Lease,
        ttl: Duration,
        grace: u32,
        max_grace: u32,
    ) -> impl Future<Output = Result<Instant, LeaseError>> + Send;

    /// Give `lease` up: the name is claimable at once, not at expiry.
    /// Idempotent — releasing a lost lease is `Ok(())`.
    fn release(&self, lease: Self::Lease) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Why a lease operation failed. Distinct from [`StoreError`] so the
/// runner can tell "lost" (stop cleanly) from "down" (back off and
/// retry) without inspecting strings.
#[derive(Debug)]
pub enum LeaseError {
    /// `acquire`: another holder owns the name.
    Taken,
    /// `renew`: the lease is gone — expired or taken over. The holder
    /// must stop and must never write a checkpoint again.
    Lost,
    /// The store itself failed. `renewed_until` is the deadline the
    /// now-doubtful lease was last known good until; `None` means the
    /// renewal never reached a store that could have accepted it.
    Store {
        /// The underlying store failure.
        error: StoreError,
        /// The deadline by which the lease is presumed still held.
        renewed_until: Option<Instant>,
    },
}

impl core::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Taken => f.write_str("the lease is held by another driver"),
            Self::Lost => f.write_str("the lease was lost: the driver must stop"),
            Self::Store { error, .. } => write!(f, "the lease store failed: {error}"),
        }
    }
}

impl core::error::Error for LeaseError {}

/// How long a lease lives, how long a dead holder may still claim it
/// (`grace`), and how long *any* holder may keep it (`max_grace`).
///
/// The defaults — 5 s / 3 / 12 — mean renew every 5 s and a run that
/// can no longer renew holds the name for at most one minute. Note
/// what `max_grace` also costs a *healthy* run: the cap is measured
/// from acquire and renewing does not extend it, so a lease is
/// surrendered — [`RunError::LeaseLost`](crate::runner::RunError) —
/// after `ttl × max_grace` (one minute at the defaults) even while
/// renewing perfectly. That is the deliberate ceiling on accidental
/// captivity; the driver does not re-acquire, so a long-lived
/// projector wraps its run in a loop and treats `LeaseLost` as a
/// routine handover, not a failure.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LeasePolicy {
    /// The renewal rhythm: a renewal is due each `ttl`.
    pub ttl: Duration,
    /// How many `ttl`s a missed renewal may survive.
    pub grace: u32,
    /// How many `ttl`s from acquire any lease may live, renewed or not.
    pub max_grace: u32,
}

impl LeasePolicy {
    /// A policy: renew every `ttl`, survive `grace` missed renewals,
    /// never live past `max_grace` renewals from acquire.
    ///
    /// # Panics
    ///
    /// When `ttl` is zero, `grace` is zero, or `max_grace` undercuts
    /// `grace`: a zero ttl makes every lease instantly stale, and a
    /// zero grace never holds one — the second `acquire` succeeds while
    /// the first holder still runs. The struct literal stays available
    /// to tests that need exactly those degenerates. A store may be
    /// stricter (Postgres requires `max_grace > grace`); its refusal
    /// surfaces as [`LeaseError::Store`].
    pub const fn new(ttl: Duration, grace: u32, max_grace: u32) -> Self {
        assert!(!ttl.is_zero(), "a lease's ttl must be positive");
        assert!(grace >= 1, "a lease's grace must allow one missed renewal");
        assert!(
            max_grace >= grace,
            "a lease's max_grace cannot undercut its grace"
        );
        Self {
            ttl,
            grace,
            max_grace,
        }
    }
}

impl Default for LeasePolicy {
    fn default() -> Self {
        Self::new(Duration::from_secs(5), 3, 12)
    }
}

/// The in-memory lease store: the default port for tests and examples,
/// exclusive within one process.
pub struct InMemoryLeaseStore {
    rows: Mutex<HashMap<String, LeaseRow>>,
}

#[derive(Clone, Debug)]
struct LeaseRow {
    holder: Uuid,
    renewed_at: Instant,
    acquired_at: Instant,
    ttl: Duration,
    grace: u32,
    max_grace: u32,
}

impl LeaseRow {
    fn held_at(&self, now: Instant) -> bool {
        now.duration_since(self.renewed_at) < self.ttl * self.grace
            && now.duration_since(self.acquired_at) < self.ttl * self.max_grace
    }
}

impl Default for InMemoryLeaseStore {
    fn default() -> Self {
        Self {
            rows: Mutex::new(HashMap::new()),
        }
    }
}

impl InMemoryLeaseStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, LeaseRow>> {
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether `name` is currently held, for tests and observers.
    pub fn is_held(&self, name: &str) -> bool {
        self.lock()
            .get(name)
            .is_some_and(|row| row.held_at(Instant::now()))
    }

    /// Force `name` past its grace — the test seam for "the holder
    /// stalled": the next renewal or acquire sees the lease as expired.
    ///
    /// # Panics
    ///
    /// When the monotonic clock's uptime is shorter than the lease
    /// window being backdated past — impossible on any real machine,
    /// but the `checked_sub` must say so rather than wrap.
    #[cfg(any(test, feature = "testing"))]
    pub fn expire(&self, name: &str) {
        if let Some(row) = self.lock().get_mut(name) {
            row.renewed_at = Instant::now()
                .checked_sub(row.ttl * row.grace + Duration::from_secs(1))
                .expect("the monotonic clock predates the lease window");
        }
    }

    /// Force `name` past its `max_grace` while renewed within its
    /// grace — the test seam for "no lease outlives max_grace": the
    /// next renewal fails on the acquired-at bound alone. Backdating
    /// both anchors would not discriminate: the grace clause would
    /// fail first, and a store with no acquired-at check would pass.
    ///
    /// # Panics
    ///
    /// When the monotonic clock's uptime is shorter than the lease
    /// window being backdated past — impossible on any real machine,
    /// but the `checked_sub` must say so rather than wrap.
    #[cfg(any(test, feature = "testing"))]
    pub fn expire_at_max_grace(&self, name: &str) {
        if let Some(row) = self.lock().get_mut(name) {
            row.acquired_at = Instant::now()
                .checked_sub(row.ttl * row.max_grace + Duration::from_secs(1))
                .expect("the monotonic clock predates the lease window");
        }
    }
}

/// The in-memory lease handle: the holder id and the name it holds.
/// The time state lives in the store's row — the handle's copy could
/// only drift.
#[derive(Debug)]
pub struct InMemoryLease {
    name: String,
    holder: Uuid,
}

impl ProjectorLease for InMemoryLeaseStore {
    type Lease = InMemoryLease;

    async fn acquire(
        &self,
        name: &str,
        ttl: Duration,
        grace: u32,
        max_grace: u32,
    ) -> Result<Self::Lease, LeaseError> {
        let mut rows = self.lock();
        let now = Instant::now();
        if let Some(row) = rows.get(name)
            && row.held_at(now)
        {
            return Err(LeaseError::Taken);
        }
        let holder = Uuid::new_v4();
        rows.insert(
            name.to_owned(),
            LeaseRow {
                holder,
                renewed_at: now,
                acquired_at: now,
                ttl,
                grace,
                max_grace,
            },
        );
        Ok(InMemoryLease {
            name: name.to_owned(),
            holder,
        })
    }

    async fn renew(
        &self,
        lease: &mut Self::Lease,
        ttl: Duration,
        grace: u32,
        max_grace: u32,
    ) -> Result<Instant, LeaseError> {
        let mut rows = self.lock();
        let now = Instant::now();
        let Some(row) = rows.get_mut(&lease.name) else {
            return Err(LeaseError::Lost);
        };
        if row.holder != lease.holder || !row.held_at(now) {
            return Err(LeaseError::Lost);
        }
        row.renewed_at = now;
        row.ttl = ttl;
        row.grace = grace;
        row.max_grace = max_grace;
        Ok(now)
    }

    async fn release(&self, lease: Self::Lease) -> Result<(), StoreError> {
        let mut rows = self.lock();
        if let Some(row) = rows.get(&lease.name)
            && row.holder == lease.holder
        {
            rows.remove(&lease.name);
        }
        Ok(())
    }
}

macro_rules! impl_lease_delegation {
    ($pointer:ty) => {
        impl<L: ProjectorLease + ?Sized> ProjectorLease for $pointer {
            type Lease = L::Lease;

            fn acquire(
                &self,
                name: &str,
                ttl: Duration,
                grace: u32,
                max_grace: u32,
            ) -> impl Future<Output = Result<Self::Lease, LeaseError>> + Send {
                (**self).acquire(name, ttl, grace, max_grace)
            }

            fn renew(
                &self,
                lease: &mut Self::Lease,
                ttl: Duration,
                grace: u32,
                max_grace: u32,
            ) -> impl Future<Output = Result<Instant, LeaseError>> + Send {
                (**self).renew(lease, ttl, grace, max_grace)
            }

            fn release(
                &self,
                lease: Self::Lease,
            ) -> impl Future<Output = Result<(), StoreError>> + Send {
                (**self).release(lease)
            }
        }
    };
}

impl_lease_delegation!(&L);
impl_lease_delegation!(std::sync::Arc<L>);

/// Run the [`ProjectorLease`] contract against `make_store`'s stores.
///
/// Every implementation runs it, behind this crate's `testing`
/// feature. A fresh store per call is not required — each case uses
/// its own name, so one shared store (a clone of one pool, say)
/// works; the factory is called per case so a fresh store *can* be
/// given.
///
/// # Panics
///
/// When the store broke the lease contract: a fresh name did not
/// acquire, a held name did, a live lease expired or a lost one
/// renewed, or grace or `max_grace` was not honored. The message
/// names the broken case.
#[cfg(any(test, feature = "testing"))]
pub fn lease_store_contract<L: ProjectorLease>(make_store: impl Fn() -> L) {
    use futures::executor::block_on;

    let policy = LeasePolicy::default();

    // A 1 ms ttl with grace 1 makes every lease expire on the next
    // microsecond tick — the cleanest way to drive both the "held" and
    // "lost" arms without a clock, and valid against stores that check
    // `ttl_ms > 0` (the SQL implementations do).
    let zero = LeasePolicy {
        ttl: Duration::from_millis(1),
        grace: 1,
        max_grace: 2, // max_grace > grace
    };

    // One name, one holder.
    let store = make_store();
    let lease = block_on(store.acquire("ledger", policy.ttl, policy.grace, policy.max_grace))
        .expect("a fresh name acquires");
    let contested = LeasePolicy {
        ttl: Duration::from_millis(1),
        grace: policy.grace,
        max_grace: policy.max_grace,
    };
    assert!(matches!(
        block_on(store.acquire(
            "ledger",
            contested.ttl,
            contested.grace,
            contested.max_grace
        )),
        Err(LeaseError::Taken)
    ));
    block_on(store.release(lease)).expect("release");

    // A released name is claimable.
    let lease = block_on(store.acquire("ledger", policy.ttl, policy.grace, policy.max_grace))
        .expect("released is claimable");
    block_on(store.release(lease)).expect("release");

    // Renewal keeps a lease; a never-renewed lease expires after
    // ttl * grace — wait the short arm out, then renew is lost.
    let store = make_store();
    let mut lease =
        block_on(store.acquire("ledger", zero.ttl, zero.grace, zero.max_grace)).expect("acquire");
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        block_on(store.renew(&mut lease, zero.ttl, zero.grace, zero.max_grace)),
        Err(LeaseError::Lost)
    ));

    // A lease renewed on schedule still dies at `ttl * max_grace` from
    // acquire — a stuck holder cannot jail the name forever. The
    // renewal lands within grace; the final check lands past
    // max_grace but within grace of that renewal, so the acquired-at
    // bound is the one that fails. (Sleeping past max_grace *without*
    // renewing would not do: the grace bound lapses first, and a store
    // without the acquired-at check would pass.)
    let store = make_store();
    let bounded = LeasePolicy {
        ttl: Duration::from_millis(100),
        grace: 3,
        max_grace: 4,
    };
    let mut lease =
        block_on(store.acquire("max-grace", bounded.ttl, bounded.grace, bounded.max_grace))
            .expect("acquire");
    std::thread::sleep(Duration::from_millis(200)); // within grace (300 ms)
    block_on(store.renew(&mut lease, bounded.ttl, bounded.grace, bounded.max_grace))
        .expect("renewed in time");
    std::thread::sleep(Duration::from_millis(260)); // past max_grace (400 ms)
    assert!(matches!(
        block_on(store.renew(&mut lease, bounded.ttl, bounded.grace, bounded.max_grace)),
        Err(LeaseError::Lost)
    ));

    // Releasing with a handle that no longer owns the row is a no-op:
    // the previous holder's expired handle must not free the new
    // holder's lease. The first holder takes the short-lived policy so
    // it expires; the new one takes the default, so it is still held at
    // the final check — `Taken` there can only mean the stale release
    // left the row alone.
    let store = make_store();
    let stale =
        block_on(store.acquire("stale", zero.ttl, zero.grace, zero.max_grace)).expect("acquire");
    std::thread::sleep(Duration::from_millis(10)); // past ttl * grace
    let current = block_on(store.acquire("stale", policy.ttl, policy.grace, policy.max_grace))
        .expect("the expired name is claimable");
    block_on(store.release(stale)).expect("releasing a stale handle is Ok");
    assert!(matches!(
        block_on(store.acquire("stale", policy.ttl, policy.grace, policy.max_grace)),
        Err(LeaseError::Taken) // the new holder was not disturbed
    ));
    block_on(store.release(current)).expect("release");
}
