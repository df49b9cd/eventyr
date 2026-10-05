//! The `ProjectorLease` port: one driver per checkpoint name (0.7.9).
//!
//! Two copies of a projector against one checkpoint corrupt the read
//! model the runner is otherwise careful about: both fetch, both apply,
//! and their acks interleave, so a later checkpoint can bury an earlier
//! one's unapplied batches. The runner cannot see this — exclusivity is
//! a *driver* property, not a machine transition.
//!
//! A lease is acquired before the checkpoint is read, renewed before
//! each fetch that is due and before every ack (the renewal is the
//! guard on the checkpoint write), and released when the driver stops.
//! Losing it ends the run with the last *successfully acked* checkpoint
//! as the resume point.

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

/// How long a lease lives and how long a dead holder may still claim
/// it (`grace`), and how long *any* holder may keep it (`max_grace`).
///
/// The defaults — 5 s / 3 / 12 — mean renew every 5 s and a run that
/// can no longer renew holds the name for at most one minute.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LeasePolicy {
    /// The renewal rhythm: a renewal is due each `ttl`.
    pub ttl: Duration,
    /// How many `ttl`s a missed renewal may survive.
    pub grace: u32,
    /// How many `ttl`s from acquire any lease may live, renewed or not.
    pub max_grace: u32,
}

impl Default for LeasePolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(5),
            grace: 3,
            max_grace: 12,
        }
    }
}

/// A lease for drivers that do not want one — the default third port.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLease;

impl ProjectorLease for NoLease {
    type Lease = ();

    async fn acquire(
        &self,
        _: &str,
        _: Duration,
        _: u32,
        _: u32,
    ) -> Result<Self::Lease, LeaseError> {
        Ok(())
    }

    async fn renew(
        &self,
        _: &mut Self::Lease,
        _: Duration,
        _: u32,
        _: u32,
    ) -> Result<Instant, LeaseError> {
        Ok(Instant::now())
    }

    async fn release(&self, _: Self::Lease) -> Result<(), StoreError> {
        Ok(())
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

    /// Force `name` to expire — the test seam for "the holder died".
    #[cfg(any(test, feature = "testing"))]
    pub fn expire(&self, name: &str) {
        if let Some(row) = self.lock().get_mut(name) {
            row.renewed_at = Instant::now()
                .checked_sub(row.ttl * row.grace + Duration::from_secs(1))
                .unwrap_or_else(Instant::now);
        }
    }
}

/// The in-memory lease handle: the holder id and when it was acquired.
#[derive(Debug)]
pub struct InMemoryLease {
    name: String,
    holder: Uuid,
    renewed_at: Instant,
    acquired_at: Instant,
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
            renewed_at: now,
            acquired_at: now,
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
        lease.renewed_at = now;
        lease.acquired_at = row.acquired_at;
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

/// Run the [`ProjectorLease`] contract against `make_store`'s fresh
/// stores. Every implementation runs it, behind this crate's `testing`
/// feature.
#[cfg(any(test, feature = "testing"))]
pub fn lease_store_contract<L: ProjectorLease>(make_store: impl Fn() -> L) {
    use futures::executor::block_on;

    let policy = LeasePolicy::default();

    // Ttl of 0 makes every lease expiry immediate — the cleanest way to
    // drive both the "held" and "lost" arms without a clock.
    let zero = LeasePolicy {
        ttl: Duration::ZERO,
        grace: 1,
        max_grace: 1,
    };

    // One name, one holder.
    let store = make_store();
    let lease = block_on(store.acquire("ledger", policy.ttl, policy.grace, policy.max_grace))
        .expect("a fresh name acquires");
    assert!(matches!(
        block_on(store.acquire("ledger", zero.ttl, zero.grace, zero.max_grace)),
        Err(LeaseError::Taken)
    ));
    block_on(store.release(lease)).expect("release");

    // A released name is claimable.
    let lease = block_on(store.acquire("ledger", policy.ttl, policy.grace, policy.max_grace))
        .expect("released is claimable");
    block_on(store.release(lease)).expect("release");

    // Renewal keeps a lease; a never-renewed lease expires under zero
    // ttl.
    let store = make_store();
    let mut lease =
        block_on(store.acquire("ledger", zero.ttl, zero.grace, zero.max_grace)).expect("acquire");
    assert!(matches!(
        block_on(store.renew(&mut lease, zero.ttl, zero.grace, zero.max_grace)),
        Err(LeaseError::Lost)
    ));
}
