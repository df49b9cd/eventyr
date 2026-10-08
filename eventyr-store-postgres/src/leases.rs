//! A [`ProjectorLease`], behind the `leases` feature: each subscription
//! name is held by at most one driver, recorded in the
//! `projector_leases` table (migration `0012_leases`).
//!
//! The table lives in the database the projection reads from.

use std::time::{Duration, Instant};

use eventyr_core::error::StoreError;
use eventyr_subscription::lease::{LeaseError, ProjectorLease};

use crate::{PgStore, PgStoreError};

/// Subscription leases in Postgres, one row per subscription name.
///
/// A row's `holder` is the fencing handle — a renewal matched on name
/// *and* holder that affects no row means the lease was taken over or
/// has expired, and the caller's driver stops.
#[derive(Clone)]
pub struct PgLeaseStore {
    pool: sqlx::postgres::PgPool,
}

/// A lease handle: the holder id and the row's fencing version.
#[derive(Clone, Debug)]
pub struct PgLease {
    name: String,
    holder: uuid::Uuid,
    version: i64,
}

impl PgLeaseStore {
    /// A lease store over `store`'s pool.
    pub fn new(store: &PgStore<impl Send>) -> Self {
        Self {
            pool: store.pool().clone(),
        }
    }

    /// A lease store over a pool of its own. Run
    /// [`migrate`](crate::store::migrate) on the database first.
    pub fn from_pool(pool: sqlx::postgres::PgPool) -> Self {
        Self { pool }
    }
}

impl ProjectorLease for PgLeaseStore {
    type Lease = PgLease;

    async fn acquire(
        &self,
        name: &str,
        ttl: Duration,
        grace: u32,
        max_grace: u32,
    ) -> Result<Self::Lease, LeaseError> {
        let holder = uuid::Uuid::new_v4();
        let ttl_ms = millis(ttl);
        // Claim the row only if it does not exist or its previous holder
        // has expired past its grace.
        let row: Option<(i64,)> = sqlx::query_as(
            "INSERT INTO projector_leases \
                 (name, holder, ttl_ms, grace, max_grace, version) \
             VALUES ($1, $2, $3, $4, $5, 0) \
             ON CONFLICT (name) DO UPDATE SET \
                 holder = EXCLUDED.holder, \
                 acquired_at = now(), \
                 renewed_at = now(), \
                 ttl_ms = EXCLUDED.ttl_ms, \
                 grace = EXCLUDED.grace, \
                 max_grace = EXCLUDED.max_grace, \
                 version = 0 \
             WHERE now() >= projector_leases.renewed_at \
                     + (projector_leases.ttl_ms * projector_leases.grace) * INTERVAL '1 ms' \
                OR now() >= projector_leases.acquired_at \
                     + (projector_leases.ttl_ms * projector_leases.max_grace) * INTERVAL '1 ms' \
             RETURNING version",
        )
        .bind(name)
        .bind(holder)
        .bind(ttl_ms)
        .bind(i64::from(grace))
        .bind(i64::from(max_grace))
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| LeaseError::Store {
            error: PgStoreError::into_store(error),
            renewed_until: None,
        })?;
        match row {
            Some((version,)) => Ok(PgLease {
                name: name.to_owned(),
                holder,
                version,
            }),
            None => Err(LeaseError::Taken),
        }
    }

    async fn renew(
        &self,
        lease: &mut Self::Lease,
        ttl: Duration,
        grace: u32,
        max_grace: u32,
    ) -> Result<Instant, LeaseError> {
        let ttl_ms = millis(ttl);
        // Bump version only if this holder still owns the row; an
        // empty row set means the lease is gone.
        let row = sqlx::query_as(
            "UPDATE projector_leases SET \
                 renewed_at = now(), \
                 ttl_ms = $3, \
                 grace = $4, \
                 max_grace = $5, \
                 version = version + 1 \
             WHERE name = $1 AND holder = $2 AND version = $6 \
                AND now() < renewed_at + (ttl_ms * grace) * INTERVAL '1 ms' \
                AND now() < acquired_at + (ttl_ms * max_grace) * INTERVAL '1 ms' \
             RETURNING version",
        )
        .bind(&lease.name)
        .bind(lease.holder)
        .bind(ttl_ms)
        .bind(i64::from(grace))
        .bind(i64::from(max_grace))
        .bind(lease.version)
        .fetch_optional(&self.pool)
        .await;
        let row = match row {
            Ok(row) => row,
            Err(error) => {
                // The renewal never reached a decision — a pool timeout,
                // a dropped connection. The row still says how long the
                // lease was last known good for, so ask it: a transient
                // blip rides the grace window instead of stopping the
                // projector. The row's own stored policy and the
                // database's clock decide — no client skew in the
                // arithmetic. A row that is gone (taken over), a row
                // already past its bounds, or a store that cannot answer
                // at all is a lost lease: the driver stops rather than
                // guess.
                let error = PgStoreError::into_store(error);
                let renewed_until = self.presumed_held_until(&lease.name, lease.holder).await;
                return Err(LeaseError::Store {
                    error,
                    renewed_until,
                });
            }
        };
        match row {
            Some((version,)) => {
                lease.version = version;
                Ok(Instant::now())
            }
            None => Err(LeaseError::Lost),
        }
    }

    async fn release(&self, lease: Self::Lease) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM projector_leases WHERE name = $1 AND holder = $2")
            .bind(&lease.name)
            .bind(lease.holder)
            .execute(&self.pool)
            .await
            .map_err(PgStoreError::into_store)?;
        Ok(())
    }
}

impl PgLeaseStore {
    /// How long the named lease is presumed still held by `holder`:
    /// the smaller of its grace and max-grace bounds, from the row's
    /// own stored policy and the database's clock. `None` when the row
    /// is gone, past both bounds, or the store cannot answer.
    async fn presumed_held_until(&self, name: &str, holder: uuid::Uuid) -> Option<Instant> {
        let row = sqlx::query_as::<_, (i64, i64)>(
            "SELECT \
                 ceil(extract(epoch FROM (renewed_at \
                     + (ttl_ms * grace) * INTERVAL '1 ms' - now())) * 1000)::bigint, \
                ceil(extract(epoch FROM (acquired_at \
                     + (ttl_ms * max_grace) * INTERVAL '1 ms' - now())) * 1000)::bigint \
             FROM projector_leases WHERE name = $1 AND holder = $2",
        )
        .bind(name)
        .bind(holder)
        .fetch_optional(&self.pool)
        .await
        .ok()??;
        let (grace_left_ms, max_left_ms) = row;
        let left_ms = grace_left_ms.min(max_left_ms);
        if left_ms <= 0 {
            return None;
        }
        Instant::now().checked_add(Duration::from_millis(u64::try_from(left_ms).ok()?))
    }
}

/// `Duration` to whole milliseconds, matching the row's schema. A
/// duration past `i64` milliseconds saturates: the interval arithmetic
/// in the queries errors into a store failure, which is reportable —
/// a panic in the driver loop is not.
fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}
