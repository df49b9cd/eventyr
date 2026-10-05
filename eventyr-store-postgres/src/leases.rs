//! A [`ProjectorLease`], behind the `leases` feature: each subscription
//! name is held by at most one driver, recorded in the
//! `projector_leases` table (migration `0012_leases`) in the database
//! the projection reads from.

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
        let row: Option<(i64,)> = sqlx::query_as(
            "UPDATE projector_leases SET \
                 renewed_at = now(), \
                 ttl_ms = $3, \
                 grace = $4, \
                 max_grace = $5, \
                 version = version + 1 \
             WHERE name = $1 AND holder = $2 AND version = $6 \
             RETURNING version",
        )
        .bind(&lease.name)
        .bind(lease.holder)
        .bind(ttl_ms)
        .bind(i64::from(grace))
        .bind(i64::from(max_grace))
        .bind(lease.version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| LeaseError::Store {
            error: PgStoreError::into_store(error),
            renewed_until: None,
        })?;
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

/// `Duration` to whole milliseconds, matching the row's schema.
fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).expect("a lease policy's ttl fits in milliseconds")
}
