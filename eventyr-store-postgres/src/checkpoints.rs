//! A [`CheckpointStore`], behind the `checkpoints` feature: each
//! subscription's last-acked position in the `checkpoints` table
//! (migration `0011_checkpoints`), in the database the projection reads
//! from.
//!
//! A restart resumes where it left off.

use eventyr_core::error::StoreError;
use eventyr_core::subscription_machine::Checkpoint;
use eventyr_core::vocabulary::Sequence;
use eventyr_store::store::sql_position;
use eventyr_subscription::checkpoint::CheckpointStore;

use crate::{PgStore, PgStoreError};

/// Subscription checkpoints in Postgres, one row per subscription name.
///
/// [`store`](CheckpointStore::store) is one autocommitted upsert, durable
/// when it returns.
#[derive(Clone)]
pub struct PgCheckpointStore {
    pool: sqlx::postgres::PgPool,
}

impl PgCheckpointStore {
    /// A checkpoint store over `store`'s pool.
    pub fn new(store: &PgStore<impl Send>) -> Self {
        Self {
            pool: store.pool().clone(),
        }
    }

    /// A checkpoint store over a pool of its own. Run
    /// [`migrate`](crate::store::migrate) on the database first.
    pub fn from_pool(pool: sqlx::postgres::PgPool) -> Self {
        Self { pool }
    }
}

impl CheckpointStore for PgCheckpointStore {
    async fn load(&self, name: &str) -> Result<Checkpoint, StoreError> {
        let stored: Option<i64> =
            sqlx::query_scalar("SELECT global_sequence FROM checkpoints WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(PgStoreError::into_store)?;
        let Some(sequence) = stored else {
            return Ok(Checkpoint::ORIGIN);
        };
        let sequence = u64::try_from(sequence).map_err(|_| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "negative checkpoint: {sequence}"
            )))
        })?;
        Ok(Checkpoint::new(Sequence::new(sequence)))
    }

    async fn store(&self, name: &str, checkpoint: Checkpoint) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO checkpoints (name, global_sequence) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE \
             SET global_sequence = EXCLUDED.global_sequence, updated_at = now()",
        )
        .bind(name)
        .bind(sql_position(checkpoint.as_sequence().as_u64()))
        .execute(&self.pool)
        .await
        .map_err(PgStoreError::into_store)?;
        Ok(())
    }
}
