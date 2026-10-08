//! The Postgres commit signal (roadmap 0.7.2): `LISTEN eventyr_commits`.
//!
//! Every append raises `NOTIFY eventyr_commits` inside its transaction
//! (migration 0007); Postgres delivers it when the transaction commits.
//! [`PgCommitSignal`] opens a dedicated listening connection per
//! subscriber, so a caught-up projector wakes on the commit instead of
//! after its idle sleep — across processes, since the notification
//! comes from the database.

use eventyr_core::error::StoreError;
use eventyr_store::notify::{CommitListener, CommitSignal};
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};

use crate::PgStoreError;

/// The channel every append notifies.
pub const CHANNEL: &str = "eventyr_commits";

/// Subscribes to a database's commits.
///
/// Each [`subscribe`](CommitSignal::subscribe) opens its own connection
/// (with the store pool's connect options — the same database, user,
/// and `search_path`), outside the store's pool, so a listener never
/// takes a connection the store needs. The channel is database-wide:
/// a listener may wake for a commit in another schema and poll for
/// nothing, which costs one empty poll.
#[derive(Clone)]
pub struct PgCommitSignal {
    pool: PgPool,
}

impl PgCommitSignal {
    /// A signal over the store's database.
    pub fn new<E>(store: &crate::PgStore<E>) -> Self {
        Self {
            pool: store.pool().clone(),
        }
    }
}

impl CommitSignal for PgCommitSignal {
    type Listener = PgCommitListener;

    async fn subscribe(&self) -> Result<Self::Listener, StoreError> {
        // A one-connection pool of its own: the listener's reconnects
        // draw from it, never from the store's pool.
        let options = (*self.pool.connect_options()).clone();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with(options)
            .await
            .map_err(PgStoreError::into_store)?;
        let mut listener = PgListener::connect_with(&pool)
            .await
            .map_err(PgStoreError::into_store)?;
        // Once LISTEN returns, every later commit is queued for this
        // connection — arming happens here, before the caller's poll.
        listener
            .listen(CHANNEL)
            .await
            .map_err(PgStoreError::into_store)?;
        Ok(PgCommitListener { listener })
    }
}

/// A listening connection. Dropping it closes the connection.
pub struct PgCommitListener {
    listener: PgListener,
}

impl CommitListener for PgCommitListener {
    /// Cancel-safe: sqlx's receive keeps a partly read message in its
    /// buffer, and a notification is consumed only when this call
    /// returns. A lost connection resolves as a wake-up — commits may
    /// have happened while it was down, so polling is the right answer
    /// — and the listener reconnects on the next call.
    async fn committed(&mut self) -> Result<(), StoreError> {
        // `None` is a lost connection; either way, poll.
        self.listener
            .try_recv()
            .await
            .map_err(PgStoreError::into_store)?;
        // Coalesce whatever else already arrived.
        while self.listener.next_buffered().is_some() {}
        Ok(())
    }
}
