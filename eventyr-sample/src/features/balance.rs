//! The read model: a `balances` view the projector maintains, served
//! without touching the event store's log.
//!
//! The query routes to whichever view store the wiring named: the
//! in-memory backend's [`InMemoryBalances`], or the Postgres backend's
//! `PgViewStore` — the row the projection last wrote, which on the
//! database-backed deployment every replica shares. The stored
//! [`ViewRow`] gives the read-your-writes position the edge exposes as
//! `as_of`.

use eventyr::prelude::{EventEnvelope, Sequence};
use eventyr::projection::prelude::{View, ViewRow, ViewStore};
use eventyr_mediator::prelude::*;
use serde::Serialize;

use crate::domain::{AccountEvent, Cash, Deposited, Withdrawn};
use crate::features::{ApiError, Outcome};

/// One account's row: the balance the read side knows.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Balance {
    /// The current balance, in cents.
    pub balance: Cash,
}

impl View<AccountEvent> for Balance {
    fn initial() -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &EventEnvelope<AccountEvent>) {
        match &event.event {
            AccountEvent::Opened(_) => self.balance = Cash::ZERO,
            AccountEvent::Deposited(Deposited { amount, .. }) => self.balance.0 += amount.0,
            AccountEvent::Withdrawn(Withdrawn { amount, .. }) => self.balance.0 -= amount.0,
        }
    }
}

/// The view the projection maintains: one row per account.
pub const BALANCES: &str = "balances";

/// The row's key for an event — its account, or `None` to pass it by.
pub fn balance_key(event: &EventEnvelope<AccountEvent>) -> Option<String> {
    event.event.account_id().map(|account| account.to_string())
}

/// The balances' view store the in-memory wiring keeps in process: an
/// in-memory `ViewStore` the read path serves, and the runner maintains,
/// without a database round-trip.
pub type InMemoryBalances = eventyr::projection::prelude::InMemoryViewStore<Balance>;

/// What a read-your-writes endpoint returns: the row, and the global
/// sequence it is current at, so a caller can tell a fresh write's
/// balance from a stale one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BalanceRead {
    /// The balance, in cents.
    pub balance: Cash,
    /// The sequence the row is current at.
    pub as_of: Sequence,
}

impl BalanceRead {
    /// A row as the edge serves it: `None` when the account has no row.
    pub fn of(row: Option<ViewRow<Balance>>) -> Option<Self> {
        row.map(|row| Self {
            balance: row.value.balance,
            as_of: row.version,
        })
    }
}

/// Reads one account's balance.
#[derive(Clone, Debug)]
pub struct GetBalance {
    /// The account to read.
    pub account: u64,
}

impl Request for GetBalance {
    type Response = Outcome<Option<BalanceRead>>;
}

/// The balance query handler — a facade, not a second view store: it
/// names the view the composition's `views` field serves, and the
/// error maps one `StoreError` arm per status. The mediator's pointer
/// delegation covers the `Arc<V>` the field holds.
pub struct BalanceHandler<V> {
    views: std::sync::Arc<V>,
}

impl<V> Clone for BalanceHandler<V> {
    fn clone(&self) -> Self {
        Self {
            views: self.views.clone(),
        }
    }
}

impl<V> BalanceHandler<V> {
    /// The constructor the composition field calls.
    pub fn new(views: std::sync::Arc<V>) -> Self {
        Self { views }
    }
}

impl<M, V> RequestHandler<GetBalance, M> for BalanceHandler<V>
where
    M: Sync,
    V: ViewStore<Balance> + Send + Sync,
{
    async fn handle(&self, _mediator: &M, request: GetBalance) -> Outcome<Option<BalanceRead>> {
        self.views
            .load(BALANCES, &request.account.to_string())
            .await
            .map(BalanceRead::of)
            .map_err(|failure| ApiError::from(&failure))
    }
}
