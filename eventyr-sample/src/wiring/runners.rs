//! The runners: the projection that maintains the `balances` view, and
//! the wire-fee saga that reacts to deposits. Both run as the same
//! function on every replica; the lease decides which one projects.
//!
//! A replica is one task: catch up, then stand by for a commit and
//! re-run, holding the lease while it projects. The policy stops at
//! catch-up on a cold start, then the runner loops; `LeaseLost` is a
//! failover reclaiming a stalled holder, and its checkpoint is already
//! written by the incumbent, so this one just stands by again.
//!
//! The runners are generic over the store's concrete types — the
//! projection's source, the checkpoint store, the lease store, the view
//! store, and the commit signal — so the in-memory and the Postgres
//! compositions drive the same two functions.

use std::sync::Arc;
use std::time::Duration;

use eventyr::prelude::{EventEnvelope, Saga, StoreError, StreamId};
use eventyr::projection::prelude::{ViewProjection, ViewStore};
use eventyr::store::prelude::{EventStore, SnapshotStore, StreamsAll};
use eventyr::subscription::prelude::{
    CheckpointStore, CommitSignal, LeasePolicy, Projector, ProjectorLease, RunError,
    SagaProjection, SkipRedelivered, StoreSubscription, SubscriptionPolicy,
};
use eventyr_mediator::store::saga_dispatcher;
use tokio::sync::watch;
use tokio::time::sleep;

use crate::domain::{
    Account, AccountCommand, AccountEvent, AccountId, AccountState, Cash, Deposited,
};
use crate::features::{BALANCES, Balance, balance_key};
use crate::wiring::App;

/// How long a replica that lost the lease waits before it tries again —
/// short enough that a standby takes over well under a second, long
/// enough that the loop does not spin.
const STANDBY_POLL: Duration = Duration::from_millis(100);

/// The lease: renew every `ttl`, presumed dead after `grace` missed
/// renewals, never held past `max_grace` renewals without re-acquiring —
/// short enough that a standby takes over well under a second.
const LEASE: LeasePolicy = LeasePolicy {
    ttl: Duration::from_millis(100),
    grace: 3,
    max_grace: 100,
};

/// The projection policy: sixteen events per batch, 50 ms between
/// caught-up polls, 200 ms backoff after a failed apply. The runner
/// loops on it, so the cold start stops at catch-up and the loop
/// re-runs.
const POLICY: SubscriptionPolicy =
    SubscriptionPolicy::new(16, Duration::from_millis(50), Duration::from_millis(200))
        .stop_at_catch_up();

/// The projection and saga inputs, generic over the store's concrete
/// types so either backend's composition hands its own over: the store
/// as every projection's source and signal, and the three ports it
/// supplies, each behind its `Arc`.
pub struct Inputs<Store, Checkpoints, Leases, Views, Signal>
where
    Store: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    Checkpoints: CheckpointStore + Send + Sync + 'static,
    Leases: ProjectorLease + Send + Sync + 'static,
    Views: ViewStore<Balance> + Send + Sync + 'static,
    Signal: CommitSignal + Send + Sync + 'static,
{
    /// The event store the projection subscribes to.
    pub source: Arc<Store>,
    /// The store as the commit signal that wakes the runner.
    pub signal: Arc<Signal>,
    /// The checkpoints port.
    pub checkpoints: Arc<Checkpoints>,
    /// The lease store the replicas compete through.
    pub leases: Arc<Leases>,
    /// The read model the projection maintains.
    pub views: Arc<Views>,
}

impl<Store, Checkpoints, Leases, Views, Signal> Clone
    for Inputs<Store, Checkpoints, Leases, Views, Signal>
where
    Store: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    Checkpoints: CheckpointStore + Send + Sync + 'static,
    Leases: ProjectorLease + Send + Sync + 'static,
    Views: ViewStore<Balance> + Send + Sync + 'static,
    Signal: CommitSignal + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            signal: self.signal.clone(),
            checkpoints: self.checkpoints.clone(),
            leases: self.leases.clone(),
            views: self.views.clone(),
        }
    }
}

/// Run one leased projector to shutdown: catch up, then stand by for a
/// commit or for the lease to come free and re-run. `LeaseLost` is a
/// failover reclaiming a stalled holder; the incumbent's checkpoint is
/// already written, so this one stands by again.
async fn run_leased<P, Store, Checkpoints, Leases, Signal>(
    name: &'static str,
    projection: impl FnMut() -> P,
    source: &Arc<Store>,
    checkpoints: &Arc<Checkpoints>,
    leases: &Arc<Leases>,
    signal: &Arc<Signal>,
    shutdown: &watch::Sender<bool>,
) -> Result<(), StoreError>
where
    P: eventyr::subscription::prelude::Projection<Event = AccountEvent>,
    P::Error: std::fmt::Display + Send + Sync + 'static,
    Store: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    Checkpoints: CheckpointStore + Send + Sync + 'static,
    Leases: ProjectorLease + Send + Sync + 'static,
    Signal: CommitSignal + Send + Sync + 'static,
{
    let mut shutdown = shutdown.subscribe();
    let mut projection = projection;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let result = Projector::new(
            name,
            StoreSubscription::new(source.clone()),
            checkpoints.clone(),
            projection(),
        )
        .with_policy(POLICY)
        .wake_on(signal.clone())
        .lease_with_woken(leases.clone())
        .with_policy(LEASE)
        .run_woken_leased(sleep)
        .await;
        match result {
            Ok(_) => return Ok(()),
            Err(RunError::Taken { .. } | RunError::LeaseLost { .. }) => {
                // Another replica holds the lease, or reclaimed it from a
                // stall. Stand by for a commit or for shutdown.
                tokio::select! {
                    _ = shutdown.changed() => return Ok(()),
                    _ = sleep(STANDBY_POLL) => {}
                }
            }
            Err(RunError::Store(error)) => return Err(error),
        }
    }
}

/// Runs the `balances` projection until shutdown: catch up, then stand
/// by for commits and re-run, holding the lease while the projection
/// writes.
pub async fn run_projection<Store, Checkpoints, Leases, Views, Signal>(
    inputs: &Inputs<Store, Checkpoints, Leases, Views, Signal>,
    shutdown: &watch::Sender<bool>,
) -> Result<(), StoreError>
where
    Store: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    Checkpoints: CheckpointStore + Send + Sync + 'static,
    Leases: ProjectorLease + Send + Sync + 'static,
    Views: ViewStore<Balance> + Send + Sync + 'static,
    Signal: CommitSignal + Send + Sync + 'static,
{
    let views = inputs.views.clone();
    run_leased(
        BALANCES,
        move || ViewProjection::new(BALANCES, views.clone(), balance_key),
        &inputs.source,
        &inputs.checkpoints,
        &inputs.leases,
        &inputs.signal,
        shutdown,
    )
    .await
}

/// The wire fee: every deposit into an account at or above
/// [`WireFee::MINIMUM`] is followed by a withdrawal of
/// `amount / WireFee::RATE` — the bank's charge. The saga reads through
/// a filter on the account's tag, so only account streams drive it.
#[derive(Clone, Copy)]
struct WireFee;

impl WireFee {
    /// The smallest deposit the fee applies to, in cents.
    const MINIMUM: Cash = Cash(10);
    /// The fee is a tenth of the deposit.
    const RATE: u64 = 10;

    /// The fee for a deposit of `deposited`, when one applies.
    fn fee(&self, deposited: Cash) -> Option<Cash> {
        (deposited >= Self::MINIMUM).then_some(Cash(deposited.0 / Self::RATE))
    }
}

impl Saga for WireFee {
    type Event = AccountEvent;
    type Command = AccountCommand;

    fn name(&self) -> &str {
        "wire-fee"
    }

    fn react(&self, event: &EventEnvelope<AccountEvent>) -> Vec<(StreamId, AccountCommand)> {
        let account_of = |account: u64| StreamId::for_aggregate::<Account>(&AccountId(account));
        match &event.event {
            AccountEvent::Deposited(Deposited { account, amount }) => self
                .fee(*amount)
                .into_iter()
                .map(|charge| {
                    (
                        account_of(*account),
                        AccountCommand::Withdraw { amount: charge },
                    )
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// Runs the wire-fee saga: `SkipRedelivered` keeps a replayed event's
/// dispatch from re-running, and `saga_dispatcher` routes each command
/// back through the mediator — stamped and idempotent — so a replayed
/// event never executes twice.
pub async fn run_saga<Store, Snapshots, Checkpoints, Leases, Views, Signal>(
    inputs: &Inputs<Store, Checkpoints, Leases, Views, Signal>,
    app: &App<Store, Snapshots, Views>,
    shutdown: &watch::Sender<bool>,
) -> Result<(), StoreError>
where
    Store: EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Store: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Checkpoints: CheckpointStore + Send + Sync + 'static,
    Leases: ProjectorLease + Send + Sync + 'static,
    Views: ViewStore<Balance> + Send + Sync + 'static,
    Signal: CommitSignal + Send + Sync + 'static,
    Arc<Store>: EventStore<Event = AccountEvent> + Send + Sync,
    Arc<Snapshots>: SnapshotStore<State = AccountState> + Send + Sync,
{
    let app = app.clone();
    run_leased(
        "wire-fee",
        move || {
            SkipRedelivered::new(SagaProjection::new(
                WireFee,
                saga_dispatcher::<Account, _, _>(
                    app.clone(),
                    eventyr_mediator::store::parse_id::<AccountId>,
                ),
            ))
        },
        &inputs.source,
        &inputs.checkpoints,
        &inputs.leases,
        &inputs.signal,
        shutdown,
    )
    .await
}
