//! The runners: the projection that maintains the `balances` view, and
//! the wire-fee saga that reacts to deposits. Both run as the same
//! function on every replica; the lease decides which one projects.
//!
//! A replica is one task: catch up, then stand by for commits and re-run,
//! holding the lease while it projects. The policy stops at catch-up on a
//! cold start, then the runner loops; `LeaseLost` is a failover
//! reclaiming a stalled holder, and its checkpoint is already written by
//! the incumbent, so this one just stands by again.
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

/// The lease: renew every `ttl`, presumed dead after `grace` missed
/// renewals, never held past `max_grace` renewals without re-acquiring —
/// short enough that a standby takes over well under a second.
const LEASE: LeasePolicy = LeasePolicy {
    ttl: Duration::from_millis(100),
    grace: 3,
    max_grace: 100,
};

/// The projection policy: sixteen events per batch, 50 ms between
/// caught-up polls, 200 ms backoff after a failed apply. The runner loops
/// on it, so the cold start stops at catch-up and the loop re-runs.
const POLICY: SubscriptionPolicy =
    SubscriptionPolicy::new(16, Duration::from_millis(50), Duration::from_millis(200))
        .stop_at_catch_up();

/// The projection and saga inputs, generic over the store's concrete
/// types so either backend's composition hands its own over: the store as
/// every projection's source and signal, and the three ports it supplies,
/// each behind its `Arc`.
pub struct Inputs<S, C, L, V, W>
where
    S: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    C: CheckpointStore + Send + Sync + 'static,
    L: ProjectorLease + Send + Sync + 'static,
    V: ViewStore<Balance> + Send + Sync + 'static,
    W: CommitSignal + Send + Sync + 'static,
{
    /// The event store the projection subscribes to.
    pub source: Arc<S>,
    /// The store as the commit signal that wakes the runner.
    pub signal: Arc<W>,
    /// The checkpoints port.
    pub checkpoints: Arc<C>,
    /// The lease store the replicas compete through.
    pub leases: Arc<L>,
    /// The read model the projection maintains.
    pub views: Arc<V>,
}

impl<S, C, L, V, W> Clone for Inputs<S, C, L, V, W>
where
    S: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    C: CheckpointStore + Send + Sync + 'static,
    L: ProjectorLease + Send + Sync + 'static,
    V: ViewStore<Balance> + Send + Sync + 'static,
    W: CommitSignal + Send + Sync + 'static,
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

/// Runs the `balances` projection until shutdown: catch up, then stand
/// by for commits and re-run, holding the lease while the projection
/// writes. `LeaseLost` is a failover reclaiming a stalled holder; its
/// checkpoint is already written by the incumbent, so this one just
/// stands by.
pub async fn run_projection_once<S, C, L, V, W>(
    inputs: &Inputs<S, C, L, V, W>,
    shutdown: &watch::Sender<bool>,
) -> Result<(), StoreError>
where
    S: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    C: CheckpointStore + Send + Sync + 'static,
    L: ProjectorLease + Send + Sync + 'static,
    V: ViewStore<Balance> + Send + Sync + 'static,
    W: CommitSignal + Send + Sync + 'static,
{
    let mut shutdown = shutdown.subscribe();
    loop {
        let result = Projector::new(
            BALANCES,
            StoreSubscription::new(inputs.source.clone()),
            inputs.checkpoints.clone(),
            ViewProjection::new(BALANCES, inputs.views.clone(), balance_key),
        )
        .with_policy(POLICY)
        .wake_on(inputs.signal.clone())
        .lease_with_woken(inputs.leases.clone())
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
                    _ = sleep(Duration::from_millis(100)) => {}
                }
            }
            Err(RunError::Store(error)) => return Err(error),
        }
    }
}

/// The wire fee: every deposit into an account is followed by a
/// withdrawal of a tenth from it — the bank's charge. The saga reads
/// through a filter on the account's tag (the DCB's read-side cousin),
/// so only account streams drive it.
#[derive(Clone, Copy)]
struct WireFee;

impl Saga for WireFee {
    type Event = AccountEvent;
    type Command = AccountCommand;

    fn name(&self) -> &str {
        "wire-fee"
    }

    fn react(&self, event: &EventEnvelope<AccountEvent>) -> Vec<(StreamId, AccountCommand)> {
        let account_of = |account: u64| StreamId::for_aggregate::<Account>(&AccountId(account));
        match &event.event {
            AccountEvent::Deposited(Deposited { account, amount }) if amount.0 >= 10 => vec![(
                account_of(*account),
                AccountCommand::Withdraw {
                    amount: Cash(amount.0 / 10),
                },
            )],
            _ => Vec::new(),
        }
    }
}

/// Runs the wire-fee saga: `SkipRedelivered` keeps a replayed event's
/// dispatch from re-running, and `saga_dispatcher` routes each command
/// back through the mediator — stamped and idempotent — so a replayed
/// event never executes twice.
pub async fn run_saga<S, SS, C, L, V, W>(
    inputs: &Inputs<S, C, L, V, W>,
    app: &App<S, SS, V>,
    shutdown: &watch::Sender<bool>,
) -> Result<(), StoreError>
where
    S: EventStore<Event = AccountEvent> + Send + Sync + 'static,
    S: StreamsAll<Event = AccountEvent> + Send + Sync + 'static,
    SS: SnapshotStore<State = AccountState> + Send + Sync + 'static,
    C: CheckpointStore + Send + Sync + 'static,
    L: ProjectorLease + Send + Sync + 'static,
    V: ViewStore<Balance> + Send + Sync + 'static,
    W: CommitSignal + Send + Sync + 'static,
    Arc<S>: EventStore<Event = AccountEvent> + Send + Sync,
    Arc<SS>: SnapshotStore<State = AccountState> + Send + Sync,
{
    let mut shutdown = shutdown.subscribe();
    loop {
        let result = Projector::new(
            "wire-fee",
            StoreSubscription::new(inputs.source.clone()),
            inputs.checkpoints.clone(),
            SkipRedelivered::new(SagaProjection::new(
                WireFee,
                saga_dispatcher::<Account, _, _>(
                    app.clone(),
                    eventyr_mediator::store::parse_id::<AccountId>,
                ),
            )),
        )
        .with_policy(POLICY)
        .wake_on(inputs.signal.clone())
        .lease_with_woken(inputs.leases.clone())
        .with_policy(LEASE)
        .run_woken_leased(sleep)
        .await;
        match result {
            Ok(_) => return Ok(()),
            Err(RunError::Taken { .. } | RunError::LeaseLost { .. }) => {
                tokio::select! {
                    _ = shutdown.changed() => return Ok(()),
                    _ = sleep(Duration::from_millis(100)) => {}
                }
            }
            Err(RunError::Store(error)) => return Err(error),
        }
    }
}
