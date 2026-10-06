//! The projector runner: the async driver for a
//! [`SubscriptionMachine`], and the §6 `Projection` trait.
//!
//! A driver is a boring loop: interpret the action, do the I/O, report
//! the input — fetch from the [`SubscriptionSource`], apply to the
//! [`Projection`], ack through the [`CheckpointStore`], and feed results
//! back until the machine is done. All policy — redelivery, the ack
//! boundary, idle and retry timing — lives in the machine.
//!
//! Per §6's "no built-in consumer loop": nothing here owns a task. The
//! caller owns the loop — `tokio::spawn(projector.run(..))` — so pause,
//! degrade, and shutdown stay the caller's decision.

use core::future::Future;

use std::sync::Arc;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::subscription::{
    FailurePolicy, SleepReason, SubscriptionAction, SubscriptionInput, SubscriptionMachine,
    SubscriptionOutcome, SubscriptionPolicy,
};
use eventyr_store::metrics::names::{
    ACK_FAILURES, PARK_FAILURES, PARKED_EVENTS, PROJECTED_EVENTS, PROJECTION_FETCH_SPAN,
};
use eventyr_store::metrics::{Metrics, NoopMetrics};
use eventyr_store::notify::{CommitListener, CommitSignal, NoSignal};

use crate::checkpoint::CheckpointStore;
use crate::lease::{LeaseError, LeasePolicy, ProjectorLease};
use crate::parked::{NoParking, ParkedEvent, ParkedStore};
use crate::source::SubscriptionSource;

/// A read model: folds the global event stream into a queryable shape.
///
/// §6 verbatim. At-least-once: `apply` may see the same envelope twice
/// (the ack moves only after a batch applies), so it must be idempotent.
pub trait Projection: Send {
    /// The domain event folded into the projection.
    type Event: Send;
    /// Rejection of one event: the runner backs off and redelivers the
    /// whole batch from the last ack — a transient failure surfaces the
    /// same way as a genuinely unprocessable event.
    type Error;

    /// Left-fold one event into the read model.
    fn apply(
        &mut self,
        event: &EventEnvelope<Self::Event>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// One [`Projection`] over many: applies each event to every member, in
/// order, before the ack moves.
///
/// A rejection from any member rejects the batch, so the runner backs
/// off and redelivers the batch to every member — the at-least-once
/// contract applies to the fan-out as one projection.
pub struct Fanout<P> {
    projections: Vec<P>,
}

impl<P> Fanout<P> {
    /// A fan-out applying each event to every member of `projections`,
    /// in order.
    pub fn new(projections: Vec<P>) -> Self {
        Self { projections }
    }
}

impl<P> Projection for Fanout<P>
where
    P: Projection,
    P::Event: Sync,
{
    type Event = P::Event;
    type Error = P::Error;

    async fn apply(&mut self, event: &EventEnvelope<Self::Event>) -> Result<(), Self::Error> {
        for projection in &mut self.projections {
            projection.apply(event).await?;
        }
        Ok(())
    }
}

/// A projection made idempotent: wraps another `Projection` and skips
/// any envelope its stream position says was already folded.
///
/// At-least-once is literal: the ack moves only after a batch applies,
/// so a redelivered batch re-applies the events before the one that
/// failed. A projection that counts or mutates must not fold twice, and
/// §6 asks every projection to be idempotent without helping. This
/// wrapper is that help — a first-class duplicate check, not a
/// work-around in each projection.
///
/// Because a stream appends from `Version(1)` upward with no gaps, the
/// newest position *this projection instance* has folded per stream
/// exactly partitions "already folded" from "new". An envelope at or
/// below it is a redelivery and is skipped; the fold runs only for
/// events past it. The map is in-memory, so a *new run* folding from a
/// checkpoint behind an envelope that was applied before a crash still
/// sees it — the wrapper dedupes within a run, the checkpoint dedupes
/// between runs. A projection that survives restarts by folding from
/// the start (a rebuild, an inline view moving to async) needs no
/// dedupe state at all: it is total on re-read.
///
/// The inner projection's own failure still fails the envelope (the
/// position is recorded only after a successful fold), so a poison
/// event under `FailurePolicy::Park` retries as usual — and its batch
/// will not re-count the events before it.
pub struct SkipRedelivered<P> {
    inner: P,
    applied: std::collections::BTreeMap<eventyr_core::vocabulary::StreamId, u64>,
}

impl<P> SkipRedelivered<P> {
    /// `inner`, with redelivered envelopes skipped.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            applied: std::collections::BTreeMap::new(),
        }
    }

    /// The wrapped projection.
    pub fn into_inner(self) -> P {
        self.inner
    }
}

impl<P> Projection for SkipRedelivered<P>
where
    P: Projection,
    P::Event: Sync,
{
    type Event = P::Event;
    type Error = P::Error;

    async fn apply(&mut self, event: &EventEnvelope<Self::Event>) -> Result<(), Self::Error> {
        if event.version.as_u64() <= *self.applied.get(&event.stream_id).unwrap_or(&0) {
            return Ok(()); // a redelivery: already folded
        }
        self.inner.apply(event).await?;
        self.applied
            .insert(event.stream_id.clone(), event.version.as_u64());
        Ok(())
    }
}

/// The driver's `caught_up` hook: called once per idle sleep, the pulse
/// that says the projection has caught up to its source's head. A test
/// that wants to assert on a live read model waits on it instead of on a
/// timer (Emmett's `whenCaughtUp()`).
///
/// The trait is one method so any notification primitive can serve as
/// the listener — a `tokio::sync::Notify`, an event-listener, a test's
/// counter — and the driver carries a `&dyn Catch` like its metrics.
pub trait Catch: Send + Sync {
    /// Fire the pulse.
    fn caught_up(&self);
}

/// The no-signal catch: the default port.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCatch;

impl Catch for NoCatch {
    fn caught_up(&self) {}
}

/// The driver's optional ports: what [`drive_projector`] wakes on,
/// parks into, signals on catch-up, and reports to. Each defaults to
/// doing nothing.
///
/// ```
/// use eventyr_store::metrics::NoopMetrics;
/// use eventyr_subscription::parked::InMemoryParkedStore;
/// use eventyr_subscription::runner::DriverPorts;
///
/// let parked = InMemoryParkedStore::<u64>::new();
/// let ports = DriverPorts::new()
///     .park_into(&parked)
///     .with_metrics(&NoopMetrics);
/// # let _ = ports;
/// ```
pub struct DriverPorts<'m, W = NoSignal, K = NoParking> {
    wake: W,
    parked: K,
    metrics: &'m dyn Metrics,
    catch: &'m dyn Catch,
}

impl DriverPorts<'static> {
    /// No wake-ups (idle sleeps run their course), no parked store
    /// ([`NoParking`] refuses every park), [`NoopMetrics`], and no
    /// catch-up pulse.
    pub fn new() -> Self {
        Self {
            wake: NoSignal,
            parked: NoParking,
            metrics: &NoopMetrics,
            catch: &NoCatch,
        }
    }
}

impl Default for DriverPorts<'static> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'m, W, K> DriverPorts<'m, W, K> {
    /// End an *idle* sleep early when `listener` reports a commit
    /// (0.7.2): a caught-up projector polls at once instead of after
    /// `idle_sleep`.
    ///
    /// Arm `listener` (via
    /// [`CommitSignal::subscribe`])
    /// before driving: a commit after arming and before the first idle
    /// sleep is remembered, so none slips between a poll and the wait.
    /// Backoff sleeps ([`SleepReason::Backoff`]) ignore it and run their
    /// course. A broken listener (`committed` returning `Err`) is dropped
    /// for the rest of the run and the driver falls back to its timer —
    /// the poll was authoritative all along.
    pub fn wake_on<W2: CommitListener>(self, listener: W2) -> DriverPorts<'m, W2, K> {
        DriverPorts {
            wake: listener,
            parked: self.parked,
            metrics: self.metrics,
            catch: self.catch,
        }
    }

    /// Record the events the machine parks (0.7.7) in `parked`. Only a
    /// machine whose policy is
    /// [`FailurePolicy::Park`]
    /// ever parks.
    pub fn park_into<K2>(self, parked: K2) -> DriverPorts<'m, W, K2> {
        DriverPorts {
            wake: self.wake,
            parked,
            metrics: self.metrics,
            catch: self.catch,
        }
    }

    /// Report each fetch, apply, park, and ack through `metrics`
    /// (0.5.3): projection progress, latency, and failures become
    /// observable.
    pub fn with_metrics<'n>(self, metrics: &'n dyn Metrics) -> DriverPorts<'n, W, K>
    where
        'm: 'n,
    {
        DriverPorts {
            wake: self.wake,
            parked: self.parked,
            metrics,
            catch: self.catch,
        }
    }

    /// Signal `catch` on every idle sleep of the run (§14): the
    /// projection has caught up to its source's head and the driver is
    /// pausing. A test waiting on the pulse reads the read model the
    /// batch just wrote instead of guessing a polling gap.
    pub fn caught_up_on<'n>(self, catch: &'n dyn Catch) -> DriverPorts<'n, W, K>
    where
        'm: 'n,
    {
        DriverPorts {
            wake: self.wake,
            parked: self.parked,
            metrics: self.metrics,
            catch,
        }
    }
}

/// Drives `machine` against a source, a checkpoint store, and a
/// projection until it finishes, reporting its terminal outcome.
///
/// Pausing and error hookup are the caller's call (§6's "no built-in
/// consumer loop"): `drive_projector` drives only the machine. Construct
/// the machine itself via [`Projector::run`] for the common case.
///
/// - `Fetch` → [`SubscriptionSource::fetch`]; a store error reports
///   [`Failed`](SubscriptionInput::Failed);
/// - `Apply` → [`Projection::apply`]; a rejection reports
///   [`ApplyFailed`](SubscriptionInput::ApplyFailed);
/// - `Park` → [`ParkedStore::park`] on the ports' parked store; a
///   refusal reports [`ParkFailed`](SubscriptionInput::ParkFailed) and
///   counts on [`PARK_FAILURES`];
/// - `Ack` → [`CheckpointStore::store`]; a failure reports
///   [`AckFailed`](SubscriptionInput::AckFailed) and counts on
///   [`ACK_FAILURES`];
/// - `Sleep` → `sleep(duration)`, cut short on an idle sleep by the
///   ports' listener; the machine decided the duration — only the wait
///   happens in the driver;
/// - `Done` → stop and return the outcome.
///
/// [`DriverPorts::new()`] is the plain driver: no wake-ups, no parking,
/// no metrics.
pub async fn drive_projector<E, S, C, P, F, Fut, W, K>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    mut projection: P,
    mut sleep: F,
    ports: DriverPorts<'_, W, K>,
) -> SubscriptionOutcome
where
    S: SubscriptionSource<Event = E>,
    C: CheckpointStore,
    P: Projection<Event = E>,
    P::Error: core::fmt::Display,
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    W: CommitListener,
    K: ParkedStore<E>,
    E: Clone + Send,
{
    let DriverPorts {
        wake,
        parked,
        metrics,
        catch,
    } = ports;
    let mut wake = Some(wake);
    let mut action = machine.start();
    // The batch being applied, when `Applying` began — for the latency
    // histogram. The machine owns *what* to do; the driver owns timing it.
    let mut batch_started: Option<std::time::Instant> = None;
    loop {
        action = match action {
            SubscriptionAction::Fetch { from, limit } => {
                let fetched = source.fetch(from, limit).await;
                // The sequence span this fetch covered: zero on a
                // caught-up poll.
                if let Ok(batch) = &fetched {
                    let last = batch
                        .events
                        .last()
                        .map_or(from.as_sequence().as_u64(), |e| e.sequence.as_u64());
                    metrics.gauge(
                        PROJECTION_FETCH_SPAN,
                        last.saturating_sub(from.as_sequence().as_u64()),
                    );
                }
                match fetched {
                    Ok(batch) => machine.handle(SubscriptionInput::Fetched { batch }),
                    Err(error) => machine.handle(SubscriptionInput::Failed(error)),
                }
            }
            SubscriptionAction::Apply { envelope } => {
                let sequence = envelope.sequence;
                if batch_started.is_none() {
                    batch_started = Some(std::time::Instant::now());
                }
                let result = projection.apply(&envelope).await;
                match result {
                    Ok(()) => {
                        metrics.counter(PROJECTED_EVENTS, 1);
                        machine.handle(SubscriptionInput::Applied)
                    }
                    Err(error) => {
                        batch_started = None;
                        machine.handle(SubscriptionInput::ApplyFailed {
                            error: StoreError::other(format!(
                                "projection applying sequence {sequence}: {error}"
                            )),
                        })
                    }
                }
            }
            SubscriptionAction::Park {
                envelope,
                attempts,
                error,
            } => {
                batch_started = None;
                let event = ParkedEvent {
                    subscription: name.to_owned(),
                    envelope,
                    attempts,
                    error: error.to_string(),
                };
                let recorded = parked.park(event).await;
                let recorded = recorded.inspect_err(|_| metrics.counter(PARK_FAILURES, 1));
                match recorded {
                    Ok(()) => {
                        metrics.counter(PARKED_EVENTS, 1);
                        machine.handle(SubscriptionInput::Parked)
                    }
                    Err(error) => machine.handle(SubscriptionInput::ParkFailed { error }),
                }
            }
            SubscriptionAction::Ack { checkpoint } => {
                let stored = checkpoints.store(name, checkpoint).await;
                let stored = stored.inspect_err(|_| metrics.counter(ACK_FAILURES, 1));
                match stored {
                    Ok(()) => {
                        if let Some(start) = batch_started.take() {
                            metrics.histogram(
                                eventyr_store::metrics::names::PROJECT_BATCH_LATENCY,
                                start.elapsed(),
                            );
                        }
                        machine.handle(SubscriptionInput::Acked)
                    }
                    Err(error) => {
                        batch_started = None;
                        machine.handle(SubscriptionInput::AckFailed { error })
                    }
                }
            }
            SubscriptionAction::Sleep { for_, reason } => {
                if reason == SleepReason::Idle {
                    catch.caught_up();
                }
                wait(&mut sleep, for_, reason, &mut wake).await;
                machine.handle(SubscriptionInput::Slept)
            }
            SubscriptionAction::Done(outcome) => return outcome,
        };
    }
}

/// How a leased projector run ended before the machine finished (0.7.9).
///
/// The lease is driver policy — the `SubscriptionMachine`'s protocol has
/// no transition for it — so "lost" surfaces as this error, never as a
/// machine outcome.
#[derive(Debug)]
pub enum RunError {
    /// The store the checkpoint lives in failed.
    Store(StoreError),
    /// The lease for this projector's name expired or was taken over;
    /// the driver stopped without writing a checkpoint again. The
    /// enclosed checkpoint is the last one *acked*, the position the
    /// next driver resumes from.
    LeaseLost {
        /// The projected name the lease covered.
        name: String,
        /// The last checkpoint acked before the lease went.
        checkpoint: eventyr_core::subscription::Checkpoint,
    },
    /// `acquire` on entry found the name already held: the caller backs
    /// off and retries rather than racing the holder's checkpoint.
    Taken {
        /// The name the driver wanted, held elsewhere.
        name: String,
    },
}

impl core::fmt::Display for RunError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Store(error) => write!(f, "the store failed: {error}"),
            Self::LeaseLost { name, .. } => {
                write!(f, "the lease for `{name}` was lost mid-run")
            }
            Self::Taken { name } => write!(f, "the lease for `{name}` is held elsewhere"),
        }
    }
}

impl core::error::Error for RunError {}

impl From<StoreError> for RunError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Release the held lease when the driver leaves the loop, on any path.
struct LeaseGuard<'l, Le: ProjectorLease> {
    leases: &'l Le,
    lease: Option<Le::Lease>,
}

impl<Le: ProjectorLease> LeaseGuard<'_, Le> {
    /// `lease` here; releasing it is the destructor's work. A lease
    /// dropped this way never reports to the return value — a lost
    /// one's release is best-effort Ok either way.
    fn new<'l>(leases: &'l Le, lease: Le::Lease) -> LeaseGuard<'l, Le> {
        LeaseGuard {
            leases,
            lease: Some(lease),
        }
    }

    /// The held lease mutably, for the renewal.
    fn lease_mut(&mut self) -> &mut Le::Lease {
        self.lease.as_mut().expect("the guard holds its lease")
    }

    /// Disarm: the run is over, hand the lease out for release
    /// without the Drop. The caller awaits the release.
    async fn disarm(mut self) -> Result<(), RunError> {
        let lease = self.lease.take().expect("the guard holds its lease");
        self.leases.release(lease).await?;
        core::mem::forget(self);
        Ok(())
    }
}

impl<Le: ProjectorLease> Drop for LeaseGuard<'_, Le> {
    fn drop(&mut self) {
        // The lease release is async, so the guard cannot await it here;
        // the name expires on its grace bound regardless. Only the
        // disarming path (a completed run, or `lost`) awaits release.
        let _ = self.lease.take();
    }
}

/// [`drive_projector`] under a [`ProjectorLease`] (0.7.9): hold the
/// named lease from before the checkpoint is read until the run ends —
/// renewed when due before every `Fetch` and before every `Ack` (the
/// renewal is the fence on the checkpoint write) — and stop on a loss
/// with the last acked position, never writing again.
///
/// The `SubscriptionMachine`'s protocol is untouched: nothing here is
/// a transition it can make (§7); the driver simply refuses to keep
/// driving once the name is no longer exclusive. A `LeaseLost` is the
/// signal to hand the name to a retrying supervisor, not a machine
/// state.
#[allow(clippy::too_many_arguments)]
pub async fn drive_projector_leased<E, S, C, P, F, Fut, W, K, Le>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    mut projection: P,
    mut sleep: F,
    ports: DriverPorts<'_, W, K>,
    leases: &Le,
    policy: LeasePolicy,
) -> Result<SubscriptionOutcome, RunError>
where
    S: SubscriptionSource<Event = E>,
    C: CheckpointStore,
    P: Projection<Event = E>,
    P::Error: core::fmt::Display,
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    W: CommitListener,
    K: ParkedStore<E>,
    Le: ProjectorLease,
    E: Clone + Send,
{
    let lease = match leases
        .acquire(name, policy.ttl, policy.grace, policy.max_grace)
        .await
    {
        Ok(lease) => lease,
        Err(LeaseError::Taken) => {
            return Err(RunError::Taken {
                name: name.to_owned(),
            });
        }
        Err(LeaseError::Lost) => {
            return Err(RunError::LeaseLost {
                name: name.to_owned(),
                checkpoint: eventyr_core::subscription::Checkpoint::ORIGIN,
            });
        }
        Err(LeaseError::Store { error, .. }) => return Err(RunError::Store(error)),
    };
    let mut guard = LeaseGuard::new(leases, lease);
    let DriverPorts {
        wake,
        parked,
        metrics,
        catch,
    } = ports;
    let mut wake = Some(wake);
    let mut renewed_at = std::time::Instant::now();
    let mut last_acked = checkpoints.load(name).await.map_err(RunError::Store)?;
    let mut action = machine.start();
    let mut batch_started: Option<std::time::Instant> = None;
    loop {
        action = match action {
            SubscriptionAction::Fetch { from, limit } => {
                // Renew once due; the fetch's answer is covered by a
                // currently-held lease.
                if std::time::Instant::now() >= renewed_at + policy.ttl {
                    match renew_lease(leases, guard.lease_mut(), policy).await {
                        RenewOutcome::Renewed(at) => renewed_at = at,
                        RenewOutcome::Lost => {
                            return Err(guard.lost(name, last_acked).await);
                        }
                    }
                }
                match source.fetch(from, limit).await {
                    Ok(batch) => machine.handle(SubscriptionInput::Fetched { batch }),
                    Err(error) => machine.handle(SubscriptionInput::Failed(error)),
                }
            }
            SubscriptionAction::Apply { envelope } => {
                let sequence = envelope.sequence;
                if batch_started.is_none() {
                    batch_started = Some(std::time::Instant::now());
                }
                match projection.apply(&envelope).await {
                    Ok(()) => {
                        metrics.counter(PROJECTED_EVENTS, 1);
                        machine.handle(SubscriptionInput::Applied)
                    }
                    Err(error) => {
                        batch_started = None;
                        machine.handle(SubscriptionInput::ApplyFailed {
                            error: StoreError::other(format!(
                                "projection applying sequence {sequence}: {error}"
                            )),
                        })
                    }
                }
            }
            SubscriptionAction::Park {
                envelope,
                attempts,
                error,
            } => {
                batch_started = None;
                let event = ParkedEvent {
                    subscription: name.to_owned(),
                    envelope,
                    attempts,
                    error: error.to_string(),
                };
                let recorded = parked.park(event).await;
                let recorded = recorded.inspect_err(|_| metrics.counter(PARK_FAILURES, 1));
                match recorded {
                    Ok(()) => {
                        metrics.counter(PARKED_EVENTS, 1);
                        machine.handle(SubscriptionInput::Parked)
                    }
                    Err(error) => machine.handle(SubscriptionInput::ParkFailed { error }),
                }
            }
            SubscriptionAction::Ack { checkpoint } => {
                // Renew first: the renewal *is* the fence on the
                // checkpoint write.
                match renew_lease(leases, guard.lease_mut(), policy).await {
                    RenewOutcome::Renewed(at) => renewed_at = at,
                    RenewOutcome::Lost => {
                        return Err(guard.lost(name, last_acked).await);
                    }
                }
                match checkpoints.store(name, checkpoint).await {
                    Ok(()) => {
                        last_acked = checkpoint;
                        if let Some(start) = batch_started.take() {
                            metrics.histogram(
                                eventyr_store::metrics::names::PROJECT_BATCH_LATENCY,
                                start.elapsed(),
                            );
                        }
                        metrics.counter(eventyr_store::metrics::names::LEASE_RENEWALS, 1);
                        machine.handle(SubscriptionInput::Acked)
                    }
                    Err(error) => {
                        batch_started = None;
                        machine.handle(SubscriptionInput::AckFailed { error })
                    }
                }
            }
            SubscriptionAction::Sleep { for_, reason } => {
                if reason == SleepReason::Idle {
                    catch.caught_up();
                }
                wait(&mut sleep, for_, reason, &mut wake).await;
                machine.handle(SubscriptionInput::Slept)
            }
            SubscriptionAction::Done(outcome) => {
                guard.disarm().await?;
                return Ok(outcome);
            }
        };
    }
}

/// The driver's two renewal outcomes.
enum RenewOutcome {
    Renewed(std::time::Instant),
    Lost,
}

/// Renew the lease, translating the port's error into the driver's
/// two-way answer and counting renewals.
async fn renew_lease<Le: ProjectorLease>(
    leases: &Le,
    lease: &mut Le::Lease,
    policy: LeasePolicy,
) -> RenewOutcome {
    match leases
        .renew(lease, policy.ttl, policy.grace, policy.max_grace)
        .await
    {
        Ok(at) => RenewOutcome::Renewed(at),
        Err(LeaseError::Lost) => RenewOutcome::Lost,
        Err(LeaseError::Taken) => RenewOutcome::Lost, // unreachable on renew in any conforming store
        Err(LeaseError::Store { renewed_until, .. }) => match renewed_until {
            Some(deadline) if std::time::Instant::now() < deadline => {
                RenewOutcome::Renewed(deadline)
            }
            _ => RenewOutcome::Lost, // doubtful and out of renewals stops
        },
    }
}

impl<Le: ProjectorLease> LeaseGuard<'_, Le> {
    /// Disarm with the lease lost: release best-effort, then return the
    /// last acked checkpoint as the resume point.
    async fn lost(
        mut self,
        name: &str,
        last_acked: eventyr_core::subscription::Checkpoint,
    ) -> RunError {
        if let Some(lease) = self.lease.take() {
            let _ = self.leases.release(lease).await;
        }
        core::mem::forget(self);
        RunError::LeaseLost {
            name: name.to_owned(),
            checkpoint: last_acked,
        }
    }
}

/// One `Sleep`: an idle sleep races the timer against `wake` and ends
/// at whichever comes first; a backoff, or a run without a listener,
/// waits out the timer. A listener that breaks is dropped (`*wake`
/// becomes `None`) after this wait finishes on the timer.
async fn wait<F, Fut, W>(
    sleep: &mut F,
    for_: core::time::Duration,
    reason: SleepReason,
    wake: &mut Option<W>,
) where
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    W: CommitListener,
{
    let (SleepReason::Idle, Some(listener)) = (reason, wake.as_mut()) else {
        sleep(for_).await;
        return;
    };
    let broken = {
        let timer = core::pin::pin!(sleep(for_));
        let woken = core::pin::pin!(listener.committed());
        match futures::future::select(timer, woken).await {
            futures::future::Either::Right((Err(_), timer)) => {
                // The signal broke: finish this wait on the timer.
                timer.await;
                true
            }
            // Timer or wake-up: either way, poll now.
            _ => false,
        }
    };
    if broken {
        // Poll on the timer alone from now on.
        *wake = None;
    }
}

/// The blocking [`drive_projector`]: drives `machine` to its outcome
/// without an async runtime, parking the thread through every `sleep`.
pub fn drive_projector_blocking<E, S, C, P, F, Fut, W, K>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    projection: P,
    sleep: F,
    ports: DriverPorts<'_, W, K>,
) -> SubscriptionOutcome
where
    S: SubscriptionSource<Event = E>,
    C: CheckpointStore,
    P: Projection<Event = E>,
    P::Error: core::fmt::Display,
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    W: CommitListener,
    K: ParkedStore<E>,
    E: Clone + Send,
{
    futures::executor::block_on(drive_projector(
        machine,
        name,
        source,
        checkpoints,
        projection,
        sleep,
        ports,
    ))
}

/// A bundled projector: `source` + `checkpoints` + `projection` + a
/// name to scope the checkpoint, run via [`Projector::run`].
///
/// Owns a runner per §6 — that is to say, it owns *the machine*, not
/// the loop: `run` is a future the caller spawns (e.g.
/// `tokio::spawn(projector.run(..))`).
pub struct Projector<S, C, P, W = NoSignal, K = NoParking> {
    source: S,
    checkpoints: C,
    projection: P,
    policy: SubscriptionPolicy,
    name: String,
    wake: W,
    parked: K,
    /// `None` reports to [`NoopMetrics`].
    metrics: Option<Arc<dyn Metrics>>,
}

impl<S, C, P> Projector<S, C, P> {
    /// A projector over `source` persisting to `checkpoints` into
    /// `projection`, checkpointed under `name`.
    pub fn new(name: impl Into<String>, source: S, checkpoints: C, projection: P) -> Self {
        Self {
            source,
            checkpoints,
            projection,
            policy: SubscriptionPolicy::default(),
            name: name.into(),
            wake: NoSignal,
            parked: NoParking,
            metrics: None,
        }
    }
}

impl<S, C, P, W, K> Projector<S, C, P, W, K> {
    /// Tune the subscription's batch/idle/retry policy.
    ///
    /// This sets the whole policy, its
    /// [`on_failure`](SubscriptionPolicy::on_failure) included, so call
    /// [`park_into`](Self::park_into) after it, not before. A
    /// [`FailurePolicy::Park`] needs a parked store: a projector given
    /// one without [`park_into`](Self::park_into) refuses to
    /// [`run`](Self::run) rather than retry the event forever against
    /// [`NoParking`].
    pub fn with_policy(mut self, policy: SubscriptionPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Report each fetch, apply, park, and ack through `metrics` (0.5.3)
    /// — among them [`PARKED_EVENTS`], the one to alert on. Without it
    /// the projector reports to [`NoopMetrics`].
    pub fn with_metrics(mut self, metrics: impl Metrics + 'static) -> Self {
        self.metrics = Some(Arc::new(metrics));
        self
    }

    /// Wake on commits (0.7.2): when caught up, poll as soon as
    /// `signal` reports a commit instead of after the idle sleep. The
    /// idle sleep stays the fallback, so a lost wake-up costs latency,
    /// never an event.
    ///
    /// Usually the store itself: `.wake_on(store.clone())` for the
    /// in-memory, fjall, and SQLite stores, a
    /// `PgCommitSignal` for Postgres.
    pub fn wake_on<W2: CommitSignal>(self, signal: W2) -> Projector<S, C, P, W2, K> {
        Projector {
            source: self.source,
            checkpoints: self.checkpoints,
            projection: self.projection,
            policy: self.policy,
            name: self.name,
            wake: signal,
            parked: self.parked,
            metrics: self.metrics,
        }
    }

    /// Signal `catch` on every idle sleep of the run (§14's catch-up
    /// pulse): a test asserts the read model is fresh the moment the
    /// projector would pause, not after a wall-clock sleep.
    pub fn caught_up_on<'m>(self, catch: &'m dyn Catch) -> ProjectorWithCatch<'m, S, C, P, W, K> {
        ProjectorWithCatch {
            projector: self,
            catch,
        }
    }

    /// Park events the projection keeps rejecting (0.7.7): set
    /// `policy` and record parked events in `store`. With
    /// [`FailurePolicy::Park`]
    /// a poison event no longer stalls the projector; it is recorded,
    /// skipped, and can be listed and replayed later. Without this the
    /// projector halts on it, retrying forever.
    pub fn park_into<K2>(self, store: K2, policy: FailurePolicy) -> Projector<S, C, P, W, K2> {
        Projector {
            source: self.source,
            checkpoints: self.checkpoints,
            projection: self.projection,
            policy: self.policy.on_failure(policy),
            name: self.name,
            wake: self.wake,
            parked: store,
            metrics: self.metrics,
        }
    }

    /// The name the checkpoint is stored under.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<S, C, P, K> Projector<S, C, P, NoSignal, K> {
    /// [`run`](Self::run) with the catch-up pulse: `catch` fires once
    /// per idle sleep, so a test asserts read-model state the moment
    /// the projector catches up instead of waiting a wall clock.
    pub async fn run_caught<F, Fut>(
        self,
        sleep: F,
        catch: &dyn Catch,
    ) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
    {
        self.drive_caught(sleep, catch, NoSignal).await
    }
}

impl<S, C, P, K> Projector<S, C, P, NoSignal, K> {
    /// Load the persisted checkpoint, then drive the subscription to
    /// completion, waiting via `sleep` between polls.
    ///
    /// # Errors
    ///
    /// Loading the checkpoint failed, or the policy parks
    /// ([`FailurePolicy::Park`]) but the projector has no parked store
    /// to park into.
    pub async fn run<F, Fut>(self, sleep: F) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
    {
        self.drive(sleep, NoSignal).await
    }
}

impl<S, C, P, W: CommitSignal, K> Projector<S, C, P, W, K> {
    /// [`run_woken`](Self::run_woken) with the catch-up pulse. See
    /// [`run_caught`](Projector::run_caught).
    pub async fn run_woken_caught<F, Fut>(
        self,
        sleep: F,
        catch: &dyn Catch,
    ) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
    {
        let listener = self.wake.subscribe().await?;
        self.drive_caught(sleep, catch, listener).await
    }
}

impl<S, C, P, W: CommitSignal, K> Projector<S, C, P, W, K> {
    /// Arm the commit listener, load the persisted checkpoint, then
    /// drive the subscription to completion — waiting via `sleep`
    /// between polls, or less when a commit arrives first.
    ///
    /// The listener is armed before the first poll, so no commit can
    /// fall between a poll and the wait that follows it.
    ///
    /// # Errors
    ///
    /// As [`run`](Projector::run), or arming the listener failed.
    pub async fn run_woken<F, Fut>(self, sleep: F) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
    {
        let listener = self.wake.subscribe().await?;
        self.drive(sleep, listener).await
    }
}

/// A [`Projector`] bound to a [`Catch`] pulse (§14): its drivers signal
/// each idle sleep, so a test reads the read model the run just brought
/// current.
pub struct ProjectorWithCatch<'m, S, C, P, W, K> {
    projector: Projector<S, C, P, W, K>,
    catch: &'m dyn Catch,
}

impl<S, C, P, K> ProjectorWithCatch<'_, S, C, P, NoSignal, K> {
    /// [`Projector::run`] with the pulse.
    pub async fn run<F, Fut>(self, sleep: F) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
    {
        self.projector
            .drive_caught(sleep, self.catch, NoSignal)
            .await
    }
}

impl<S, C, P, W: CommitSignal, K> ProjectorWithCatch<'_, S, C, P, W, K> {
    /// [`Projector::run_woken`] with the pulse.
    pub async fn run_woken<F, Fut>(self, sleep: F) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
    {
        let listener = self.projector.wake.subscribe().await?;
        self.projector
            .drive_caught(sleep, self.catch, listener)
            .await
    }
}

impl<S, C, P, K> Projector<S, C, P, NoSignal, K> {
    /// Run under the lease `leases` with the default [`LeasePolicy`]
    /// (0.7.9): only one driver of this `name` runs at a time; the
    /// others get [`RunError::Taken`] until it releases or expires.
    pub fn lease_with<Le: ProjectorLease>(
        self,
        leases: Le,
    ) -> LeasedProjector<S, C, P, NoSignal, K, Le> {
        LeasedProjector {
            projector: self,
            leases,
            policy: LeasePolicy::default(),
        }
    }
}

impl<S, C, P, W: CommitSignal, K> Projector<S, C, P, W, K> {
    /// [`run`](Projector::run) under a lease: see
    /// [`Projector::lease_with`].
    pub fn lease_with_woken<Le: ProjectorLease>(
        self,
        leases: Le,
    ) -> LeasedProjector<S, C, P, W, K, Le> {
        LeasedProjector {
            projector: self,
            leases,
            policy: LeasePolicy::default(),
        }
    }
}

/// A [`Projector`] bound to a lease (0.7.9): its drivers stop on
/// losing it instead of racing another holder's checkpoint.
pub struct LeasedProjector<S, C, P, W, K, Le> {
    projector: Projector<S, C, P, W, K>,
    leases: Le,
    policy: LeasePolicy,
}

impl<S, C, P, W, K, Le> LeasedProjector<S, C, P, W, K, Le> {
    /// Tune the lease's ttl/grace/max_grace. See [`LeasePolicy`].
    pub fn with_policy(mut self, policy: LeasePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The projector's name — the lease's name.
    pub fn name(&self) -> &str {
        self.projector.name()
    }

    /// Drive the subscription under the lease.
    ///
    /// # Errors
    ///
    /// [`RunError::Taken`] when the name is held elsewhere;
    /// [`RunError::LeaseLost`] when it expires mid-run, with the last
    /// acked checkpoint as the resume point.
    pub async fn run_leased<F, Fut>(self, sleep: F) -> Result<SubscriptionOutcome, RunError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
        Le: ProjectorLease,
    {
        self.drive_leased(sleep, NoSignal).await
    }

    async fn drive_leased<F, Fut, L2>(
        self,
        sleep: F,
        listener: L2,
    ) -> Result<SubscriptionOutcome, RunError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        L2: CommitListener,
        K: ParkedStore<P::Event>,
        Le: ProjectorLease,
    {
        if matches!(self.projector.policy.on_failure, FailurePolicy::Park { .. })
            && !<K as ParkedStore<P::Event>>::RECORDS
        {
            return Err(RunError::Store(StoreError::other(format!(
                "projector `{}` parks failing events but has no parked store; \
                 give it one with `park_into`",
                self.projector.name
            ))));
        }
        let mut machine = SubscriptionMachine::new(
            self.projector.policy,
            self.projector
                .checkpoints
                .load(&self.projector.name)
                .await
                .map_err(RunError::Store)?,
        );
        let metrics: &dyn Metrics = match &self.projector.metrics {
            Some(metrics) => &**metrics,
            None => &NoopMetrics,
        };
        let ports = DriverPorts::new()
            .wake_on(listener)
            .park_into(self.projector.parked)
            .with_metrics(metrics);
        drive_projector_leased(
            &mut machine,
            &self.projector.name,
            &self.projector.source,
            &self.projector.checkpoints,
            self.projector.projection,
            sleep,
            ports,
            &self.leases,
            self.policy,
        )
        .await
    }
}

impl<S, C, P, W: CommitSignal, K, Le> LeasedProjector<S, C, P, W, K, Le> {
    /// [`run_woken`](Projector::run_woken) under the lease.
    pub async fn run_woken_leased<F, Fut>(self, sleep: F) -> Result<SubscriptionOutcome, RunError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        K: ParkedStore<P::Event>,
        Le: ProjectorLease,
    {
        let listener = self
            .projector
            .wake
            .subscribe()
            .await
            .map_err(RunError::Store)?;
        self.drive_leased(sleep, listener).await
    }
}

impl<S, C, P, W, K> Projector<S, C, P, W, K> {
    async fn drive<F, Fut, L>(
        self,
        sleep: F,
        listener: L,
    ) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        L: CommitListener,
        K: ParkedStore<P::Event>,
    {
        if matches!(self.policy.on_failure, FailurePolicy::Park { .. })
            && !<K as ParkedStore<P::Event>>::RECORDS
        {
            return Err(StoreError::other(format!(
                "projector `{}` parks failing events but has no parked store; \
                 give it one with `park_into`",
                self.name
            )));
        }
        let resume = self.checkpoints.load(&self.name).await?;
        let mut machine = SubscriptionMachine::new(self.policy, resume);
        let metrics: &dyn Metrics = match &self.metrics {
            Some(metrics) => &**metrics,
            None => &NoopMetrics,
        };
        let ports = DriverPorts::new()
            .wake_on(listener)
            .park_into(self.parked)
            .with_metrics(metrics);
        Ok(drive_projector(
            &mut machine,
            &self.name,
            &self.source,
            &self.checkpoints,
            self.projection,
            sleep,
            ports,
        )
        .await)
    }

    /// As [`drive`](Self::drive) with the pulse the driver fires on every
    /// idle sleep (§14's `whenCaughtUp`).
    async fn drive_caught<F, Fut, L>(
        self,
        sleep: F,
        catch: &dyn Catch,
        listener: L,
    ) -> Result<SubscriptionOutcome, StoreError>
    where
        S: SubscriptionSource<Event = P::Event>,
        C: CheckpointStore,
        P: Projection,
        P::Error: core::fmt::Display,
        F: FnMut(core::time::Duration) -> Fut,
        Fut: Future<Output = ()>,
        P::Event: Clone + Send,
        L: CommitListener,
        K: ParkedStore<P::Event>,
    {
        if matches!(self.policy.on_failure, FailurePolicy::Park { .. })
            && !<K as ParkedStore<P::Event>>::RECORDS
        {
            return Err(StoreError::other(format!(
                "projector `{}` parks failing events but has no parked store; \
                 give it one with `park_into`",
                self.name
            )));
        }
        let resume = self.checkpoints.load(&self.name).await?;
        let mut machine = SubscriptionMachine::new(self.policy, resume);
        let metrics: &dyn Metrics = match &self.metrics {
            Some(metrics) => &**metrics,
            None => &NoopMetrics,
        };
        let ports = DriverPorts::new()
            .wake_on(listener)
            .park_into(self.parked)
            .with_metrics(metrics)
            .caught_up_on(catch);
        Ok(drive_projector(
            &mut machine,
            &self.name,
            &self.source,
            &self.checkpoints,
            self.projection,
            sleep,
            ports,
        )
        .await)
    }
}
