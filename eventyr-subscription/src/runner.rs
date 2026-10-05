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

/// The driver's optional ports: what [`drive_projector`] wakes on,
/// parks into, and reports to. Each defaults to doing nothing.
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
}

impl DriverPorts<'static> {
    /// No wake-ups (idle sleeps run their course), no parked store
    /// ([`NoParking`] refuses every park), and [`NoopMetrics`].
    pub fn new() -> Self {
        Self {
            wake: NoSignal,
            parked: NoParking,
            metrics: &NoopMetrics,
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
        }
    }

    /// Report each fetch, apply, park, and ack through `metrics`
    /// (0.5.3): projection progress, latency, and failures become
    /// observable.
    pub fn with_metrics<'n>(self, metrics: &'n dyn Metrics) -> DriverPorts<'n, W, K> {
        DriverPorts {
            wake: self.wake,
            parked: self.parked,
            metrics,
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
                    Err(_) => machine.handle(SubscriptionInput::ParkFailed),
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
                    Err(_) => {
                        batch_started = None;
                        machine.handle(SubscriptionInput::AckFailed)
                    }
                }
            }
            SubscriptionAction::Sleep { for_, reason } => {
                wait(&mut sleep, for_, reason, &mut wake).await;
                machine.handle(SubscriptionInput::Slept)
            }
            SubscriptionAction::Done(outcome) => return outcome,
        };
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
}
