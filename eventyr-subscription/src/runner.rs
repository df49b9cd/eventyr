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

use eventyr_store::metrics::names::{PARKED_EVENTS, PROJECTED_EVENTS, PROJECTION_FETCH_SPAN};

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::subscription::{
    SleepReason, SubscriptionAction, SubscriptionInput, SubscriptionMachine, SubscriptionOutcome,
    SubscriptionPolicy,
};
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
/// - `Ack` → [`CheckpointStore::store`]; a failure reports
///   [`AckFailed`](SubscriptionInput::AckFailed);
/// - `Sleep` → `sleep(duration)`; the machine decided the duration —
///   only the wait happens in the driver;
/// - `Done` → stop and return the outcome.
pub async fn drive_projector<E, S, C, P, F, Fut>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    projection: P,
    sleep: F,
) -> SubscriptionOutcome
where
    S: SubscriptionSource<Event = E>,
    C: CheckpointStore,
    P: Projection<Event = E>,
    P::Error: core::fmt::Display,
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    E: Clone + Send,
{
    drive_projector_with_metrics(
        machine,
        name,
        source,
        checkpoints,
        projection,
        sleep,
        &eventyr_store::metrics::NoopMetrics,
    )
    .await
}

/// [`drive_projector`] with metrics (0.5.3): each apply and ack reports
/// through `metrics` — projection lag and throughput become observable.
pub async fn drive_projector_with_metrics<E, S, C, P, F, Fut>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    projection: P,
    sleep: F,
    metrics: &(dyn eventyr_store::metrics::Metrics + Send + Sync),
) -> SubscriptionOutcome
where
    S: SubscriptionSource<Event = E>,
    C: CheckpointStore,
    P: Projection<Event = E>,
    P::Error: core::fmt::Display,
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    E: Clone + Send,
{
    drive_projector_woken(
        machine,
        name,
        source,
        checkpoints,
        projection,
        sleep,
        NoSignal,
        metrics,
    )
    .await
}

/// [`drive_projector_with_metrics`] that also ends an *idle* sleep early
/// when `wake` reports a commit (0.7.2) — a caught-up projector polls
/// at once instead of after `idle_sleep`.
///
/// Arm `wake` (via [`CommitSignal::subscribe`])
/// before calling: a commit after arming and before the first idle
/// sleep is remembered, so none slips between a poll and the wait.
/// Backoff sleeps ([`SleepReason::Backoff`]) ignore `wake` and run their
/// course. A broken signal (`committed` returning `Err`) is dropped for
/// the rest of the run and the driver falls back to its timer — the
/// poll was authoritative all along.
#[allow(
    clippy::too_many_arguments,
    reason = "the driver's ports, one argument each — the same shape as the other drivers"
)]
pub async fn drive_projector_woken<E, S, C, P, F, Fut, W>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    projection: P,
    sleep: F,
    wake: W,
    metrics: &(dyn eventyr_store::metrics::Metrics + Send + Sync),
) -> SubscriptionOutcome
where
    S: SubscriptionSource<Event = E>,
    C: CheckpointStore,
    P: Projection<Event = E>,
    P::Error: core::fmt::Display,
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    W: CommitListener,
    E: Clone + Send,
{
    drive_projector_parking(
        machine,
        name,
        source,
        checkpoints,
        projection,
        sleep,
        wake,
        &NoParking,
        metrics,
    )
    .await
}

/// The full driver: [`drive_projector_woken`] that also records the
/// events its machine parks (0.7.7) in `parked`. A machine whose policy
/// never parks never calls it.
#[allow(
    clippy::too_many_arguments,
    reason = "the driver's ports, one argument each — the same shape as the other drivers"
)]
pub async fn drive_projector_parking<E, S, C, P, F, Fut, W, K>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    mut projection: P,
    mut sleep: F,
    wake: W,
    parked: &K,
    metrics: &(dyn eventyr_store::metrics::Metrics + Send + Sync),
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
                match parked.park(event).await {
                    Ok(()) => {
                        metrics.counter(PARKED_EVENTS, 1);
                        machine.handle(SubscriptionInput::Parked)
                    }
                    Err(_) => machine.handle(SubscriptionInput::ParkFailed),
                }
            }
            SubscriptionAction::Ack { checkpoint } => {
                match checkpoints.store(name, checkpoint).await {
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
                let broken = match (reason, wake.as_mut()) {
                    (SleepReason::Idle, Some(listener)) => {
                        let timer = core::pin::pin!(sleep(for_));
                        let woken = core::pin::pin!(listener.committed());
                        match futures::future::select(timer, woken).await {
                            futures::future::Either::Right((Err(_), timer)) => {
                                // The signal broke: finish this wait on
                                // the timer.
                                timer.await;
                                true
                            }
                            // Timer or wake-up: either way, poll now.
                            _ => false,
                        }
                    }
                    _ => {
                        sleep(for_).await;
                        false
                    }
                };
                if broken {
                    wake = None;
                }
                machine.handle(SubscriptionInput::Slept)
            }
            SubscriptionAction::Done(outcome) => return outcome,
        };
    }
}

/// The blocking [`drive_projector`]: drives `machine` to its outcome
/// without an async runtime, parking the thread through every `sleep`.
pub fn drive_projector_blocking<E, S, C, P, F, Fut>(
    machine: &mut SubscriptionMachine<E>,
    name: &str,
    source: &S,
    checkpoints: &C,
    projection: P,
    sleep: F,
) -> SubscriptionOutcome
where
    S: SubscriptionSource<Event = E>,
    C: CheckpointStore,
    P: Projection<Event = E>,
    P::Error: core::fmt::Display,
    F: FnMut(core::time::Duration) -> Fut,
    Fut: Future<Output = ()>,
    E: Clone + Send,
{
    futures::executor::block_on(drive_projector(
        machine,
        name,
        source,
        checkpoints,
        projection,
        sleep,
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
        }
    }
}

impl<S, C, P, W, K> Projector<S, C, P, W, K> {
    /// Tune the subscription's batch/idle/retry policy.
    pub fn with_policy(mut self, policy: SubscriptionPolicy) -> Self {
        self.policy = policy;
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
        }
    }

    /// Park events the projection keeps rejecting (0.7.7): set
    /// `policy` and record parked events in `store`. With
    /// [`FailurePolicy::Park`](eventyr_core::subscription::FailurePolicy::Park)
    /// a poison event no longer stalls the projector; it is recorded,
    /// skipped, and can be listed and replayed later. Without this the
    /// projector halts on it, retrying forever.
    pub fn park_into<K2>(
        self,
        store: K2,
        policy: eventyr_core::subscription::FailurePolicy,
    ) -> Projector<S, C, P, W, K2> {
        Projector {
            source: self.source,
            checkpoints: self.checkpoints,
            projection: self.projection,
            policy: self.policy.on_failure(policy),
            name: self.name,
            wake: self.wake,
            parked: store,
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
        let resume = self.checkpoints.load(&self.name).await?;
        let mut machine = SubscriptionMachine::new(self.policy, resume);
        Ok(drive_projector_parking(
            &mut machine,
            &self.name,
            &self.source,
            &self.checkpoints,
            self.projection,
            sleep,
            listener,
            &self.parked,
            &eventyr_store::metrics::NoopMetrics,
        )
        .await)
    }
}
