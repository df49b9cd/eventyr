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

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::subscription::{
    SubscriptionAction, SubscriptionInput, SubscriptionMachine, SubscriptionOutcome,
    SubscriptionPolicy,
};

use crate::checkpoint::CheckpointStore;
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
    mut projection: P,
    mut sleep: F,
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
    let mut action = machine.start();
    loop {
        action = match action {
            SubscriptionAction::Fetch { from, limit } => {
                match source.fetch(from, limit).await {
                    Ok(batch) => machine.handle(SubscriptionInput::Fetched { batch }),
                    Err(error) => machine.handle(SubscriptionInput::Failed(error)),
                }
            }
            SubscriptionAction::Apply { envelope } => {
                let sequence = envelope.sequence;
                match projection.apply(&envelope).await {
                    Ok(()) => machine.handle(SubscriptionInput::Applied),
                    Err(error) => machine.handle(SubscriptionInput::ApplyFailed {
                        error: StoreError::other(format!(
                            "projection applying sequence {sequence}: {error}"
                        )),
                    }),
                }
            }
            SubscriptionAction::Ack { checkpoint } => {
                match checkpoints.store(name, checkpoint).await {
                    Ok(()) => machine.handle(SubscriptionInput::Acked),
                    Err(_) => machine.handle(SubscriptionInput::AckFailed),
                }
            }
            SubscriptionAction::Sleep { for_ } => {
                sleep(for_).await;
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
pub struct Projector<S, C, P> {
    source: S,
    checkpoints: C,
    projection: P,
    policy: SubscriptionPolicy,
    name: String,
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
        }
    }

    /// Tune the subscription's batch/idle/retry policy.
    pub fn with_policy(mut self, policy: SubscriptionPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The name the checkpoint is stored under.
    pub fn name(&self) -> &str {
        &self.name
    }

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
    {
        let resume = self.checkpoints.load(&self.name).await?;
        let mut machine = SubscriptionMachine::new(self.policy, resume);
        Ok(drive_projector(
            &mut machine,
            &self.name,
            &self.source,
            &self.checkpoints,
            self.projection,
            sleep,
        )
        .await)
    }
}
