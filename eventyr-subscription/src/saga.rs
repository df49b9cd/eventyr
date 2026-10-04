//! The saga runner: drive a [`SagaMachine`] per event, as a
//! [`Projection`].
//!
//! Roadmap 0.6.1's "sagas as a machine" lands here, and the split is
//! the design: the subscription runner owns *fetch/ack* (at-least-once
//! per event, checkpointed — `SubscriptionMachine`), and the saga
//! machine owns *react/dispatch* (the event's commands). A saga is a
//! projection whose "apply" dispatches its commands against the write
//! side; per event, before the ack, so a crash re-delivers the event
//! and the saga's commands re-issue — idempotency of each command is
//! the caller's to keep, exactly as for projections proper.
//!
//! [`SagaProjection`] wraps a [`Saga`] and the dispatcher so the plain
//! [`Projector`](crate::runner::Projector) runs a saga unchanged; the
//! per-event work is [`drive_saga`], which feeds the driver the
//! machine's `React`/`Dispatch` actions and reports
//! [`SagaOutcome::Done`] only once every command committed.

use eventyr_core::envelope::{EventEnvelope, Metadata};
use eventyr_core::error::StoreError;
use eventyr_core::saga::{Saga, SagaAction, SagaCommand, SagaInput, SagaMachine, SagaOutcome};
use eventyr_core::vocabulary::StreamId;

use crate::runner::Projection;

/// The I/O the saga needs the write side for: run `command` against
/// `target`'s stream (the aggregate the caller resolves), succeeding
/// only once the command's events committed.
///
/// Implemented per call site — the saga names *what* and *whose*; how
/// to reach the stream (a repository call, a batch machine, a test's
/// in-memory store) is the user's. The `Metadata` is the interaction's
/// causation: it traces back to the event that fired the saga, per the
/// 0.5.2 boundary seam.
pub struct SagaDispatch<C> {
    /// The command to execute.
    pub command: C,
    /// The stream the command targets.
    pub target: StreamId,
    /// Causation/correlation the dispatch carries.
    pub metadata: Metadata,
}

/// A [`Projection`] over a [`Saga`]: each event's reaction runs as one
/// [`SagaMachine`] interaction, dispatching each command in order.
///
/// `D` is the dispatcher the caller owns — it turns a
/// [`SagaDispatch`] into the command's commit (per-stream optimistic
/// concurrency included). The projection's error is the saga's:
/// the first dispatch that fails fails the event, and the subscription
/// runner's retry policy decides whether the event re-delivers.
pub struct SagaProjection<S, D> {
    saga: S,
    dispatcher: D,
}

impl<S, D> SagaProjection<S, D> {
    /// A saga projection: `saga` reacts; `dispatcher` commits.
    pub fn new(saga: S, dispatcher: D) -> Self {
        Self { saga, dispatcher }
    }

    /// The saga this projection drives.
    pub fn saga(&self) -> &S {
        &self.saga
    }
}

impl<S, D, Fut> Projection for SagaProjection<S, D>
where
    S: Saga + Clone,
    S::Event: Clone + Send + Sync,
    S::Command: Clone + Send,
    D: FnMut(SagaDispatch<S::Command>) -> Fut + Send,
    Fut: core::future::Future<Output = Result<(), StoreError>> + Send,
{
    type Event = S::Event;
    type Error = StoreError;

    async fn apply(
        &mut self,
        event: &EventEnvelope<Self::Event>,
    ) -> Result<(), Self::Error> {
        let mut machine = SagaMachine::new(self.saga.clone())
            .with_metadata(event.metadata.clone());
        let outcome = drive_saga(&mut machine, event, &mut self.dispatcher).await;
        match outcome {
            SagaOutcome::Done => Ok(()),
            SagaOutcome::Failed(error) => Err(error),
        }
    }
}

/// The per-event driver: run `machine` over `event`, dispatching each
/// command the saga emits through `dispatcher`.
///
/// The dispatch is the write-side step stripped to its signature:
/// metadata-stamped commands, per the interaction, and the first
/// committed outcome answers `Dispatched`. The saga's own order (each
/// dispatch committed before the next) is the only at-least-once seam
/// the interaction opens: a retry re-issues from the failed command,
/// never resends an already-committed one.
pub async fn drive_saga<S, D, Fut>(
    machine: &mut SagaMachine<S>,
    event: &EventEnvelope<S::Event>,
    dispatcher: &mut D,
) -> SagaOutcome
where
    S: Saga,
    S::Event: Clone,
    D: FnMut(SagaDispatch<S::Command>) -> Fut,
    Fut: core::future::Future<Output = Result<(), StoreError>>,
{
    let mut action = machine.start(event.clone());
    // The interaction's stamp, read once: the saga's boundary answer is
    // fixed at construction (with_metadata), so the dispatch overlay is
    // a constant for the whole interaction.
    let interaction = machine.metadata().clone();
    loop {
        action = match action {
            SagaAction::React { event } => {
                let commands = machine
                    .react(&event)
                    .into_iter()
                    .map(|mut command| {
                        command.metadata =
                            Metadata::overlay(&interaction, &event.metadata, &command.metadata);
                        command
                    })
                    .collect();
                machine.handle(SagaInput::Reacted { commands })
            }
            SagaAction::Dispatch { command } => {
                let SagaCommand {
                    command,
                    target,
                    metadata,
                } = command;
                match dispatcher(SagaDispatch {
                    command,
                    target,
                    metadata,
                })
                .await
                {
                    Ok(()) => machine.handle(SagaInput::Dispatched),
                    Err(error) => machine.handle(SagaInput::DispatchFailed(error)),
                }
            }
            SagaAction::Done(outcome) => return outcome,
        };
    }
}
