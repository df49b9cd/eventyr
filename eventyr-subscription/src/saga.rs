//! The saga runner: drive a [`SagaMachine`] per event, as a
//! [`Projection`].
//!
//! The split is the design: the subscription runner owns fetch/ack
//! (at-least-once per event, checkpointed), the saga machine owns the
//! per-event reaction. A saga is a projection whose "apply" dispatches
//! its commands against the write side — before the ack, so a crash
//! re-delivers the event and re-issues its commands. Each command's
//! idempotency is the caller's to keep, exactly as for projections.
//!
//! [`SagaProjection`] wraps a [`Saga`] and a dispatcher so the plain
//! [`Projector`](crate::runner::Projector) runs a saga unchanged;
//! [`drive_saga`] is the per-event driver underneath it.

use core::future::Future;

use eventyr_core::envelope::{EventEnvelope, Metadata};
use eventyr_core::error::StoreError;
use eventyr_core::saga::{Saga, SagaAction, SagaCommand, SagaInput, SagaMachine, SagaOutcome};

use crate::runner::Projection;

/// A [`Projection`] over a [`Saga`]: each event's reaction runs as one
/// [`SagaMachine`] interaction.
///
/// The dispatcher `D` is the caller's: it receives each
/// [`SagaCommand`] — command, target stream, metadata — and runs it
/// against the write side (a repository call, a batch machine).
/// The first dispatch that fails fails the event, and the subscription
/// runner's retry policy decides whether it re-delivers.
pub struct SagaProjection<S, D> {
    saga: S,
    dispatcher: D,
    metadata: Metadata,
}

impl<S, D> SagaProjection<S, D> {
    /// A saga projection: `saga` reacts, `dispatcher` commits.
    pub fn new(saga: S, dispatcher: D) -> Self {
        Self {
            saga,
            dispatcher,
            metadata: Metadata::default(),
        }
    }

    /// Builder-style: metadata layered onto every command this projection
    /// dispatches (see [`Metadata::overlay`]).
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

impl<S, D, Fut> Projection for SagaProjection<S, D>
where
    S: Saga + Clone,
    S::Event: Send + Sync,
    S::Command: Send,
    D: FnMut(SagaCommand<S::Command>) -> Fut + Send,
    Fut: Future<Output = Result<(), StoreError>> + Send,
{
    type Event = S::Event;
    type Error = StoreError;

    async fn apply(&mut self, event: &EventEnvelope<Self::Event>) -> Result<(), Self::Error> {
        let mut machine = SagaMachine::new(self.saga.clone()).with_metadata(self.metadata.clone());
        match drive_saga(&mut machine, event, &mut self.dispatcher).await {
            SagaOutcome::Done => Ok(()),
            SagaOutcome::Failed(error) => Err(error),
        }
    }
}

/// The per-event driver: start `machine` on `event` and run each
/// dispatch through `dispatcher`, one at a time, until the machine is
/// done.
pub async fn drive_saga<S, D, Fut>(
    machine: &mut SagaMachine<S>,
    event: &EventEnvelope<S::Event>,
    dispatcher: &mut D,
) -> SagaOutcome
where
    S: Saga,
    D: FnMut(SagaCommand<S::Command>) -> Fut,
    Fut: Future<Output = Result<(), StoreError>>,
{
    let mut action = machine.start(event);
    loop {
        action = match action {
            SagaAction::Dispatch { command } => match dispatcher(command).await {
                Ok(()) => machine.handle(SagaInput::Dispatched),
                Err(error) => machine.handle(SagaInput::DispatchFailed(error)),
            },
            SagaAction::Done(outcome) => return outcome,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventyr_core::vocabulary::{Sequence, StreamId, Version};

    /// Echoes each event's payload as one command to `ledger-<payload>`.
    #[derive(Clone)]
    struct Echo;

    impl Saga for Echo {
        type Event = u64;
        type Command = u64;

        fn name(&self) -> &str {
            "echo"
        }

        fn react(&self, event: &EventEnvelope<u64>) -> Vec<(StreamId, u64)> {
            vec![(
                StreamId::from(format!("ledger-{}", event.event)),
                event.event,
            )]
        }
    }

    fn event(payload: u64) -> EventEnvelope<u64> {
        EventEnvelope {
            sequence: Sequence::new(9),
            stream_id: StreamId::from("order-1"),
            version: Version::new(1),
            event: payload,
            metadata: Metadata::of_ids(Some("cause-event".into()), Some("corr-event".into())),
        }
    }

    #[tokio::test]
    async fn the_projection_dispatches_to_the_named_stream_with_layered_metadata() {
        let mut seen = Vec::new();
        let mut projection = SagaProjection::new(Echo, |command: SagaCommand<u64>| {
            seen.push((
                command.target.as_str().to_owned(),
                command.command,
                command.metadata.causation_id.clone(),
                command.metadata.correlation_id.clone(),
            ));
            async { Ok(()) }
        })
        .with_metadata(Metadata::of_ids(None, Some("corr-request".into())));

        projection.apply(&event(5)).await.expect("dispatched");
        drop(projection);
        assert_eq!(
            seen,
            vec![(
                "ledger-5".to_owned(),
                5,
                Some("cause-event".to_owned()),
                Some("corr-request".to_owned()),
            )]
        );
    }

    #[tokio::test]
    async fn a_failed_dispatch_fails_the_projection() {
        let mut projection = SagaProjection::new(Echo, |_: SagaCommand<u64>| async {
            Err(StoreError::Unavailable)
        });
        assert!(matches!(
            projection.apply(&event(1)).await,
            Err(StoreError::Unavailable)
        ));
    }
}
