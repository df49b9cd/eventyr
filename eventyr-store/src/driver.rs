//! The async driver: performs a [`WriteMachine`]'s actions against an
//! [`EventStore`], feeding results back until the machine is done.
//!
//! A driver is a boring loop: interpret the action, do the I/O, report
//! the input. All policy — retries, version expectations, conflict
//! handling — lives in the machine, so the same loop serves every
//! aggregate and every store.

use futures::TryStreamExt;

use eventyr_core::aggregate::Aggregate;
use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::write::{WriteAction, WriteInput, WriteMachine, WriteOutcome};

use crate::store::EventStore;

/// Drives `machine` against `store` until it finishes, returning its
/// terminal outcome.
///
/// The loop performs each action the machine emits:
///
/// - `LoadStream` → read the stream from the requested bound, collect
///   the envelopes, report [`Loaded`](WriteInput::Loaded);
/// - `Append` → call [`append`](EventStore::append), reporting
///   [`Appended`](WriteInput::Appended) or mapping a
///   [`StoreError::Conflict`] to
///   [`Conflict`](WriteInput::Conflict) and anything else to
///   [`Failed`](WriteInput::Failed);
/// - `Done` → stop and return the outcome.
///
/// A store error that is neither a conflict nor fatal-by-construction
/// ends the interaction as [`Failed`](WriteOutcome::Failed) — the
/// machine decides what is retryable, not the driver.
pub async fn drive_write<A, S>(
    machine: &mut WriteMachine<A>,
    store: &S,
) -> Result<WriteOutcome<A::Event, A::Error>, StoreError>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    let mut action = machine.start();
    loop {
        match action {
            WriteAction::LoadStream { stream_id, from } => {
                let events: Vec<EventEnvelope<A::Event>> =
                    store.stream(&stream_id, from).try_collect().await?;
                action = machine.handle(WriteInput::Loaded { events });
            }
            WriteAction::Append {
                stream_id,
                expected,
                events,
            } => {
                action = match store.append(&stream_id, expected, events).await {
                    Ok(committed) => machine.handle(WriteInput::Appended { committed }),
                    Err(StoreError::Conflict { current }) => {
                        machine.handle(WriteInput::Conflict { current })
                    }
                    Err(error) => machine.handle(WriteInput::Failed(error)),
                };
            }
            WriteAction::Done(outcome) => return Ok(outcome),
        }
    }
}
