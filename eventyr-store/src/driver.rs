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
///   the envelopes, report [`Loaded`](WriteInput::Loaded) — or
///   [`Failed`](WriteInput::Failed) when the read errors;
/// - `Append` → call [`append`](EventStore::append), reporting
///   [`Appended`](WriteInput::Appended), or mapping a
///   [`StoreError::Conflict`] to [`Conflict`](WriteInput::Conflict)
///   and anything else to [`Failed`](WriteInput::Failed);
/// - `Done` → stop and return the outcome.
///
/// Every store error — load or append — travels through the machine as
/// [`Failed`](WriteInput::Failed), so
/// [`Failed`](WriteOutcome::Failed) is the one failure shape callers
/// see: the machine owns the protocol, not the driver.
pub async fn drive_write<A, S>(
    machine: &mut WriteMachine<A>,
    store: &S,
) -> WriteOutcome<A::Event, A::Error>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    let mut action = machine.start();
    loop {
        action = match action {
            WriteAction::LoadStream { stream_id, from } => {
                let loaded: Result<Vec<EventEnvelope<A::Event>>, StoreError> =
                    store.stream(&stream_id, from).try_collect().await;
                match loaded {
                    Ok(events) => machine.handle(WriteInput::Loaded { events }),
                    Err(error) => machine.handle(WriteInput::Failed(error)),
                }
            }
            WriteAction::Append {
                stream_id,
                expected,
                events,
            } => match store.append(&stream_id, expected, events).await {
                Ok(committed) => machine.handle(WriteInput::Appended { committed }),
                Err(StoreError::Conflict { current }) => {
                    machine.handle(WriteInput::Conflict { current })
                }
                Err(error) => machine.handle(WriteInput::Failed(error)),
            },
            WriteAction::Done(outcome) => return outcome,
        };
    }
}
