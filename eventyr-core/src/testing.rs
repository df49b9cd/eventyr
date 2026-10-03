//! The scripted driver: drive a machine against a fixed script of inputs,
//! recording every action it emits.
//!
//! Being pure, this driver lives in the core: no store, no async, no I/O
//! — the transition tests for every machine are written against it.

use alloc::vec::Vec;

use crate::aggregate::Aggregate;
use crate::write::{WriteAction, WriteInput, WriteMachine};

/// Drives `machine` through `start()` and every input in `script`,
/// recording each action in order.
///
/// The script is fed verbatim: inputs after the machine finished are
/// answered with protocol-violation outcomes, exactly as they would be
/// at runtime — which is itself worth asserting on.
pub fn drive_scripted<A: Aggregate>(
    machine: &mut WriteMachine<A>,
    script: impl IntoIterator<Item = WriteInput<A::Event>>,
) -> Vec<WriteAction<A::Event, A::Error>> {
    let mut actions = Vec::new();
    actions.push(machine.start());
    for input in script {
        actions.push(machine.handle(input));
    }
    actions
}
