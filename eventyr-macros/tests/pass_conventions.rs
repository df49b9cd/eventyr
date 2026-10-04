//! The conventions, as a trybuild pass case: the derive must expand
//! cleanly in a bare crate with only `eventyr-core` in scope.

use std::fmt;

use eventyr_core::aggregate::Aggregate as _;
use eventyr_core::event_name::EventName as _;
use eventyr_macros::Aggregate;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
/// The counter instance's identifier.
struct CounterId(u64);
impl fmt::Display for CounterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
/// A domain event payload.
struct Incremented {
    by: u32,
}

#[derive(Debug)]
/// The counter's commands.
#[allow(dead_code)]
enum CounterCommand {
    /// Increment the counter.
    Increment(u32),
}

#[derive(Debug, PartialEq)]
/// The counter's domain rejections.
enum CounterError {
    /// The counter would go too high.
    TooHigh,
}
impl fmt::Display for CounterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("counter would go too high")
    }
}

#[derive(Debug, Default, PartialEq)]
/// The counter's folded state.
struct CounterState {
    count: u32,
}

fn apply(state: &mut CounterState, event: &CounterEvent) {
    match event {
        CounterEvent::Incremented(Incremented { by }) => state.count += by,
    }
}

fn decide(
    state: &CounterState,
    command: &CounterCommand,
) -> Result<Vec<CounterEvent>, CounterError> {
    match command {
        CounterCommand::Increment(by) if state.count + by > 100 => Err(CounterError::TooHigh),
        CounterCommand::Increment(by) => Ok(vec![Incremented { by: *by }.into()]),
    }
}

#[derive(Aggregate)]
#[eventyr(state = CounterState, events(Incremented))]
struct Counter;

fn main() {
    assert_eq!(Counter::NAME, "counter");
    assert_eq!(
        CounterEvent::Incremented(Incremented { by: 1 }).event_name(),
        "Incremented",
    );
}
