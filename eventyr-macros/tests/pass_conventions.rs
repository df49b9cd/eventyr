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
    // `event_derive`/`event_attr`: the stored enum serializes under
    // its container attribute.
    let json = serde_json::to_string(&stored::StoredEvent::Noted(stored::Noted {
        note: "hi".to_owned(),
    }))
    .expect("serialize");
    assert_eq!(json, r#"{"kind":"Noted","note":"hi"}"#);
    assert_eq!(
        CounterEvent::Incremented(Incremented { by: 1 }).event_name(),
        "Incremented",
    );
}

/// The event enum's opt-in codec: derives and a serde container
/// attribute through `event_derive(...)` / `event_attr(...)`.
/// The impl is exercised in `derives.rs`; here the expansion compiles.
#[allow(dead_code)]
mod stored {
    use std::fmt;

    use eventyr_macros::Aggregate;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    /// The aggregate's identifier.
    pub struct StoredId(u64);
    impl fmt::Display for StoredId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    /// A domain event payload.
    pub struct Noted {
        pub note: String,
    }

    #[derive(Debug)]
    /// The aggregate's commands.
    #[allow(dead_code)]
    pub enum StoredCommand {
        /// Note something.
        Note(String),
    }

    #[derive(Debug)]
    /// The aggregate's rejections.
    pub struct StoredError;
    impl fmt::Display for StoredError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("no")
        }
    }

    fn apply(_state: &mut Stored, _event: &StoredEvent) {}

    fn decide(_state: &Stored, command: &StoredCommand) -> Result<Vec<StoredEvent>, StoredError> {
        match command {
            StoredCommand::Note(note) => Ok(vec![Noted { note: note.clone() }.into()]),
        }
    }

    #[derive(Aggregate)]
    #[eventyr(
        id = StoredId,
        events(Noted),
        event_derive(serde::Serialize, serde::Deserialize),
        event_attr("#[serde(tag = \"kind\")]"),
    )]
    pub struct Stored;
}
