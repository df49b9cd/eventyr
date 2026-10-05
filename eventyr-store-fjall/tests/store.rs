//! The embedded store against the shared contract, plus end-to-end
//! coverage through the blocking driver — the two halves of this
//! crate's thesis made testable.

use eventyr_core::event_name::EventName;
use eventyr_store_fjall::FjallStore;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
enum ContractEvent {
    Payload { value: u64 },
}

impl EventName for ContractEvent {
    fn event_name(&self) -> &'static str {
        match self {
            ContractEvent::Payload { .. } => "Payload",
        }
    }
}

impl From<u64> for ContractEvent {
    fn from(value: u64) -> Self {
        ContractEvent::Payload { value }
    }
}

// The contract is parameterized over the factory, so each check gets a
// fresh keyspace rooted at its own tempdir.
#[test]
fn fjall_store_passes_the_event_store_contract() {
    eventyr_store_testing::event_store_contract::<ContractEvent, _>(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FjallStore::<ContractEvent>::open(dir.path()).expect("open");
        // The open keyspace owns the path; leak the tempdir so it is
        // not unlinked from under the store. (Test-only: the OS reaps
        // /tmp.)
        std::mem::forget(dir);
        store
    });
}

#[test]
fn fjall_store_passes_the_streams_all_contract() {
    eventyr_store_testing::streams_all_contract::<ContractEvent, _>(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FjallStore::<ContractEvent>::open(dir.path()).expect("open");
        std::mem::forget(dir);
        store
    });
}

#[test]
fn fjall_store_passes_the_append_batch_contract() {
    eventyr_store_testing::event_store_batch_contract::<ContractEvent, _>(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FjallStore::<ContractEvent>::open(dir.path()).expect("open");
        std::mem::forget(dir);
        store
    });
}

#[test]
fn blocking_driver_commits_through_the_embedded_store() {
    use std::fmt;

    use eventyr_core::prelude::*;
    use eventyr_store::driver;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct Id(u64);
    impl fmt::Display for Id {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
    enum Event {
        Opened { owner: String },
    }
    impl EventName for Event {
        fn event_name(&self) -> &'static str {
            match self {
                Event::Opened { .. } => "Opened",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum Command {
        Open { owner: String },
    }

    #[derive(Debug, PartialEq)]
    enum Error {
        AlreadyOpen,
    }
    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Error::AlreadyOpen => "already open",
            })
        }
    }

    #[derive(Debug, PartialEq)]
    struct State {
        open: bool,
    }

    struct Account;
    impl Aggregate for Account {
        const NAME: &'static str = "account";
        type Id = Id;
        type State = State;
        type Event = Event;
        type Command = Command;
        type Error = Error;

        fn initial(_id: &Self::Id) -> Self::State {
            State { open: false }
        }
        fn apply(state: &mut Self::State, event: &Self::Event) {
            match event {
                Event::Opened { .. } => state.open = true,
            }
        }
        fn decide(
            state: &Self::State,
            command: &Self::Command,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            match command {
                Command::Open { .. } if state.open => Err(Error::AlreadyOpen),
                Command::Open { owner } => Ok(vec![Event::Opened {
                    owner: owner.clone(),
                }]),
            }
        }
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let store = FjallStore::<Event>::open(dir.path()).expect("open");
    let id = Id(1);

    let mut machine = WriteMachine::<Account>::new(
        id.clone(),
        Command::Open { owner: "me".into() },
        RetryPolicy::default(),
    );
    let outcome = driver::drive_write_blocking(&mut machine, &store);
    assert!(matches!(outcome, WriteOutcome::Committed { .. }));

    // A second open against the folded stream rejects — the embedded
    // store honours the protocol the machine drives.
    let mut machine = WriteMachine::<Account>::new(
        id,
        Command::Open { owner: "me".into() },
        RetryPolicy::default(),
    );
    let outcome = driver::drive_write_blocking(&mut machine, &store);
    assert!(matches!(outcome, WriteOutcome::Rejected(_)));

    let _ = dir;
}

#[test]
fn fjall_store_passes_the_query_append_contract() {
    eventyr_store_testing::query_append_contract(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FjallStore::open(dir.path()).expect("open");
        std::mem::forget(dir);
        store
    });
}

#[test]
fn fjall_store_passes_the_commit_signal_contract() {
    eventyr_store_testing::commit_signal_contract::<ContractEvent, _>(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FjallStore::<ContractEvent>::open(dir.path()).expect("open");
        std::mem::forget(dir);
        store
    });
}

/// The filtered-read contract needs a stored name that depends on the
/// payload: even values are `"Even"`, odd ones `"Odd"`.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct ParityEvent(u64);

impl EventName for ParityEvent {
    fn event_name(&self) -> &'static str {
        if self.0.is_multiple_of(2) {
            "Even"
        } else {
            "Odd"
        }
    }
}

impl From<u64> for ParityEvent {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[test]
fn fjall_store_passes_the_filtered_read_contract() {
    eventyr_store_testing::filtered_read_contract::<ParityEvent, _>(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FjallStore::<ParityEvent>::open(dir.path()).expect("open");
        std::mem::forget(dir);
        store
    });
}
