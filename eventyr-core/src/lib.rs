//! # eventyr-core
//!
//! The pure sans-IO core of event sourcing: the [`Aggregate`] trait, the
//! protocol vocabulary ([`StreamId`], [`Version`], [`Sequence`],
//! [`ExpectedVersion`]), and the [`WriteMachine`] — the state machine
//! that runs the load → fold → decide → append write path.
//!
//! Nothing here performs I/O, sleeps, or knows what a runtime is: the crate
//! is `no_std + alloc` with zero required dependencies. Drivers (async,
//! blocking, or scripted) live in the store-side crates; the scripted one —
//! being pure — is right here in [`testing`].
//!
//! ## A taste
//!
//! Define an aggregate as two pure functions, then drive its write machine
//! by hand — no store, no async:
//!
//! ```
//! use std::fmt;
//! use eventyr_core::prelude::*;
//!
//! #[derive(Clone, PartialEq, Eq, Hash, Debug)]
//! struct CounterId(u64);
//! impl fmt::Display for CounterId {
//!     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//!         write!(f, "{}", self.0)
//!     }
//! }
//!
//! #[derive(Debug, PartialEq)]
//! enum CounterEvent { Incremented(u64) }
//!
//! #[derive(Debug)]
//! enum CounterCommand { Increment(u64) }
//!
//! #[derive(Debug, PartialEq)]
//! enum CounterError { TooHigh }
//! impl fmt::Display for CounterError {
//!     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//!         f.write_str("counter would go too high")
//!     }
//! }
//!
//! struct Counter;
//!
//! impl Aggregate for Counter {
//!     const NAME: &'static str = "counter";
//!     type Id = CounterId;
//!     type State = u64;
//!     type Event = CounterEvent;
//!     type Command = CounterCommand;
//!     type Error = CounterError;
//!
//!     fn initial(_id: &Self::Id) -> Self::State { 0 }
//!
//!     fn apply(state: &mut Self::State, event: &Self::Event) {
//!         match event { CounterEvent::Incremented(n) => *state += n }
//!     }
//!
//!     fn decide(state: &Self::State, command: &Self::Command)
//!         -> Result<Vec<Self::Event>, Self::Error> {
//!         match command {
//!             CounterCommand::Increment(n) if *state + n > 100 => Err(CounterError::TooHigh),
//!             CounterCommand::Increment(n) => Ok(vec![CounterEvent::Incremented(*n)]),
//!         }
//!     }
//! }
//!
//! let mut machine =
//!     WriteMachine::<Counter>::new(CounterId(7), CounterCommand::Increment(5), RetryPolicy::default());
//!
//! // The machine asks to load the stream; a driver answers.
//! let WriteAction::LoadStream { stream_id, from } = machine.start() else {
//!     unreachable!()
//! };
//! assert_eq!(stream_id.as_str(), "counter-7");
//! assert_eq!(from, Version::EMPTY);
//!
//! // Empty stream: fold, decide, ask to append — guarded by `Empty`.
//! let action = machine.handle(WriteInput::Loaded { events: vec![] });
//! let WriteAction::Append { expected, .. } = action else {
//!     unreachable!()
//! };
//! assert_eq!(expected, ExpectedVersion::Empty);
//!
//! // The store commits; the machine is done.
//! let action = machine.handle(WriteInput::Appended { committed: vec![] });
//! let WriteAction::Done(WriteOutcome::Committed(_)) = action else {
//!     unreachable!()
//! };
//! ```

#![cfg_attr(not(test), no_std)]

extern crate alloc;

pub mod aggregate;
pub mod envelope;
pub mod error;
pub mod testing;
pub mod upcast;
pub mod vocabulary;
pub mod write;

pub mod prelude {
    //! The common vocabulary: import everything and define an aggregate.

    pub use crate::aggregate::{Aggregate, AggregateId};
    pub use crate::envelope::{EventEnvelope, Metadata, NewEvent};
    pub use crate::error::{ProtocolError, StoreError, UpcastError};
    pub use crate::upcast::{RawEvent, Upcaster};
    pub use crate::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
    pub use crate::write::{RetryPolicy, WriteAction, WriteInput, WriteMachine, WriteOutcome};
}
