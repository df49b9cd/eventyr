//! # eventyr-store
//!
//! The store side of event sourcing: the [`EventStore`] and [`StreamsAll`]
//! ports, an in-memory implementation for tests and examples, the async
//! [`drive_write`] driver, and the [`AggregateRepository`] — the ergonomic
//! entry point that hides the machine behind one method.
//!
//! The traits are runtime-agnostic: `append` returns a [`Future`], the
//! streams are [`Stream`]s, and nothing here names tokio. The in-memory
//! store is synchronous under a lock; the Postgres store (0.2) will do its
//! work in the future and stream.
//!
//! ## A taste
//!
//! ```
//! use std::fmt;
//! use eventyr_core::prelude::*;
//! use eventyr_store::prelude::*;
//!
//! #[derive(Clone, PartialEq, Eq, Hash, Debug)]
//! struct AccountId(u64);
//! impl fmt::Display for AccountId {
//!     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//!         write!(f, "{}", self.0)
//!     }
//! }
//!
//! #[derive(Clone, PartialEq, Debug)]
//! enum AccountEvent { Opened, Deposited { amount: u64 } }
//!
//! #[derive(Clone, Debug)]
//! enum AccountCommand { Open, Deposit { amount: u64 } }
//!
//! #[derive(Debug, PartialEq)]
//! enum AccountError { AlreadyOpen, NotOpen }
//! impl fmt::Display for AccountError {
//!     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//!         f.write_str(match self {
//!             AccountError::AlreadyOpen => "account is already open",
//!             AccountError::NotOpen => "account is not open",
//!         })
//!     }
//! }
//!
//! #[derive(Debug)]
//! struct AccountState { open: bool, balance: u64 }
//!
//! struct Account;
//!
//! impl Aggregate for Account {
//!     const NAME: &'static str = "account";
//!     type Id = AccountId;
//!     type State = AccountState;
//!     type Event = AccountEvent;
//!     type Command = AccountCommand;
//!     type Error = AccountError;
//!
//!     fn initial(_id: &Self::Id) -> Self::State {
//!         AccountState { open: false, balance: 0 }
//!     }
//!
//!     fn apply(state: &mut Self::State, event: &Self::Event) {
//!         match event {
//!             AccountEvent::Opened => { state.open = true; state.balance = 0; }
//!             AccountEvent::Deposited { amount } => state.balance += amount,
//!         }
//!     }
//!
//!     fn decide(state: &Self::State, command: &Self::Command)
//!         -> Result<Vec<Self::Event>, Self::Error> {
//!         match command {
//!             AccountCommand::Open if state.open => Err(AccountError::AlreadyOpen),
//!             AccountCommand::Open => Ok(vec![AccountEvent::Opened]),
//!             AccountCommand::Deposit { .. } if !state.open => Err(AccountError::NotOpen),
//!             AccountCommand::Deposit { amount } =>
//!                 Ok(vec![AccountEvent::Deposited { amount: *amount }]),
//!         }
//!     }
//! }
//!
//! # async fn demo() {
//! let store = InMemoryStore::new();
//! let repository = AggregateRepository::<Account, _>::new(
//!     store,
//!     RetryPolicy::default(),
//! );
//!
//! // Open, then deposit: the second call folds the first's event.
//! repository.execute(AccountId(1), AccountCommand::Open).await.expect("open");
//! match repository.execute(AccountId(1), AccountCommand::Deposit { amount: 50 }).await {
//!     Ok(ExecutionOutcome::Committed(events)) => assert_eq!(events.len(), 1),
//!     Ok(ExecutionOutcome::Noop) => panic!("a deposit decides an event"),
//!     Err(_) => panic!("the deposit must commit"),
//! }
//! # }
//! # fn main() {
//! #     tokio::runtime::Builder::new_current_thread()
//! #         .enable_all()
//! #         .build()
//! #         .expect("runtime")
//! #         .block_on(demo());
//! # }
//! ```

pub mod driver;
pub mod memory;
pub mod repository;
pub mod store;

pub mod prelude {
    //! The store side: ports, in-memory store, driver, repository.

    pub use crate::driver::drive_write;
    pub use crate::memory::InMemoryStore;
    pub use crate::repository::{AggregateRepository, ExecutionError, ExecutionOutcome};
    pub use crate::store::{EventStore, StreamsAll};
}
