//! The feature slices: one request per user action, one handler each.
//!
//! A slice is vertical — its request, its handler, and its translation
//! of the outcome into the edge's vocabulary all live in one file. The
//! slices never name a store, a repository, or a retry policy; they
//! send [`Execute`](eventyr_mediator::store::Execute) through the
//! mediator, which routes it to the repository the composition root
//! chose. The dependency rule is Clean Architecture's: everything here
//! points at the domain, nothing points at the wiring.

use std::fmt;

use eventyr::prelude::{Metadata, StoreError};
use eventyr_mediator::store::HasMetadata;
use serde::Serialize;

pub mod balance;
pub mod deposit;
pub mod open_account;
pub mod transfer;

pub use balance::{
    BALANCES, Balance, BalanceHandler, BalanceRead, GetBalance, InMemoryBalances, balance_key,
};
pub use deposit::{Deposit, DepositHandler, Withdraw, WithdrawHandler};
pub use open_account::{OpenAccount, OpenAccountHandler, OpenedAccount};
pub use transfer::{CommittedTransfer, Transfer, TransferHandler};

/// What every command request answers: the committed version, or the
/// rejection in the edge's vocabulary.
pub type Outcome<T> = Result<T, ApiError>;

/// How a slice's request fails at the edge. These are values the
/// handler answered with — the HTTP edge maps each kind to its status,
/// and the error pipeline never sees them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ApiError {
    /// The domain rejected the command.
    Domain(String),
    /// The command conflicted past its retry budget; the caller may
    /// re-issue it.
    Conflict,
    /// The store is down; the caller may retry later.
    Unavailable,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain(rejection) => f.write_str(rejection),
            Self::Conflict => f.write_str("conflict: re-issue the command"),
            Self::Unavailable => f.write_str("the store is unavailable: retry later"),
        }
    }
}

impl std::error::Error for ApiError {}

/// Maps a store failure to its edge error: a conflict that exhausted
/// its retries is a 409, anything else is a 503.
pub fn store_failure(failure: &StoreError) -> ApiError {
    match failure {
        StoreError::Conflict { .. } => ApiError::Conflict,
        _ => ApiError::Unavailable,
    }
}

/// The interaction's metadata carrier for the requests that stamp one:
/// `StampMetadata` fills it through `HasMetadata`.
#[derive(Clone, Debug, Default)]
pub struct Stamped {
    /// Correlation and causation ids, and a caller-supplied
    /// idempotency key.
    pub metadata: Metadata,
}

impl HasMetadata for Stamped {
    fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
}
