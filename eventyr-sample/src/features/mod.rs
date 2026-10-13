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

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use eventyr::prelude::{Metadata, StoreError, Version};
use eventyr::store::prelude::{ExecutionError, ExecutionOutcome};
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

impl ApiError {
    /// The HTTP status the edge answers with.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Domain(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Conflict => StatusCode::CONFLICT,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status(), Json(self)).into_response()
    }
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

/// The error body the edge serves for every failure: the `error` field
/// the slices' [`ApiError`]s map to.
#[derive(Serialize)]
pub struct ErrorBody {
    /// The rejection, conflict, or unavailability message.
    pub error: String,
}

/// Whatever a command answers at the edge: `200` with the outcome, or
/// the failure's status and body.
pub fn respond<T: Serialize>(outcome: Outcome<T>) -> Response {
    match outcome {
        Ok(ok) => (StatusCode::OK, Json(OkBody { ok })).into_response(),
        Err(failure) => failure.into_response(),
    }
}

/// The success body the edge serves for every committed command.
#[derive(Serialize)]
pub struct OkBody<T> {
    /// The committed outcome.
    pub ok: T,
}

impl From<&StoreError> for ApiError {
    /// A store failure is two statuses at the edge: an optimistic-
    /// concurrency or boundary conflict is a 409, and anything the
    /// store can't answer for is a 503. There is no third arm by
    /// design — the store's vocabulary maps to the edge one failure
    /// kind at a time.
    fn from(failure: &StoreError) -> Self {
        match failure {
            StoreError::Conflict { .. } | StoreError::QueryConflict { .. } => Self::Conflict,
            StoreError::Unavailable
            | StoreError::StreamClosed { .. }
            | StoreError::Truncated { .. }
            | StoreError::Other(_) => Self::Unavailable,
        }
    }
}

/// The version a store execution committed at — the last committed
/// event's on the committed path, [`Version::EMPTY`] on a no-op.
pub fn committed_version<E>(outcome: &ExecutionOutcome<E>) -> Version {
    match outcome {
        ExecutionOutcome::Committed { committed, .. }
        | ExecutionOutcome::AlreadyCommitted { committed } => committed
            .last()
            .map_or(Version::EMPTY, |event| event.version),
        ExecutionOutcome::Noop => Version::EMPTY,
    }
}

/// Maps an execution's failure to the edge's vocabulary: a domain
/// rejection names itself, a store failure goes through
/// [`ApiError::from`].
pub fn execution_failure<A: eventyr::prelude::Aggregate>(failure: &ExecutionError<A>) -> ApiError {
    match failure {
        ExecutionError::Domain(rejection) => ApiError::Domain(rejection.to_string()),
        ExecutionError::Store(failure) => ApiError::from(failure),
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
