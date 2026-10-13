//! Deposits and withdrawals — one request per money movement. They
//! share their shape: name an account and an amount, get back the
//! version the movement committed at.

use eventyr::prelude::Version;
use eventyr_mediator::prelude::*;
use eventyr_mediator::store::Execute;
use eventyr_store::prelude::{ExecutionError, ExecutionOutcome};
use serde::Deserialize;

use crate::domain::{Account, AccountCommand, AccountId, Cash};
use crate::features::{ApiError, Outcome, Stamped, store_failure};

/// Deposit `amount` into `account`.
#[derive(Clone, Debug, Deserialize)]
pub struct Deposit {
    /// The account to credit.
    pub account: u64,
    /// The amount, in cents.
    pub amount: Cash,
    /// The interaction's metadata.
    #[serde(skip)]
    pub stamped: Stamped,
}

impl Request for Deposit {
    type Response = Outcome<Version>;
}

/// Translates the request into the aggregate's `Deposit` command and
/// sends it on — sub-dispatch through the mediator, so the command runs
/// through the `Execute<Account>` pipeline, stamped.
#[derive(Default)]
pub struct DepositHandler;

impl<M: Sender<Execute<Account>>> RequestHandler<Deposit, M> for DepositHandler {
    async fn handle(&self, mediator: &M, request: Deposit) -> Outcome<Version> {
        let command = Execute::<Account>::new(
            AccountId(request.account),
            AccountCommand::Deposit {
                amount: request.amount,
            },
        )
        .with_metadata(request.stamped.metadata);
        match mediator.send(command).await {
            Ok(ExecutionOutcome::Committed { committed, .. })
            | Ok(ExecutionOutcome::AlreadyCommitted { committed }) => Ok(committed
                .last()
                .map_or(Version::EMPTY, |event| event.version)),
            Ok(ExecutionOutcome::Noop) => Ok(Version::EMPTY),
            Err(ExecutionError::Domain(rejection)) => Err(ApiError::Domain(rejection.to_string())),
            Err(ExecutionError::Store(failure)) => Err(store_failure(&failure)),
        }
    }
}

/// Withdraw `amount` from `account` — the bank's one rule lives in the
/// aggregate, so this slice looks exactly like the deposit's.
#[derive(Clone, Debug, Deserialize)]
pub struct Withdraw {
    /// The account to debit.
    pub account: u64,
    /// The amount, in cents.
    pub amount: Cash,
    /// The interaction's metadata.
    #[serde(skip)]
    pub stamped: Stamped,
}

impl Request for Withdraw {
    type Response = Outcome<Version>;
}

/// The withdrawal's handler.
#[derive(Default)]
pub struct WithdrawHandler;

impl<M: Sender<Execute<Account>>> RequestHandler<Withdraw, M> for WithdrawHandler {
    async fn handle(&self, mediator: &M, request: Withdraw) -> Outcome<Version> {
        let command = Execute::<Account>::new(
            AccountId(request.account),
            AccountCommand::Withdraw {
                amount: request.amount,
            },
        )
        .with_metadata(request.stamped.metadata);
        match mediator.send(command).await {
            Ok(ExecutionOutcome::Committed { committed, .. })
            | Ok(ExecutionOutcome::AlreadyCommitted { committed }) => Ok(committed
                .last()
                .map_or(Version::EMPTY, |event| event.version)),
            Ok(ExecutionOutcome::Noop) => Ok(Version::EMPTY),
            Err(ExecutionError::Domain(rejection)) => Err(ApiError::Domain(rejection.to_string())),
            Err(ExecutionError::Store(failure)) => Err(store_failure(&failure)),
        }
    }
}
