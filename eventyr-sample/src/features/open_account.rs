//! Opens an account.

use eventyr::prelude::Version;
use eventyr_mediator::prelude::*;
use eventyr_mediator::store::Execute;
use eventyr_store::prelude::ExecutionOutcome;
use serde::{Deserialize, Serialize};

use crate::domain::{Account, AccountCommand, AccountId};
use crate::features::{ApiError, Outcome, Stamped, execution_failure};

/// Open an account for `owner`.
#[derive(Clone, Debug, Deserialize)]
pub struct OpenAccount {
    /// The account the caller assigns — the bank issues ids up front,
    /// so an idempotent retry of this request names the same account.
    pub account: u64,
    /// The account holder's name.
    pub owner: String,
    /// The interaction's metadata.
    #[serde(skip)]
    pub stamped: Stamped,
}

impl Request for OpenAccount {
    type Response = Outcome<OpenedAccount>;
}

/// What opening an account answers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OpenedAccount {
    /// The opened account.
    pub account: AccountId,
    /// The version the open committed at.
    pub version: Version,
    /// `false` when this request repeated an earlier commit — the
    /// caller's retry under the same idempotency key.
    pub opened: bool,
}

/// Translates the request into the aggregate's command and sends it
/// on — sub-dispatch through the mediator, so `Execute<Account>`'s own
/// pipeline stamps the metadata.
#[derive(Default)]
pub struct OpenAccountHandler;

impl<M: Sender<Execute<Account>>> RequestHandler<OpenAccount, M> for OpenAccountHandler {
    async fn handle(&self, mediator: &M, request: OpenAccount) -> Outcome<OpenedAccount> {
        let account = AccountId(request.account);
        let command = Execute::<Account>::new(
            account,
            AccountCommand::Open {
                owner: request.owner,
            },
        )
        .with_metadata(request.stamped.metadata);
        match mediator.send(command).await {
            Ok(outcome @ ExecutionOutcome::Committed { .. }) => Ok(OpenedAccount {
                account,
                version: crate::features::committed_version(&outcome),
                opened: true,
            }),
            Ok(outcome @ ExecutionOutcome::AlreadyCommitted { .. }) => Ok(OpenedAccount {
                account,
                version: crate::features::committed_version(&outcome),
                opened: false,
            }),
            Ok(ExecutionOutcome::Noop) => Err(ApiError::Domain("open produced no events".into())),
            Err(failure) => Err(execution_failure(&failure)),
        }
    }
}
