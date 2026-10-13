//! Transfers: two accounts, one atomic batch — the cross-aggregate
//! rule that does not fit one stream. The decision is the command
//! itself, the boundary is the two accounts' streams, and the batch
//! machine commits the withdrawal and the deposit together.

use std::collections::BTreeMap;
use std::sync::Arc;

use eventyr::prelude::{
    AggregateBoundary, BatchDecision, BatchMachine, BatchOutcome, Decide, Metadata, RetryPolicy,
    StreamId, Version,
};
use eventyr_mediator::prelude::*;
use eventyr_mediator::store::HasMetadata;
use eventyr_store::prelude::{EventStore, drive_write_batch};
use serde::{Deserialize, Serialize};

use crate::domain::{
    Account, AccountCommand, AccountError, AccountEvent, AccountId, AccountState, Cash, Deposited,
    Withdrawn,
};
use crate::features::{ApiError, Outcome, Stamped, store_failure};
use eventyr::prelude::Aggregate;

/// A transfer between two accounts — the batch-machine decision. The
/// decider *is* the command (`Command = Self`), so one clone serves
/// both slots; the boundary is the two accounts' streams, named once
/// by `AggregateBoundary`. The caller's key makes a retry idempotent:
/// the same batch under the same key commits once.
#[derive(Clone, Debug, Deserialize)]
pub struct Transfer {
    /// The account to debit.
    pub from: u64,
    /// The account to credit.
    pub to: u64,
    /// The amount, in cents.
    pub amount: Cash,
    /// The caller's idempotency key: a retry under the same key.
    #[serde(rename = "idempotency_key")]
    pub key: Option<String>,
    /// The interaction's metadata.
    #[serde(skip)]
    pub stamped: Stamped,
}

impl Request for Transfer {
    type Response = Outcome<CommittedTransfer>;
}

impl HasMetadata for Transfer {
    fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.stamped.metadata
    }
}

/// What a transfer answers: the version the withdrawal committed at on
/// `from`, and the version the deposit committed at on `to` — the
/// batch's atomicity means both exist or neither.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CommittedTransfer {
    /// The version the withdrawal committed at on `from`.
    pub from_version: Version,
    /// The version the deposit committed at on `to`.
    pub to_version: Version,
    /// `false` when this request repeated an earlier commit — the
    /// caller's retry under the same idempotency key.
    pub transferred: bool,
}

impl AggregateBoundary<Account> for Transfer {
    fn boundary(&self) -> Vec<AccountId> {
        vec![AccountId(self.from), AccountId(self.to)]
    }
}

impl Transfer {
    fn stream_of(&self, id: u64) -> StreamId {
        StreamId::for_aggregate::<Account>(&AccountId(id))
    }

    /// `for_aggregates` (roadmap 0.4) derives the streams and the
    /// per-stream folds from the boundary the decider names — the
    /// manual `BatchMachine::new` shape, but one place.
    fn machine(&self) -> BatchMachine<AccountEvent, AccountError, Transfer> {
        let mut metadata = self.stamped.metadata.clone();
        if let Some(key) = &self.key
            && metadata.idempotency_key.is_none()
        {
            metadata.idempotency_key = Some(key.clone());
        }
        BatchMachine::for_aggregates(self, RetryPolicy::default()).with_metadata(metadata)
    }
}

impl Decide<AccountEvent, AccountError> for Transfer {
    type Command = Self;

    fn decide(
        &self,
        folded: &BTreeMap<StreamId, Box<dyn std::any::Any + Send>>,
        command: &Self,
    ) -> BatchDecision<AccountEvent, AccountError> {
        let state_of = |id: u64| -> AccountState {
            folded
                .get(&self.stream_of(id))
                .and_then(|state| state.downcast_ref::<AccountState>())
                .cloned()
                .expect("the machine folds every boundary stream")
        };
        let from = state_of(command.from);
        let to = state_of(command.to);
        if let Err(error) = Account::decide(
            &from,
            &AccountCommand::Withdraw {
                amount: command.amount,
            },
        ) {
            return BatchDecision::reject(error);
        }
        if let Err(error) = Account::decide(
            &to,
            &AccountCommand::Deposit {
                amount: command.amount,
            },
        ) {
            return BatchDecision::reject(error);
        }
        BatchDecision::of([
            (
                self.stream_of(command.from),
                Withdrawn {
                    account: command.from,
                    amount: command.amount,
                }
                .into(),
            ),
            (
                self.stream_of(command.to),
                Deposited {
                    account: command.to,
                    amount: command.amount,
                }
                .into(),
            ),
        ])
    }
}

/// Runs the transfer against whichever store the wiring named — the
/// only slice that names the store, because the batch machine needs
/// the event store directly (there is no `Execute` for a batch).
pub struct TransferHandler<S> {
    store: Arc<S>,
}

impl<S> TransferHandler<S> {
    /// The constructor the composition field calls.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

impl<M, S> RequestHandler<Transfer, M> for TransferHandler<S>
where
    M: Sync,
    S: EventStore<Event = AccountEvent> + Send + Sync,
{
    async fn handle(&self, _mediator: &M, request: Transfer) -> Outcome<CommittedTransfer> {
        let transferred = request.clone();
        let mut machine = request.machine();
        match drive_write_batch(&mut machine, &*self.store).await {
            BatchOutcome::Committed { committed } => Ok(CommittedTransfer {
                from_version: version_of(&committed, &transferred.stream_of(transferred.from)),
                to_version: version_of(&committed, &transferred.stream_of(transferred.to)),
                transferred: true,
            }),
            BatchOutcome::AlreadyCommitted { committed } => Ok(CommittedTransfer {
                from_version: version_of(&committed, &transferred.stream_of(transferred.from)),
                to_version: version_of(&committed, &transferred.stream_of(transferred.to)),
                transferred: false,
            }),
            BatchOutcome::Noop => Ok(CommittedTransfer {
                from_version: Version::EMPTY,
                to_version: Version::EMPTY,
                transferred: false,
            }),
            BatchOutcome::Rejected(rejection) => Err(ApiError::Domain(rejection.to_string())),
            BatchOutcome::Failed(failure) => Err(store_failure(&failure)),
        }
    }
}

/// The version the stream committed at, or `EMPTY` when the batch did
/// not write it (a no-op).
fn version_of(
    committed: &[eventyr::prelude::CommittedStream<AccountEvent>],
    stream: &StreamId,
) -> Version {
    committed
        .iter()
        .find(|entry| &entry.stream_id == stream)
        .and_then(|entry| entry.events.last().map(|event| event.version))
        .unwrap_or(Version::EMPTY)
}
