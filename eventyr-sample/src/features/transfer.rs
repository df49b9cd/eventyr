//! Transfers: two accounts, one atomic batch — the cross-aggregate
//! rule that does not fit one stream. The rule lives on its own
//! decider ([`WireTransfer`]); the slice only names the two accounts,
//! the amount, and the caller's idempotency key.

use std::collections::BTreeMap;

use eventyr::prelude::{
    AggregateBoundary, BatchDecision, BatchMachine, BatchOutcome, CommittedStream, Decide,
    Metadata, RetryPolicy, StreamId, Version,
};
use eventyr_mediator::prelude::*;
use eventyr_mediator::store::HasMetadata;
use eventyr_store::prelude::{EventStore, drive_write_batch};
use serde::{Deserialize, Serialize};

use crate::domain::{
    Account, AccountCommand, AccountError, AccountEvent, AccountId, AccountState, Cash, Deposited,
    Withdrawn,
};
use crate::features::{ApiError, Outcome, Stamped};
use eventyr::prelude::Aggregate;

/// The transfer's decider: the boundary, the rule, and the events the
/// batch writes. This is the value the [`BatchMachine`] drives; the
/// slice's [`Transfer`] borrows it.
#[derive(Clone, Debug)]
pub struct WireTransfer {
    /// The account to debit.
    pub from: AccountId,
    /// The account to credit.
    pub to: AccountId,
    /// The amount, in cents.
    pub amount: Cash,
    /// The interaction's metadata, idempotency key included.
    pub metadata: Metadata,
}

impl AggregateBoundary<Account> for WireTransfer {
    fn boundary(&self) -> Vec<AccountId> {
        vec![self.from, self.to]
    }
}

impl WireTransfer {
    fn stream_of(&self, id: AccountId) -> StreamId {
        StreamId::for_aggregate::<Account>(&id)
    }

    /// `for_aggregates` (roadmap 0.4) derives the streams and the
    /// per-stream folds from the boundary the decider names — the
    /// manual `BatchMachine::new` shape, but one place.
    pub fn machine(&self) -> BatchMachine<AccountEvent, AccountError, WireTransfer> {
        BatchMachine::for_aggregates(self, RetryPolicy::default())
            .with_metadata(self.metadata.clone())
    }
}

impl Decide<AccountEvent, AccountError> for WireTransfer {
    type Command = Self;

    fn decide(
        &self,
        folded: &BTreeMap<StreamId, Box<dyn std::any::Any + Send>>,
        command: &Self,
    ) -> BatchDecision<AccountEvent, AccountError> {
        let state_of = |id: AccountId| -> AccountState {
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
                    account: command.from.0,
                    amount: command.amount,
                }
                .into(),
            ),
            (
                self.stream_of(command.to),
                Deposited {
                    account: command.to.0,
                    amount: command.amount,
                }
                .into(),
            ),
        ])
    }
}

/// A transfer between two accounts. The caller's key makes a retry
/// idempotent: the same batch under the same key commits once.
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

impl Transfer {
    /// The batch-machine decider this transfer names, with the
    /// interaction's metadata layered on (the caller's key wins if the
    /// mediator's stamp did not set one).
    fn decider(&self) -> WireTransfer {
        let mut metadata = self.stamped.metadata.clone();
        if let Some(key) = &self.key
            && metadata.idempotency_key.is_none()
        {
            metadata.idempotency_key = Some(key.clone());
        }
        WireTransfer {
            from: AccountId(self.from),
            to: AccountId(self.to),
            amount: self.amount,
            metadata,
        }
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

/// Runs the transfer through the batch machine the composition root
/// names. The slice holds no store of its own; the handler is the
/// composition's field, and the store comes with it.
pub struct TransferHandler<S> {
    store: std::sync::Arc<S>,
}

impl<S> TransferHandler<S> {
    /// The constructor the composition field calls.
    pub fn new(store: std::sync::Arc<S>) -> Self {
        Self { store }
    }
}

impl<M, S> RequestHandler<Transfer, M> for TransferHandler<S>
where
    M: Sync,
    S: EventStore<Event = AccountEvent> + Send + Sync,
{
    async fn handle(&self, _mediator: &M, request: Transfer) -> Outcome<CommittedTransfer> {
        let decider = request.decider();
        let mut machine = decider.machine();
        let from_stream = decider.stream_of(decider.from);
        let to_stream = decider.stream_of(decider.to);
        match drive_write_batch(&mut machine, &*self.store).await {
            BatchOutcome::Committed { committed } => Ok(CommittedTransfer {
                from_version: version_of(&committed, &from_stream),
                to_version: version_of(&committed, &to_stream),
                transferred: true,
            }),
            BatchOutcome::AlreadyCommitted { committed } => Ok(CommittedTransfer {
                from_version: version_of(&committed, &from_stream),
                to_version: version_of(&committed, &to_stream),
                transferred: false,
            }),
            BatchOutcome::Noop => Ok(CommittedTransfer {
                from_version: Version::EMPTY,
                to_version: Version::EMPTY,
                transferred: false,
            }),
            BatchOutcome::Rejected(rejection) => Err(ApiError::Domain(rejection.to_string())),
            BatchOutcome::Failed(failure) => Err(ApiError::from(&failure)),
        }
    }
}

/// The version the stream committed at, or `EMPTY` when the batch did
/// not write it (a no-op).
fn version_of(committed: &[CommittedStream<AccountEvent>], stream: &StreamId) -> Version {
    committed
        .iter()
        .find(|entry| &entry.stream_id == stream)
        .and_then(|entry| entry.events.last().map(|event| event.version))
        .unwrap_or(Version::EMPTY)
}
