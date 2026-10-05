//! End-to-end tests for the batch machine: driver → atomic in-memory
//! store, over the canonical two-account transfer.

use std::collections::BTreeMap;
use std::sync::Arc;

use eventyr_core::batch::{AggregateFold, BatchDecision, BatchMachine, Decide, Fold};
use eventyr_core::prelude::*;
use eventyr_core::testing::account::{
    Account, AccountCommand, AccountError, AccountEvent, AccountId, AccountState,
};
use eventyr_store::prelude::*;
use futures::TryStreamExt;

/// A transfer between two accounts — the canonical multi-stream batch.
struct Transfer {
    from: u64,
    to: u64,
    amount: u64,
}

fn stream_of(id: u64) -> StreamId {
    StreamId::for_aggregate::<Account>(&AccountId(id))
}

fn folds(ids: &[u64]) -> BTreeMap<StreamId, Box<dyn Fold<AccountEvent>>> {
    ids.iter()
        .map(|&id| {
            (
                stream_of(id),
                Box::new(AggregateFold::<Account>(AccountId(id))) as Box<dyn Fold<_>>,
            )
        })
        .collect()
}

fn state(folded: &BTreeMap<StreamId, Box<dyn core::any::Any + Send>>, id: u64) -> &AccountState {
    folded
        .get(&stream_of(id))
        .and_then(|state| state.downcast_ref::<AccountState>())
        .expect("the account fold typed this state")
}

impl Decide<AccountEvent, AccountError> for Transfer {
    type Command = Self;

    fn decide(
        &self,
        folded: &BTreeMap<StreamId, Box<dyn core::any::Any + Send>>,
        command: &Self,
    ) -> BatchDecision<AccountEvent, AccountError> {
        if let Err(error) = Account::decide(
            state(folded, command.from),
            &AccountCommand::Withdraw {
                amount: command.amount,
            },
        ) {
            return BatchDecision::reject(error);
        }
        if let Err(error) = Account::decide(
            state(folded, command.to),
            &AccountCommand::Deposit {
                amount: command.amount,
            },
        ) {
            return BatchDecision::reject(error);
        }
        BatchDecision::of(
            vec![
                AccountEvent::Withdrawn {
                    amount: command.amount,
                },
                AccountEvent::Deposited {
                    amount: command.amount,
                },
            ],
            vec![stream_of(command.from), stream_of(command.to)],
        )
    }
}

fn transfer_machine(
    from: u64,
    to: u64,
    amount: u64,
) -> BatchMachine<AccountEvent, AccountError, Transfer> {
    let transfer = Transfer { from, to, amount };
    BatchMachine::new(
        vec![stream_of(from), stream_of(to)],
        folds(&[from, to]),
        Transfer { from, to, amount },
        transfer,
        RetryPolicy::default(),
    )
}

/// Seed both accounts: open them and fund `from` with 10, each through
/// the single-stream repository — the batch drives only the transfer.
fn open_and_fund(store: &Arc<InMemoryStore<AccountEvent>>) {
    let repo = AggregateRepository::<Account, _>::new(store.clone(), RetryPolicy::default());
    futures::executor::block_on(async {
        repo.execute(AccountId(1), AccountCommand::Open { owner: "a".into() })
            .await
            .expect("open a");
        repo.execute(AccountId(2), AccountCommand::Open { owner: "b".into() })
            .await
            .expect("open b");
        repo.execute(AccountId(1), AccountCommand::Deposit { amount: 10 })
            .await
            .expect("fund a");
    });
}

#[tokio::test]
async fn a_transfer_commits_atomically_across_two_streams() {
    let store = Arc::new(InMemoryStore::new());
    open_and_fund(&store);

    let outcome = drive_write_batch(&mut transfer_machine(1, 2, 5), &*store).await;
    let BatchOutcome::Committed { committed } = outcome else {
        panic!("the transfer must commit")
    };
    assert_eq!(committed.len(), 2);
    // Stream 1 (from, balance 10): withdrawn 5 → 5. Stream 2: +5 → 5.
    let from: Vec<_> = store
        .stream(&stream_of(1), Version::EMPTY)
        .try_collect::<Vec<_>>()
        .await
        .expect("read");
    let to: Vec<_> = store
        .stream(&stream_of(2), Version::EMPTY)
        .try_collect::<Vec<_>>()
        .await
        .expect("read");
    // from: opened, deposited 10, withdrawn 5.
    assert_eq!(from.len(), 3);
    assert!(matches!(
        from[2].event,
        AccountEvent::Withdrawn { amount: 5 }
    ));
    // to: opened, deposited 5.
    assert_eq!(to.len(), 2);
    assert!(matches!(to[1].event, AccountEvent::Deposited { amount: 5 }));
}

#[tokio::test]
async fn a_transfer_against_insufficient_funds_commits_nothing() {
    let store = Arc::new(InMemoryStore::new());
    open_and_fund(&store);

    // Transfer 50 out of a 10-balance account.
    let outcome = drive_write_batch(&mut transfer_machine(1, 2, 50), &*store).await;
    assert!(matches!(
        outcome,
        BatchOutcome::Rejected(AccountError::InsufficientFunds)
    ));
    // Nothing was appended to either stream.
    let from: Vec<_> = store
        .stream(&stream_of(1), Version::EMPTY)
        .try_collect::<Vec<_>>()
        .await
        .expect("read");
    assert_eq!(from.len(), 2, "no withdrawal"); // opened, deposited
}

#[tokio::test]
async fn the_blocking_driver_runs_the_same_protocol() {
    let store = Arc::new(InMemoryStore::new());
    open_and_fund(&store);

    let outcome = drive_write_batch_blocking(&mut transfer_machine(1, 2, 5), &*store);
    assert!(matches!(outcome, BatchOutcome::Committed { .. }));
    let to: Vec<_> = store
        .stream(&stream_of(2), Version::EMPTY)
        .try_collect::<Vec<_>>()
        .await
        .expect("read");
    assert!(matches!(to[1].event, AccountEvent::Deposited { amount: 5 }));
}

#[tokio::test]
async fn a_transfer_stamps_its_metadata_on_both_sides() {
    let store = Arc::new(InMemoryStore::new());
    open_and_fund(&store);

    let metadata =
        eventyr_core::envelope::Metadata::of_ids(Some("xfer-1".into()), Some("request-9".into()));
    let mut machine = transfer_machine(1, 2, 5).with_metadata(metadata.clone());
    let outcome = drive_write_batch(&mut machine, &*store).await;
    let BatchOutcome::Committed { .. } = outcome else {
        panic!("the transfer must commit")
    };
    // Both streams' trailing events carry the interaction's metadata.
    for stream in [stream_of(1), stream_of(2)] {
        let events: Vec<_> = store
            .stream(&stream, Version::EMPTY)
            .try_collect()
            .await
            .expect("read");
        let last = events.last().expect("a committed event");
        assert_eq!(last.metadata.causation_id.as_deref(), Some("xfer-1"));
        assert_eq!(last.metadata.correlation_id.as_deref(), Some("request-9"));
    }
}
