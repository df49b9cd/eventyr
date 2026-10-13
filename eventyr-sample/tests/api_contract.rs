//! The edge's contract over the in-memory backend: open an account,
//! deposit, withdraw, transfer, read the balance — and the rules that
//! hold across all of it.

use eventyr_sample::domain::Cash;
use eventyr_sample::features::{
    ApiError, BalanceRead, Deposit, GetBalance, OpenAccount, Transfer, Withdraw,
};
use eventyr_sample::wiring::{Backend, MemorySample, compose, runners};

/// Compose the in-memory sample, unwrapping the backend tag.
async fn memory_sample() -> MemorySample {
    compose(&Backend::Memory)
        .await
        .expect("the in-memory sample composes")
        .memory()
        .expect("the in-memory backend composes in memory")
}

/// Balance is a projection: the read path serves the row the
/// runner maintains, and a test needs the row current before the read.
/// One projection pass brings the view up to the writes; the cold
/// start stops at catch-up, then the read-your-writes holds.
async fn project_balances(sample: &MemorySample) {
    runners::run_projection(&sample.inputs, &sample.shutdown)
        .await
        .expect("the projection catches up");
}

/// Read one account's balance after the projection has caught up.
async fn read_balance(sample: &MemorySample, account: u64) -> BalanceRead {
    sample
        .app
        .send(GetBalance { account })
        .await
        .expect("the read serves")
        .expect("the account is known")
}

#[tokio::test]
async fn a_full_account_lifecycle_commits_in_order() {
    let sample = memory_sample().await;
    let app = &sample.app;

    assert!(
        matches!(
            app.send(OpenAccount {
                account: 1,
                owner: "ada".into(),
                stamped: Default::default(),
            })
            .await,
            Ok(result) if result.opened
        ),
        "the account opens once",
    );

    let deposited = app
        .send(Deposit {
            account: 1,
            amount: Cash(100),
            stamped: Default::default(),
        })
        .await
        .expect("a deposit commits");
    let withdrawn = app
        .send(Withdraw {
            account: 1,
            amount: Cash(30),
            stamped: Default::default(),
        })
        .await
        .expect("a withdrawal commits");
    assert!(
        withdrawn > deposited,
        "the version advanced again: {withdrawn:?}"
    );

    project_balances(&sample).await;
    let balance = read_balance(&sample, 1).await;
    assert_eq!(balance.balance.0, 70, "the projection saw the movements");
}

#[tokio::test]
async fn a_domain_rejection_is_a_value_not_a_panic() {
    let sample = memory_sample().await;
    let rejected = sample
        .app
        .send(Withdraw {
            account: 9,
            amount: Cash(5),
            stamped: Default::default(),
        })
        .await;
    assert!(
        matches!(&rejected, Err(ApiError::Domain(reason)) if reason.contains("not open")),
        "a withdraw on a closed account is rejected: {rejected:?}"
    );
}

#[tokio::test]
async fn a_transfer_retries_under_one_key() {
    let sample = memory_sample().await;
    let app = &sample.app;
    for account in [1u64, 2] {
        app.send(OpenAccount {
            account,
            owner: "the bank".into(),
            stamped: Default::default(),
        })
        .await
        .expect("the account opens");
        app.send(Deposit {
            account,
            amount: Cash(100),
            stamped: Default::default(),
        })
        .await
        .expect("the deposit commits");
    }

    let once = app
        .send(Transfer {
            from: 1,
            to: 2,
            amount: Cash(40),
            key: Some("xfer-1".into()),
            stamped: Default::default(),
        })
        .await
        .expect("the transfer commits");
    assert!(once.transferred, "the batch committed: {once:?}");

    let twice = app
        .send(Transfer {
            from: 1,
            to: 2,
            amount: Cash(40),
            key: Some("xfer-1".into()),
            stamped: Default::default(),
        })
        .await
        .expect("the retry is absorbed");
    assert!(!twice.transferred, "the same key does not re-commit");
}
