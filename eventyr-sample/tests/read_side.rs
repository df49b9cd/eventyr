//! The read side: the projection maintains the balances' view off the
//! store's events, and the query serves the row without touching the
//! log.

use eventyr_sample::domain::Cash;
use eventyr_sample::features::{BalanceRead, Deposit, GetBalance, OpenAccount};
use eventyr_sample::wiring::{Backend, MemorySample, compose, runners};

/// Compose the in-memory sample, unwrapping the backend tag.
async fn memory_sample() -> MemorySample {
    compose(&Backend::Memory)
        .await
        .expect("the in-memory sample composes")
        .memory()
        .expect("the in-memory backend composes in memory")
}

/// Bring the balances view up to the store's writes: one projection
/// pass, stopping at catch-up.
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

// The setup: three accounts, three deposits. The expected balances are
// the deposits — the projection's work, not the store's.
const SEED: [(u64, u64); 3] = [(1, 50), (2, 100), (3, 150)];

#[tokio::test]
async fn the_read_side_serves_the_projection() {
    let sample = memory_sample().await;
    for (account, amount) in SEED {
        sample
            .app
            .send(OpenAccount {
                account,
                owner: "the bank".into(),
                stamped: Default::default(),
            })
            .await
            .expect("the account opens");
        sample
            .app
            .send(Deposit {
                account,
                amount: Cash(amount),
                stamped: Default::default(),
            })
            .await
            .expect("the deposit commits");
    }

    project_balances(&sample).await;

    for (account, expected) in SEED {
        let balance = read_balance(&sample, account).await;
        assert_eq!(
            balance.balance.0, expected,
            "account {account} reads its deposit"
        );
    }
}
