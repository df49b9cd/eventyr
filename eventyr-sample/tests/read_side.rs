//! The read side: the projection maintains the balances' view off the
//! store, and the read serves it without touching the event log.

use eventyr_sample::domain::Cash;
use eventyr_sample::features::{Deposit, GetBalance, OpenAccount};
use eventyr_sample::wiring::{Backend, compose, runners};

#[tokio::test]
async fn the_read_side_serves_the_projection() {
    let sample = compose(&Backend::Memory)
        .await
        .expect("the in-memory sample composes")
        .into_memory();
    let app = &sample.app;

    for account in [1u64, 2, 3] {
        app.send(OpenAccount {
            account,
            owner: "the bank".into(),
            stamped: Default::default(),
        })
        .await
        .expect("each account opens");
        app.send(Deposit {
            account,
            amount: Cash(account * 50),
            stamped: Default::default(),
        })
        .await
        .expect("each deposit commits");
    }

    runners::run_projection_once(&sample.inputs, &sample.shutdown)
        .await
        .expect("the projection catches up");

    let check = |account: u64, expect: u64| {
        let app = &sample.app;
        async move {
            let balance = app
                .send(GetBalance { account })
                .await
                .expect("the read serves")
                .expect("the account is known");
            assert_eq!(balance.balance.0, expect, "account {account}");
        }
    };
    check(1, 50).await;
    check(2, 100).await;
    check(3, 150).await;
}
