//! The §14 smaller items: a projection-side given/when/then and the
//! runner's catch-up pulse.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventyr_core::prelude::*;
use eventyr_store::prelude::*;
use eventyr_subscription::prelude::{
    Catch, InMemoryCheckpointStore, Projection, ProjectionScenario, Projector, StoreSubscription,
    SubscriptionOutcome, SubscriptionPolicy,
};
use futures::future;

#[tokio::test]
async fn a_projection_scenario_folds_then_asserts() {
    use eventyr_core::testing::account::AccountEvent;
    struct Balances {
        by_account: std::collections::BTreeMap<u64, u64>,
    }
    impl Projection for Balances {
        type Event = AccountEvent;
        type Error = std::convert::Infallible;
        async fn apply(&mut self, event: &EventEnvelope<AccountEvent>) -> Result<(), Self::Error> {
            let id: u64 = event
                .stream_id
                .as_str()
                .trim_start_matches("account-")
                .parse()
                .expect("an account stream");
            match &event.event {
                AccountEvent::Opened { .. } => {
                    self.by_account.entry(id).or_insert(0);
                }
                AccountEvent::Deposited { amount } => {
                    *self.by_account.entry(id).or_default() += amount;
                }
                AccountEvent::Withdrawn { amount } => {
                    *self.by_account.entry(id).or_default() -= amount;
                }
            }
            Ok(())
        }
    }

    let env = |seq: u64, acct: u64, version: u64, event: AccountEvent| EventEnvelope {
        sequence: Sequence::new(seq),
        stream_id: StreamId::from(format!("account-{acct}")),
        version: Version::new(version),
        event,
        metadata: Metadata::default(),
    };

    ProjectionScenario::over(
        "balances",
        Balances {
            by_account: Default::default(),
        },
    )
    .given(vec![
        env(1, 1, 1, AccountEvent::Opened { owner: "a".into() }),
        env(2, 1, 2, AccountEvent::Deposited { amount: 100 }),
        env(3, 1, 3, AccountEvent::Withdrawn { amount: 30 }),
    ])
    .when()
    .await
    .then(|_, balances| assert_eq!(balances.by_account[&1], 70));
}

#[tokio::test]
async fn a_projection_scenario_drives_a_custom_call() {
    use eventyr_core::testing::account::AccountEvent;
    struct Balances {
        total: u64,
    }
    impl Projection for Balances {
        type Event = AccountEvent;
        type Error = std::convert::Infallible;
        async fn apply(&mut self, event: &EventEnvelope<AccountEvent>) -> Result<(), Self::Error> {
            if let AccountEvent::Deposited { amount } = &event.event {
                self.total += amount;
            }
            Ok(())
        }
    }

    let env = |seq: u64, version: u64, amount: u64| EventEnvelope {
        sequence: Sequence::new(seq),
        stream_id: StreamId::from("account-7"),
        version: Version::new(version),
        event: AccountEvent::Deposited { amount },
        metadata: Metadata::default(),
    };

    ProjectionScenario::over("balances", Balances { total: 0 })
        .when_driven(|mut balances: Balances| async move {
            // Drive the projection by hand — the way a test that has to
            // call its fold directly does — and hand the projection back.
            Projection::apply(&mut balances, &env(1, 1, 50))
                .await
                .unwrap();
            Ok::<_, std::convert::Infallible>(balances)
        })
        .await
        .then(|_, balances| assert_eq!(balances.total, 50));
}

/// The runner's "would idle now" pulse: a test that needs the read
/// model fresh waits on it, not on a timer.
mod catches {
    use super::*;

    struct Pulse {
        fired: Mutex<usize>,
    }
    impl Catch for Pulse {
        fn caught_up(&self) {
            *self.fired.lock().expect("poisoned") += 1;
        }
    }

    #[derive(Clone, Default)]
    struct Seen(Arc<Mutex<Vec<u64>>>);
    impl Projection for Seen {
        type Event = u64;
        type Error = std::convert::Infallible;
        async fn apply(&mut self, event: &EventEnvelope<u64>) -> Result<(), Self::Error> {
            self.0.lock().expect("poisoned").push(event.event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn stop_at_catch_up_ends_before_the_pulse_fires() {
        let store = Arc::new(InMemoryStore::new());
        store
            .append(
                &StreamId::from("account-1"),
                ExpectedVersion::Empty,
                vec![NewEvent::new(11), NewEvent::new(12)],
            )
            .await
            .expect("seed");

        let seen = Seen::default();
        let pulse = Pulse {
            fired: Mutex::new(0),
        };
        let outcome = Projector::new(
            "seen",
            StoreSubscription::new(Arc::clone(&store)),
            InMemoryCheckpointStore::new(),
            seen.clone(),
        )
        .with_policy(SubscriptionPolicy::new(64, Duration::ZERO, Duration::ZERO).stop_at_catch_up())
        .caught_up_on(&pulse)
        .run(|_| future::ready(()))
        .await
        .expect("run");

        assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
        assert_eq!(*seen.0.lock().expect("poisoned"), vec![11, 12]);
        // Stop-at-catch-up ends at the first caught-up poll — the
        // pulse's sleep boundary never runs after it, so the pulse count
        // is what the machine's pre-sleep exactly was: none.
        assert_eq!(*pulse.fired.lock().expect("poisoned"), 0);
    }

    #[tokio::test]
    async fn the_caught_up_pulse_fires_at_the_first_idle() {
        let store = Arc::new(InMemoryStore::new());
        store
            .append(
                &StreamId::from("account-1"),
                ExpectedVersion::Empty,
                vec![NewEvent::new(11), NewEvent::new(12)],
            )
            .await
            .expect("seed");

        let seen = Seen::default();
        let pulse = Arc::new(Pulse {
            fired: Mutex::new(0),
        });
        // A perennial run (no stop_at_catch_up) with a zero idle sleep:
        // the first caught-up poll reaches the idle boundary and fires
        // the pulse, then keeps polling. Spawned, because it never ends
        // on its own; the pulse is the observable.
        let run = {
            let store = Arc::clone(&store);
            let seen = seen.clone();
            let pulse = Arc::clone(&pulse);
            tokio::spawn(async move {
                Projector::new(
                    "seen",
                    StoreSubscription::new(store),
                    InMemoryCheckpointStore::new(),
                    seen,
                )
                .with_policy(SubscriptionPolicy::new(64, Duration::ZERO, Duration::ZERO))
                .caught_up_on(&*pulse)
                // Yield on every idle sleep: the in-memory store and a
                // ready sleep never suspend, so on the test's
                // single-threaded runtime a perennial run would starve
                // the task waiting on the pulse.
                .run(|_| tokio::task::yield_now())
                .await
            })
        };
        // Condition-based wait: the pulse fires at the first idle
        // boundary, whenever that lands.
        while *pulse.fired.lock().expect("poisoned") == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        run.abort();
        assert_eq!(*seen.0.lock().expect("poisoned"), vec![11, 12]);
    }
}
