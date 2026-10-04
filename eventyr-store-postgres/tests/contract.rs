//! The store contract against a real Postgres: the proof that `PgStore`
//! honours the protocol the in-memory store is compared to.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

use serde::{Deserialize, Serialize};

use eventyr_core::event_name::EventName;
use eventyr_store_postgres::PgStore;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
enum ContractEvent {
    Payload { value: u64 },
}

impl EventName for ContractEvent {
    fn event_name(&self) -> &'static str {
        match self {
            ContractEvent::Payload { .. } => "Payload",
        }
    }
}

impl From<u64> for ContractEvent {
    fn from(value: u64) -> Self {
        ContractEvent::Payload { value }
    }
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_contract() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let store = runtime
        .block_on(PgStore::<ContractEvent>::connect(&url))
        .expect("connect and migrate");
    // The suite's `block_on` calls poll sqlx futures against this
    // runtime; enter it so a reactor is always beneath them. One pool
    // is shared across checks — the checks use per-check stream ids,
    // so reuse cannot leak state between checks.
    let _guard = runtime.enter();
    let make_store = || store.clone();
    eventyr_store_testing::event_store_contract::<ContractEvent, _>(make_store);
    eventyr_store_testing::streams_all_contract::<ContractEvent, _>(make_store);
    eventyr_store_testing::event_store_batch_contract::<ContractEvent, _>(make_store);
}
