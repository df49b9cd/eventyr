//! End-to-end for the view store: the shared
//! [`view_store_contract`](eventyr_projection::view::view_store_contract)
//! against a real Postgres (migration `0003_views`).
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

mod common;

use eventyr_projection::view::view_store_contract;
use eventyr_store_postgres::PgStore;
use eventyr_store_postgres::views::PgViewStore;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Balance {
    balance: u64,
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_view_store_passes_the_view_store_contract() {
    let url = common::url();
    let runtime = common::runtime();
    {
        let _guard = runtime.enter();
        let events: PgStore<serde_json::Value> = common::fresh_store(&runtime, &url, "views");
        let store = PgViewStore::<Balance>::new(&events);
        view_store_contract(
            || &store,
            |version| Balance {
                balance: version * 10,
            },
        );
    }
    runtime.block_on(common::cleanup_schemas());
}
