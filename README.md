# eventyr

[![CI](https://github.com/df49b9cd/eventyr/actions/workflows/ci.yml/badge.svg)](https://github.com/df49b9cd/eventyr/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT_OR_Apache--2.0-blue)](#license)
[![Rust: 1.99+](https://img.shields.io/badge/rust-1.99%2B-orange)](#installation)

Event sourcing for Rust — pure machines, thin drivers.

A library of small, composable traits, not a framework. The domain is two
pure functions (`decide`/`apply`); every multi-step interaction — the
load → fold → decide → append write path, projection checkpointing with
at-least-once delivery — is a sans-IO state machine that consumes results
and emits actions as data. Drivers perform the I/O. The same machine runs
under tokio, blocking code, or a scripted test harness with zero mocks.
See [DESIGN.md](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md)
for the full design.

Non-goals worth naming: not a runtime or actor framework, no HTTP or
transport, no baked-in bus integrations (`EventBus` stays a
user-implementable trait), no ORM, no DDD toolkit.

## Status

**Pre-release, unpublished.** The crates are not yet on crates.io; the
workspace version is 0.1.0. The 0.x.y numbers the docs cite (0.7.9,
roadmap 0.8, …) are DESIGN.md milestones, not crate versions. Until the
first release, use a git dependency:

```toml
[dependencies]
eventyr = { git = "https://github.com/df49b9cd/eventyr" }
```

Stability: the API is still allowed to break between milestones; pin a
commit for anything you build against today.

## Installation

**Rust 1.99 or newer** (also the current stable at the time of writing).

```toml
[dependencies]
# Everything default: the core, the derives, the store side, timestamps.
eventyr = "0.1"

# The same, plus the durable stores and read side you use:
# eventyr = { version = "0.1", features = ["postgres", "postgres_snapshots"] }
# eventyr = { version = "0.1", features = ["sqlite", "sqlite_views"] }
```

Until the crates are published, substitute the git dependency above.
Tokio is not a dependency of the library — any async runtime (or none,
via the blocking drivers) drives the same machines.

## Crates

| Crate | Contents | Status |
|---|---|---|
| `eventyr` | Umbrella: re-exports core plus, behind features, the store/subscription/projection sides, and a prelude | shipped |
| `eventyr-core` | `Aggregate`, protocol vocabulary (`StreamId`/`Version`/`Sequence`/`ExpectedVersion`/`StoreError`/envelope/`Metadata`), `WriteMachine`, `BatchMachine`, `BoundaryMachine` (dynamic consistency boundaries: `Tag`/`Tagged`/`Query`/`Decision`), idempotent commands via `Metadata::idempotency_key`, `SubscriptionMachine`, `SagaMachine`, upcast vocabulary (`Upcaster`/`RawEvent`/`UpcastError`) — `no_std + alloc`, zero deps | shipped |
| `eventyr-store` | `EventStore`/`StreamsAll` ports, the opt-in `QueryAppend`, `CommitSignal` and `StreamLifecycle` ports, `InMemoryStore`, `drive_write` driver, `AggregateRepository` (with the `load` / `load_at` / `load_until` read path, 0.7.8), prelude | shipped |
| `eventyr-macros` | `#[derive(Aggregate)]`, `#[derive(EventName)]` — convention wiring, sugar not API | shipped |
| `eventyr-store-postgres` | sqlx-based `EventStore`/`StreamsAll` (`PgStore`, `append_events` PL/pgSQL); `snapshots`, `views`, `checkpoints`, and `leases` (0.7.9) features | shipped |
| `eventyr-store-sqlite` | Embedded SQLite store (`rusqlite`) — the contract suite's reference port, with `snapshots`, `views`, `checkpoints`, `shred`, and `parked` substores | shipped |
| `eventyr-projection` | Read-path correctness layer: `UpcasterChain`/`ClosureUpcaster`, `UpcastingSource` (raw→typed), `RebuildPlan`/`SchemaVersion`/`checkpoint_key`; `View`/`ViewProjection`, and inline views written in the append transaction (`inline` feature, run by the Postgres and SQLite stores) | shipped |
| `eventyr-subscription` | Catch-up runner: `Projection` trait, `Projector`/`drive_projector` (`drive_projector_blocking` too; `wake_on`/`run_woken` poll on commit instead of after the idle sleep), `Fanout`, `SkipRedelivered` (at-least-once made idempotent), `CheckpointStore`/`InMemoryCheckpointStore`, `StoreSubscription`, `FilteredSubscription`, `FailurePolicy`/`ParkedStore`, `ProjectorLease`/`InMemoryLeaseStore` (one driver per checkpoint name, 0.7.9), `SagaProjection` (the reactive saga runner); `EventBus` trait behind its `bus` feature | shipped |
| `eventyr-store-fjall` | Embedded `EventStore`/`StreamsAll` over fjall — transactional appends, no server, driveable without an async runtime | shipped |
| `eventyr-shred` | Crypto-shredding: `Sensitive<T>` personal fields, `Shredder`, the `ShreddingStore` wrapper, and the bring-your-own `Cipher` and `KeyStore` seams with their contracts | shipped |
| `eventyr-shred-aes-gcm` / `eventyr-shred-chacha` | Cipher adapters for `eventyr-shred`: AES-256-GCM and XChaCha20-Poly1305 | shipped |
| `eventyr-store-testing` | The store contract: `event_store_contract` / `event_store_batch_contract` / `streams_all_contract` / `snapshot_contract` / `query_append_contract` / `commit_signal_contract` / `filtered_read_contract` / `lifecycle_contract`, self-tested against the in-memory store | shipped |

Postgres persistence is a standalone crate the umbrella pulls in as its
own feature; the embedded stores (`eventyr-store-fjall`,
`eventyr-store-sqlite`) likewise. Sagas ship in core (`SagaMachine`) with
their subscription runner (`SagaProjection`) beside the projector.

## Umbrella features

| Feature | Default | Enables |
|---|---|---|
| `time` | yes | `Metadata::timestamp` (stores that populate it: Postgres via `created_at`; the read path's `load_until` needs it) |
| `macros` | yes | `#[derive(Aggregate)]`, `#[derive(EventName)]` via the prelude |
| `store` | yes | `eventyr::store` — ports, `InMemoryStore`, `drive_write`, `AggregateRepository` |
| `subscription` | no | `eventyr::subscription` — `Projection`, `Projector`, checkpoint store |
| `bus` | no | `subscription` + the `EventBus` seam for your own transport (live push itself is the ungated `CommitSignal` port) |
| `projection` | no | `subscription` + `eventyr::projection` — upcaster chains, rebuilds |
| `postgres` | no | `eventyr::postgres` — the Postgres store (`postgres_snapshots` adds its `SnapshotStore`; `postgres_checkpoints`, `postgres_leases`, `postgres_views` add its checkpoint, lease, and view stores) |
| `fjall` | no | `eventyr::fjall` — the embedded store (`fjall_snapshots` adds its snapshot store) |
| `sqlite` | no | `eventyr::sqlite` — the embedded SQLite store; `sqlite_snapshots`, `sqlite_views`, `sqlite_checkpoints`, `sqlite_shred`, `sqlite_parked` for its substores |
| `shred` | no | `eventyr::shred` — crypto-shredding; pick a cipher with `shred_aes_gcm` / `shred_chacha`, and `shred_parked` seals parked events |
| `metrics` | no | the `Metrics` port's tracing backend (`TracingMetrics`), passed to `drive_write_with_metrics` or `Projector::with_metrics` |

## A taste

Define an aggregate as two pure functions, then open and deposit through
the repository — the machine behind it is pure; the `InMemoryStore` here
keeps the example self-contained:

```rust
use std::fmt;
use eventyr::prelude::*;
use eventyr::store::prelude::*;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct AccountId(u64);
impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, PartialEq, Debug)]
enum AccountEvent { Opened, Deposited { amount: u64 } }

#[derive(Clone, Debug)]
enum AccountCommand { Open, Deposit { amount: u64 } }

#[derive(Debug, PartialEq)]
enum AccountError { AlreadyOpen, NotOpen }
impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AccountError::AlreadyOpen => "account is already open",
            AccountError::NotOpen => "account is not open",
        })
    }
}

#[derive(Debug)]
struct AccountState { open: bool, balance: u64 }

struct Account;

impl Aggregate for Account {
    const NAME: &'static str = "account";
    type Id = AccountId;
    type State = AccountState;
    type Event = AccountEvent;
    type Command = AccountCommand;
    type Error = AccountError;

    fn initial(_id: &Self::Id) -> Self::State {
        AccountState { open: false, balance: 0 }
    }

    fn apply(state: &mut Self::State, event: &Self::Event) {
        match event {
            AccountEvent::Opened => { state.open = true; state.balance = 0; }
            AccountEvent::Deposited { amount } => state.balance += amount,
        }
    }

    fn decide(state: &Self::State, command: &Self::Command)
        -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::Open if state.open => Err(AccountError::AlreadyOpen),
            AccountCommand::Open => Ok(vec![AccountEvent::Opened]),
            AccountCommand::Deposit { .. } if !state.open => Err(AccountError::NotOpen),
            AccountCommand::Deposit { amount } =>
                Ok(vec![AccountEvent::Deposited { amount: *amount }]),
        }
    }
}

// A repository needs an async runtime to drive its machines. Tokio is a
// dev-dependency of this crate only; any runtime works.
#[tokio::main]
async fn main() {
    let store = InMemoryStore::new();
    let repository = AggregateRepository::<Account, _>::new(
        store,
        RetryPolicy::default(),
    );

    // Open, then deposit: the second call folds the first's event.
    repository.execute(AccountId(1), AccountCommand::Open).await.expect("open");
    match repository.execute(AccountId(1), AccountCommand::Deposit { amount: 50 }).await {
        Ok(ExecutionOutcome::Committed { committed, .. }) => assert_eq!(committed.len(), 1),
        Ok(_) => panic!("a deposit decides an event"),
        Err(_) => panic!("the deposit must commit"),
    }
}
```

## Examples

Run any example from the workspace root; each states the features it needs.

| Example | Shows | Run |
|---|---|---|
| [`bank`](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/bank.rs) | One domain through the write path, snapshots, the idempotent saga, parking, shredding, live push, upcasting | `cargo run -p eventyr --example bank --all-features` |
| [`enrollment`](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/enrollment.rs) | A dynamic consistency boundary | `cargo run -p eventyr --example enrollment` |
| [`loan_eligibility`](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/loan_eligibility.rs) | A narrowed validation query; the blocking boundary driver | `cargo run -p eventyr --example loan_eligibility` |
| [`inline_view`](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/inline_view.rs) | Inline and async views: SQLite rows written in the append transaction, the same fold replayed as a projection | `cargo run -p eventyr --example inline_view --features sqlite_views` |
| [`distributed`](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/distributed.rs) | Several nodes writing and projecting at once: conflicts, a cross-node retry, projector failover, read-your-writes — embedded on SQLite; on Postgres too when `EVENTYR_PG_URL` is set | `cargo run -p eventyr --example distributed --features sqlite_views,sqlite_checkpoints,postgres_checkpoints,postgres_leases,postgres_views` |

## Deriving through the umbrella

The derives target whichever crate your manifest depends on — `eventyr`
or `eventyr-core` — resolved automatically, so a plain
`#[derive(Aggregate)]` works through the umbrella's prelude. Only a
*renamed* dependency needs the escape hatch:

```rust,ignore
#[derive(Aggregate)]
#[eventyr(crate = "renamed_eventyr")]
struct Account;
```

## Design

The design document is the constitution of this workspace:

- [DESIGN.md](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md) — the full design
- [§4.5 · the write machine](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#45-the-write-machine--sans-io-core-of-the-repository) — the machine/driver split, the flagship explanation
- [§7 · machine modeling rules](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#7-machine-modeling-rules-sans-io-discipline) — the sans-IO discipline and the machine table
- [§12 · roadmap](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#12-roadmap) — what is shipped, what is pending

## License

Licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE) at your option.
