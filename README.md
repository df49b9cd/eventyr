# eventyr

Event sourcing for Rust — pure machines, thin drivers.

A library of small, composable traits, not a framework. The domain is two
pure functions (`decide`/`apply`); every multi-step interaction — the
load → fold → decide → append write path, projection checkpointing with
at-least-once delivery — is a sans-IO state machine that consumes results
and emits actions as data. Drivers perform the I/O. The same machine runs
under tokio, blocking code, or a scripted test harness with zero mocks.
See [DESIGN.md](DESIGN.md) for the full design.

Non-goals worth naming: not a runtime or actor framework, no HTTP or
transport, no baked-in bus integrations (`EventBus` stays a
user-implementable trait), no ORM, no DDD toolkit.

## Crates

| Crate | Contents | Status |
|---|---|---|
| `eventyr` | Umbrella: re-exports core plus, behind features, the store/subscription/projection sides, and a prelude | shipped |
| `eventyr-core` | `Aggregate`, protocol vocabulary (`StreamId`/`Version`/`Sequence`/`ExpectedVersion`/`StoreError`/envelope/`Metadata`), `WriteMachine`, `BatchMachine`, `BoundaryMachine` (dynamic consistency boundaries: `Tag`/`Tagged`/`Query`/`Decision`), idempotent commands via `Metadata::idempotency_key`, `SubscriptionMachine`, upcast vocabulary (`Upcaster`/`RawEvent`/`UpcastError`) — `no_std + alloc`, zero deps | shipped |
| `eventyr-store` | `EventStore`/`StreamsAll` ports, the opt-in `QueryAppend`, `CommitSignal` and `StreamLifecycle` ports, `InMemoryStore`, `drive_write` driver, `AggregateRepository`, prelude | shipped |
| `eventyr-macros` | `#[derive(Aggregate)]`, `#[derive(EventName)]` — convention wiring, sugar not API | shipped |
| `eventyr-store-postgres` | sqlx-based `EventStore`/`StreamsAll` (`PgStore`, `append_events` PL/pgSQL, DESIGN §9's single-table sketch made real) — a standalone crate, not an umbrella feature yet | shipped |
| `eventyr-projection` | Read-path correctness layer: `UpcasterChain`/`ClosureUpcaster`, `UpcastingSource` (raw→typed), `RebuildPlan`/`SchemaVersion`/`checkpoint_key`; `View`/`ViewProjection`, and inline views written in the append transaction (`inline` feature, run by the Postgres and SQLite stores) | shipped |
| `eventyr-subscription` | Catch-up runner: `Projection` trait, `Projector`/`drive_projector` (`drive_projector_blocking` too; `wake_on`/`drive_projector_woken` poll on commit instead of after the idle sleep), `Fanout`, `CheckpointStore`/`InMemoryCheckpointStore`, `StoreSubscription`, `FilteredSubscription` (prefix/type filters that checkpoint past skipped events), `FailurePolicy`/`ParkedStore` (park poison events instead of stalling); `EventBus` trait behind its `bus` feature | shipped |
| `eventyr-store-fjall` | Embedded `EventStore`/`StreamsAll` over fjall — transactional appends, no server, driveable without an async runtime | shipped |
| `eventyr-shred` | Crypto-shredding: `Sensitive<T>` personal fields, `Shredder`, the `ShreddingStore` wrapper, and the bring-your-own `Cipher` and `KeyStore` seams with their contracts | shipped |
| `eventyr-shred-aes-gcm` / `eventyr-shred-chacha` | Cipher adapters for `eventyr-shred`: AES-256-GCM and XChaCha20-Poly1305 | shipped |
| `eventyr-store-testing` | The store contract: `event_store_contract` / `streams_all_contract` / `snapshot_contract` / `query_append_contract` / `commit_signal_contract` / `filtered_read_contract` / `lifecycle_contract`, self-tested against the in-memory store | shipped |

Postgres persistence is a standalone crate today; the umbrella pulls it in
as its own feature. The embedded store arrives as `eventyr-store-fjall`,
also behind its own umbrella feature.

## Umbrella features

| Feature | Default | Enables |
|---|---|---|
| `time` | yes | `Metadata::timestamp` |
| `macros` | yes | `#[derive(Aggregate)]`, `#[derive(EventName)]` via the prelude |
| `store` | yes | `eventyr::store` — ports, `InMemoryStore`, `drive_write`, `AggregateRepository` |
| `subscription` | no | `eventyr::subscription` — `Projection`, `Projector`, checkpoint store |
| `bus` | no | `subscription` + the `EventBus` live-push trait |
| `projection` | no | `subscription` + `eventyr::projection` — upcaster chains, rebuilds |
| `fjall` | no | `eventyr::fjall` — the embedded store |

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

# async fn demo() {
let store = InMemoryStore::new();
let repository = AggregateRepository::<Account, _>::new(
    store,
    RetryPolicy::default(),
);

// Open, then deposit: the second call folds the first's event.
repository.execute(AccountId(1), AccountCommand::Open).await.expect("open");
match repository.execute(AccountId(1), AccountCommand::Deposit { amount: 50 }).await {
    Ok(ExecutionOutcome::Committed { committed: events, .. }) => assert_eq!(events.len(), 1),
    Ok(_) => panic!("a deposit decides an event"),
    Err(_) => panic!("the deposit must commit"),
}
# }
# fn main() {
#     tokio::runtime::Builder::new_current_thread()
#         .enable_all()
#         .build()
#         .expect("runtime")
#         .block_on(demo());
# }
```

## Design

The design document is the constitution of this workspace:

- [DESIGN.md](DESIGN.md) — the full design
- [§4.5 · the write machine](DESIGN.md#45-the-write-machine--sans-io-core-of-the-repository) — the machine/driver split, the flagship explanation
- [§7 · machine modeling rules](DESIGN.md#7-machine-modeling-rules-sans-io-discipline) — the sans-IO discipline and the machine table
- [§12 · roadmap](DESIGN.md#12-roadmap) — what is shipped, what is pending
