# eventyr

Event sourcing for Rust — pure machines, thin drivers.

> **Status: pre-0.1.** `eventyr-core` (the `Aggregate` trait, the protocol
> vocabulary, and the `WriteMachine` with its transition tests) is implemented.
> The store side (`EventStore`, drivers, repository) is next. See
> [DESIGN.md](DESIGN.md) for the full design.

Eventyr is a library of small, composable traits — not a framework. The domain
is two pure functions; multi-step protocols (the write path, projection
checkpointing) are sans-IO state machines that consume results and emit
actions; drivers perform the I/O. The same machine runs under tokio, blocking
code, or a deterministic test harness.

## Crates

| Crate | Contents |
|---|---|
| `eventyr` | Umbrella: re-exports everything, plus a prelude |
| `eventyr-core` | `Aggregate`, protocol vocabulary, `WriteMachine` — `no_std + alloc`, zero deps |
| `eventyr-store` | `EventStore`/`StreamsAll` traits, in-memory store, drivers, repository *(planned)* |
| `eventyr-macros` | `#[derive(Aggregate)]` etc. *(planned)* |
| `eventyr-store-postgres` | sqlx-based store *(planned)* |
| `eventyr-projection` | Projector runner, checkpointing *(planned)* |
| `eventyr-subscription` | Catch-up subscriptions, event bus trait *(planned)* |

## A taste

```rust
use eventyr::prelude::*;

// Drive the write machine by hand — no store, no async, no I/O.
let mut machine = WriteMachine::<MyAggregate>::new(
    MyAggregateId(7),
    MyCommand::DoIt,
    RetryPolicy::default(),
);

match machine.start() {
    WriteAction::LoadStream { stream_id, from } => { /* read the stream */ }
    _ => unreachable!(),
}
```
