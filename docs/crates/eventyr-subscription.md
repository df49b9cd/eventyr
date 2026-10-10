# eventyr-subscription

The read-side runtime: the projector runner that drives core's
`SubscriptionMachine` against any `StreamsAll` store, plus the auxiliary
ports that make it durable — checkpoints, leases, parked-event stores — and
the saga driver. This crate is where at-least-once delivery becomes an
operational loop.

**Position:** above core + store; [eventyr-projection](eventyr-projection.md)
and the adapter crates' checkpoint/lease/parked stores sit on top of it.
Parent: [ARCHITECTURE.md](../ARCHITECTURE.md).

## Features

* `bus` — the `EventBus` / `Subscription` live-push seam (§2 of the
  design's event-bus sketch): publish to an in-process bus, poll batches,
  ack.
* `tokio_notify` — the `Catch` impl over `Arc<tokio::sync::Notify>`, for
  hosts already on tokio (the core `Catch` trait has an impl for it under
  this feature).
* `testing` — exports the contract suites
  (`checkpoint_store_contract`, `lease_store_contract`,
  `parked_store_contract`) for adapter crates' tests.

## The runner (`runner.rs`)

* **`Projection`** — the fold port: `type Event; type Error; apply(&mut
  self, &EventEnvelope)`. Combinators: **`Fanout<P>`** (one source, many
  projections), **`SkipRedelivered<P>`** (per-stream newest-folded-version
  map; dedupes within a run, tolerating at-least-once redelivery).
* **`Catch`** — the wake seam: `caught_up()` is polled when the machine
  sleeps idle; the tokio `Notify` impl shortens idle waits. `NoCatch`
  never fires.
* **`DriverPorts<'m, W = NoSignal, K = NoParking>`** — what the driver
  needs beyond the source: `wake_on(signal)`, `park_into(store, policy)`,
  `with_metrics`, `caught_up_on(catch)`.
* **`drive_projector(machine, name, source, checkpoints, projection,
  sleep, ports)`** — the loop: `Fetch` → `source.fetch(from, limit)`,
  `Apply` → `projection.apply`, `Ack` → `checkpoints.store`,
  `Sleep` → real time, unless a wake or catch shortens it
  (`SleepReason::Idle` only — `Backoff` always sleeps in full).
  `Park`/`ParkFailed` flow to the parked store; failures there are metric
  counters and a backoff, never a skip.
* **`RunError`** — how a run ends: `Store(StoreError)`,
  `LeaseLost{name, checkpoint}`, `Taken{name}`.
* **`drive_projector_blocking`** — the block_on twin, for hosts without a
  runtime.
* **`Projector<S, C, P, W, K>`** — the application-facing handle:
  `new(name, source, checkpoints, projection)`, `with_policy`,
  `with_metrics`, `wake_on`, `caught_up_on`, `park_into`, then `run(sleep)`
  or `run_woken(sleep)` (a run woken by a signal). **A park policy without
  a parked store refuses to run** — refuse at configuration time, not at
  the first poison event.
* **`drive_projector_leased(machine, name, source, checkpoints, leases,
  policy, …)`** and **`LeasedProjector`** (`lease_with`,
  `run_leased`/`run_woken_leased`) — the fleet shape: acquire → run →
  release, with renewals **before every fetch and before every ack**. A
  run that cannot renew (or is taken over) ends `LeaseLost{name,
  checkpoint}`; the caller loops and re-acquires; the new holder resumes
  from the checkpoint store. `stop_at_catch_up` runs end at
  `CaughtUp{checkpoint}` instead of sleeping — the rebuild/one-shot shape.

## Auxiliary ports

| Port | Contract | Reference impl |
|---|---|---|
| `CheckpointStore` (`checkpoint.rs`) | `load(name) -> Checkpoint`, `store(name, cp)`; **last-write-wins** (CAS is 0.8.1; leases are today's guard) | `InMemoryCheckpointStore` |
| `ProjectorLease` (`lease.rs`) | `acquire(name, ttl, grace, max_grace)`, `renew(&mut lease, …) -> Instant`, `release` | `InMemoryLeaseStore` (with `is_held`/`expire` test seams) |
| `ParkedStore` (`parked.rs`) | `park(ParkedEvent)`, `list`, `remove` | `InMemoryParkedStore`; `NoParking` refuses |
| `SubscriptionSource` (`source.rs`) | `fetch(from: Checkpoint, max) -> Batch` | `StoreSubscription` (over any `StreamsAll`), `FilteredSubscription` |

Details worth knowing:

* **`LeasePolicy{ttl: 5s, grace: 3, max_grace: 12}`** — a healthy run ends
  `LeaseLost` at `ttl × max_grace` (the absolute cap from acquire), which
  is what bounds a zombie holder. `LeaseError::{Taken, Lost,
  Store{error, renewed_until}}` names the failure precisely.
* **`ParkedEvent<E>{subscription, envelope, attempts, error: String}`** —
  poison events are *inspectable*: list and remove are first-class, the
  alert metric is `eventyr_parked_events_total`
  ([ARCHITECTURE.md §10](../ARCHITECTURE.md#10-the-failure-model)).
* **`FilteredSubscription`** wraps a source with an `EventFilter` and a
  scan limit (`DEFAULT_SCAN_LIMIT = 4096`, `scan_limit()` to read it) —
  used with `stream_all_filtered` when the store pushes it down, or the
  default client-side filter otherwise.
* Each port ships its `*_contract` suite (behind `testing`) — the durable
  implementations in [postgres](eventyr-store-postgres.md) and
  [sqlite](eventyr-store-sqlite.md) run them.

## Sagas (`saga.rs`)

`SagaProjection<S, D>`: a `Saga` plus a dispatcher closure
`FnMut(SagaCommand) -> Future<Output = Result<(), StoreError>>`. Driven by
`drive_saga` under the same projector machinery — each event dispatches
its `(StreamId, Command)` reactions through the caller's dispatcher
(typically a repository `execute_with_metadata`), idempotent by the saga
key (`"{name}:{sequence}:{index}"`). With `with_metadata`, causation/
correlation flow through. The machine is in
[core](eventyr-core.md#sagamachine-saga); this is its driver.

## Bus (`bus`)

`EventBus::publish` / `Subscription::{poll, ack}` — an in-process
live-push seam for hosts that want push-style batching between services.
The log remains the source of truth; the bus carries *notification*, the
same way `CommitSignal` does.

## Testing

* `ProjectionScenario::over(name, projection).given(..).when(..)` /
  `when_driven(f)` → `ProjectionOutcome` with `then(|name, &p| …)` and
  `projection()` — the same BDD shape as core's `Scenario`, for
  projections.
* The contract suites (`checkpoint_store_contract`,
  `lease_store_contract`, `parked_store_contract`) gate the durable
  implementations.
* Core's `projector_scripted` pins the machine; these tests pin the loops.

## Limits

* Checkpoint stores are last-write-wins — leases are the concurrency guard
  today; CAS checkpoints are roadmap 0.8.1.
* The bus is in-process; there is no network transport (by design — the
  log is the transport).
* Parked stores: durable impls are SQLite today; Postgres is 0.8.6.
