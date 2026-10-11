# eventyr-store

The I/O half of the split: the port traits the core machines' actions imply,
the in-memory reference store, the drivers that run the machines against
any port implementation, and the `AggregateRepository` facade. This crate is
where "sans-IO" ends — everything here may await, but nothing here owns
protocol logic.

**Position:** the first crate above core. Depends on eventyr-core; the
adapters (postgres, sqlite, fjall), eventyr-subscription, and eventyr-shred
depend on it. Parent: [ARCHITECTURE.md](../ARCHITECTURE.md).

## Features

* `tracing` — `TracingMetrics`, the reference `Metrics` backend (emits at
  target `eventyr.metrics`).
* `time` — timestamp plumbing the stores fill into `Metadata`.
* Otherwise dependency-free: `futures::executor` (for the blocking
  drivers), `event-listener` (for `LocalCommitSignal`).

## The ports (`store.rs`)

Capabilities are separate traits; a store implements what it can, and
callers discover support through trait bounds — a projector over a store
without `StreamsAll` is a compile error, not a runtime probe.

| Port | Methods | Notes |
|---|---|---|
| `EventStore` | `append(stream, expected, events)`, `append_batch(Vec<StreamAppend>)`, `stream(&StreamId, from: Version)` | the base port; every adapter implements it |
| `StreamsAll` | `stream_all(from: Sequence)`, `stream_all_filtered(from, &EventFilter, max, scan_limit)` | filtered reads have a **client-side default** (requires `Event: EventName`) so simple stores work; adapters override to push down |
| `StreamLifecycle` | `close_stream`, `truncate_before` | tombstone and head cut |
| `QueryAppend` | `read(&Query, after)`, `append_if(Vec<StreamAppend>, AppendCondition)` | DCB; `Event: EventName + Tagged` |

Mechanics, uniform across every port:

* Async via RPITIT: `fn …(&self, …) -> impl Future<Output = …> + Send`.
* `&self` receivers — a store is shared, not owned (`Arc<PgStore<E>>`
  is the normal handle).
* Blanket delegation impls for `&S` and `Arc<S>`, which is what makes the
  [shred](eventyr-shred.md) decorator pattern compose.
* Errors are `StoreError` throughout (taxonomy in
  [ARCHITECTURE.md §10](../ARCHITECTURE.md#10-the-failure-model)).

Shared helpers ports and adapters lean on:

* `expected_version_matches(expected, current)` — the OCC check every
  append path runs.
* `validate_batch` — a stream may appear at most once in a batch.
* `read_starts_before_cut` / `TruncatePlan{Noop, Cut(n)}` /
  `plan_truncate` — truncation bookkeeping so adapters share the
  cut-line arithmetic.
* `sql_position` — saturating i64 conversion for SQL limits.
* `append_batch_fallback` — a default `append_batch` as a single
  transaction's worth of appends for adapters that can't do better.
* `selected`, `all_events` — read-side stream plumbing.

`EventFilter{stream_prefixes, event_types}` with `all`/`stream_prefix`/
`event_types` constructors and `matches`/`selects`; reads return
`FilteredRead{events, scanned}` — the scan bound is part of the data, not
just a cap (see [ARCHITECTURE.md §7](../ARCHITECTURE.md#7-the-read-path)).

## `InMemoryStore` (`memory.rs`)

The reference implementation — used by tests, examples, and as the
quick-start store. One `Mutex<Inner>` holding streams, the global log, the
sequence counter, per-stream heads, closed-set, and truncation cuts, plus
a `LocalCommitSignal` fed on every commit. Implements all four log ports
and `CommitSignal`. Global reads (`StreamsAll`) scan the log under the one
lock — trivially consistent; `append_batch` is one critical section, so
atomicity is free. Contract-tested by the same suites as the durable
stores.

## The drivers (`driver.rs`)

Each driver is a loop: take the machine's actions, perform them against
the ports, feed the inputs back. No logic lives here — a driver that
branched on domain meaning would be a bug.

* **`drive_write(machine, store, sleep)** (+`_with_metrics`)** — the async
  single-stream write loop. Answers `LoadSnapshot` with `None` (plain
  machines never ask; snapshot-aware machines take the next driver).
* **`drive_write_with_snapshots(machine, store, snapshots, sleep)**
  (+`_and_metrics`)** — additionally answers `LoadSnapshot` from the
  snapshot store and persists offered snapshots **fire-and-forget,
  newest-wins** on `Committed`. A failed snapshot save is logged
  (metrics counter), never an error: snapshots are a hint.
* **`drive_write_blocking(..)`** — the same machines through
  `futures::executor::block_on`, for non-async hosts. Same for the
  snapshot-aware variant.
* **`drive_write_batch(machine, store, sleep)` (+metrics, +blocking)** —
  the `BatchMachine` loop. Loads are sequential per stream (they're under
  one transaction on the append side anyway); one `AppendBatch` action
  maps to one `append_batch` port call.
* **`drive_boundary(..)` (+metrics, +blocking)** — the `BoundaryMachine`
  loop: `Read` → `store.read(query, after)`, `Append` →
  `append_if`.
* Shared helpers `load_stream` / `append_and_report` keep the loops small.

The projector driver lives one crate up in
[eventyr-subscription](eventyr-subscription.md) — this crate drives only
the three write-side machines.

## `AggregateRepository` (`repository.rs`)

The facade applications actually hold:

* `AggregateRepository<A, S, SS = ()>` — aggregate `A`, its state `S`,
  snapshot state `SS`. `new(store, retry)` / `from_policy(write_policy)`.
* **`execute(&id, command)`** (+ `execute_with_metadata`) — builds a
  `WriteMachine` and runs `drive_write` to completion, mapping
  `WriteOutcome` into `ExecutionOutcome{Committed, AlreadyCommitted,
  Noop}` with `ExecutionError::{Domain(A::Error), Store(StoreError)}`.
* **`with_snapshots(snapshot_store, policy)`** — switches to snapshot-aware
  machines and `drive_write_with_snapshots`;
  `execute_with_snapshots(_and_metadata)`, `load_with_snapshots`,
  `load_at_with_snapshots` follow.
* **`load` / `load_at`** — fold state at head / at a version, returned as
  `Loaded{state, version}`. `fold_while` re-checks stream identity and
  contiguity — the same invariants the machine enforces on the write path,
  so a store bug can't silently produce a torn state.
* `load_until(..)` (behind `time`) — fold up to a timestamp.

## Auxiliary ports in this crate

* **`SnapshotStore`** (`snapshot_store.rs`) — `load(stream) ->
  Option<Snapshot>`, `save(Snapshot)`; **newest-wins by version** is the
  contract (a stale save must not overwrite a newer one). `InMemorySnapshotStore`
  is the reference; durable impls in the adapters. `snapshot_contract`
  tests it.
* **`CommitSignal`** (`notify.rs`) — `type Listener: CommitListener`;
  `Listener::committed()` is a cancel-safe future. **A hint, never a
  correctness mechanism**: wake signals shorten idle sleeps; correctness is
  always the poll. `NoSignal` never fires; `LocalCommitSignal` is
  event-listener-based and in-process; `PgCommitSignal` (adapter crate)
  rides `NOTIFY`.
* **`metrics`** — re-exports core's port plus the driver-specific
  instrument names (`PARK_FAILURES`, `ACK_FAILURES`,
  `LEASE_RENEWALS`, `LEASE_RENEW_FAILURES`, `LEASE_LOST`,
  `LEASE_RELEASE_FAILURES`) and `TracingMetrics` behind `tracing`.

## Prelude

Drivers, the in-memory store, all port types, repository types, snapshot
and commit-signal ports, metrics — the application import is
`eventyr_store::prelude::*` (and it is what the umbrella's `store` feature
re-exports).

## Testing

* Runs [eventyr-store-testing](eventyr-store-testing.md)'s full suite
  against `InMemoryStore` — the in-memory store is a *contract store*, not
  a toy.
* The drivers are exercised through the repository tests and the
  examples; scripted-driver tests in core pin machine behavior so the
  driver loops only need round-trip coverage.

## Limits

* No durable store ships in this crate by design — pick an
  [adapter](../ARCHITECTURE.md#5-the-ports-eventyr-store-and-friends).
* `append_batch` atomicity is a port *promise*; the fallback loops
  streams, so adapters that can't do a multi-stream transaction don't
  implement `append_batch` at all rather than lying about atomicity.
* Roadmap 0.8.1 moves checkpoint stores to CAS semantics (in
  [subscription](eventyr-subscription.md)); 0.8.2 adds store identity to
  the signal/lock names.
