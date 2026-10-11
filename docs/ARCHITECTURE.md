# Eventyr architecture

This is the as-built architecture record for the eventyr workspace: what
shipped, how the crates fit together, and why the seams sit where they do.
[DESIGN.md](../DESIGN.md) is the constitution — goals, non-goals, alternatives
weighed, and the roadmap; this document describes the system those decisions
produced. [GUIDE.md](../GUIDE.md) teaches the concepts in dependency order;
per-crate depth lives in [crates/](crates/).

Workspace state at time of writing: milestone 0.7.9, workspace version 0.1.0,
unpublished. Everything described here exists and is tested in CI.

## 1. The shape of the library: pure machines, thin drivers

Eventyr is an event-sourcing toolkit built on one idea, applied everywhere:

> **A machine is a pure state transition. A driver is a runtime that feeds a
> machine and performs the actions it asks for.**

A machine is a plain struct. It performs no I/O, allocates nothing it doesn't
return, measures no time, and never panics on bad input — it ends with a
failure outcome instead. It consumes *inputs* (facts about the world: a stream
was loaded, an append committed, a store failed) and emits *actions* (data:
load this stream, append these events, sleep for this long). Every driver —
async Tokio, blocking, in-memory scripted — is a loop that shuttles between a
machine and the outside world. The protocol logic lives in the machine, once,
and the drivers are deliberately boring.

This is the sans-IO discipline, written down as rules in
[DESIGN.md §7](../DESIGN.md#7-machine-modeling-rules-sans-io-discipline):

1. A machine is a struct with a `handle` transition: `inputs in → actions
   out, machine mutates itself`. Nothing else.
2. Actions are data. They name what to do, never how.
3. One machine per protocol; one driver per runtime. The write protocol has
   exactly one machine (`WriteMachine`) and three drivers (async, blocking,
   scripted).
4. Machines never panic. A protocol violation ends the run:
   `Done(Failed(StoreError::Other(ProtocolError)))`. After `Done`, every
   further input is itself a violation.
5. Time is not observed, it is requested: a machine that wants to wait emits
   a `Sleep` action with a reason; the driver decides how (and whether) to
   sleep it.

The payoff: every protocol in the library is unit-testable without a runtime
or a database — proptest runs tens of thousands of input interleavings
against the machines directly — and the same machine code runs over
Postgres, SQLite, fjall, in-memory, and a shredding wrapper without change.

## 2. Crate layering

```
                        eventyr (umbrella: features, re-exports, examples)
                              │
        ┌───────────┬─────────┼──────────┬─────────────┬─────────────┐
   eventyr-     eventyr-  eventyr-   eventyr-      eventyr-      eventyr-
   postgres     sqlite    fjall     subscription  projection     shred
   (sqlx)       (rusqlite)(fjall)       │              │        (+aes-gcm,
        │           │        │          │              │         +chacha)
        └───────────┴────┬───┴──────────┴───┬──────────┴─────────────┘
                        eventyr-store       eventyr-shred also wraps store
                           │  (ports, drivers, repository, InMemoryStore)
                           │        eventyr-subscription also depends on store
                           │        eventyr-projection depends on subscription
                           ▼
                       eventyr-core  (no_std + alloc, zero required deps)
                           ▲
                       eventyr-macros  (proc-macro, optional via core's `macros`)
                       eventyr-store-testing  (dev-only, contract suites)
```

The layering rule (the placement rule, [DESIGN.md §3](../DESIGN.md#3-crate-layout)):

* **eventyr-core** holds anything a *machine transitions on*: the `Aggregate`
  trait, the protocol vocabulary, and the five machines. It is `no_std` +
  `alloc` with zero required dependencies, so the domain model and the
  protocol logic can be depended on by anything, tested with `miri`, and
  never drag a runtime in.
* **eventyr-store** holds the *ports* — the I/O trait surface the machines'
  actions imply — plus the in-memory store, the drivers, and the repository.
  Ports are traits because they are the seam between machine-land and
  runtime-land.
* Store adapters (postgres, sqlite, fjall) depend on core + store and
  implement the ports. Nothing above them knows which store is underneath.
* **eventyr-subscription** (runner, checkpoints, leases, parking) and
  **eventyr-projection** (upcasting, views, rebuilds) form the read-side
  layers; each is one machine plus its drivers and stores.
* **eventyr-shred** wraps *any* store: it is a decorator, not a peer adapter.
* **eventyr-macros** is a proc-macro crate used only through core's optional
  `macros` feature; **eventyr-store-testing** is consumed as a dev-dependency
  by every store and by shred.

Per-crate documents: [eventyr](crates/eventyr.md),
[eventyr-core](crates/eventyr-core.md), [eventyr-macros](crates/eventyr-macros.md),
[eventyr-store](crates/eventyr-store.md),
[eventyr-subscription](crates/eventyr-subscription.md),
[eventyr-projection](crates/eventyr-projection.md),
[eventyr-store-postgres](crates/eventyr-store-postgres.md),
[eventyr-store-sqlite](crates/eventyr-store-sqlite.md),
[eventyr-store-fjall](crates/eventyr-store-fjall.md),
[eventyr-store-testing](crates/eventyr-store-testing.md),
[eventyr-shred](crates/eventyr-shred.md),
[eventyr-shred-aes-gcm](crates/eventyr-shred-aes-gcm.md),
[eventyr-shred-chacha](crates/eventyr-shred-chacha.md).

## 3. The protocol vocabulary (core)

Everything a machine or port speaks is defined once, in core:

* **`StreamId`** — a stream's name. `StreamId::for_aggregate::<A>(&id)`
  produces `"{aggregate-name}-{id}"`, the convention the repository and the
  snapshot policy rely on.
* **`Version`** — per-stream position, starting at 0 (`Version::EMPTY`).
  `ExpectedVersion { Any, Exact(v), Empty }` is the optimistic-concurrency
  token; `ExpectedVersion::after(v)` gives `Empty` for an empty stream and
  `Exact(v)` otherwise, so callers don't branch.
* **`Sequence`** — the global, append-ordered log position, starting at 0
  (`Sequence::START`). Checkpoints are sequences.
* **`EventEnvelope<E>`** — the stored event: `sequence`, `stream_id`,
  `version`, the typed `event`, and `metadata`.
* **`Metadata`** — `causation_id`/`correlation_id`/`idempotency_key` (+ an
  optional `timestamp` behind the `time` feature). The **idempotency key is
  the dedupe record**: stores that see the same key on the same stream return
  `AlreadyCommitted` instead of appending twice. Keys are never inherited
  from an interaction's metadata — a replayed command must produce a fresh
  key or none.
* **`NewEvent<E>`** — an event plus its metadata, before it has a position.
* **`StoreError`** — the failure taxonomy (see §10). Protocol violations ride
  inside it as `Other(ProtocolError)`.
* **`Aggregate`** — the domain seam: `NAME`, `Id`, `State`, `Event`,
  `Command`, `Error`; `initial`, `apply` (fold), `decide` (pure decision).
  `AggregateId` is a blanket trait. `Optional` lets a state be an `Option`.
  The `#[derive(Aggregate)]` macro ([eventyr-macros](crates/eventyr-macros.md))
  wires the conventional pieces together from free functions.

The vocabulary is deliberately small: everything else in the workspace is
either a machine over this vocabulary, a port consuming it, or an adapter
implementing a port.

## 4. The five machines

All machines live in eventyr-core. Each is a sans-IO automaton with inputs,
actions, and a `Done` outcome; every driver in the workspace drives one of
them.

| Machine | Module | Protocol | Outcome type |
|---|---|---|---|
| `WriteMachine` | `core::write` | load stream (± snapshot) → decide → append, with retries and conflict translation | `WriteOutcome` |
| `BatchMachine` | `core::batch` | load several streams → decide across them → append atomically | `BatchOutcome` |
| `BoundaryMachine` | `core::boundary` | read a dynamic-consistency boundary → decide → conditional append | `BoundaryOutcome` |
| `SubscriptionMachine` | `core::subscription_machine` | fetch → apply → ack, with catch-up, backoff, shutdown, parking | `SubscriptionOutcome` |
| `SagaMachine` | `core::saga` | react to one event → dispatch commands, idempotently | `SagaOutcome` |

Shape shared by all five:

* `Machine::new(...)` builds; `start()` (or `start(input)`) returns the first
  actions; `handle(input)` advances and returns the next actions.
* Actions are returned as a `Vec` (a step may legitimately ask for several
  appends, or an apply then an ack).
* A step ending in the `Done` action is final; drivers stop looping.
* Every violation path lands in `Done(Failed(StoreError::Other(ProtocolError)))
  )` rather than a panic — the stores' contract tests push interleavings that
  would panic in a hand-written state machine.
* Where retry matters, the machine owns the *budget* (`RetryPolicy`, default
  3) and the driver owns the timing (machines never sleep by themselves; the
  subscription machine is the one that requests `Sleep{for_, reason}`, and
  only `SleepReason::Idle` may end early on a wake signal).

The five protocols in one paragraph each:

**WriteMachine** ([core doc](crates/eventyr-core.md)) drives the classic
command flow: load the stream (and optionally the newest snapshot), fold the
delta, run `decide`, and append with `ExpectedVersion::after(head)`. It
translates `Conflict{current}` into either a retry (reload only the delta,
re-decide) or `WriteOutcome::Rejected`/`Failed`; an idempotent replay (same
idempotency key already committed) comes back as `AlreadyCommitted` without
re-running `decide`. With snapshots enabled (`with_snapshots`), the machine
tracks the version the snapshot was taken at, folds only the delta, and
offers a fresh snapshot on commit when the policy is due
(`SnapshotPolicy::is_due(base, committed)`).

**BatchMachine** loads several named streams (each through its own `Fold`),
presents all loaded states to one `Decide`, and appends the resulting
`StreamAppend`s atomically — a multi-stream `decide` with all-or-nothing
semantics. `for_aggregates` lifts a single-aggregate decider over a *set* of
aggregates: the boundary is dynamic, chosen per command. Stream lists are
sorted and deduplicated up front so appends are deterministic; conflicts
attribute to a stream; an unnamed conflict attributes to the first stream.

**BoundaryMachine** is dynamic consistency boundaries ([DESIGN.md
§6](../DESIGN.md#6-projections--subscriptions-eventyr-subscription-eventyr-projection)'s
DCB): a query over event types and `Tag`s selects the relevant history of
*other* streams (tags are derived from events via `Tagged`, never stored —
they are a pure function, so they can never drift from the events). The
machine reads, folds, decides, and appends under an `AppendCondition{query,
after}`: the append is accepted only if no event matching the query landed
after `after`. A `QueryConflict{sequence}` either retries with an
acknowledged checkpoint or rejects, never silently commits.

**SubscriptionMachine** is the read-side engine: from a checkpoint, fetch a
batch (at most `batch_size`, default 128), apply each event to the
projection, ack the checkpoint, repeat; on catch-up, sleep
`SleepReason::Idle` for the idle interval (default 100 ms) unless a wake
signal short-circuits it; on failure, either halt (the default
`FailurePolicy::Halt`) or park the poison event and move on
(`FailurePolicy::Park{retries}`, recorded in a `ParkedStore`) and back off
(`SleepReason::Backoff`, default 1 s — never shortened). `Shutdown` drains
the in-flight batch, does one final re-read to catch commits racing the stop,
survives exactly one failure during shutdown, and always ends
`Stopped{checkpoint}`. Checkpoints move only after the whole batch applied,
and never regress — at-least-once delivery with idempotent projection apply
is the contract.

**SagaMachine** is deliberately tiny: for one event, a `Saga::react` produces
`(StreamId, Command)` pairs; the machine dispatches each (keyed
`"{saga}:{sequence}:{index}"` for idempotency) and finishes. Sagas are
long-running *because the log is the state*: each reaction is a fresh,
idempotent command dispatch driven by the subscription runner, not a
persisted process.

## 5. The ports (eventyr-store and friends)

Ports are the traits the actions imply. Each names a capability; stores
implement the ones they can.

**Log ports** (eventyr-store):

| Port | Capability | Notes |
|---|---|---|
| `EventStore` | `append` one stream with `ExpectedVersion`; `append_batch` many streams atomically; `stream` one stream from a version | the base port every adapter implements |
| `StreamsAll` | `stream_all` from a sequence; `stream_all_filtered` with an `EventFilter` and scan bound | the subscription read side; filtered reads are an overridable default (client-side) so stores can push them down |
| `StreamLifecycle` | `close_stream`, `truncate_before` | tombstone + head cut, [§8](#8-delivery-idempotency-and-lifecycle-semantics) |
| `QueryAppend` | `read(query, after)` and `append_if(appends, condition)` | DCB; requires `EventName + Tagged` events |

**Auxiliary ports**, each with an in-memory implementation and at least one
durable one:

| Port | Crate home | Durable impls |
|---|---|---|
| `SnapshotStore` | eventyr-store | Postgres, SQLite, fjall |
| `CommitSignal` | eventyr-store | `LocalCommitSignal` (any store), Postgres `NOTIFY` |
| `CheckpointStore` | eventyr-subscription | Postgres, SQLite |
| `ProjectorLease` | eventyr-subscription | Postgres (rows, not advisory locks) |
| `ParkedStore` | eventyr-subscription | SQLite; `NoParking` refuses |
| `SubscriptionSource` | eventyr-subscription | over any `StreamsAll` |
| `Upcaster` / registry | eventyr-projection | in-crate chain + registry |
| `ViewStore` / `InlineView` | eventyr-projection | Postgres, SQLite, in-memory |
| `Metrics` | eventyr-core (seam), eventyr-store (tracing backend) | `NoopMetrics` default |
| `Cipher` / `KeyStore` | eventyr-shred | AES-256-GCM, XChaCha20-Poly1305; SQLite key store, in-memory |

Port mechanics worth knowing (they are uniform):

* All port methods are `&self` and async via RPITIT (`-> impl Future +
  Send`), so a store is cheap to share (`Arc<PgStore<E>>`) and needs no
  lifetime gymnastics. Blanket impls delegate for `&S` and `Arc<S>`.
* Ports return `Result<_, StoreError>`; adapters map their native errors
  onto the taxonomy (§10) — notably connection-class failures become
  `Unavailable`, so callers can distinguish "retry me" from "bug".
* A port method that a store *can't* implement meaningfully is simply not
  implemented — capability discovery is via trait bounds, so a projector
  over a store without `StreamsAll` is a compile error, not a runtime one.

## 6. The write path

A command arrives; the application holds an `Aggregate` and an
`AggregateRepository<A, S, SS>`:

1. `execute(&id, command)` starts a `WriteMachine` (plain, or
   `with_snapshots` when a snapshot store is attached).
2. The async driver (`drive_write`) loops: it answers `LoadStream` from the
   store's `stream(&id, from)` — from the snapshot version when
   snapshots are on — and feeds `Loaded{events}` back. The machine validates
   stream identity and contiguity, folds the delta, and runs `decide`.
3. A decision with no events ends `Noop`. A domain error ends
   `Rejected(err)`. Otherwise the machine emits `Append{stream_id, expected,
   events}` with `ExpectedVersion::after(head)` — and the metadata the
   caller provided, including any idempotency key.
4. The store appends atomically under its concurrency control (§9):
   * success → `Appended{committed}` → the machine finishes
     `Committed{committed, snapshot}` (offering a due snapshot — the driver
     saves it fire-and-forget, newest-wins; a lost snapshot costs a fold,
     not correctness);
   * a version conflict → `Conflict{current}` → the machine retries
     (reload the delta only, re-decide) up to `RetryPolicy::max_retries`
     (default 3), then ends `Failed(StoreError::Conflict{..})`;
   * an idempotency hit → `AlreadyCommitted{committed}`;
   * a store failure → `Failed(err)`; the blocking driver
     (`drive_write_blocking`) runs the identical machine through
     `futures::executor::block_on`.

`append_batch` writes multiple streams in one store transaction (§9), which
is what makes `BatchMachine` and `BoundaryMachine` decisions atomic. Batch
loads are sequential per stream in the drivers; the atomicity that matters
is on the append side. The repository, drivers, in-memory store, and metrics
seams are documented in [crates/eventyr-store.md](crates/eventyr-store.md).

## 7. The read path

**Point reads** (`EventStore::stream`, `from: Version`) replay one stream.
Stores reject reads that start before a truncation cut with
`Truncated` and retain the stream head, so a reader that held an old position
learns the cut instead of silently skipping.

**Log reads** (`StreamsAll::stream_all`, from a `Sequence`) page through the
whole log in append order — keyset pagination (page size 512 in both Postgres
and SQLite), never `OFFSET`. The visibility rule
([DESIGN.md §6](../DESIGN.md#6-projections--subscriptions-eventyr-subscription-eventyr-projection)):
*a later sequence is never visible before an earlier one that will commit* —
multi-statement append transactions in Postgres and SQLite make whole batches
appear atomically, so a consumer can't observe a torn batch.

**Filtered reads** (`StreamsAll::stream_all_filtered`) return
`FilteredRead{events, scanned}`: the events that matched an
`EventFilter{stream_prefixes, event_types}`, plus how far the scan actually
went, bounded by the scan limit. The default implementation filters
client-side over `stream_all`; the Postgres and SQLite adapters override it
so the store does the byte-exact prefix matching (`starts_with` /
`substr` — never `LIKE`, whose escaping semantics are a trap) and the scan
bound is pushed down. A consumer acking past a scanned-but-unmatched run is
an acknowledged gap by design: the projector's checkpoint says "I have seen
everything up to N", not "everything up to N matched my filter".

**Projections** run through `Projector<S, C, P, W, K>` in
eventyr-subscription: a `SubscriptionSource` (over any `StreamsAll`) feeds
the `SubscriptionMachine`, a `Projection::apply` folds events, a
`CheckpointStore` records progress, a wake signal (`CommitSignal`) shortens
idle sleeps, and — with `FailurePolicy::Park` — a `ParkedStore` records
poison events. Leases (`ProjectorLease`, `LeasePolicy{ttl: 5s, grace: 3,
max_grace: 12}`) let exactly one projector own a name across a fleet:
renewed before every fetch and every ack; a run that loses its lease ends
`LeaseLost` with the checkpoint it reached, and the caller loops — the new
holder resumes from the store. `SkipRedelivered` deduplicates per stream by
folded version inside a run; `Fanout` fans one stream into several
projections. Full detail: [crates/eventyr-subscription.md](crates/eventyr-subscription.md).

**Views and upcasting** (eventyr-projection) layer on top:
`UpcastingSource`/`UpcasterChain` upgrade stored events to the current
schema *only on the projection read path* — aggregate loads decode the
stored shape directly, so upcasters are a projection concern, not a
write-side migration. `ViewProjection` + `ViewStore` give newest-wins
materialized views keyed by `(name, id)`; `InlineView`s fold *inside the
append transaction* (Postgres, SQLite) so a view row and its event commit
together; `RebuildPlan` re-projects a view under a schema version bump with
its own checkpoint key (`"{name}@v{N}"`). Detail:
[crates/eventyr-projection.md](crates/eventyr-projection.md).

**Sagas** run as a `SagaProjection` under the same projector machinery: each
event dispatches its reactions as ordinary (idempotent) commands. Detail in
[crates/eventyr-subscription.md](crates/eventyr-subscription.md).

## 8. Delivery, idempotency, and lifecycle semantics

These guarantees are uniform across every store, because the machines
enforce them and the contracts test them:

* **At-least-once delivery.** A projector that crashes after applying a
  batch but before its checkpoint write replays the batch. The contract is
  *idempotent apply*: the projection fold must tolerate duplicates. The
  checkpoint moves only after the whole batch applied, and it never
  regresses. Gaps in the sequence are tolerated (a `Noop`/rejected write
  never consumes a sequence), so `Checkpoint` is "seen through", not "seen
  every integer below".
* **Idempotent writes.** The idempotency key in `Metadata` is the dedupe
  record; the stream itself is where it is recorded (Postgres stores it as a
  column with the same uniqueness reasoning; every store checks it within the
  append path). A replayed command with the same key yields
  `AlreadyCommitted` with the original commit — not a second event. Machines
  skip `decide` entirely on the replay path. Saga dispatches are keyed
  `"{saga}:{sequence}:{index}"` so a redelivered reaction cannot double-fire.
* **Closed streams.** `close_stream` writes a tombstone; every append path
  checks it and answers `StoreError::StreamClosed`. Reopening is a new
  decision to make deliberately (no soft delete, no store-emitted marker
  events).
* **Truncated streams.** `truncate_before` moves a per-stream cut and keeps
  the head. Reads that would start before the cut fail
  `StoreError::Truncated{first}` rather than silently skipping; appends
  continue from the head version. Checkpointed consumers are unaffected —
  truncation is a read concern, not a rewrite.
* **No store-emitted markers.** A closed or truncated stream is state, not
  an event: replayed history stays faithful to what the domain decided.

## 9. Concurrency and ordering per store

The append path is where correctness lives, so each adapter documents its
locking; the machines rely on two invariants — *per-stream versions are
unique and gapless* and *sequences are global and append-ordered*.

**Postgres** (detail: [crates/eventyr-store-postgres.md](crates/eventyr-store-postgres.md)):

* `append_events` is a stored procedure: it takes a session-level or
  transaction-level **commit-order advisory lock** —
  `pg_advisory_xact_lock(7300160413598463541)`, wrapped as
  `eventyr_commit_order_lock()` — so sequences are handed out in one place.
  Migration 0006 introduced it; 0010 consolidated the helpers.
* Multi-stream batches take **per-stream locks first** —
  `eventyr_lock_stream(hashtext(stream_id))` — acquired in sorted stream-id
  order, then the commit-order lock, then the insert. Sorted acquisition is
  the deadlock-avoidance rule; the commit-order lock serializes only the
  tiny sequence-allocation window, not the whole transaction.
* A single-stream append is one autocommitted procedure call — unless
  inline views are attached, in which case it opens a transaction so the
  view rows commit with the events.
* `append_if` (DCB) follows the same order: stream locks → commit-order
  lock → condition scan → writes. The condition check and the writes are in
  one transaction, so no matching event can slip in between.
* Reads run in `REPEATABLE READ READ ONLY` transactions (a snapshot per
  page), which is what makes the §7 visibility rule hold without read locks.
* Live push: `NOTIFY eventyr_commits` fires inside the commit path;
  `PgCommitSignal` holds one dedicated pooled connection running `LISTEN`.

**SQLite** (detail: [crates/eventyr-store-sqlite.md](crates/eventyr-store-sqlite.md)):
one writer at a time is the engine's own contract. The adapter serializes
writes through a single `Arc<Mutex<Connection>>`; `append_within` checks
lifecycle, head, and expectation inside one `BEGIN IMMEDIATE` transaction;
multi-process use is possible but requires WAL and a `busy_timeout`
(documented in the crate, set via `from_connection`). The in-memory store is
a single mutex, so `append_batch` is trivially atomic.

**fjall** (detail: [crates/eventyr-store-fjall.md](crates/eventyr-store-fjall.md)):
single-writer LSM — one `SingleWriterTxDatabase`, transactions at
`PersistMode::SyncAll`. `write_in` is two-pass (collect writes, then commit),
and the global sequence counter lives in a `meta` keyspace. Engine
`Io`/`Locked` errors map to `Unavailable`. Single-process only by design.

## 10. The failure model

One error type crosses every boundary: `StoreError`.

| Variant | Meaning | Who produces it |
|---|---|---|
| `Conflict{stream_id, current}` | optimistic-concurrency failure | stores, on version mismatch |
| `QueryConflict{sequence}` | DCB condition failure | stores, on `append_if` |
| `StreamClosed{stream_id}` | tombstoned stream | stores, on append to closed |
| `Truncated{stream_id, first}` | read below the cut | stores, on reads before `first_kept` |
| `Unavailable` | the store can't be reached / is locked | adapters, on connection-class failures |
| `Other(Arc<dyn Error + Send + Sync>)` | anything else, including `ProtocolError` | drivers (violations), adapters (payload/corrupt rows) |

Two structural choices matter:

* **Protocol violations are not panics.** `ProtocolError(&'static str)` rides
  inside `Other`; `is_protocol_violation()` distinguishes it. A machine that
  receives an out-of-order or malformed input ends its run — in production
  that is a bug report with a machine-internal transition name in it, not a
  crash loop.
* **Adapters classify before wrapping.** Postgres maps SQLSTATE P0001 with
  the conflict hint (`"{stream}:{version}"`) to `Conflict` and the custom
  `EV001` to `StreamClosed`; everything connection-shaped becomes
  `Unavailable` so callers can back off rather than die. Adapter-specific
  error enums (`PgStoreError`, `InlineViewError`, `ShredError`, …) exist
  where the adapter needs its own taxonomy before crossing into
  `StoreError`.

Adjacent failure policies, each a deliberate choice:

* **Write retries** are optimistic only: `RetryPolicy` (default 3) re-loads
  and re-decides; there is no write-side park or dead-letter.
* **Projection failures** are either `Halt` (default — stop the projector,
  surface loudly) or `Park{retries}`: record the poison event in a
  `ParkedStore` (sealed first, under shred), move the checkpoint, keep
  going. `eventyr_parked_events_total` is the alert metric; parked events
  are visible and removable through the store. `NoParking` refuses the run
  at configuration time rather than at the first poison event.
* **Snapshots are hints.** Newest-wins save, fire-and-forget; a corrupt or
  stale snapshot costs a fold from the recorded version, never correctness.
* **Crypto-shredding failures are loud.** A `WrongAlgorithm`,
  `SubjectErased`, or malformed-JSON error ends the read; JSON errors are
  redacted to category + position (`JsonKind`) so a shredded payload never
  leaks through an error message. `Debug` for `Sensitive` never prints the
  value.

## 11. Deployment topologies

Three shapes the shipped pieces compose into; the crate docs carry the
operational caveats, the important ones are these:

* **Embedded** (SQLite or fjall): one process, everything local. Checkpoints
  can live beside the log (`SqliteCheckpointStore::beside`); the wake signal
  is in-process only. SQLite needs WAL + `busy_timeout` for multi-process;
  fjall is single-process, period.
* **Single primary Postgres**: the default multi-node shape. Appends go
  through one writable primary (the commit-order lock is a database-wide
  serial point — [DESIGN.md §15](../DESIGN.md#15-running-distributed-2026-10-and-roadmap-08)
  discusses the boundaries and the 0.8 plans); consumers (`PgCheckpointStore`,
  `PgLeaseStore`, `PgViewStore`, `PgCommitSignal`) can run on any number of
  nodes; async (streaming-) replication lets in-DB consumers rewind together.
  `LISTEN/NOTIFY` does not fire on standbys — wake signals are for nodes
  with their own primary connection, with idle-poll as the fallback.
* **Fleet of projectors**: leases make the set of runners fungible. A run
  that loses its lease (holder gone past grace, or takeover) ends
  `LeaseLost{name, checkpoint}`; the caller loops and re-acquires; the new
  holder resumes from the checkpoint store. Renewals happen before fetches
  and before acks, so a stall is detected by the store, not by a heartbeat
  guess.

The `distributed` example (`eventyr/examples/distributed.rs`) wires the full
Postgres shape across two simulated nodes, including inline views and
leases; `bank` exercises the embedded + subscription + shred path.

## 12. Features and the build matrix

Core is `no_std + alloc` with features `time`, `macros`, `serde` (all off by
default; the umbrella's default turns on `time + macros + store`). Every
other crate adds capability features that only pull their dependencies in:

* The umbrella (`eventyr`) is the composition surface: `store`,
  `subscription`, `bus`, `projection`, `postgres` (+`_snapshots`,
  `_checkpoints`, `_leases`, `_views`), `fjall`(+`_snapshots`),
  `sqlite`(+`_snapshots`, `_views`, `_checkpoints`, `_shred`, `_parked`),
  `shred`(+`_aes_gcm`, `_chacha`, `_parked`), `metrics`, and the legacy
  alias `snapshots` → `postgres_snapshots`.
* Adapter crates keep their auxiliary stores behind features
  (`snapshots`, `views`, `checkpoints`, `leases`, `parked`, `shred`) so an
  embedded deployment doesn't compile the schema for things it doesn't use.
* Nothing in the workspace requires a runtime to *compile*; runtimes appear
  only behind features (`tokio` in subscription behind `tokio_notify` /
  example needs, `sqlx` in postgres). The blocking drivers use
  `futures::executor::block_on`, not a runtime thread.

CI verifies the matrix: fmt, `clippy -D warnings`, `doc -D warnings` on
all-features; `cargo hack --each-feature --no-dev-deps` on the umbrella;
a `no_std` build for core on `thumbv7em-none-eabihf` (with `serde` and
`time` on); MSRV 1.99 check; miri on core's machine proptests; and the
Postgres-backed contract tests against a real Postgres 17 service.

## 13. Testing architecture

Testing follows the layering, and this is where the machine/driver split
pays off:

1. **Machine proptests** (eventyr-core, run under `miri` in CI): drive
   `WriteMachine`, `BatchMachine`, `SubscriptionMachine` with generated
   input interleavings. Invariants: never panics; a shutdown always ends
   `Stopped`; a checkpoint never regresses and moves only on ack; a folded
   version is always the stream head.
2. **Scripted drivers** (core's `testing` module): a driver that answers
   from a script and asserts the machine's actions, used to pin exact
   transition behavior (`scripted`, `drive_scripted`,
   `projector_scripted`, …) without any runtime.
3. **Scenario harness** (core's `testing`): `Scenario::<A>::given(..).when(..)`
   for aggregate-level BDD-style tests, plus the canonical `account`
   fixture and the `enrollment` fixture in core's boundary module.
4. **Store contracts** (eventyr-store-testing; detail:
   [crates/eventyr-store-testing.md](crates/eventyr-store-testing.md)):
   suites — `event_store_contract`, `event_store_batch_contract`,
   `streams_all_contract`, `filtered_read_contract`, `lifecycle_contract`,
   `lifecycle_query_append_contract`, `query_append_contract`,
   `commit_signal_contract`, `snapshot_contract` — that every store adapter
   runs in its own tests, in-memory, SQLite, and Postgres alike. They run
   without an async runtime (block_on), and the Postgres run is
   `#[ignore]`d by default and serialized (`--test-threads=1`) in CI.
   Auxiliary ports have analogous contracts
   (`checkpoint_store_contract`, `lease_store_contract`,
   `parked_store_contract`, `view_store_contract`, `cipher_contract`,
   `key_store_contract`).
5. **Crate-level suites**: adapters test their error mapping and
   store-specific behavior (lock ordering, pagination, truncation) beyond
   the contracts; the umbrella's `tests/bank_story.rs` mirrors the bank
   example end to end; macro crates use `trybuild` UI tests.
6. **Examples as smoke targets**: CI runs the examples (`bank`,
   `enrollment`, `loan_eligibility`, `inline_view`, `distributed`) as
   integration smoke tests, each behind exactly the features it uses.

## 14. Extension points

The seams a downstream library or application is expected to build on, in
rough order of frequency:

* **Implement `Aggregate`** (or derive it) — the primary extension. Also
  `Tagged` on events for DCB, `Saga::react` for reactions,
  `Projection::apply` and `View` for read models.
* **Implement a `Metrics` backend** — three methods, called by the drivers;
  `TracingMetrics` is the shipped reference.
* **Implement `Cipher` + `KeyStore`** — crypto-shredding with your own KMS;
  the shipped adapters are one-newtype examples (`AeadCipher` makes a new
  AEAD a ~20-line crate).
* **Wrap a store** — `ShreddingStore` is the in-tree example of the
  decorator pattern: implement the ports by delegation. The blanket
  `&S`/`Arc<S>` impls mean wrappers compose.
* **Write an adapter** — implement `EventStore` (+ whichever ports apply)
  for a new backend and run the contract suites from
  eventyr-store-testing. SQLite is the reference port for what "complete"
  means ([CONTRIBUTING.md](../CONTRIBUTING.md)).
* **Drive a machine yourself** — all five machines are public; an exotic
  runtime can drive them directly (the scripted drivers in core's `testing`
  module are the worked example).

## 15. Known limits and the roadmap boundary

As-built boundaries that a reader should not cross expecting more, each
with its [DESIGN.md roadmap](../DESIGN.md#12-roadmap) entry:

* **Checkpoints are last-write-wins**, not CAS — two projectors on one name
  can overwrite each other (leases are the guard; CAS checkpoints are
  0.8.1).
* **Per-store lock identity**: the commit-order lock and channel names are
  database-wide in Postgres — two eventyr schemas in one database contend
  and cross-notify (per-store identity is 0.8.2).
* **Parked events are in-process by default** — the Postgres parked store is
  planned (0.8.6); today SQLite is the durable option and the in-memory one
  dies with the process.
* **Slices and sharding are 0.9/0.10**: today the unit of scale-out is the
  lease-named projector, not a partitioned log.
* **RYW tokens (0.8.3), saga keys on event ids, partitioned-projection
  supersession** — planned changes, documented in DESIGN.md §15, not yet
  in the code.

Where this document and DESIGN.md disagree, DESIGN.md wins and this file
has a bug in it.
