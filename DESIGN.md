# Eventyr — Design

**Event sourcing for Rust.** Pure machines, thin drivers — a library of small, composable traits, not a framework.

*Eventyr* is Danish/Norwegian for *adventure, fairy tale*. The name is free on crates.io as of 2026-10-03.

---

## 1. Design goals

1. **Trait-first, framework-last.** The core is a handful of small traits (`Aggregate`, `EventStore`, `Projection`, `Subscription`). Users can adopt the write side without the read side, and implement a store without adopting the aggregate pattern. This is the lesson of `eventually` (trait-first, well-liked) vs `esrs` (opinionated, Prima-specific) and `thalo` (runtime lock-in: wasm + sled).
2. **Pure domain, I/O at the edges.** `decide` and `apply` are pure functions. All I/O lives in stores, subscriptions, and the repository shell. `katha` proves this shape works beautifully in Rust; `eventcore` proves it scales to multi-stream commands.
3. **Sans-IO machines for multi-step logic.** Anything that is more than one step — the load→decide→append→retry write path, projection checkpointing — is a *pure state machine*: it consumes results and emits actions; a driver performs the I/O. This is the discipline of sans-IO protocol stacks (`quinn-proto`, `str0m`, Python's `h11`/`h2`), applied to event sourcing. Concurrency logic becomes testable without a database and runnable under any runtime.
4. **Async at the rim only.** Machines and domain traits are sync-pure; async lives in store traits and drivers via `async fn` in trait (stable since 1.75) — no `#[async_trait]` allocation, no `Pin<Box<...>>` signatures like `eventually`'s old API. The same machines run under tokio, blocking code, or a deterministic test harness.
5. **Concrete enums, no `dyn Event`.** Events are plain Rust enums — exhaustive `match`, compile-time exhaustiveness, no downcasting. `mnesis`'s core pitch, and it fits Eventyr's identity.
6. **Zero required dependencies in the core crate.** `serde` is optional (feature-gated); the core traits don't need it. Stores that serialize pull it in themselves. The machine core is `no_std + alloc` — it needs no runtime at all; its single optional feature is `time` (the envelope timestamp, off in core, on in the umbrella).
7. **Derive macros are sugar, not the API.** `#[derive(Aggregate)]` can generate the event enum + serde glue (like `sourcery`/`thalo`), but everything the macro generates is writable by hand. No `export_aggregate!`-style runtime coupling.
8. **Postgres-first persistence.** The flagship store is Postgres (like `esrs`'s `PgStore`, `eventually-postgres`), with an in-memory store for tests. Sled/fjall embedded stores are community/feature-gated additions.
9. **Versioning is a first-class concern.** Optimistic concurrency (`ExpectedVersion`), event upcasting, and deprecation are designed in from day one — the things that separate a demo from a production library (esrs's `Schema`/`Persistable`, cqrs-es's upcasters).

## 2. Non-goals

- Not a runtime or an actor framework (no wasm, no embedded database as the center of the world — that's `thalo`'s identity).
- No HTTP/transport layer, no message-bus integrations baked into core (esrs ships rabbit/kafka buses; Eventyr keeps `EventBus` as a trait users implement).
- No ORM, no code generation beyond optional derive macros.
- Not a full DDD toolkit (no value-object/entity macros — that's `eventide`'s hexagonal bet).
- No command bus, query bus, or mediator. Routing a command is a function call in the caller's code; routing it to one node is the caller's load balancer or framework (§15.1).
- No state-stored aggregates. Every read-side feature (projections, sagas, upcasting, shredding, parking, leases) consumes the event log, and a state-stored aggregate has none (§15.1).

## 3. Crate layout

```
eventyr/                    # umbrella: re-exports core + prelude
├── eventyr-core/           # machines + protocol vocabulary; no_std + alloc; zero deps
├── eventyr-store/          # async EventStore/StreamsAll traits, in-memory impl, drivers, repository
├── eventyr-macros/         # #[derive(Aggregate)] etc. (proc-macro crate)
├── eventyr-store-postgres/ # sqlx-based store, migrations
├── eventyr-projection/     # read path: upcaster chains, raw→typed sources, schema-versioned rebuilds
└── eventyr-subscription/   # catch-up subscriptions, event bus trait
```

**Placement rule.** A type lives in `eventyr-core` if and only if a machine transitions on it: the domain traits (`Aggregate`), the protocol vocabulary (`StreamId`, `Version`, `Sequence`, `ExpectedVersion`, `StoreError`, `NewEvent`, `EventEnvelope`, `Metadata`), and the machines themselves. Everything that performs or abstracts I/O — `EventStore`, `StreamsAll`, `Projection`, `Subscription`, the drivers, the repository — lives in the store-side crates. Store-side crates depend on core; core depends on nothing. This is what keeps the core `no_std + alloc` with zero dependencies: machines reference only vocabulary types.

`Metadata.timestamp` is the one concession: `Option<OffsetDateTime>` exists only behind `eventyr-core`'s `time` feature (off in core, on in the umbrella) — without it the field is absent and timestamps travel as opaque metadata.

Feature flags on the umbrella crate mirror the crates: `macros` (default), `store` (default), `postgres`, `snapshots` (the Postgres snapshot store), `projection`, `subscription`. This is the `eventcore`/`eventide` workspace pattern — it keeps the core dependency-free and lets users pay only for what they use.

## 4. Core: domain & protocol vocabulary (eventyr-core)

### 4.1 Aggregate — the functional core

The aggregate is *two pure functions* plus identity. State is rebuilt by folding; commands are decided against state. This is the `katha`/`eventcore` shape, with `eventually`'s `Optional` trick for "not yet created" states:

```rust
pub trait Aggregate {
    /// Stable aggregate type name — used to build stream ids and store tables.
    const NAME: &'static str; // e.g. "bank_account"

    type Id: AggregateId;
    type State;
    type Event;
    type Command;
    type Error;

    /// The state before any event. `Option<T>`-style aggregates return `None`.
    fn initial(id: &Self::Id) -> Self::State;

    /// Pure: fold one event into state. Must never fail.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// Pure: decide which events a command produces. May reject.
    fn decide(state: &Self::State, command: &Self::Command)
        -> Result<Vec<Self::Event>, Self::Error>;
}
```

Design notes:

- **`apply` takes `&mut State` and can't fail.** Events are facts; applying a fact is total. If an event can't be applied, the model is wrong and the process should stop loudly. (cqrs-es and katha agree; esrs's fallible apply invites silent corruption.)
- **`decide` is `&self`-free and pure.** No `&self` receiver means the aggregate *type* is just a namespace for behavior — no actor, no dependencies smuggled in. Dependencies (clocks, catalogs) belong in the `Command` or a `Context` parameter added by the repository layer, not in the trait. This keeps domain logic unit-testable with zero mocking.
- **`Optional` adapter** (`eventyr::Optional`): for aggregates whose initial state is "doesn't exist", `State = Option<T>` is so common that we ship a blanket adapter, like `eventually`'s `Optional`/`AsAggregate`.
- **`initial`, not `Default`.** The trait constructs its own starting state — given the id, for aggregates that embed it — so states without a meaningful `Default` don't fight the trait. The repository folds from `A::initial`.
- **An id a payload needs lives in the state.** `decide(state, command)` never sees the id, deliberately — no dependencies smuggled in — so an event payload that must carry it (a payload-derived `Tagged` event is the common case, 0.7.1) gets it from state: give the state an `id` field and start it with the derive's `initial = MyState::new(id)`. The bank example's `Deposited { account, .. }` works exactly this way.

### 4.2 Identity, versions & store errors

Newtypes for the three position concepts — one representation everywhere (trait signatures, machines, SQL):

```rust
pub struct StreamId(String); // e.g. "bank_account-<uuid>"
pub struct Version(u64);    // 1-based position within a stream
pub struct Sequence(u64);   // global, store-assigned, monotonically increasing (gaps allowed: identity columns burn values on rolled-back appends)

pub trait AggregateId: Clone + Eq + std::hash::Hash + std::fmt::Debug {}

impl StreamId {
    /// `"{NAME}-{id}"` — the single place the id→stream mapping exists.
    pub fn for_aggregate<A: Aggregate>(id: &A::Id) -> Self;
}
```

`StreamId::for_aggregate` is the only place that knows the mapping: the write machine calls it once at `start`, stores just persist the string. (esrs's `const NAME`, made load-bearing.)

The store error is also core vocabulary — the machine protocol must distinguish failure kinds, so the port speaks one error language:

```rust
/// The failure kinds the protocols distinguish. Store implementations map
/// their concrete errors into these at the port boundary; `Other` carries
/// the source along for diagnostics (`core::error::Error` is no_std since 1.81).
pub enum StoreError {
    /// Optimistic-concurrency violation; the write machine may retry.
    Conflict { current: Version },
    /// Transient failure; retryable per policy.
    Unavailable,
    /// Anything else: abort.
    Other(Box<dyn core::error::Error + Send + Sync>),
}
```

### 4.3 Event envelope & versioning

```rust
pub struct EventEnvelope<E> {
    pub sequence: Sequence,   // global position, store-assigned
    pub stream_id: StreamId,
    pub version: Version,     // position within the stream
    pub event: E,             // the domain event itself
    pub metadata: Metadata,
}

pub struct Metadata {
    pub causation_id: Option<String>,
    pub correlation_id: Option<String>,
    #[cfg(feature = "time")] // off in core, on in the umbrella
    pub timestamp: Option<OffsetDateTime>,
}
```

- The envelope separates **storage concerns** (ids, versions, metadata) from the **domain event** (sourcery's "IDs and metadata live in an envelope, not payloads" — right call; it keeps domain enums clean and serde payloads stable).
- `version` is 1-based within the stream; `ExpectedVersion` guards appends:

```rust
pub enum ExpectedVersion {
    Any,             // no optimistic locking
    Exact(Version),  // conflict if stream version != this
    Empty,           // stream must not exist yet
}
```

### 4.4 Upcasting

Upcasting is a chain of pure transformers applied on read, cqrs-es style. An upcaster that cannot transform a payload it recognizes must fail loudly — a silently dropped historical event is data loss:

```rust
/// Transforms one historical event shape into the current one.
/// The chain selects upcasters by event type; a selected upcaster that
/// cannot parse its payload is an `Err` — never a silent drop.
pub trait Upcaster<E>: Send + Sync {
    fn upcast(&self, raw: RawEvent) -> Result<E, UpcastError>;
}
```

### 4.5 The write machine — sans-IO core of the repository

The write path is *not* a pure function: load → fold → decide → append can *conflict* and retry. Modeling it as a pure function (or burying the retry loop in an async method) is where concurrency bugs live. Instead, following the sans-IO discipline, the write path is a **pure state machine** that consumes results and emits actions — the driver performs the I/O:

```rust
/// What the machine wants the driver to do.
pub enum WriteAction<E, Err> {
    /// Read the stream (from `from`, exclusive) to rebuild state.
    LoadStream { stream_id: StreamId, from: Version },
    /// Append events, guarded by the expected version.
    Append { stream_id: StreamId, expected: ExpectedVersion, events: Vec<NewEvent<E>> },
    /// Terminal: the interaction's outcome.
    Done(WriteOutcome<E, Err>),
}

/// What the driver reports back to the machine.
pub enum WriteInput<E> {
    Loaded { events: Vec<EventEnvelope<E>> },
    Appended { committed: Vec<EventEnvelope<E>> },
    Conflict { current: Version },
    Failed(StoreError),
}

pub struct WriteMachine<A: Aggregate> { /* state: phase, folded state, retries */ }

impl<A: Aggregate> WriteMachine<A> {
    /// Begin: returns the first action (always `LoadStream`).
    pub fn start(&mut self) -> WriteAction<A::Event, A::Error>;
    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: WriteInput<A::Event>) -> WriteAction<A::Event, A::Error>;
}
```

The machine's transitions are exactly the repository protocol: on `Loaded` → fold with `A::apply` → run `A::decide` → emit `Append`; on `Appended` → emit `Done(Ok)`; on `Conflict` → if retries remain, emit `LoadStream` again (re-fold, re-decide against fresh state), else `Done(Err(Conflict))`.

The separation of concerns is the point: **the aggregate decides *what* (domain policy); the machine decides *how* (interaction protocol); the driver performs it (I/O).** Retry policy, conflict handling, and idempotency are all machine state — inspectable, testable, and identical under every runtime.

## 5. Store traits & drivers (eventyr-store)

### 5.1 EventStore & StreamsAll — the persistence port

```rust
pub trait EventStore {
    type Event: Send;

    /// Append is transactional: all events or none.
    fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<Self::Event>>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<Self::Event>>, StoreError>> + Send;

    /// Stream events of one aggregate, from `from` (exclusive) onward.
    /// The method is synchronous (it constructs the stream); the store
    /// does its I/O as the stream is polled.
    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send;
}

/// Global ordered stream — the projection/subscription backbone.
/// A store that can't provide it can still implement `EventStore`
/// (aggregate persistence); subscriptions require `StreamsAll`.
pub trait StreamsAll: EventStore {
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send;
}
```

- **`&self`, not `&mut self`.** Stores are shared (`Arc`, connection pools) and serve concurrent appends to different streams; internal synchronization is the store's concern. This also keeps the repository's `execute(&self)` honest.
- **`append` is transactional**: all events or none (eventually's contract, esrs's `persist`).
- **`ExpectedVersion` on append** gives optimistic concurrency; stores return `StoreError::Conflict` on mismatch. The machine retries `decide` on conflict (bounded retries, configurable).
- **The `StreamsAll` split keeps honesty**: aggregate persistence needs only `EventStore`; projections and subscriptions require `StreamsAll`.
- **No `remove`/`delete` in the core trait.** Event streams are the system of record; hard-deleting events is a GDPR/ops concern handled by a separate `StreamPruner` (esrs has `delete`; we deliberately demote it — deleting history is the exception, not the store's job).

### 5.2 Drivers — the imperative shell

A driver is a boring loop: perform the action, feed the result back. 0.1 ships two over the same machines; the blocking driver arrives with the embedded stores (0.4):

```rust
/// Async driver over any `EventStore` (tokio, smol, anything).
/// Every store error — load or append — travels through the machine as
/// `Failed`, so `WriteOutcome::Failed` is the one failure shape callers
/// see: the machine owns the protocol, not the driver.
pub async fn drive_write<A, S>(
    machine: &mut WriteMachine<A>,
    store: &S,
) -> WriteOutcome<A::Event, A::Error>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>;

/// Test driver: scripted inputs, recorded actions — no store at all.
/// Returns every action the machine emitted, ending with `Done(outcome)`.
/// Pure, so it lives in `eventyr-core::testing`, not the store crate.
pub fn drive_scripted<A>(
    machine: &mut WriteMachine<A>,
    script: impl IntoIterator<Item = WriteInput<A::Event>>,
) -> Vec<WriteAction<A::Event, A::Error>>;

/// Blocking driver for CLI/embedded use — 0.4, with the embedded stores.
pub fn drive_write_blocking<A, S>(...);
```

### 5.3 Repository — the ergonomic entry point

The repository remains the method most users touch — now a thin wrapper over the machine and driver:

```rust
pub enum ExecutionError<A: Aggregate> {
    /// `decide` rejected the command.
    Domain(A::Error),
    /// The store failed: conflict after exhausting retries, or a fatal error.
    Store(StoreError),
}

impl<A, S> AggregateRepository<A, S>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    /// Load → fold → decide → append, with optimistic-concurrency retry.
    pub async fn execute(
        &self,
        id: A::Id,
        command: A::Command,
    ) -> Result<ExecutionOutcome<A::Event>, ExecutionError<A>> {
        let mut machine = WriteMachine::new(id, command, self.retry_policy);
        drive_write(&mut machine, &self.store).await
    }
}
```

This is `katha`'s `make_handler` / `sourcerer`'s `GenericRepository` / `eventcore`'s executor — all converged on the same protocol — but with the protocol *extracted* into a testable machine instead of an async loop. `load(id)` / `load_at(id, version)` and (with `time`) `load_until(id, timestamp)` are the read half (0.7.8): one fold, no machine, snapshot-seeded on a `with_snapshots` repository (`load_with_snapshots` / `load_at_with_snapshots`).

## 6. Projections & subscriptions (eventyr-subscription, eventyr-projection)

```rust
pub trait Projection {
    type Event;
    type Error;

    /// Left-fold one event into the read model. At-least-once; must be idempotent.
    async fn apply(&mut self, event: &EventEnvelope<Self::Event>) -> Result<(), Self::Error>;
}

pub trait Subscription {
    type Event;
    type Error;

    /// Poll for events after the checkpoint. Returns when caught up or batch drained.
    async fn poll(&mut self, checkpoint: Checkpoint) -> Result<Batch, Self::Error>;
    async fn ack(&mut self, checkpoint: Checkpoint) -> Result<(), Self::Error>;
}
```

- **At-least-once delivery, idempotent apply** — the only honest contract (thalo's guarantee, EventStoreDB's model). The help for *being* idempotent is `SkipRedelivered` (0.7.7's companion): a `Projection` wrapper that records the newest folded position per stream and skips anything redelivered at or below it, so the batch retry in 0.7.7 cannot re-count the events before a poison one. The wrapper dedupes within a run; the checkpoint dedupes between runs; a rebuild from the start needs neither.
- **Checkpointed catch-up subscriptions** over any `StreamsAll` store (eventually's `Subscription::checkpoint/resume`): the projector runner persists the last-acked global sequence, so restarts resume without reprocessing.
- **The projector is also a machine** — `ProjectorMachine`, per the canonical table in §7; the runner is just its driver. This is where sans-IO pays off most: at-least-once semantics, redelivery after failure, and resume-from-checkpoint are exactly the kind of multi-step, failure-prone protocol that should never be an untestable async loop. The driver additionally owns a fourth port, the `ProjectorLease` (0.7.9): one driver per checkpoint name, renewed on the batch boundary, so two configured projectors against the same name never corrupt the read model. The implemented machine (`SubscriptionMachine` in `eventyr-core`, named for its §6-facing role) adds the table's *implied* steps as explicit actions: `Fetch` is the poll §6's `Subscription::poll` implies (`Batch` must be requested), and `Slept` is the driver's answer to `Sleep` — no clock in the machine. That delta between the table's four names and the implemented protocol is intentional.
- **No built-in consumer loop** — the runner is a plain `tokio` task users spawn; Eventyr ships a `Projector` helper but doesn't own the event loop (mnesis's "the loop is the consumer's" — right for a library).

## 7. Machine modeling rules (sans-IO discipline)

**Terminology.** "Machine" is shorthand for *state machine* — the word the Rust sans-IO tradition itself uses (str0m: "an enormous state machine"; quinn-proto: "state machine mutators"; Firezone: "pure state machines"). Formally the pattern descends from the Mealy machine: output is a function of (state, input), and input causes a transition. It is *not* a statechart (no declarative transition-table DSL, à la `statig`/XState) and *not* a saga/process-manager (machines are ephemeral and per-interaction, never themselves persisted). The Python sans-IO canon calls the same pattern a *protocol implementation* — same idea, different headline word.

Every multi-step interaction in Eventyr follows the same four rules, taken from how sans-IO protocol stacks (`quinn-proto`, `str0m`, `h11`/`h2`) are built:

1. **A machine is a struct with a `handle`-style transition.** It consumes an input (a driver result) and emits an action (an I/O request). No clocks, no randomness, no I/O inside — anything environmental enters as input data.
2. **Actions are data, not calls.** `WriteAction::Append { ... }` is an enum variant the driver interprets. This makes protocols serializable, loggable, and replayable — you can record a session's actions and replay it deterministically.
3. **One machine per protocol, one driver per runtime.** The write path, the projector, and (later) the subscription are separate machines. Drivers are interchangeable: async, blocking, or scripted. A bug in the retry policy reproduces in a unit test with a `Vec` of inputs — no database, no flakiness, no `#[tokio::test]`.
4. **Machines never panic on bad input; they emit `Done(Err(...))`.** A driver bug or a store misbehaving is a protocol event, not an exception. (Machines may `debug_assert!` on truly impossible states.)

Where machines live in Eventyr (this table is the single source for each machine's protocol):

| Machine | Input | Output (actions) | Protocol |
|---|---|---|---|
| `WriteMachine` | `Loaded`/`SnapshotLoaded`/`Appended`/`Conflict`/`Failed` | `LoadStream`/`LoadSnapshot`/`Append`/`Done` | load→fold→decide→append, conflict retry; with `with_snapshots`, snapshot-load then delta-load, monotonicity-guarded, and a fire-and-forget snapshot offer on `Committed`; a keyed command whose earlier commit is in the stream ends `AlreadyCommitted` |
| `BatchMachine` | `Loaded` (one per stream)/`Appended`/`Conflict`/`Failed` | `LoadStreams`/`AppendBatch`/`Done` | load the fixed, sorted boundary → fold each stream → `Decide` against every fold, routing each event to a stream (`RoutedDecision`) → one atomic append, each stream guarded by its own version; a conflict re-reads only the stream it names (an unnamed one is attributed to the first stream and checked) and re-decides |
| `BoundaryMachine` | `Read`/`Appended`/`Conflict`/`Failed` | `Read`/`Append`/`Done` | read the decision's query → fold → decide (`RoutedDecision`) → append under an `AppendCondition` (nothing matching the validation query after the position read); a conflict re-reads the delta and re-decides |
| `ProjectorMachine` (`SubscriptionMachine`) | `Fetched`/`Applied`/`ApplyFailed`/`Parked`/`ParkFailed`/`Acked`/`AckFailed`/`Slept`/`Failed`/`Shutdown` | `Fetch`/`Apply`/`Park`/`Ack`/`Sleep`/`Done` | at-least-once apply, checkpoint, resume; a failed apply, park, or ack backs off and redelivers from the last ack; under `FailurePolicy::Park` an event rejected past its budget is parked and skipped; `Shutdown` finishes and acks the in-flight batch, survives a failure (one closing re-read), and ends `Stopped` at the last ack |
| `SagaMachine` | `Dispatched`/`DispatchFailed` | `Dispatch`/`Done` | per event: `start` runs the pure `react`, then one dispatch per `(target stream, command)` in order; checkpoint and redelivery stay with the subscription it runs inside (`SagaProjection`) |

Snapshots deliberately did **not** become their own machine: the §7 table's planned `SnapshotMachine` was folded into `WriteMachine::with_snapshots` as an opt-in preload, because the write protocol (load→decide→append→retry) is the same interaction either way — snapshot loading is just an initial skip-ahead in the same fold, and snapshot saving is a fire-and-forget offer on the commit outcome, not a new machine phase.

Formally, the aggregate is itself a single-step state machine — `apply` *is* its transition function, `decide` its output function. Eventyr reserves the word *machine* for multi-step, driver-facing **interaction protocols** around the domain: many round-trips with the outside world, not one pure step. The distinction is step count, not formal kind.

## 8. Derive macros (eventyr-macros)

`#[derive(Aggregate)]` sits on the aggregate *marker* struct (a unit struct — the aggregate type is a namespace, not a state holder) and wires the `Aggregate` impl by convention: `NAME` is the snake_cased struct name, `Id`/`Event`/`Command`/`Error` follow the `{Ident}...` position convention, `State` is `Self` (point `state` at a type for the marker-plus-state shape), `initial` is `Default::default()` — or `Self` when a unit struct is its own state — and `apply`/`decide` delegate to same-module free functions. Every convention is overridable with `#[eventyr(...)]` attributes (`name`, `id`, `state`, `event`, `event_enum`, `command`, `error`, `initial`, `apply`, `decide`, `crate`); `crate` retargets the generated code at the umbrella crate (the serde `crate = "..."` pattern), and `initial` sees the id as `id`. The generated enum takes `event_derive(Serialize, ...)` and `event_attr("#[serde(...)]")` for the codec attributes a store or shred pipeline needs — it stays plain Rust, so all of this is writable by hand.

`#[eventyr(events(Opened, Deposited))]` generates the event enum from payload structs (the sourcery pattern): one newtype variant per payload, a `From<Payload>` conversion for building events in `decide`, and an `EventName` impl naming each variant after its payload. Name it with `event_enum = BankEvent` (default: `{Ident}Event`); the `Event` type follows the enum. The reverse conversion is a `match` — the point of a concrete enum.

Everything is hand-writable; the macro is sugar. `#[derive(EventName)]` gives stable event type names for storage without reflection — pin historical names with `#[eventyr(name = "...")]` on the variant (or on the struct, for the single-payload form). Diagnostics are trybuild-verified: unknown or duplicate attributes, non-unit structs, and non-path `decide` all fail with one clear error, not a wall of follow-ons.

## 9. Postgres store sketch

Single-table, stream-scoped optimistic locking — the standard, boring, correct design (esrs, eventually-postgres, eventcore-postgres all converge here):

```sql
CREATE TABLE events (
    global_sequence  BIGSERIAL PRIMARY KEY,
    stream_id       TEXT        NOT NULL,
    stream_version  BIGINT      NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    metadata        JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, stream_version)
);
```

The `UNIQUE` constraint's backing index serves the per-stream lookups; no separate index is needed. Append = one transaction: `SELECT ... FOR UPDATE` (or `INSERT ... ON CONFLICT` + version check) then batch insert. `stream_all` = keyset pagination on `global_sequence`. The store serializes via `serde` + `serde_json` (feature-gated in the store crate, never in core).

## 10. Testing story

- **Pure core = table-driven tests.** `decide`/`apply` test with plain asserts, no mocks, no async, no store. Given/when/then helpers in `eventyr::testing`.
- **Machines = transition tests.** Each machine gets exhaustive transition tests: a `Vec` of inputs → expected action sequence (what `drive_scripted` returns). Conflict-retry, at-least-once redelivery, and checkpoint resume are all tested as pure data — the tests that are hardest to write against a real store become trivial. This is the sans-IO payoff: the concurrency-critical 10% of the library gets the strongest verification, not the weakest.
- **In-memory store** doubles as the acceptance-test store; contract tests (a `StoreContract` test-suite trait, eventcore-testing style) that any third-party store must pass to claim compatibility.
- **`TestScenario`** (0.2, shipped): `Scenario::given(id, events).when(command).then_events(events)` in `eventyr_core::testing`, re-exported through the prelude — pure domain tests with labeled assertion failures, borrowed from eventcore's testing crate.
- **Verification discipline** (mnesis's bar): proptest for machine invariants (e.g. "a machine that receives `Appended` always emits `Done`", "checkpoint never regresses"), `miri` in CI over the core's unit tests (scoped to what a CI job can run in minutes), `trybuild` for macro diagnostics.

## 11. What we take from each library

| Library | Take |
|---|---|
| eventually | Trait-first modularity; `Optional` aggregate adapter; `Expected`/`Select` versioning discipline |
| thalo | Derive-macro ergonomics (`Command`/`Event` derives, `events![]`); at-least-once projection guarantee |
| cqrs-es | Upcaster chains; snapshot-store mode as an option, not the default |
| esrs | Postgres-first pragmatism; `Schema` decoupling of domain events from storage; `const NAME` made load-bearing |
| eventcore | Multi-stream commands as a *later* extension (dynamic consistency boundaries); `TestScenario` testing API |
| katha | Pure-function aggregate (`init`/`apply`/`execute`); "I/O at the edges" philosophy |
| sans-IO (`quinn-proto`, `str0m`, `h11`/`h2`) | Machine/driver split for every multi-step protocol; actions-as-data; deterministic replay testing |
| mnesis | Concrete enums, no `dyn Event`; pluggable codecs; verification discipline (proptest/miri) |
| sourcery | Envelope-carried metadata; `#[derive(Aggregate)]` generating the event enum |
| kameo_es | Command-as-struct (`Command<C>` per-command impls) as an alternative style the derive can generate |
| eventide | Workspace layering (domain/application/infra crates); upcasting chain as a first-class concept |

## 12. Roadmap

- **0.1** — shipped. Core traits + protocol vocabulary, `WriteMachine` with transition tests, in-memory store, async + scripted drivers, repository wrapper, derive macros, and the `Optional` state adapter (`State = Option<T>` for "does not exist yet" aggregates, §4.1).
- **0.2** — partially shipped: Postgres store + migrations (`eventyr-store-postgres`), upcasters (upcast vocabulary in core, chains and raw→typed sources in `eventyr-projection`), checkpointed subscriptions (`SubscriptionMachine`, the projector runner). Also shipped: `TestScenario` (`Scenario`/`Outcome` in `eventyr_core::testing`).
- **0.3** — partially shipped: the projection read path (upcaster chains, schema-versioned rebuilds), `EventBus` trait (behind `eventyr-subscription`'s `bus` feature). Also shipped: snapshot support, opt-in per repository — `SnapshotPolicy`/`WritePolicy` and `WriteMachine::with_snapshots` in core (one `LoadSnapshot` action, delta `LoadStream`, the snapshot-version monotonicity guard in the fold, and the fire-and-forget `OfferSnapshot` on `WriteOutcome::Committed`), `SnapshotStore` + `InMemorySnapshotStore` and `AggregateRepository::with_snapshots` in `eventyr-store`, the `snapshots` table/persistence in `eventyr-store-postgres` behind its `snapshots` feature, mirrored by the umbrella's `snapshots` feature. Single-feed fan-out to N projections on one checkpoint, if ever built, is a combinator over the existing `SubscriptionMachine` (`Fanout` multiplexing the one `Apply` action to N idempotent members) — never a new machine, and deferred until a "must advance together" use case justifies it.
- **0.4** — shipped: the blocking write driver (`drive_write_blocking` / `drive_write_with_snapshots_blocking` in `eventyr-store`, `drive_projector_blocking` in `eventyr-subscription`), the contract-test crate (`eventyr-store-testing`: `event_store_contract`, `streams_all_contract`, `snapshot_contract`, `event_store_batch_contract` — the eventcore-testing idea as a callable suite, self-tested against the in-memory store and wired to gate the Postgres and fjall stores), the embedded store (`eventyr-store-fjall`: fjall-backed `EventStore`/`StreamsAll`/`SnapshotStore`, serde in the store, no runtime needed to drive), and multi-stream commands (eventcore-style, below).

- **0.5** — **shipped**: no new subsystem, four width pieces — (1) the upcaster registry (`eventyr_core::version_registry`), (2) metadata/correlation made load-bearing at the driver boundary (`with_metadata` on both machines; `execute_with_metadata` on the repository), (3) the `Metrics` port + `tracing` instrumentation behind the `metrics` feature, and (4) **the worked example**: `eventyr/examples/bank.rs` — one domain touching every shipped feature (a derived `Account` aggregate with `Optional` state and payload-shaped events, two metadata-carrying opens, a cross-account transfer on the batch machine, a `Projection` ledger rebuilt off the stream, and the `AmountV1` → `Deposited` rename upcast end to end), runnable as a binary and gated as a test. See below.
- **0.6** — shipped: sagas, views, SQLite; see §13. **0.7** — in progress (0.7.1–0.7.9 shipped), planned: dynamic consistency boundaries, live push, inline views, erasure, and operational hardening; see §14.
- **0.8** — planned: running Eventyr across many processes and stores. Checkpoints a stale projector cannot overwrite, a commit-order lock scoped to one store, read-your-writes tokens, event ids and the saga keys built on them, rewind detection, and the Postgres deployment contract; hybrid logical clocks evaluated for order across stores. See §15 and §16.
- **0.9** — planned, breaking: slices. Order per slice instead of per store, writers concurrent across slices, projectors leasing slices, and DCB conditions scoped by tag. One slice behaves exactly as 0.8. See §16.
- **0.10** — planned: `ShardedStore` (writers beyond one database, sharded on a tag) and HLC-merged projections. See §16.

### 0.4 multi-stream commands — the shape

Multi-stream commands deliberately come *last*: they complicate the mental model, and Eventyr's identity is "small, composable, boring in the good way". The single-stream core shipped first; the batch extension is a *combinator over* it, not a new runtime.

A command's **consistency boundary** — the set of streams it reads and writes atomically — is fixed per interaction by the *caller*, not computed by the store. Eventcore's `StreamResolver` (deducing the boundary from the command) is deliberately *not* a machine concept: the caller resolves the command to its stream set and hands it to the machine. That keeps the protocol small and total, and dynamic boundaries a thin wrapper over a proven core rather than a second machine.

- **`eventyr-core::batch`** — the sans-IO `BatchMachine`: `LoadStreams` (the fixed boundary, deduplicated and sorted) → fold each stream through its own per-stream `Fold` (`AggregateFold` bridges an `Aggregate`; there is no shared state) → `Decide::decide` reads *every* folded state and returns a `BatchDecision` (validation + routing: `of([(stream, event), ..])`, `to(stream, events)`, `reject`, `noop`) → one atomic `AppendBatch` of per-stream, per-version-guarded `StreamAppend`s. A conflict re-reads only the stream that moved and re-decides. Driven by `drive_write_batch` / `drive_write_batch_blocking` (async and blocking, one protocol each).
- **The port** — `EventStore::append_batch`: every append or none, each guarded by its own `ExpectedVersion`. Stores that cannot commit atomically across streams use the `append_batch_fallback` helper, which handles the degenerate cases (empty / single-stream) and fails the rest with an honest `Unsupported`-style error. The in-memory store (one lock), the fjall store (one write transaction), and Postgres (one transaction, per-stream advisory locks taken in sorted order — no deadlock) all implement it for real; the `event_store_batch_contract` gates them.

Cross-stream invariants live in the `Decide` (it sees every folded state); routing lives in the `BatchDecision` (each event names its target stream). The boundary being fixed at construction is what keeps both total.

### 0.5 — the hardening release

No new subsystem; four width pieces, in order.

### 0.5.1 Upcaster registry — versioning made real on the read path

0.2 shipped upcast *vocabulary* (`RawEvent`, `Upcaster`) and chains (`eventyr-projection::chain`), and 0.3 shipped the read path over them — but nothing yet *runs* an upcaster against a stored event at read time, and nothing *verifies* a chain is well-formed at startup. 0.5 closes that loop: an **upcaster registry** — the set of upcasters a store/projection knows, indexed by `(event_type, schema_version)` — that both Postgres reads and projection sources consult, and that fails loudly at startup on a broken chain (a gap in versions, a cycle, a name collision). cqrs-es calls these upcasters; esrs calls the whole seam `Schema`. Eventyr keeps §4.4's discipline: upcasting is *data transformation toward the current schema*, decided per event type, never a code migration of stored payloads.

The registry is a plain value, not a machine (one lookup per event is one step — §7 keeps machines for multi-step protocols). It is honest where the chain can't be: `OrphanedType` (a stored event no upcaster selects), `DanglingVersion` (a v2 upcaster with no v1 base), `AmbiguousTarget` (two upcasters writing the same current type). All surface at registration as errors, never as silently-dropped events at read.

### 0.5.2 Metadata & correlation — the observability seam

§4.2/§4.3 carry `Metadata { causation_id, correlation_id, timestamp }` end to end, but nothing *produces* them: today the caller grafts ids onto `NewEvent`s by hand. 0.5 adds the read-side/writer-side seam that makes them load-bearing: a `RequestContext` (or `Correlation` vocabulary type) the repository/driver stamps onto every event of an interaction — correlation id of the request, causation id of the event that prompted it — so a whole saga traces one id without the domain carrying it. This is `sourcery`'s envelope-carried metadata done on purpose: ids are protocol vocabulary, produced at the driver boundary, never smuggled into `decide`.

### 0.5.3 Runtime metrics & lifecycle hooks — the operations story

§2 rules out baked-in transports but not *telemetry*. The store buses and the projector runner need `tracing` spans and counters at the protocol boundaries (append latency, conflict-retries, projection lag) or a deployed system is blind. 0.5 adds a thin `Metrics` port — counters/gauges/histograms as a trait, a `tracing` impl behind a feature flag, `NoopMetrics` the default — so instrumenting a store or projector is an option, not a framework takeover. This is the §6 runner's natural companion: the runner is already the seam a production deploy wraps.

### 0.5.4 The example — a worked domain end to end

Everything above is the means; the example is the proof the design holds in one sitting. 0.5 ships one small, complete domain — a bank (`open → deposit → withdraw → transfer`, a cross-aggregate transfer on the 0.4 batch machine, upcast a renamed event, rebuild a read model, snapshot a long-lived account) as a runnable binary plus a mirroring test suite. It is the doc's §1–§6 with code under it, and the thing a new reader runs before trusting the abstraction. (This is `eventually`'s `examples/` and `esrs`'s demo done the Eventyr way: one domain, every feature, no toy-versus-real gap.)

### 0.5 — what it is *not*

Still not a framework: no HTTP server, no message-bus drivers beyond the `EventBus` trait, no actor runtime (§2 stands). The four pieces are width (observability, a worked example, upcasting made real, metadata made load-bearing), not a second core. The next *structural* release, if there is one, is 0.6: process managers / sagas as a machine — events in, commands out, per §7 — deliberately deferred until the write and read sides have both shipped at least one production-grade store.

## 13. Competitive scan (2026-10) and roadmap 0.6

0.5 closed the write path, the read path, snapshots, multi-stream commands, upcasting, metadata, and metrics. The Rust field has since shifted: `thalo` is unmaintained (its README redirects to SierraDB/kameo_es); the actively-developed serious crates are `cqrs-es`, `esrs`, `eventually-rs`, `fmodel-rust`, and `kameo_es`. The scan below lists what each ships that 0.5 does not, and whether we take it, defer it, or reject it on §1/§2 grounds. Two deliberate gaps are *not* listed as 0.6 work because they contradict the identity:

- **No framework takeover** — no HTTP server, no lambda/axum glue, no actor runtime (`cqrs-es`'s serverless bet, `kameo_es`'s actor hosting). §2 stands.
- **No baked-in transports** — `esrs`'s Kafka/RabbitMQ buses are exactly the lock-in Eventyr's `EventBus`-as-trait seam refuses. A transport is an adapter outside the boundary, not a feature.

Everything else on the competitors' list is fair game if it can be shipped as a seam, a store, or a machine — not a runtime.

### 0.6.1 — Sagas / process managers as a machine

The single structural gap. `esrs` ships first-class `Policies` (fire-and-forget event handlers that re-enter the write side) and `cqrs-es`/`fmodel` both model sagas. Eventyr has the *fixed-boundary* version of a process manager in `BatchMachine` (a command that atomically spans a known set of streams), but nothing *reactive*: no "when event X lands, issue command Y against stream Z" construct that survives restarts. **Shipped:** `Saga::react` maps one event to `(target stream, command)` pairs, purely; `SagaMachine` turns them into `Dispatch` actions answered one at a time, with the interaction's metadata layered over the event's own ids. It deliberately does **not** checkpoint: `SagaProjection` runs it inside the existing subscription runner, which already owns at-least-once delivery, so a crash between dispatch and ack re-delivers the event and re-issues its commands — idempotency is each command's to keep, as for projections. One more row in the §7 table, never a daemon. This is the version of sagas that respects §2: the *protocol* ships, the transport stays the user's.

### 0.6.2 — Two-layer schema decoupling (`Schema`/`Persistable` as a named seam)

`esrs`'s one idea Eventyr has only implicitly: a first-class separation between `Aggregate::Event` and what is *stored*. Eventyr's story today is upcasting (`UpcasterRegistry`, `VersionedSource`) — strong on the rename/reshape axis — but the "stored row shape ≠ domain event shape" mapping (explicit serde rename, storage-only fields, drop-a-field) has no named trait. 0.6.2 adds a narrow `Codec`/`Schema` trait per store (not in core): the place where an event's persisted form is declared, so the registry's raw-payload work has a typed entry point instead of living only inside `eventyr-store-postgres`'s JSON columns.

**Withdrawn.** A first cut shipped the traits (`Persistable`, `SchemaCodec`, `DecodeEvent`) and one codec per store, but no store routed its reads or writes through them, and the events table has no column to persist a declared schema version. An unwired seam costs readers more than it saves, so it was removed. It returns only together with a store that uses it end to end: a `schema_version` column, and the Postgres append/read path going through the codec.

### 0.6.3 — A read-model store adapter (`ViewRepository` equivalent)

`cqrs-es`'s most-advertised read-side feature Eventyr entirely lacks: a per-aggregate materialized view, persisted transactionally with the command, in the same database. Eventyr's projection story is deliberately rebuild-first (fold the global stream, checkpoint, done) — which covers analytics and audit, but not the "look it up by id, fast, right after the write" query that makes a CQRS demo feel complete. 0.6.3 adds a `ViewStore` port (a projection whose state *is* a row) and one Postgres implementation, as an alternative read-path driver alongside `RebuildPlan`. Placement: `eventyr-projection`, never core — it is read-side glue per §3.

### 0.6.4 — A second durable store: SQLite / the community-store seam

`cqrs-es` ships Postgres, MySQL, and DynamoDB (plus community SQLite); `kameo_es` ships projections over Postgres/SQLite/MongoDB. Eventyr ships Postgres + fjall + in-memory — enough to prove the port, not enough to make "bring your own database" credible. 0.6.4 does *not* ship a third official store; it ships the thing that makes third-party stores safe: the SQLite reference port (`eventyr-store-sqlite`, against `rusqlite`/`sqlx-sqlite`) as a worked example of the contract-test suite, plus the `append_batch`/`StreamsAll` conformance tiers documented. The goal is the seam proven by two shipped databases, not a fourth one to maintain.

### 0.6.5 — Aggregate-state caching as an opt-in policy

`thalo` and `kameo_es` both advertise in-memory aggregate caching (LRU / actor-resident state) as a headline performance feature. Eventyr folds the (optionally snapshot-seeded) stream per command — correct and deterministic, but it means a hot aggregate pays a reload per write. 0.6.5 adds caching *as a snapshot-policy extension*, not a new state store: a `CachePolicy` on the repository that keeps the last-known `Snapshot` in-process and revalidates against the stream's version on write. It is snapshot promotion, not a second source of truth — the write machine already treats snapshots as read-side shortcuts, so the correctness story does not change.

**Resolved without new code.** A first cut seeded decisions from the cached state *without* reading the stream, relying on the append's version check to catch staleness — but rejections and no-ops never append, so they were decided on stale state and returned to the caller, and a failed write left the cache stale permanently. It was removed. The supported shape is the existing snapshot path: `AggregateRepository::with_snapshots` over `InMemorySnapshotStore` with `SnapshotPolicy::new(1)` keeps the latest state in-process and reads only the events since it, so every outcome — commit, rejection, or no-op — is decided against the stream.

### 0.6 — what it is *not*

Still no framework. No HTTP, no lambda, no serverless demo app, no code-generated aggregates beyond the existing derive sugar — and no message-bus *implementation* even though `esrs` ships two. The pieces that shipped (0.6.1, 0.6.3, 0.6.4) are all seams, stores, or machines: they add surface where the field already proves the pattern, and they keep every new capability behind a trait the caller instantiates.

## 14. Competitive scan II (2026-10) and roadmap 0.7

§13 compared Eventyr with the aggregate-centric Rust crates. This pass widens the field: the newer Rust crates (`disintegrate`, `happenstance`, `eventcore`, `sourcery`, UmaDB's `umadb-dcb`) and the strongest tools in other ecosystems (Marten, KurrentDB, Eventuous, Emmett, Axon). They agree on one structural shift and several operational pieces that 0.6 does not have. §2 still holds. Message-bus producers and gateways (Eventuous, `esrs`), web-framework glue (Emmett, Eventuous), actor hosting (`kameo_es`), and server-side projection runtimes (KurrentDB's JS projections) were all seen again and are rejected again on the same grounds.

0.7 is planned, not shipped. Items are in priority order, and each names the seam it lands on, because "seam, store, or machine — never a runtime" remains the bar.

### 0.7.1 — Dynamic consistency boundaries as a machine

The field has moved here. `disintegrate`, `happenstance` (built entirely on it), UmaDB, and Marten 9 all implement the Dynamic Consistency Boundary (DCB) specification. Events carry **tags**. A decision reads the events a **query** selects (event types × tags), folds them into whatever state it needs, and appends conditioned on *nothing matching that query having been appended since the read*. The consistency boundary is the query, chosen per decision. It can span what aggregates would separate, without a saga.

Eventyr's `BatchMachine` (0.4) is the fixed-boundary special case: the caller names the streams and each stream is guarded by its own version. §12 rejected `eventcore`'s `StreamResolver` because a boundary computed by the store is not a machine concept. DCB does not contradict that, because the *caller* still writes the query. It only replaces "set of streams" with "set of matching events" as the unit of conflict.

- **`eventyr-core::boundary`**: a sans-IO `BoundaryMachine`. `Read(query)` → fold the selected events in sequence order → `decide` → `Append { events, condition: (query, after: Sequence) }`. A conflict re-reads and re-decides, exactly like the write machine's retry. Also an optional **validation query** (disintegrate's refinement): a narrower query used for the append condition than for the fold, so an event that changes the state without invalidating the decision (a deposit during a withdrawal) does not force a retry.
- **The port**: a separate `QueryAppend` trait (`read(query)`, `append_if(events, condition)`), plus `tags` on `NewEvent`/`EventEnvelope`. It is opt-in per store, the way `StreamsAll` is opt-in today. `EventStore` stays the minimum.
- **Stores**: in-memory and fjall (one write transaction, already serial). SQLite (single writer). Postgres via tag rows guarded by a captured-version predicate at read-committed isolation, which is Marten's approach, rather than `SERIALIZABLE`. A `query_append_contract` in `eventyr-store-testing` gates all four.

`Aggregate` and `BatchMachine` stay. A stream is the degenerate query "this stream's tag", so a store that implements `QueryAppend` can run all three machines.

**Shipped**, with two departures from the plan above:

- **Tags are derived, not stored.** A `Tagged` trait (`fn tags(&self) -> Vec<Tag>`) sits next to `EventName`. Tags are a pure function of the event, the same as its stored name, so neither `NewEvent` nor `EventEnvelope` gained a field, and no tag can drift from the payload it describes. A stored tag would also be wrong for every event written before its type gained the tag. Deriving them means a query is correct on any history. The cost is that stores match tags on the decoded event: they prefilter by event type where they can (Postgres indexes `(event_type, global_sequence)`) and scan otherwise. A tag index is a later optimisation and does not change the protocol.
- **Postgres serializes with a commit-order lock, not tag rows.** Every append (migration 0006's `append_events`) holds one transaction-scoped advisory lock from drawing its global sequence until it commits. `append_if` takes the appended streams' advisory locks (sorted, the same order `append_batch` uses), then that lock, then checks the condition and writes. Holding it means no event is in flight: the check sees every committed event, and nothing commits between the check and the write. Without it, a condition check under READ COMMITTED reads past an uncommitted matching row and oversells; a deterministic test holds such a row open and fails without the lock. Marten's tag-row predicate avoids serializing all writers but only guards writers that also write tag rows. Conditional appends serialize with every write on Postgres for now. Per-tag locking is the optimisation if that becomes a bottleneck.

  The same lock closes an older bug that 0.7.2's work on live push found. Before 0006, a transaction could draw sequence N and stay open while another committed N+1. A projector polling in between saw N+1, checkpointed past N, and never delivered it. `StreamsAll` forbids this, and Postgres broke it whenever two appends to different streams overlapped. The first cut of 0.7.1 used `LOCK TABLE events IN SHARE ROW EXCLUSIVE MODE`, which covered the condition check but not the ordering. The commit-order lock covers both. Its cost: Postgres appends to different streams no longer commit in parallel, only their pre-insert work (stream lock, version check) overlaps. That is the price of a gap-free, commit-ordered global sequence without a separate sequencing table.

The machine is `eventyr_core::boundary::BoundaryMachine`, run with `drive_boundary` / `drive_boundary_blocking`. `StoreError::QueryConflict { sequence }` is the conflict variant. When the validation query is wider than the fold query, the next condition moves past an acknowledged conflict, so a retry can succeed. All four stores pass `query_append_contract` against the course-enrollment fixture in `eventyr_core::boundary::enrollment`. Postgres also has a 12-way race, the in-flight-writer test, and a test that a later sequence never commits before an earlier one.

### 0.7.2 — Live push from a shipped store

0.3 shipped `EventBus` as a trait, and no store implements it, so every projector polls. `disintegrate-postgres` has a `listener` feature, SierraDB's subscriptions switch from history to live events without a gap, and Emmett and Eventuous treat real-time subscriptions as table stakes. 0.7.2 implements the bus for Postgres (`LISTEN`/`NOTIFY` on append, behind a `listener` feature) and for the in-memory store (a broadcast channel). The bus's §6 contract is unchanged: a push is only a wake-up hint, the checkpoint poll stays authoritative, and a lost notification costs latency, never correctness.

**Shipped**, with three departures from the plan above:

- **A `CommitSignal` port, not an `EventBus` implementation.** `EventBus::publish` pushes envelopes *out*, so implementing it would mean the store calls a publisher after each commit. That is the wrong direction for Postgres, where the commit itself raises `NOTIFY`, and it carries more than a hint needs. The new port in `eventyr_store::notify` is narrower. `CommitSignal::subscribe` arms a `CommitListener`, and its `committed()` resolves once a commit has happened since arming or since the last call. It carries no events. Arming comes *before* the subscriber's first poll, so a commit between a poll and the wait that follows is remembered, never missed. `EventBus` stays what §2 made it, the seam for a user's own transport.
- **The machine learns why it sleeps.** `SubscriptionAction::Sleep` gained a `SleepReason`. `Idle` (the last poll was empty) may end early on a commit; `Backoff` (a failed apply or ack) runs its course, so a failing projection is not hammered on every write. `drive_projector` with `DriverPorts::wake_on(listener)` races idle sleeps against the listener. `Projector::wake_on(signal).run_woken(sleep)` is the bundled form. A listener that errors is dropped for the rest of the run, and the driver falls back to its timer.
- **Every store signals, not only in-memory and Postgres.** The in-memory, fjall, and SQLite stores share `LocalCommitSignal`, a generation counter and an `event-listener` `Event` that is raised after each commit that wrote events. It covers commits made through the store's own handles; another process writing the same SQLite file is seen at the next timed poll. Postgres raises `NOTIFY eventyr_commits` inside `append_events` (migration 0007), which Postgres delivers on commit and never on rollback. `PgCommitSignal` gives each subscriber its own listening connection outside the store's pool, and it wakes on commits from any process. The channel is database-wide, so a commit in another schema costs one empty poll. A lost connection resolves as a wake-up and reconnects on the next wait. Nothing is behind a feature flag. The signal is opt-in per projector (`wake_on`), and a projector that doesn't use it behaves exactly as before.

`commit_signal_contract` in `eventyr-store-testing` gates the three embedded stores. Postgres has timed tests against a real database, including an end-to-end one where a projector with a one-hour idle sleep applies a new event within seconds. That test fails when the `NOTIFY` is removed.

Starting this work turned up a Postgres bug in `StreamsAll`. Global sequences could become visible out of commit order, so a projector could skip an event for good. The fix is migration 0006's commit-order lock, which landed with 0.7.1 (see above). A wake-up that makes projectors poll sooner makes that race likelier to bite, so it had to be closed first.

### 0.7.3 — Inline views: the transactional half of 0.6.3

§13 promised a view "persisted transactionally with the command". 0.6.3 shipped the subscriber-driven half (`ViewProjection` run by a `Projector`), which is eventually consistent. Marten and Emmett call the other half *inline* projections, and `esrs` calls it `TransactionalEventHandler`: the view row is written in the same transaction as the append, so a read straight after the write sees it. 0.7.3 adds an `InlineView` option to the Postgres and SQLite stores. The store applies registered `View`s to the committed envelopes inside the append transaction, and a failing view rolls back the append. Same `View` fold, same `ViewStore` rows, so a view can move between inline and async without a rewrite. Emmett documents the caveat, and it carries over: inline *multi-stream* views can contend under concurrent writes, so the docs steer those to the async path.

**Shipped.** `eventyr_projection::inline` (behind its `inline` feature) is the store-agnostic half. It is pure: `InlineView` is the type-erased face of a `View` and its key function, with JSON as the erasure boundary. `Inline<V, K>` adapts any serde-able `View`. `rows_touched` names the rows a commit's events fold into, and `fold_inline` folds the events over the loaded rows and returns the rows to write. `PgStore::with_inline_views` and `SqliteStore::with_inline_views` (SQLite's new `views` feature) run it inside every append path: single, batch, and conditional. A row that cannot be folded fails the append. The rows are the ones `PgViewStore` and the new `SqliteViewStore` read, under the same newest-wins guard. An async `ViewProjection` replaying the log over inline-written rows folds nothing twice, so a view moves between inline and async without a rebuild.

Emmett's contention caveat does not carry over to Postgres the way the plan above expected. Every Postgres append already holds 0006's commit-order lock to commit, so two inline folds into one shared row cannot both read the old value. Inline folds need no lock of their own, and a multi-stream inline view costs no more than a per-stream one. What it costs instead is time inside that serialized section, so the docs ask for cheap inline views. The race test, 24 appends to different streams folding into one row, loses updates only when both the commit-order lock and a row lock are absent. If per-tag locking ever relaxes the commit-order lock, inline rows need their own lock, and the test says so. SQLite has one writer and no race to close.

### 0.7.4 — Filtered reads on the global stream

`StreamsAll::stream_all` returns every event. A projection that wants only `account-*` streams, or three event types, reads the whole log and discards the rest on the client. KurrentDB filters on the server by stream prefix or event type, and Marten and Emmett have `canHandle` / category filters. 0.7.4 adds `stream_all_filtered(from, filter)` with a default implementation that filters on the client, so existing stores keep compiling, and indexed overrides for Postgres and SQLite. KurrentDB's subtlety comes with it: when matches are sparse, a filtered read must still report how far it has *scanned*, so the subscription machine can checkpoint past long unmatched runs instead of re-scanning them after every restart.

**Shipped.** `StreamsAll::stream_all_filtered(from, filter, max, scan_limit)` returns a `FilteredRead` with the selected events and `scanned`, the highest sequence the read looked at. `EventFilter` selects by stream-id prefix and stored event name (KurrentDB's two server-side filters, minus regular expressions). The default implementation filters `stream_all` on the client. Postgres and SQLite override it: they read the scan bound first (the `scan_limit`-th row after `from`, or the head if nearer), then the matching rows up to that bound. A commit landing between the two reads is therefore past the bound and is never skipped. Prefixes match byte for byte (`starts_with` on Postgres, `substr` on SQLite, never `LIKE`), so `%` and `_` in a stream id are ordinary characters.

On the subscription side, `Batch` gained `scanned`. The machine acks it when it lies past the batch's last event, and a batch with no events but a scan past the checkpoint is acked without applying anything. A scan bound below the last event or behind the checkpoint is a protocol violation. `FilteredSubscription` is the source, and its `scan_limit` (4096 by default) bounds one poll's cost over a sparse filter.

The scan bound leans on `StreamsAll`'s visibility rule: no sequence below it may become visible later. On Postgres that is 0006's commit-order lock. Before it, the scan bound would have been a second way to skip a late-committing event.

`filtered_read_contract` gates the default (in-memory and fjall) and both overrides. An end-to-end driver test runs a projector over one matching event followed by 500 non-matching ones and checks that the persisted checkpoint is the head. With the scan bound dropped, it stays at sequence 1 and the test fails.

### 0.7.5 — Event ids and idempotent commands

`EventEnvelope` has no stable event id. `SagaProjection` leaves idempotency "each command's to keep", but gives the command nothing to key on. `kameo_es` tracks causation for idempotency, and the DCB reference implementation of *Understanding Event Sourcing* (`eventsourcing_book`) makes saga-issued commands idempotent by carrying the triggering event's id. 0.7.5 adds an `event_id` to the envelope (assigned at append, persisted by every store, checked by the contract suite) and an optional idempotency key on an interaction: the repository records `(key → committed outcome)`, and a replayed key returns the recorded outcome instead of re-deciding. The saga runner stamps the triggering event's id as the key, so at-least-once redelivery becomes effectively-once at the command boundary.

**Shipped, simpler than planned.** There is no separate `event_id` and no `(key → outcome)` table. The idempotency key travels in `Metadata::idempotency_key`, so every store persists it beside the causation and correlation ids (SQLite gains a column, added to existing databases on open; Postgres stores it in the `metadata` JSONB through migration 0008's eight-argument `append_events`; fjall writes it into its JSON row). The stream the command writes to is the record. A keyed write machine checks the stream it already loads for events carrying its key and, if it finds them, ends at `WriteOutcome::AlreadyCommitted` with that earlier commit instead of deciding again. That gives three things a side table would not:

- **Nothing new to keep consistent.** The key is written in the same append as the events, so "committed" and "recorded" cannot diverge, and the record survives restarts and works across processes with only the event log.
- **Concurrent duplicates are caught by the existing retry.** Two runs of one keyed command both load, decide, and append; one conflicts, reloads the delta, finds the winner's key, and returns it. A test races 16 duplicates and gets exactly one commit.
- **Rejections stay re-decidable.** A rejected or no-op command appends nothing, so it leaves no trace, and running it again decides against current state, which may now accept it. A side table recording "rejected" would replay a stale answer.

The costs are that a keyed command loads its whole stream (a snapshots-on machine skips its snapshot when keyed, since the earlier commit may lie before it), and that the key is only checked against the streams the command touches. That is exactly where a duplicate's events would land. The batch machine checks every stream of its boundary. The boundary (DCB) machine checks the events its query reads, which covers decisions that read what they write (the usual DCB shape) and is documented as its limit.

Sagas need no stamping step in the runner. `SagaMachine` gives each command the key `"{saga}:{sequence}:{index}"` (the saga's new `Saga::name`, the triggering event's global sequence, and the command's position in the reaction). The reaction is pure and redelivery carries the same envelope, so a re-issued command carries the key it carried the first time. A command's key is never inherited from the event that triggered it: it names a different command. The end-to-end test fails a saga's first checkpoint write so its event is redelivered. The fee command is dispatched twice, committed once, and the payer is charged once. With the keys removed from the saga machine, the charge happens twice and the test fails. The repository tests fail the same way when the machine's key check is disabled.

### 0.7.6 — Stream lifecycle and data erasure

Eventyr cannot delete, archive, or expire a stream. `eventyr-projection::rebuild` defers retention to the stores, and none of them provide it. The field covers this from several angles. KurrentDB has soft delete, hard delete (tombstone), `TruncateBefore`, and `$maxAge`/`$maxCount`. Marten archives streams to a cold partition. Axon Data Protection and `commanded-shredder` offer **crypto-shredding**: personal fields are encrypted under a per-data-subject key held outside the log, and deleting the key erases them without rewriting history. 0.7.6 has two parts:

- **Lifecycle on the port**: `delete_stream` (soft: reads return empty, appends continue from the old version; and tombstone: further appends are refused) and `truncate_before`, both optional and both covered by a new contract tier. Projections see a lifecycle marker in the global stream, not a silent gap, so a view can drop its row.
- **Crypto-shredding as a codec layer**: this is where the withdrawn 0.6.2 seam returns, wired end to end as its withdrawal note requires. A `KeyStore` port (in-memory plus one SQL implementation) and an encrypting codec that wraps a store's payload serialization. Encryption happens on append and decryption on read. A shredded field decodes to a declared replacement value, never to an error, because erasure must not break rebuilds.

**Shipped, with the plan changed in both halves.**

*Lifecycle.* `StreamLifecycle` (opt-in, like `StreamsAll`) has `close_stream` and `truncate_before`. There is no soft delete. The write protocol decides against a stream's whole folded history, so a deleted stream that accepted appends again would fold an empty history and append at version *N + 1*, a decision taken against state it never saw. Close is a tombstone: every append path (single, batch, conditional) fails with `StoreError::StreamClosed`, and the history stays readable. Truncation keeps the stream's version, and a read that starts before the cut fails with `StoreError::Truncated` rather than folding a partial history (the repository test shows why: without its `Opened` event, an account would reject a valid deposit). Truncate only below the snapshot readers start from. The store emits no lifecycle marker, since a store cannot construct a domain event; callers append their own `Closed`/`Archived` variant first. Stores keep the lifecycle in a `stream_lifecycle` table (SQLite; Postgres migration 0009, where `append_events` checks it under the stream lock so raw SQL callers are covered too), in a fjall partition, and in the in-memory store, which now tracks heads and sequences explicitly because truncation breaks "version = number of events". `lifecycle_contract` gates all four.

*Crypto-shredding.* Field-level in the domain event, not a whole-payload codec per store, so the withdrawn 0.6.2 seam stays withdrawn. A personal field is a `Sensitive<T>` naming its data subject, so one event can carry several subjects' data, each erased separately. A new crate, `eventyr-shred`, holds the pieces:

- **`Cipher`**: a bring-your-own seam (generate a key, encrypt and decrypt with associated data). The core crate ships no cipher. `eventyr-shred-aes-gcm` (AES-256-GCM, 96-bit random nonces) and `eventyr-shred-chacha` (XChaCha20-Poly1305, 192-bit random nonces) are adapter crates, in the same style as the store crates. Both pass `cipher_contract`, which checks fresh nonces, wrong key, wrong associated data, a flipped bit, truncation, and wrong key length. A deterministic, unauthenticated cipher fails it.
- **`KeyStore`**: subject keys kept outside the event log. `InMemoryKeyStore` here, and `SqliteKeyStore` behind SQLite's `shred` feature (better on its own database file). `key_store_contract` pins the property erasure rests on: an erased subject stays erased, and its key cannot be recreated.
- **`Shredder`** seals every `Plain` field and opens every `Sealed` one by walking the event's JSON for the `$sensitive` tag, so the event type needs no trait. The subject is bound as associated data, so a sealed field moved to another subject fails to open. A field whose key was erased opens as `Shredded`, which domain code reads with `get`/`or`, so a rebuild never stops on it. New data for an erased subject is refused, never written in the clear.
- **`ShreddingStore`** wraps any store: every write path seals, and every read opens. It passes the event-store, global-stream, batch, and lifecycle contracts. Tests check that the plaintext never reaches the SQLite file and that erasure removes the key bytes from the key file.

Erasure covers the event log only. Snapshots, view rows, and logs that copied personal data out of it must be cleared separately; the crate docs say so. Parked events (0.7.7) are a copy too: `ShreddingParkedStore` (eventyr-shred's `parked` feature) wraps the parked store so rejects are recorded sealed and an erased subject's parked record reads back `Shredded` — while a record's free-text rejection (`ParkedEvent::error`) may itself quote personal data, and clearing a subject means clearing those too. Keys zeroize on drop behind `zeroize`; serde failures report category and position, never the decrypted bytes.

### 0.7.7 — Poison events: parking instead of stalling

The projector runner backs off and redelivers indefinitely, so one event that a projection cannot apply stalls that projection permanently. KurrentDB parks a message after `maxRetryCount`, and Eventuous wraps handlers in retry policies. 0.7.7 adds a `FailurePolicy` to `SubscriptionPolicy`. `Retry(n)` then `Park` records the event in a `ParkedStore` (in-memory plus SQL, alongside checkpoints) and advances. `Halt` keeps today's behavior and stays the default, because skipping an event is a correctness decision the caller must opt into. Parked events can be listed and replayed. This is a new `SubscriptionMachine` transition, not a second machine.

**Shipped as planned.** `SubscriptionPolicy::on_failure` takes a `FailurePolicy`. `Halt` is the default and keeps today's behaviour: back off and redeliver forever. `Park { retries }` lets the projection reject an event `retries` more times, then emits a new `SubscriptionAction::Park` carrying the envelope, the attempt count, and the last rejection. The driver records it in a `ParkedStore`. On `Parked` the machine carries on past the event exactly as if it had applied, and the batch acks as usual. On `ParkFailed` it backs off and redelivers, so an event is never skipped unless it was durably recorded. Only the projection's rejections count against an event; a failed ack or fetch is the infrastructure's fault, not the event's. An event that later applies resets its count.

`ParkedStore` (`park`, `list`, `remove`) lives beside `CheckpointStore` in `eventyr-subscription`, with `InMemoryParkedStore`, `NoParking` (the default: refuses, so nothing is skipped by accident — a `Park` policy over it fails at `run`, and a refusal drove directly counts on `eventyr_park_failures_total`), and a `parked_store_contract` behind the crate's `testing` feature. SQLite gets `SqliteParkedStore` behind a `parked` feature, keeping the whole envelope and its metadata. `Projector::park_into(store, policy)` wires both. The driver reports every park on a new `eventyr_parked_events_total` metric: any value above zero means a projection is missing events, so it is the one to alert on.

Replay is the caller's: list the parked events, apply each to the fixed projection, and `remove` it. The end-to-end test does exactly that. The projection must be idempotent, as for any at-least-once delivery. A retry redelivers the batch from the last ack, so the events before a poison one arrive again with it (the test's first draft forgot that and counted them twice).

### 0.7.8 — Reading state without a command

`AggregateRepository` only executes commands, so there is no `load(id)` for a query handler, a debugging tool, or an integration test. Marten also loads as of a version or timestamp. 0.7.8 adds `load(id)` and `load_at(id, version)` on the repository (snapshot-seeded where configured), plus `load_until(id, timestamp)` when the `time` feature is on. No machine is needed: it is one fold, which §7 keeps out of the machine table.

**Shipped.** All three live on `eventyr-store`'s `AggregateRepository` — one fold, no machine (per §7 a load has nothing multi-step to branch on), each returning `Loaded { state, version }` so "as of" answers carry how far the fold actually read. The plain `load`/`load_at` are on the snapshots-off repository; `load_with_snapshots`/`load_at_with_snapshots` seed from the newest persisted `Snapshot` (never past a `load_at`'s own bound, and a failed seed read fails the load — the write path's rule). `load_until` is never seeded: a snapshot records a version, not an instant, and only stores that write `metadata.timestamp` (the in-memory store via the caller, Postgres via `created_at`) can answer it — an event without one is a `StoreError::Other` naming the stream, not a guess. Stopping at an out-of-order timestamp stops at the *first* later event; `<=` keeps one commit's shared `created_at` together.

### 0.7.9 — Projector exclusivity

Nothing stops two copies of the same projector from running against one checkpoint and corrupting the read model. Emmett names this problem and documents a single-replica workaround. Partitioned parallelism stays deferred: hash events by stream id across N members is scheduled only after the lease exists to coordinate them — and Eventuous's out-of-order *checkpoint commit handler* is rejected as a single-checkpoint alternative, because it trades a simple invariant for gap bookkeeping.

**Shipped.** §7's table gains no row — the lease is driver policy, not a machine transition. `ProjectorLease` (`acquire` → `renew` per due `Fetch` and before every `Ack` on the batch boundary, `release` on exit) lives in `eventyr-subscription`; the driver now carries a third defaulted port (`DriverPorts::lease_with`, `NoLease` when absent) and a new `LeasedProjector` (`Projector::lease_with`/`run_leased`, `.run_woken_leased`) surfaces the lost lease as `RunError::{Store, LeaseLost { name, checkpoint }, Taken { name }}` — `Taken` on entry, `LeaseLost` mid-run with the last acked checkpoint as the resume point. Nothing touches `SubscriptionMachine`'s protocol. Two implementations ship: `InMemoryLeaseStore` (ungated, for tests and single-process runs) and `PgLeaseStore` behind `eventyr-store-postgres`'s `leases` feature as a `projector_leases` row (migration 0012) — the row, not an advisory lock, because advisory locks are connection-pinned and sqlx pools hand renewals to whichever connection is free (the exact cost `PgCommitSignal`'s dedicated connection pays and its docs flag). SQLite and fjall get no impl this round: SQLite's cross-process exclusivity is a `BEGIN IMMEDIATE` transaction that must stay open — blocking the very read model the lease protects — and fjall is single-process by design. A `LeasePolicy { ttl, grace, max_grace }` (5 s / 3 / 12 ⇒ ≈ one minute of accidental captivity) is set on the projector. The lease deliberately does **not** put a fencing token into `CheckpointStore::store` — that would widen a shipped port and co-locate the checkpoint and lease in one transaction; the residual window is a lease lost mid-batch acking onto a successor's write, which the at-least-once idempotent-apply contract already tolerates. Fencing with a token in the checkpoint row is the documented follow-up. `lease_store_contract` (behind `testing`) is the port's gate.

**Revised in §15.** The follow-up is a compare-and-set checkpoint, not a fencing token: that is what Axon, Marten, and Emmett ship, and none of them uses a monotonic token (§15.3, 0.8.1).



### Smaller items

- **Projection testing** — **shipped.** `ProjectionScenario` in `eventyr-subscription` (`over`/`given`/`run`/custom `when`/`then`), the read-side counterpart of `Scenario`: fold the seeded envelopes through the projection and assert on the projection, with no store and no `tokio`. The catch-up pulse is a `Catch` port on the driver (`DriverPorts::caught_up_on`, and the bundled `Projector::caught_up_on(..).run_caught(..)` / `run_woken_caught(..)`), called at every idle sleep — the shape of Emmett's `whenCaughtUp()` but at the machine's idle boundary, never a wall clock.
- **Focused examples**: `bank.rs` stays the end-to-end proof — now covering the full shipped surface (snapshots, the saga with its idempotency keys, closure, shredding and erasure, parking and replay, commit-woken projections) with the mirroring suite in `eventyr/tests/bank_story.rs` gating it. The single-topic examples follow the pieces above as they ship, like `sourcery`'s example set: `enrollment.rs` (a DCB decision) and `loan_eligibility.rs` (the narrowed validation query, and the blocking boundary driver) and `inline_view.rs` (inline and async views over SQLite) are shipped. CI runs every example, so their asserts gate merges, not just their compilation.

### Deferred

- **Multi-tenancy** (Marten's conjoined tenancy and per-tenant partitions, Emmett's partitioned tables). It is real demand, but in Eventyr it is mostly a store concern: a tenant column, per-tenant sequences, and per-tenant checkpoints. It waits until a user needs it and DCB tags have shown whether a tenant should be a tag or a partition.
- **More official stores** (MySQL/DynamoDB like `cqrs-es`, KurrentDB/MongoDB like Eventuous). 0.6.4's position stands: the contract suite is what keeps third-party stores safe, and Eventyr does not maintain a fifth database.

### 0.7 — what it is *not*

Still no framework. DCB is a machine and a port. Live push, inline views, filtered reads, lifecycle, and leases are store capabilities behind opt-in traits. Event ids, parking, and erasure extend vocabulary and policy that already exist. Nothing in 0.7 adds a runtime, a transport, or a daemon the caller does not drive.

## 15. Running distributed (2026-10) and roadmap 0.8

§13 and §14 compared features. This pass asks what changes when Eventyr runs across many processes, nodes, and databases, and how the field handles the same problems. The scan covered the Rust crates (`cqrs-es`, `esrs`, `eventually-rs`, `fmodel-rust`, `disintegrate`, `kameo_es`/SierraDB, `sourcery`), Marten (with its async daemon and Wolverine), Emmett, Eventuous, KurrentDB, Axon, Akka Persistence/Projections, Propulsion, and Message DB. Projects' own documentation and source were read; anything below that is inference is marked as such.

### 15.1 Position: event sourcing with its read side, never a CQRS framework

Event sourcing on its own is not a usable product. The log answers "give me stream X" and nothing else, so every other query needs a projection, and that split is CQRS. Eventyr already ships the read side: projections and checkpoints, views (async and inline), sagas, filtered reads, live push, parking, and leases. The crates already list `cqrs` as a crates.io keyword.

Two things that commonly travel under the CQRS name are refused, now as §2 non-goals:

- **Buses and mediators.** A command or query bus routes a call to a handler, which in one process is a function call, and across processes needs a transport §2 already refuses. The field confirms the split: Marten does not route commands, Wolverine (a separate framework) does; Axon's consistent-hash routing and Akka's cluster sharding are framework features, not store features. Eventyr's repository is stateless, so any node can execute any command, and the store's optimistic concurrency settles races. Hot aggregates are the cost: many nodes writing one stream multiply conflict retries. The answer is caller-side routing by stream id (consistent hashing at the load balancer, or a framework that offers it), documented, not built.
- **State-stored aggregates.** `decide`/`apply` is the Decider shape, and `fmodel-rust` runs one decider over either an event store or a state store. Eventyr could do the same with one machine and a `StateStore` port, but nothing on the read side would work for it: projections, sagas, upcasting, shredding, parking, and leases all consume the log, and a state-stored aggregate produces none. Publishing its changes would need an outbox, which reintroduces the dual write the log exists to avoid. Integration with systems that are not event-sourced goes through the caller's `EventBus`.

### 15.2 What a store is

**A store is one event log with its own global sequence.** Inside one store, every event has a unique `Sequence`, gap-free and in commit order; positions compare inside one store and mean nothing across two. Concretely:

| Backend | One store is | What shares it |
|---|---|---|
| Postgres | One `events` table (database + schema) | Every `PgStore`, pool, process, and node pointed at that table |
| SQLite | One database file | Every connection to the file |
| fjall | One database directory | One process (fjall is single-process) |
| In-memory | One `InMemoryStore` value | Whatever holds it |

A store is not the `EventStore` trait (the interface), not a Rust value or a process (twenty nodes on one table are one store), not a database server (one server can host several stores), and not a read replica (a lagging copy of the same store). `SnapshotStore`, `ViewStore`, `CheckpointStore`, `ParkedStore`, `KeyStore`, and the lease store hold derived or side data; "store" alone means the log.

**On Postgres, the sequence and the lock have different scopes.** The sequence is the table's identity column (migration 0001). The commit-order lock is one constant advisory key, `pg_advisory_xact_lock(7300160413598463541)` (migration 0006, repeated by 0007–0010), and advisory locks are database-wide. Two `events` tables in different schemas of one database have independent sequences but still serialize every commit against each other. The `eventyr_commits` NOTIFY channel (0007) is database-wide too, so listeners wake on each other's commits (a spurious poll, not a correctness problem). 0.8.2 fixes the lock's scope.

Eventyr has no store identity in its vocabulary: no `StoreId`, and nothing in an envelope says which log it came from. Every cross-store feature below needs one.

### 15.3 How the field handles distribution

**Global order and the gap problem.** A projector polling `WHERE sequence > checkpoint` skips an event whose sequence was drawn but not yet committed when a later one became visible. Every system answers this one of five ways:

| Approach | Who | Cost |
|---|---|---|
| Serialize commits | Eventyr (database-wide lock), Message DB (advisory lock per *category*), KurrentDB and Axon Server (one leader per cluster or context) | Write throughput |
| Read only below the oldest open transaction (`xid8` and `pg_snapshot_xmin`) | Emmett, `sourcery` | One long transaction stalls every consumer; order is by transaction start, not commit |
| Detect gaps and wait for evidence | Marten (`pg_locks`, `pg_snapshot_xip`, since 9.23), `disintegrate` (advisory-lock "epoch"), Axon on JPA/JDBC (`GapAwareTrackingToken`) | Complexity; stalls behind idle-in-transaction sessions |
| Wait at gaps on a timer, or tombstone them | Eventuous, Akka JDBC | Can skip committed events |
| No global order | `cqrs-es`, `esrs`, `eventually-rs`, SierraDB (gap-free per partition, with a confirmed watermark), Akka R2DBC (1024 slices, timestamp offsets with backtracking) | No cross-stream catch-up, or per-partition only |

The read-side schemes have shipped real event-skipping bugs: Marten's time-based gap skip lost committed events (marten#4964, fixed by evidence-gated waiting), Eventuous Postgres subscriptions skipped events in production (eventuous#222), and Axon on Hibernate pooled sequences assigned indexes out of order (axon#3374). Eventyr's lock is the KurrentDB and Message DB answer, and it stands. Message DB scopes the same lock per category, which is the precedent for 0.8.2.

**Write scaling.** Nobody shards one log while keeping one global order. Marten partitions events by tenant with per-tenant sequences and per-tenant high water marks; Emmett has a partition column over one shared sequence; Axon gives each context its own log and leader; Akka R2DBC spreads 1024 slices across tables and databases; SierraDB hashes streams into replicated partitions. All of them give up order across the split. For Eventyr the unit of the split is the store (§15.2), and the multi-tenancy item in §14's *Deferred* list is the same decision: a tenant is a store, a tag, or a partition.

**Parallel projections.** The common shape partitions the checkpoint: Axon splits a processor into segments (one token each, split and merged at runtime), Akka assigns slice ranges to instances, Message DB's consumer groups take `hash(id) % size = member`, and Eventuous reorders acknowledgements inside one process (`CheckpointCommitHandler`). Marten never shipped user-defined sharding (marten#3703). One consumer per checkpoint, which 0.7.9 enforces, is therefore not a ceiling the field has broken without partitioning.

**One consumer, and stale consumers.** No system in the scan uses monotonic fencing tokens. The strongest pair two things instead: the checkpoint write is a compare-and-set (Axon's `storeToken` fails on an owner mismatch, Marten's progression update carries `WHERE last_seq_id = ?`, Emmett's `store_processor_checkpoint` returns `MISMATCH`), and the projection's writes commit in the same transaction as the checkpoint, so a stale owner's writes roll back with it (inference from Marten's and Axon's source and docs). Akka relies on split-brain-resolver timing and documents that two instances "overwrite each other's offset storage"; Eventuous's SQL checkpoint write is a blind update. Eventyr is at the Eventuous end: the lease renewal is a compare-and-set, but `PgCheckpointStore::store` is a blind upsert (`ON CONFLICT DO UPDATE SET global_sequence = EXCLUDED.global_sequence`).

**Read-your-writes.** Returning the commit position is common (Emmett's `lastEventGlobalPosition`, Eventuous's `GlobalPosition`, KurrentDB's position, Eventyr's committed envelopes). Inline views are common (Marten, Emmett, `esrs`'s `TransactionalEventHandler`, Eventyr 0.7.3). Waiting is rare and carries warnings: Marten's `QueryForNonStaleData` documents timeouts, Emmett's `whenCaughtUp` is for tests. `sourcery` ships the full pattern in Rust: `update_tracked` returns a serializable `ConsistencyToken`, and `wait_for`/`read_after` wait on it. Axon pushes instead of waiting (a subscription query, then the command, then the update). Marten's `FetchLatest` reads the async row and folds the stream's newer events in memory.

**Cross-service idempotency.** Eventyr's key-in-the-stream (0.7.5) is as strong as KurrentDB's event-id idempotency and needs no side table. `kameo_es` keys causation on the *source stream and its version*, not a global position. Wolverine and Akka commit the outbox or offset in the same transaction as the handler's writes, the same principle as the checkpoint above.

### 15.4 What it costs Eventyr today

- **Writes serialize per Postgres database** (per file on SQLite, per directory on fjall). Inline views run inside the locked section, so each one slows every writer, not only its own stream's. Scaling writes means more databases, not more schemas, until 0.8.2.
- **Each append pays its own flush.** The lock is held through the WAL flush and, under synchronous replication, the standby's acknowledgement (Postgres releases transaction locks after both). Group commit cannot batch these: appenders queued on the lock are not flushing, so `commit_delay` does not count them. A store's ceiling is roughly one append per commit round trip, a few hundred to about a thousand a second with a 1 ms cross-zone standby (an estimate, not a measurement). `append_batch` pays the round trip once for many events.
- **One consumer per projection.** A projection's throughput is what one consumer sustains.
- **A stale projector is detected only by its lease.** Between losing the lease and noticing, it can ack a checkpoint behind its successor's. `ViewStore`'s newest-wins guard absorbs that for view rows; a non-idempotent `Projection` or an external side effect does not.
- **Positions are store-local.** A sequence from one store means nothing to a projection over another, and a view built from two stores has a set of checkpoints, not one.
- **Saga keys are store-local.** `idempotency_key` is `"{saga}:{sequence}:{index}"`, so two sagas of the same name over different stores can mint the same key.
- **Waiting after a write couples the write path to the read side.** A command that blocks until a view catches up stalls whenever the projector fails over (up to `ttl × grace`, 15 s at the defaults, while a lease survives missed renewals), and N API nodes polling `CheckpointStore::load` load the checkpoint table. `CommitSignal` does not help: it fires on event commits, not checkpoint writes. Reaching a checkpoint does not mean an event applied, either; parking (0.7.7) advances past it.

### 0.8.1 — Checkpoints a stale projector cannot overwrite

The fencing follow-up 0.7.9 named, in the shape the field ships rather than as a token. An opt-in `ConditionalCheckpoints` port beside `CheckpointStore`: `store_if(name, expected, checkpoint)` writes only if the stored checkpoint is still `expected`, and reports a mismatch. The driver keeps the last acked checkpoint already, so it knows `expected`. A mismatch ends the run as `RunError::LeaseLost`, the same outcome a lost renewal produces, with the stored checkpoint as the resume point. Postgres and SQLite implement it with one `UPDATE … WHERE global_sequence = $expected`; the contract suite gains a case where two drivers race one name.

That detects a stale writer at the ack; it does not undo what the stale writer applied before it. The second half is a transactional projection: a `Projection` whose batch writes and checkpoint commit in one database transaction, so a failed `store_if` rolls the batch back (Marten's and Axon's shape). That needs a projection that can run inside the driver's transaction, which `ViewProjection` over `PgViewStore` can and an arbitrary `Projection` cannot; it ships for views first. `CheckpointStore` itself is not widened.

### 0.8.2 — A commit-order lock per store

Derive the advisory key from the store instead of a constant: the `events` table's OID, or a hash of its schema, so two stores in one database stop serializing against each other and "a store" means the same thing for the sequence and the lock. The per-stream locks (`hashtext(stream_id)`) share the same key space; a collision over-serializes and never deadlocks, as 0006 already notes. The NOTIFY channel takes the schema into its name for the same reason. This is a migration, and it is the cheapest way to scale Postgres writes inside one database until 0.9 serializes per slice instead (§16.4).

**Store identity** lands with it: a caller-assigned store name (`PgStore::named`, and the equivalent on the other stores), carried in positions that leave the store. It is the vocabulary 0.8.3 and the cross-store work below need.

### 0.8.3 — Read-your-writes tokens

Replace "wait until the projector catches up" with a token checked on the read side. A committed write yields a `Position { store, sequence }` (the highest sequence it committed). The client carries it to the query. For a single-row read, compare the row's own `ViewRow::version` to the token: every saved row records the sequence of the newest event folded into it, so this needs no checkpoint read, works on a read replica (the version travels with the row), and works on any node. It applies only to rows the command's events fold into. For list and multi-row reads, fall back to the checkpoint, with the parking caveat stated in the result type ("position reached", not "event applied"). Both wait for a bounded time and then return an explicit stale result rather than blocking.

`sourcery` is the Rust precedent; Marten's timeout warnings are a requirement. This is a small driver-side helper in `eventyr-subscription`, not a machine. It is for production reads; tests wait on the driver's idle boundary instead (§14's *Smaller items*). Marten's `FetchLatest` (row plus an in-memory fold of newer events) was considered and set aside: a view row records a global sequence while a stream read starts from a stream version, so catching a row up means reading its whole stream and skipping by sequence, and a row that folds several streams cannot be caught up from one.

### 0.8.4 — Partitioned projections

The parallelism 0.7.9 deferred until a lease existed. `EventFilter` gains a hash partition (`member` of `count`, selected by a stable hash of the stream id); each member runs its own checkpoint name and its own lease, so nothing in `SubscriptionMachine` changes and no gap bookkeeping appears (Eventuous's commit handler stays rejected, per 0.7.9). Per-stream order holds; order across streams does not, which is what Axon, Akka, and Message DB also give. The open question is the hash: the Postgres and SQLite filtered-read overrides must compute exactly the Rust hash in SQL. Either the hash is defined in SQL-friendly terms, or the append stores a partition bucket column. Changing `count` means a rebuild in the first cut; Axon-style split and merge is deferred.

**Superseded by §16.4.** Slices put the partition into the log itself, so projectors lease slices instead of filtering a shared feed, and changing the number of instances moves leases instead of forcing a rebuild.

### 0.8.5 — Saga keys that survive more than one store

Key saga-issued commands on the triggering event's stream and version, as `kameo_es` does: `"{saga}:{stream}:{version}:{index}"`. Stream ids are unique across stores where the caller makes them so, and the key no longer depends on a store-local sequence. The change has one hazard: an event delivered before the upgrade under the old key and redelivered after it under the new key is a duplicate the stream cannot recognise. Upgrade with the saga projector drained (caught up and stopped), or have the write machine accept both key shapes for one release.

**Revised by §16.3.** Stream versions are reused after an asynchronous failover just as sequences are, so `{stream}:{version}` collides the same way. The key is built from a random event id instead: `"{saga}:{event_id}:{index}"`. The upgrade hazard above is unchanged.

### 15.5 Global order across stores

Within one store, Eventyr keeps the commit-order lock (§15.3) through 0.8; 0.9 narrows it to one lock per slice (§16.4). The documented escape hatch, if a single database's write throughput becomes the bottleneck, is Emmett's and `sourcery`'s transaction-id read (`xid8` plus `pg_snapshot_xmin`): writers stop serializing, readers wait below the oldest open transaction, and `Checkpoint` becomes a `(transaction id, sequence)` pair. It is not scheduled; the lock has not yet been the bottleneck for anyone.

Across stores there is no order today. The candidate mechanism is a hybrid logical clock.

**What an HLC is.** Kulkarni, Demirbas et al. (2014): a timestamp `(l, c)`, where `l` is the largest physical time seen and `c` a counter that breaks ties while `l` stands still. A local event takes `l = max(l, now)`; receiving a timestamp takes `l = max(l, remote.l, now)`, with `c` advanced to stay above both. Compared as a pair, it guarantees that if `e` happened before `f`, `hlc(e) < hlc(f)`, and it never drifts from wall time by more than the clock-skew bound. It fits in 64 bits. CockroachDB (MVCC versions), YugabyteDB (hybrid time), MongoDB (cluster time), and Kudu all build on it, and Zenoh stamps every write with one.

**What it does not give.** It is not gap-free, not a total order (equal stamps need a node-id tie-break), and above all it does not tell a reader that *nothing at or below T can still appear*. Two transactions can commit in the opposite order to their HLCs, exactly as identity columns can. Every system that runs a change feed on HLCs adds a separate watermark for that: CockroachDB's closed timestamps (a promise not to write below T, about 3 s behind by default, kept moving on idle ranges by a side transport) and resolved timestamps (the minimum of that and the oldest open transaction); MongoDB's no-op writes that move an idle shard's clock forward; Spanner's and Kudu's commit wait. Akka R2DBC, often cited here, uses no HLC at all: it orders by transaction start time and rescans a two-minute window to catch late commits. The common shape: a watermark always needs either a per-writer progress record with a promise not to write below it, or a wait sized to the skew bound. The HLC is only the coordinate system the watermark is written in.

**Within one store, an HLC replaces nothing.** A poller reading `WHERE hlc > checkpoint` skips the slower of two concurrent commits, so it needs the commit-order lock (or `xid8`) as much as `Sequence` does. `mnesis` reached the same conclusion (mnesis#144: carry the HLC in metadata, do not index it, because "HLC is a poor subscription cursor"), and Restate keeps its log sequence number as the authority with the HLC beside it. `Sequence` stays the cursor.

**Across stores, it is the right merge order.** Planned shape, not scheduled:

- **Stamping.** Each store stamps every append with an HLC *inside* its commit-order lock, so HLC order equals `Sequence` order within the store. On Postgres the database is the clock node: the stamp is `max(last stamp, caller's stamp, the server's clock)` with the counter rules above, computed in `append_events` and persisted with the row, so it survives restarts and needs no clock agreement among application nodes. The stamp travels in `Metadata`; the HLC value type is vocabulary (no clock in it), so it fits core's placement rule, and reading the clock stays store-side.
- **Causality only where propagated.** The ordering guarantee holds only for timestamps that travel. The command carries the caller's HLC in its metadata, the store advances past it before stamping, and `SagaMachine` puts the triggering event's HLC on each dispatched command. Causality that flows outside Eventyr (an HTTP round trip, a UI) is lost unless the caller threads the stamp through.
- **A merged projection.** A projection over several stores emits in `(hlc, store, sequence)` order, but only up to `W = min over stores of closed(store)`, where `closed` is the stamp below which that store will never commit again. Under the commit-order lock, a store's closed stamp is simply its newest stamp, so no intent tracking is needed. An idle store must advance it with a heartbeat append or a dedicated row, or one quiet store freezes the merge; merge latency is the slowest heartbeat plus the skew allowance. The resume cursor is the vector `{store → Sequence}`, which is exact; the HLC orders the merge and never resumes it. That is the split Fly's Corrosion fork of cr-sqlite uses (a per-site version vector for progress, a `uhlc` stamp for time).
- **Tokens.** For one store, `Position { store, sequence }` (0.8.3) is exact and stays the token. An HLC token is the compact form when one token must span several stores (MongoDB's `afterClusterTime`), and it needs the closed-stamp machinery above to decide when a reader has caught up. A vector of positions is the exact alternative.
- **Bounding a bad clock.** An HLC absorbs a remote stamp from the future. CockroachDB rejects stamps beyond a max offset and stops a node whose clock drifts too far; `uhlc` rejects past a configurable `max_delta`. Stamps arriving in command metadata are caller input, so the store rejects any that run further ahead than a configured bound.
- **Implementation.** The algorithm is about thirty lines, `no_std`, with no dependencies — owned in core rather than imported. `uhlc` (Zenoh, Corrosion) is the mature reference; `hlc-gen` packs a lock-free 64-bit form.

**Status.** The stamp in metadata (stamping, propagation, and the bound) is cheap, store-local, and useful on its own for cross-store causality and audit; it is the first piece if a user runs several stores. The merged projection and the HLC token wait until someone needs a view over more than one store.

### 0.8 — what it is *not*

Still no framework. No command routing, no cluster membership, no leader election, no sharding of one store. Every 0.8 item is a port, a store migration, or a driver helper, and the caller still decides how many stores to run and where commands go.

## 16. Horizontal scale: slices, shards, and the consistency boundary

The target: Eventyr copes with many concurrent writers and many projectors on every adapter, out of the box, and the same application code runs on an embedded device and in a large cloud deployment. §15 explains why the 0.8 design cannot get there. A gap-free total order needs one sequencer, and every write has to pass through it: Eventyr's commit-order lock, KurrentDB's leader, Axon Server's leader per context. Whatever the sequencer is, it caps write throughput. Every system in the §15 scan that scales writes splits the log and keeps order only within each part (Kafka partitions, SierraDB partitions, Akka slices, Marten tenants, Message DB categories).

So 0.9 changes the core guarantee:

> **0.8:** one gap-free, commit-ordered sequence per store.
> **0.9:** one gap-free, commit-ordered sequence per *slice*. Across slices, order is causal (§15.5's hybrid logical clock), and only for consumers that ask for it.

**One slice is exactly 0.8.** That is what makes "it shouldn't matter where it runs" true. An embedded deployment runs one slice and sees today's behaviour; a cloud deployment runs many slices and many shards. The domain, the machines, and their guarantees are the same in both; the slice count and the hosting are deployment settings. Every adapter implements slices, and the contract suite and the bank example run at one slice and at many.

### 16.1 Three units

| Unit | Is | Order | Transactions | DCB boundary |
|---|---|---|---|---|
| Slice | A bucket of the partition-key hash | Gap-free, commit-ordered | — | May span many |
| Store (a shard) | One database (§15.2) | Per slice | Atomic across its slices | Must fit inside one |
| `ShardedStore` | Several stores behind one router | Causal (HLC), opt-in | None across shards | Cross-shard invariants go through sagas |

### 16.2 The Postgres deployment contract

Eventyr's order is only as durable as the commit behind it. A failover that loses commits or a restore from backup rewinds the database to an earlier point in its history. Everything inside the database rewinds with it and stays consistent; anything that took a position outside the database before the rewind is now wrong. The contract, stated in the store's docs:

- **One writable primary per store.** Supported: a single primary with physical replicas (Patroni, CloudNativePG, RDS Multi-AZ) and provisioned Aurora (a promoted replica loses no acknowledged commit). Unsupported: multi-writer or sharded Postgres. EDB PGD's advisory locks are not replicated and it resolves conflicts after commit; Citus sequences carry a node id in their high bits; Aurora Limitless sequences are out of order across routers; CockroachDB and YugabyteDB cache sequences per node or connection. Routing all writes to one node (PGD's write leader, Citus's coordinator) restores the single-writer case for ordering but stays unsupported. Write scaling is §16.6's job, not the database's.
- **Synchronous replication for failover without loss.** `synchronous_commit = on` or `remote_apply` with a synchronous standby. One residual: if the wait for the standby is cancelled, the transaction is already committed locally and Postgres only warns.
- **Asynchronous failover can reuse positions.** Postgres logs sequences 32 values ahead, so a promoted standby either skips ahead (a gap) or, if the old primary's last WAL never arrived, hands out the same numbers again for different events. A consumer that saw the lost events resumes past their positions and silently misses the new ones, while keeping the effects of events that no longer exist.
- **Consumers inside the event store's database are safe.** A physical standby replays the primary's WAL in order, so a promoted standby holds an exact earlier state: if a checkpoint row survived, every event it covers survived too. Checkpoints, view rows, parked events, and leases that live in the event store's database therefore rewind together and stay correct. This is the documented default. Read models in another database, broker messages, emails, saga commands to other stores, and positions handed to clients are outside, and need §16.3's rewind detection.
- **Replicas are for reading.** A standby makes commits visible in WAL order, and the commit-order lock is held until after the commit record is flushed, so appends reach a replica in position order, only later. Projectors can read from replicas. `LISTEN` and `NOTIFY` do not work on a standby, and a notification can arrive before the replica has applied the commit, so projectors on replicas poll; `PgCommitSignal` connects to the primary, and its reconnect must reach the new primary after a failover.

The same rewind applies to SQLite and fjall restored from a backup.

### 16.3 Identity that survives a rewind (0.8)

Additive, no break; this is the 0.8 half of §16.

- **Event ids.** Every event gets a random 128-bit id (UUIDv7) assigned store-side at append; core gains the `EventId` value type and stays free of a clock and a random-number generator. 0.7.5 decided against an event id, correctly for a single store: the stream is the record. It breaks once a key crosses into another store or survives a rewind.
- **Saga keys on event ids.** `idempotency_key` is `"{saga}:{sequence}:{index}"` today. After an asynchronous failover of the source store, a different event can arrive at a reused sequence; its command carries the same key, and a target in another store drops it as already done. `"{saga}:{event_id}:{index}"` cannot collide. A saga whose target is in the same store as its source is unaffected either way: its commands rewind with their trigger.
- **Rewind detection.** A checkpoint records the last applied event's id beside its position. On resume, the driver reads the event at that position; a different id (or none) means history was rewritten, and the run ends with a new `RunError::HistoryRewritten` instead of diverging silently, as `FailurePolicy::Halt` does for a poison event. In-database checkpoints always pass, by §16.2. Truncation (0.7.6) removes events on purpose, so a store that truncates must report a position it cut as truncated, not missing, or a deliberate cut reads as a rewind. This rides on 0.8.1's checkpoint change.
- **Store identity** (0.8.2) and the **HLC stamp in metadata** (§15.5) ship in the same phase. Both are inputs to 0.9.

### 16.4 Slices (0.9)

**Assignment.** Every event carries a partition key, which defaults to its stream id. The store hashes it to a 16-bit bucket with a hash core defines (a few lines, no dependency, identical in Rust and SQL because the bucket is computed once and stored on the event). A store with `2^k` slices puts a bucket in slice `bucket mod 2^k`. `k` is chosen at creation and defaults to 0, one slice. Because the bucket is stored, a slice can later be split in two by doubling the count; the split takes effect at a recorded position and needs no rewrite of history. Split mechanics come after 0.9.

**Positions.** `Position { store, slice, sequence }` replaces `Sequence` wherever a position leaves the store: checkpoints, tokens, envelopes. Within a slice the sequence is gap-free and commit-ordered, as 0.8's is within a store. `StreamsAll` gains `slices()` and `stream_slice(slice, from)`; `stream_all` stays as the one-slice special case.

**Writers.** A single-stream append serializes only within its slice. On Postgres the slice's sequence moves from an identity column to a `slice_heads` row incremented in the append's transaction. That row lock serializes the slice and is held to commit, replacing the database-wide advisory lock; rollback returns the numbers, so the sequence is gap-free by construction and the 32-value sequence cache disappears. Appends to different slices commit in parallel, and group commit batches their flushes again.

**Live push has to change with it.** Postgres's `NOTIFY` takes a database-wide lock at commit (`PreCommit_Notify`), so an append path that notifies on every commit serializes all writers again. With more than one slice, `PgCommitSignal` stops notifying from the append: projectors poll, or a single notifier watches `slice_heads` and raises the hint at most once per interval.

**Clocks per slice.** §15.5 stamps the HLC inside the store's lock from the store's last stamp. With slices, one last-stamp row would be a store-wide serialization point again, so each slice is its own HLC node: its last stamp lives in its `slice_heads` row, and the server's clock is the physical part. Causality between slices still holds where it is propagated: a command carries the stamps it read, and the append advances past them.

**Batches.** `append_batch` takes the slice rows it touches in sorted order and stays atomic within one store. Streams that must commit together should share a partition key, and so a slice; that is Kafka's message key. A batch across slices is allowed but costs concurrency.

**Adapters.** In-memory, SQLite, and fjall implement slices as a column and a counter per slice. SQLite and fjall have one writer, so their slices add projector parallelism but no write parallelism: the same semantics with less concurrency. Core stays `no_std`.

### 16.5 Projectors over slices (0.9)

- **Checkpoints and leases are per slice.** `CheckpointStore` and the lease store are keyed by `(name, slice)`. A projector instance acquires leases on a set of slices and runs one `SubscriptionMachine` per slice; nothing in the machine changes. Adding or removing instances moves leases. Nothing is rebuilt (this replaces 0.8.4).
- **Order is per slice.** A projection sees each slice in order and the slices interleaved. A projection that needs order across slices opts into an HLC merge with a per-slice closed stamp (§15.5) and pays the latency of the slowest slice.
- **View rows.** `ViewRow::version` becomes a `Position`, and the newest-wins guard compares positions within a slice. A row that folds events from one slice (any row keyed by stream, which is the common case) keeps one guard. A row that folds events from several slices keeps a guard per slice it has seen. Inline views keep their 0.7.3 caveat: a row many slices write to is a contention point.
- **Read-your-writes** (0.8.3) carries the slice in its token and compares against the row's guard for that slice.

### 16.6 DCB under slices (0.9)

A dynamic consistency boundary is a query over tags, and tags cut across slices by design. In `enrollment.rs` a decision reads the events tagged `course:rust` or `student:ada`; they live in many streams and therefore many slices, and a write to any of them can invalidate the decision. So a DCB condition cannot be scoped by slice. It is scoped by tag.

**Tag locks, not slice locks.** On Postgres, as transaction-level advisory locks in a key space derived from the store:

- **Every append** takes *shared* locks on the tags and event types it writes, plus one store-wide key.
- **A conditional append** takes, for each item in its query, an *exclusive* lock on one key: one of the item's tags if it has any (a matching event must carry all of them, so its writer locked that tag), otherwise each of its event types; `Query::all` takes the store-wide key. It then checks and writes while holding them.
- Locks are taken in sorted order, tag and type locks before slice rows, so nothing deadlocks; a hash collision over-serializes and never deadlocks.

Plain appends never block each other; they wait only while a decision on one of their tags is in flight. Two decisions conflict only if their tags overlap: `rust/ada` and `rust/bob` serialize on `course:rust`, which is correct because they compete for seats, while `sql/cy` runs beside them. Contention follows the domain's, not the slice layout. The cost is a tag index (0005 matches tags on the decoded event today) and lock-table slots for tag-heavy batches (`max_locks_per_transaction`). SQLite, fjall, and in-memory have one writer and need no tag locks.

**`after` becomes a vector.** `AppendCondition.after` is one `Sequence` today. With slices it is the slice heads at read time, `{slice → sequence}`, and the check asks whether any matching event in slice `s` is above `after[s]`. That is safe because each slice still commits in order. An HLC cannot stand in for it: a writer with a lagging clock can commit a matching event after the read with a lower stamp, and the check would miss it. `QueryAppend::read` takes the same vector. The DCB specification permits equivalent functionality, so this changes its API, not its guarantee. With one slice the vector is one number, today's condition.

**Fold order is causal.** A decision folds its events in global order today, which no longer exists. Each event a decision appends is stamped above the highest HLC among the events it read, so everything a decision depended on sorts before what it produced. The decision read folds in `(hlc, slice, sequence)` order. Events that are concurrent and neither read the other fold in either order: if both had been conditional appends over overlapping queries, one would have failed and re-read.

### 16.7 Shards: writers beyond one database (0.10)

`ShardedStore` composes stores: it is an adapter over N stores, not a framework, and each shard is a store with its own identity, slices, and Postgres contract. It routes on a **shard tag**: the store designates one tag kind (`tenant:*`, `school:*`), every event carries exactly one, and a stream's events must all carry the same one (checked at append).

- **Batches** must stay within one shard; otherwise `CrossShard`.
- **DCB** queries must include the shard tag in every item, so the boundary lies in one shard. A query without it is rejected with `CrossShard` rather than checked against one shard and passed. Tags cut across every partitioning, so a boundary that reaches into two databases would need a distributed transaction, which Eventyr refuses. None of the DCB stores in the §14 scan (`disintegrate`, UmaDB, Marten) shard either. For most systems the tenant is the natural shard key: enrollment is per school, so its course and student invariants stay within one shard.
- **Cross-shard invariants** go through sagas: reserve in one shard, confirm in the other, compensate on failure.
- **Projectors** lease `(shard, slice)` units across the whole set; positions already name their store.
- Moving a tenant between shards (copy, then tombstone) is deferred.

### 16.8 What it costs

- **No total order across slices.** Code that reasons "N+1 came after N" across streams must use per-slice order or the HLC. This is the one thing the design gives up, and horizontal writers are impossible without giving it up.
- **A breaking change.** `Position` replaces `Sequence` in checkpoints, tokens, envelopes, view guards, and `AppendCondition`. Hence 0.9, not 0.8.
- **DCB scales only as far as its boundaries are narrow.** Tag locks make disjoint decisions parallel; a decision over `Query::all` serializes with every writer, and no decision crosses shards.
- **Hot streams stay hot.** One stream lives in one slice; many nodes writing it still multiply conflict retries. Routing commands for a stream to one node stays the caller's (§15.1).

### 16.9 Phasing

1. **0.8, additive:** CAS checkpoints with rewind detection (0.8.1), a per-store lock and store identity (0.8.2), read-your-writes tokens (0.8.3), event ids and saga keys on them (§16.3), the HLC stamp in metadata, and the deployment contract (§16.2).
2. **0.9, breaking, behaviour unchanged at one slice:** slices and `Position` in the vocabulary and every adapter; per-slice checkpoints and leases; the Postgres `slice_heads` sequence with per-slice serialization and live push off the append path; DCB's tag locks, `after` vector, and causal fold order.
3. **0.10:** `ShardedStore` with shard-tag routing; HLC-merged projections; slice splitting by doubling.

### 16 — what it is *not*

Still no framework. No consensus protocol, no cluster membership, no distributed transactions, no command routing. Coordination stays where it already lives: the database's locks within a store, leases between projectors, and sagas between shards. The caller still decides how many slices and shards to run, and where.
