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

Feature flags on the umbrella crate mirror the crates: `macros` (default), `store` (default), `postgres`, `projection`, `subscription`. This is the `eventcore`/`eventide` workspace pattern — it keeps the core dependency-free and lets users pay only for what they use.

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

This is `katha`'s `make_handler` / `sourcerer`'s `GenericRepository` / `eventcore`'s executor — all converged on the same protocol — but with the protocol *extracted* into a testable machine instead of an async loop.

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

- **At-least-once delivery, idempotent apply** — the only honest contract (thalo's guarantee, EventStoreDB's model).
- **Checkpointed catch-up subscriptions** over any `StreamsAll` store (eventually's `Subscription::checkpoint/resume`): the projector runner persists the last-acked global sequence, so restarts resume without reprocessing.
- **The projector is also a machine** — `ProjectorMachine`, per the canonical table in §7; the runner is just its driver. This is where sans-IO pays off most: at-least-once semantics, redelivery after failure, and resume-from-checkpoint are exactly the kind of multi-step, failure-prone protocol that should never be an untestable async loop. The implemented machine (`SubscriptionMachine` in `eventyr-core`, named for its §6-facing role) adds the table's *implied* steps as explicit actions: `Fetch` is the poll §6's `Subscription::poll` implies (`Batch` must be requested), and `Slept` is the driver's answer to `Sleep` — no clock in the machine. That delta between the table's four names and the implemented protocol is intentional.
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
| `WriteMachine` | `Loaded`/`Appended`/`Conflict`/`Failed` | `LoadStream`/`Append`/`Done` | load→fold→decide→append, conflict retry |
| `ProjectorMachine` | `Batch`/`ApplyFailed`/`AckFailed` | `Apply`/`Ack`/`Sleep`/`Done` | at-least-once apply, checkpoint, resume |
| `SnapshotMachine` (0.3) | `Loaded`/`Appended` | `LoadSnapshot`/`LoadDelta`/`Append` | snapshot + delta rebuild |

Formally, the aggregate is itself a single-step state machine — `apply` *is* its transition function, `decide` its output function. Eventyr reserves the word *machine* for multi-step, driver-facing **interaction protocols** around the domain: many round-trips with the outside world, not one pure step. The distinction is step count, not formal kind.

## 8. Derive macros (eventyr-macros)

`#[derive(Aggregate)]` sits on the aggregate *marker* struct (a unit struct — the aggregate type is a namespace, not a state holder) and wires the `Aggregate` impl by convention: `NAME` is the snake_cased struct name, `Id`/`Event`/`Command`/`Error` follow the `{Ident}...` position convention, `State` is `Self` (point `state` at a type for the marker-plus-state shape), `initial` is `Default::default()` — or `Self` when a unit struct is its own state — and `apply`/`decide` delegate to same-module free functions. Every convention is overridable with `#[eventyr(...)]` attributes (`name`, `id`, `state`, `event`, `event_enum`, `command`, `error`, `initial`, `apply`, `decide`, `crate`); `crate` retargets the generated code at the umbrella crate (the serde `crate = "..."` pattern), and `initial` sees the id as `id`.

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
- **`TestScenario`** (0.2): `given(events).when(command).then(events)` — the API that makes the library feel good, borrowed from eventcore's testing crate.
- **Verification discipline** (mnesis's bar): proptest for machine invariants (e.g. "a machine that receives `Appended` always emits `Done`", "checkpoint never regresses"), `miri` in CI for the `no_std` core, `trybuild` for macro diagnostics.

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

- **0.1** — shipped. Core traits + protocol vocabulary, `WriteMachine` with transition tests, in-memory store, async + scripted drivers, repository wrapper, derive macros.
- **0.2** — partially shipped: Postgres store + migrations (`eventyr-store-postgres`), upcasters (upcast vocabulary in core, chains and raw→typed sources in `eventyr-projection`), checkpointed subscriptions (`SubscriptionMachine`, the projector runner). Pending: `TestScenario` (`given(events).when(command).then(events)`).
- **0.3** — partially shipped: the projection read path (upcaster chains, schema-versioned rebuilds), `EventBus` trait (behind `eventyr-subscription`'s `bus` feature). Pending: snapshot support (opt-in, `SnapshotMachine`). Single-feed fan-out to N projections on one checkpoint, if ever built, is a combinator over the existing `SubscriptionMachine` (`Fanout` multiplexing the one `Apply` action to N idempotent members) — never a new machine, and deferred until a "must advance together" use case justifies it.
- **0.4+** — multi-stream commands (eventcore-style), embedded stores (fjall/sled) + the blocking driver, contract-test crate.

Multi-stream commands deliberately come *last*: they complicate the mental model, and Eventyr's identity is "small, composable, boring in the good way". Ship the single-stream core first; add `StreamResolver`-style dynamic boundaries once the core is proven.
