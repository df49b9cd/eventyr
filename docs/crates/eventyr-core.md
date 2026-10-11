# eventyr-core

The pure heart of the workspace: the domain seam (`Aggregate`), the protocol
vocabulary every other crate speaks, and the five state machines. Everything
else in eventyr is a driver over a machine defined here, a port implied by
one, or an adapter implementing a port.

**Position:** the bottom of the graph — no required dependencies, depended
on by every other crate. Parent: [ARCHITECTURE.md](../ARCHITECTURE.md).

## Build profile

* `no_std` + `alloc` (a tiny `__private::Vec` alias keeps the door open for
  a custom allocator story). CI builds it for `thumbv7em-none-eabihf`, with
  `serde` and `time` on, to prove the claim.
* Workspace lints: `unsafe_code = forbid`, `missing_docs = warn` — every
  public item is documented.
* Features (all off by default):
  * `time` — adds `Metadata::timestamp` and the machinery the stores use to
    fill it (`load_until` on the repository is in eventyr-store, but the
    vocabulary it reads is here).
  * `macros` — re-exports the derives from
    [eventyr-macros](eventyr-macros.md) into the prelude.
  * `serde` — derives for the vocabulary types.
* Dev-dependency on `proptest`; the machine suites run under `miri` in CI
  with `PROPTEST_CASES=4`.

## Module map

| Module | Contents |
|---|---|
| `aggregate` | `Aggregate`, `AggregateId` (blanket), `Optional` |
| `vocabulary` | `StreamId`, `Version`, `Sequence`, `ExpectedVersion` |
| `envelope` | `EventEnvelope`, `Metadata`, `NewEvent` |
| `error` | `StoreError`, `ProtocolError`, `UpcastError` |
| `event_name` | `EventName` (the stored `event_type` name) |
| `metrics` | `Metrics`, `NoopMetrics`, instrument `names` |
| `write` | `WriteMachine` and its protocol |
| `batch` | `BatchMachine`, `Fold`, `Decide`, `RoutedDecision` |
| `boundary` | `BoundaryMachine`, `Tag`, `Tagged`, `Query`, `AppendCondition`, `Decision` |
| `subscription_machine` | `SubscriptionMachine`, `Checkpoint`, `Batch`, `SubscriptionPolicy`, `FailurePolicy` |
| `saga` | `SagaMachine`, `Saga`, `SagaCommand` |
| `snapshot` | `Snapshot`, `SnapshotPolicy`, `WritePolicy`, `HasSnapshotState` |
| `upcast` / `version_registry` | `RawEvent`, `Upcaster`, `EventSchemaVersion` |
| `testing` | scripted drivers, `Scenario`, the `account` fixture |

The prelude re-exports all of it (derives included, under `macros`).

## The domain seam

```rust
trait Aggregate {
    const NAME: &'static str;
    type Id: AggregateId;
    type State;
    type Event;
    type Command;
    type Error: Debug + Display;
    fn initial(&Self::Id) -> Self::State;
    fn apply(&mut Self::State, &Self::Event);
    fn decide(&Self::State, &Self::Command) -> Result<Vec<Self::Event>, Self::Error>;
}
```

`decide` and `apply` are both pure — no clock, no I/O, no randomness beyond
what the caller injects. That purity is what lets the repository fold state
deterministically and lets the machines re-run `decide` on retry without
side effects. `AggregateId` is a blanket impl over `Clone + Eq + Hash +
Debug + Display`, so domain ids need no ceremony. `Optional` covers the
"state may not exist yet" shape with `apply_state(&mut Option<Self>, &Event)`.

`StreamId::for_aggregate::<A>(&id)` is the naming convention
`"{NAME}-{id}"` — the repository, the snapshot machinery, and the DCB
fixtures all rely on it being stable.

## The five machines

The machine rules are in [ARCHITECTURE.md §1](../ARCHITECTURE.md#1-the-shape-of-the-library-pure-machines-thin-drivers);
this table is the per-machine protocol reference.

### `WriteMachine` (`write`)

* `WriteMachine::new(id, command, retry)` — plain; `.with_snapshots(id,
  command, retry, SnapshotPolicy)` — snapshot-aware (state type `S`, base
  store state `SS`).
* `start()` → actions; `handle(input)` → actions.
* Actions: `LoadStream{stream_id, from}`, `LoadSnapshot{stream_id}`,
  `Append{stream_id, expected, events: Vec<NewEvent>}`,
  `Done(WriteOutcome)`.
* Inputs: `Loaded{events}`, `SnapshotLoaded{snapshot}`,
  `Appended{committed}`, `Conflict{current}`, `Failed(StoreError)`
  (with `From<StoreError>` mapping `Conflict` → `Conflict{stream_id: None}`).
* Outcomes: `Committed{committed, snapshot: Option<OfferSnapshot>}`,
  `AlreadyCommitted{committed}`, `Noop`, `Rejected(err)`, `Failed(err)`.
* Internal phases: `LoadingSnapshot` / `Loading` / `Appending` / `Done`.

Invariants the machine enforces on `Loaded`:

* Every envelope's `stream_id` must match the requested stream — a store
  answering with another stream's events is a protocol violation.
* Versions must be contiguous from the load point (this doubles as the
  snapshot monotonicity guard: a snapshot may never be followed by a hole).
* A conflict reported at or below the already-folded version is likewise a
  violation (the store lied about which version it holds).
* Retry reloads **only the delta** — from the last known version, not
  from zero — and re-runs `decide`.
* On `Appended`, if snapshots are on and the policy is due
  (`committed - base >= every`), the outcome carries an `OfferSnapshot`;
  the **driver** persists it fire-and-forget. A keyed idempotent replay
  (`AlreadyCommitted`) never offers a snapshot.

`RetryPolicy{max_retries}` defaults to 3; `NEVER` disables retries. The
internal `RetryBudget` is what the machine counts down.

### `BatchMachine` (`batch`)

Multi-stream decide, atomic append.

* `Fold<E>` — how to fold one stream's events into a state box:
  `AggregateFold<A>` folds an aggregate's state for `A::Id`; `NoFold`
  collects nothing (the decider only needs the events' presence, not folded
  state).
* `Decide<E, Err>` — `decide(&self, states: &BTreeMap<StreamId, Box<dyn Any
  + Send>>, &Command) -> BatchDecision`. A `RoutedDecision` builder routes
  events to streams (`of(stream, events)` / `to(stream, …)`), `reject`s,
  or `noop`s.
* `BatchMachine::new(streams, folds, decide, command, retry)` — sorts and
  deduplicates the stream list; `from_decider` and
  `for_aggregates::<A>(decider, retry)` (via `AggregateBoundary<A>`) are
  the convenience constructors. The latter makes the consistency boundary
  *dynamic*: the decider names which aggregate streams a command touches.
* Actions: `LoadStreams{streams, from: BTreeMap}`, `AppendBatch{appends}`,
  `Done`. Inputs: `Loaded{stream_id, events}` (one per stream),
  `Appended{committed}`, `Conflict{stream, current}`, `Failed`.
* Outcomes: `Committed`, `AlreadyCommitted`, `Noop`, `Rejected`, `Failed`.
* A conflict with no stream attributed goes to the first stream in the
  (sorted) list; events routed outside the declared stream set are a
  protocol violation — the boundary is the contract.

### `BoundaryMachine` (`boundary`)

Dynamic consistency boundaries (DCB) — decide against *other* streams'
history selected by a query.

* `Tag::of(kind, value)` → `"kind:value"`; `Tagged::tags(&event) ->
  Vec<Tag>` is a **pure function of the event**. Tags are never stored;
  they can't drift.
* `QueryItem{types: Vec<&'static str>, tags: Vec<Tag>}` — types OR tags
  within an item; `Query{items}` — items OR'd. `Query::of/types/any/or`,
  `matches(event)` tests a decoded event.
* `AppendCondition{query, after: Sequence}` — "append only if nothing
  matching `query` landed after `after`".
* `Decision` trait: like `Aggregate` plus `query()` (the boundary) and
  `validation()` (defaults to `query()`; a narrower re-check on retry).
* Actions: `Read{query, after}`, `Append{appends, condition}`, `Done`.
  Appends inside a boundary use `ExpectedVersion::Any` — the condition, not
  the stream version, is the concurrency token.
* Inputs: `Read{events}`, `Appended`, `Conflict{sequence}`, `Failed` —
  `From<StoreError>` maps `QueryConflict` → `Conflict{sequence}`, anything
  else → `Failed`.
* `acknowledged` moves the condition's `after` past a conflicting event
  that lies outside the fold query: the boundary re-validates against a
  fresh `after` instead of rejecting outright.
* `enrollment` is the worked fixture (courses with a two-student cap) used
  by `query_append_contract` in
  [eventyr-store-testing](eventyr-store-testing.md).

### `SubscriptionMachine` (`subscription_machine`)

* `Checkpoint(Sequence)`, `ORIGIN` = the start.
* `Batch<E>{events, upper: Option<Checkpoint>, scanned: Option<Checkpoint>}`
  — `upper` is the batch's commit bound; `scanned` is the filter-scan
  bound (filtered reads ack past unmatched runs).
* `SubscriptionPolicy{batch_size: 128, idle_sleep: 100ms, retry_sleep: 1s,
  stop_at_catch_up: false, on_failure: Halt}` — `FailurePolicy::Halt` is
  the default; `Park{retries}` moves poison events to a `ParkedStore`.
* Actions: `Fetch{from, limit}`, `Apply{envelope}`, `Park{envelope,
  attempts, error}`, `Ack{checkpoint}`, `Sleep{for_, reason}`, `Done`.
  `SleepReason::Idle` may be cut short by a wake signal; `Backoff` may not.
* Inputs: `Fetched`, `Applied`, `ApplyFailed`, `Parked`, `ParkFailed`,
  `Acked`, `AckFailed`, `Slept`, `Failed`, `Shutdown`.
* Outcomes: `Stopped{checkpoint}`, `CaughtUp{checkpoint}`, `Failed`.
* `Shutdown` semantics: drains the in-flight batch (applies and acks it),
  performs one closing re-read to catch a commit racing the stop, tolerates
  exactly one failure during shutdown, and always ends `Stopped`.

Proptest invariants (mirri'd in CI): never panics on any input
interleaving; `Shutdown` always ends in `Stopped`; the checkpoint never
regresses and moves only on `Ack`.

### `SagaMachine` (`saga`)

The smallest machine: `Saga::react(&self, &EventEnvelope) -> Vec<(StreamId,
Command)>` for one event; the machine dispatches each target (with
`SagaCommand` metadata) and finishes. Idempotency keys are
`idempotency_key(saga, event, index)` → `"{name}:{sequence}:{index}"`, so a
redelivered reaction re-dispatch collides in the store and comes back
`AlreadyCommitted`. Long-running sagas are *not* a persisted process: the
log is the state, each reaction a fresh dispatch — see
[eventyr-subscription](eventyr-subscription.md).

## Supporting types

* **`snapshot`**: `Snapshot<S>{stream_id, version, state}`;
  `SnapshotPolicy{every: NonZeroU64}` with `is_due(base, committed)`;
  `WritePolicy` bundles retry + snapshot policy; `HasSnapshotState` is a
  blanket impl over `Clone` states (any `S: Clone` can snapshot).
* **`metrics`**: `Metrics` (counter/gauge/histogram, `&self`,
  `Send + Sync`), `NoopMetrics`, and the stable `names` module —
  `eventyr_appends_total`, `eventyr_conflicts_total`,
  `eventyr_snapshots_total`, `eventyr_projected_events_total`,
  `eventyr_parked_events_total`, `eventyr_projection_fetch_span`,
  `eventyr_append_seconds`, `eventyr_project_batch_seconds`. Backends ship
  elsewhere ([eventyr-store](eventyr-store.md) has `TracingMetrics`).
* **`upcast`**: `RawEvent{event_type, schema_version, payload: Vec<u8>}` —
  the wire shape before decoding; `Upcaster<E>` upgrades a `RawEvent` to a
  typed event. **`version_registry`**: `EventSchemaVersion(u32)` (with
  `V1`) and the `VersionedRaw` alias the
  [projection](eventyr-projection.md) registry keys on.
* **`error`**: the `StoreError` taxonomy — see [ARCHITECTURE.md
  §10](../ARCHITECTURE.md#10-the-failure-model). `StoreError::other(msg)`
  and `StoreError::protocol(..)` are the constructors drivers use;
  `is_protocol_violation()` separates driver-detected violations from store
  failures.

## `testing` module

The in-crate test kit, public so downstream machines can be tested the same
way:

* **Scripted drivers** — `scripted`, `drive_scripted`,
  `projector_scripted`, `batch_scripted`, `boundary_scripted`: answer a
  machine's inputs from a script, assert its actions. A worked example of
  driving a machine by hand with no runtime at all.
* **`Scenario`** — `Scenario::<A>::given(&id, history).when(&command)` →
  `Outcome` with `then_events`, `then_none`, `then_error`,
  `then_error_matching`, `then_result`, `with_state` (pre-command state).
* **`account` fixture** — the canonical aggregate
  (open/deposit/withdraw/check-balance) used across the workspace's tests;
  the `enrollment` fixture in `boundary` plays the same role for DCB.

## Invariants summary

1. Machines never panic; violations end `Done(Failed(Other(ProtocolError)))`.
2. After `Done`, any input is a violation.
3. `Loaded` events must be from the requested stream and contiguous.
4. The checkpoint moves only after the whole batch applied, and never
   regresses.
5. `decide` is pure; retry re-runs it on fresh state.
6. Tags derive from events; nothing about them is stored.

## Limits

* Upcasting vocabulary lives here, but upcasting runs only on the projection
  read path ([eventyr-projection](eventyr-projection.md)); aggregate loads
  decode the stored shape directly, by design.
* Roadmap 0.8: saga keys move from `(sequence, index)` to event ids;
  0.8.3 adds read-your-write tokens — both will touch this crate's
  protocols.
