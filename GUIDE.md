# Eventyr — a newcomer's guide

Eventyr is event sourcing for Rust: **pure machines, thin drivers**. The
domain is two pure functions; every multi-step interaction — the write
path, projection checkpointing — is a sans-IO state machine you can test
without a database and drive under any runtime. This guide walks the
concepts in the order you'd meet them, linking out to the API docs, the
examples, and DESIGN.md (the design record, and the constitution of
this workspace) for the full argument.

## 1. What this is

Event sourcing stores *what happened* — an append-only log of domain
events — and derives everything else from it: current state by folding
a stream, read models by projecting the log, and integrations by
reacting to events. Eventyr is a library of small, composable traits for
that shape, not a framework: no HTTP server, no message bus, no actor
runtime (DESIGN.md §1–§2 name the non-goals and the libraries each
design decision learned from).

Everything you write is yours: the aggregate, the events, the
projections. Everything Eventyr provides is a trait you implement or a
machine you drive.

## 2. The domain: two pure functions

An aggregate is `apply` and `decide` — [§4.1](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#41-aggregate--the-functional-core)
of the design, in code:

- `apply(state, event)` folds one event into state. It cannot fail —
  events are facts.
- `decide(state, command)` chooses which events a command produces, or
  rejects it. Pure: no I/O, no clocks, no dependencies smuggled in.

State is rebuilt by folding, never stored on the write path (the
`State = Option<T>` "doesn't exist yet" shape has an
[`Optional`](https://github.com/df49b9cd/eventyr/blob/main/eventyr-core/src/aggregate.rs)
helper; `#[derive(Aggregate)]` can wire the trait and generate the
event enum — sugar, everything stays hand-writable).

Because both functions are pure, domain tests are plain asserts — and
[`Scenario`](https://github.com/df49b9cd/eventyr/blob/main/eventyr-core/src/testing.rs)
gives them given/when/then shape with no store, no async, no mocks:

```rust,ignore
Scenario::<Account>::given(&id, [AccountEvent::Opened])
    .when(&AccountCommand::Deposit { amount: 50 })
    .then_events(&[AccountEvent::Deposited { amount: 50 }]);
```

## 3. Writing: the machine behind `execute`

`AggregateRepository::execute(id, command)` runs the whole write
interaction: load the stream → fold → `decide` → append, retrying on an
optimistic-concurrency conflict. Underneath it is not an async loop but
a **pure state machine** — [§4.5](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#45-the-write-machine--sans-io-core-of-the-repository):
the machine consumes results and emits actions as data
(`LoadStream`, `Append`, `Done`); a driver performs the I/O. The
concurrency-critical 10% of the library is therefore tested as pure
data, and the same machine runs under tokio, blocking code, or a
scripted test harness.

You'll rarely drive the machine yourself — `execute` and its
`ExecutionOutcome` (committed events, or the domain's rejection) are the
usual surface, with `execute_with_metadata` stamping correlation and
idempotency ids across a whole interaction. Reach for the machine
directly when you need the protocol itself: custom drivers, scripted
replay in tests ([§7](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#7-machine-modeling-rules-sans-io-discipline)
is the discipline), or metrics at every transition.

The [bank example](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/bank.rs)
runs one domain through this path end to end — a runnable binary whose
asserts gate CI.

## 4. Picking a store

The store is a trait — `EventStore` plus the optional `StreamsAll` the
read side needs ([source](https://github.com/df49b9cd/eventyr/blob/main/eventyr-store/src/store.rs)) —
not a choice you're locked into:

- **In-memory** for tests — the `Scenario` and contract suites run on it.
- **SQLite** (`eventyr-store-sqlite`) — embedded; the reference port for
  the contract suite every third-party store must pass.
- **fjall** (`eventyr-store-fjall`) — embedded, no server, no runtime
  needed to drive.
- **Postgres** (`eventyr-store-postgres`) — the production store:
  migrations compiled in, optimistic concurrency, global ordering
  enforced by a commit-order lock, inline views, leases. Its crate docs
  state the [deployment contract](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#162-the-postgres-deployment-contract)
  (single writable primary, synchronous replication for failover without
  loss) — read it before running it against user-facing traffic.

A third-party store earns compatibility by passing the contract suites
in `eventyr-store-testing`, not by being on an approved list. Snapshots
are opt-in per repository (`with_snapshots`) when streams grow long.

## 5. Reading: projections

A projection folds the global event stream into a read model
([§6](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#6-projections--subscriptions-eventyr-subscription-eventyr-projection)):
you implement `Projection::apply`, and the `Projector` runner delivers
batches, persists the last-acked checkpoint (restarts resume), and
spaces its polls. The contract is **at-least-once**: `apply` may see the
same envelope twice, so it must be idempotent — key state by
`(stream, version)`, or wrap the projection in
[`SkipRedelivered`](https://github.com/df49b9cd/eventyr/blob/main/eventyr-subscription/src/runner.rs)
when it can't.

The runner you spawn is yours — Eventyr ships the `Projector` helper,
not the event loop. Production settings live here too: a `CommitSignal`
wakes a projector on commits instead of the idle timer;
`FailurePolicy::Park` records poison events (ones your projection keeps
rejecting) in a `ParkedStore` and advances — list them, fix the
projection, replay; `Halt` (the default) stalls the projection until a
human looks.

## 6. Views: query it now

Projections are eventually consistent. When a query must see a write
immediately, a **view** is the answer — one row per id, folded from the
events: `View`/`ViewProjection` over a `ViewStore` (Postgres, SQLite),
run by the same `Projector`. The **inline** variant writes the row
inside the append's own transaction, so a read straight after the write
sees it — keep inline views cheap; they run inside the append's
serialized section. The same fold defines both, so a view moves
between async and inline without a rewrite. The
[inline_view example](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/inline_view.rs)
shows both halves on SQLite.

## 7. Reactions: sagas

A saga reacts to events with commands against other streams: you write
the pure `Saga::react` — event in, `(target stream, command)` pairs out
— and `SagaProjection` runs it inside the subscription runner, so it
inherits at-least-once delivery, checkpoints, and leases. Because a
redelivered event re-issues its commands, each one carries an
idempotency key (`"{saga}:{sequence}:{index}"`) and the write machine
ends `AlreadyCommitted` instead of double-charging — idempotency stays
each command's to keep. This is the version of process managers that
respects the no-framework rule: the protocol ships, the transport stays
yours ([§13's 0.6.1](https://github.com/df49b9cd/eventyr/blob/main/DESIGN.md#061--sagas--process-managers-as-a-machine)).

## 8. Beyond one stream

Two shapes when an invariant spans streams:

- **A fixed boundary** (`BatchMachine`): the caller names the streams;
  each is guarded by its own version; one atomic append. The cross-account
  transfer in the bank example.
- **A dynamic boundary** (`BoundaryMachine`, DCB): events carry derived
  *tags*; a decision reads what its *query* selects, folds, and appends
  under the condition that nothing matching arrived since the read.
  Boundaries follow the domain, not the aggregate layout. The
  [enrollment example](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/enrollment.rs)
  is the canonical demo; the
  [loan_eligibility example](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/loan_eligibility.rs)
  shows the narrowed validation query and the blocking driver.

## 9. Running it in production

- **One projector per checkpoint name.** Leases (0.7.9) enforce it: a
  projector holds a lease, renewed on batch boundaries; a second copy
  fails `Taken`, a lost lease ends `LeaseLost` with the resume
  checkpoint. In-memory lease stores for single-process; `PgLeaseStore`
  for multi-node.
- **Conflicts are the concurrency story.** Writers serialize per
  stream; any node can execute any command; retries are bounded. The
  [distributed example](https://github.com/df49b9cd/eventyr/blob/main/eventyr/examples/distributed.rs)
  shows the shape — three nodes, one leased projector, failover —
  embedded on SQLite and clustered on Postgres through the same code.
- **Erasure** is crypto-shredding (`eventyr-shred`): personal fields
  sealed per data-subject, keys outside the log; deleting a key erases
  without rewriting history. It is not access control —
  [SECURITY.md](SECURITY.md) scopes it honestly.
- **Observability** is a port: implement `Metrics` (or take
  `TracingMetrics`) at the driver boundaries; `eventyr_parked_events_total`
  above zero is the one to alert on.

## 10. Where the design lives

Every section above compresses a DESIGN.md section that states the
whole argument — alternatives weighed, the field scanned, costs named:

| Topic | Section |
|---|---|
| Design goals and non-goals | §1–§2 |
| The aggregate and its purity | §4.1 |
| Versions, conflicts, the store error | §4.2–§4.3 |
| The write machine (sans-IO) | §4.5 |
| The store traits and drivers | §5 |
| Projections and subscriptions | §6 |
| The machine-modeling rules | §7 |
| Derives | §8 |
| The Postgres store | §9 |
| Testing discipline | §10 |
| What we took from each library | §11 |
| The roadmap (shipped vs planned) | §12 |

The [CHANGELOG](CHANGELOG.md) records what ships per milestone;
[CONTRIBUTING.md](CONTRIBUTING.md) is how to add to it.
