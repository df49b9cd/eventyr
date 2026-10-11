# eventyr-projection

The schema-evolution and read-model layer: upcasters that upgrade stored
events on the projection read path, typed and raw sources, materialized
views (post-hoc and inline-in-transaction), and versioned rebuilds. It
sits on [eventyr-subscription](eventyr-subscription.md)'s runner — every
piece here is either a `SubscriptionSource`, a `Projection`, or a store
under one.

**Position:** the top read-side crate. Parent:
[ARCHITECTURE.md](../ARCHITECTURE.md); placement rationale in
[DESIGN.md §6](../../DESIGN.md#6-projections--subscriptions-eventyr-subscription-eventyr-projection).

## Features

* `inline` (pulls serde + serde_json) — `InlineView` and the JSON row
  erasure boundary.
* `testing` — `view_store_contract` for adapter tests.

## Upcasting (`chain.rs`, `registry.rs`)

Upcasting upgrades a stored event payload to the current schema *on read*.
The boundary is deliberate: **projection read path only** — aggregate
loads decode the stored shape directly. Upcasting is a projection concern;
a write-side upcaster would be a schema migration, which is a different
tool.

Two shapes for two styles:

* **`UpcasterChain<E>`** (`chain.rs`) — an ordered chain for one event
  type: `with(event_type, upcaster)` (latest registration for a type
  shadows earlier — one chain, one authority), `upcast(&mut RawEvent)`
  rewrites the payload in place. `ClosureUpcaster` adapts a function.
  The chain itself implements `Upcaster`, so it composes.
* **`UpcasterRegistry`** (`registry.rs`) — for multi-version event sets:
  `VersionUpcaster`/`VersionRung` describe `(event_type,
  schema_version) → upcaster` steps; `build()` **validates the ladder** and
  refuses (`RegistryError::{DuplicateRung, DanglingVersion, MissingBase,
  UnknownType, UnknownVersion}`) a chain that skips a version or dangles.
  Loud at build time, because a silent gap would be a wrong projection.
* **`UpcastingSource<S, E>`** (`source.rs`) — a `SubscriptionSource`
  decorator: fetch raw, upcast, decode. Failures surface as
  `StoreError::other("upcasting sequence N: …")` — loud, no dead-letter;
  a projection that can't upcast must stop, not skip
  ([ARCHITECTURE.md §10](../ARCHITECTURE.md#10-the-failure-model)).
  `VersionedSource<S>` is the registry-driven twin.

## Views (`view.rs`)

* **`View<E>`** — a fold with `initial()` and `apply(&mut self, &Event)`:
  the projection that maintains one row's value.
* **`ViewRow<V>{version: Sequence, value: V}`** — the stored row, stamped
  with the last event sequence that produced it.
* **`ViewStore<V>`** — `load(name, id) -> Option<ViewRow>`, `save(name,
  id, row)`; **newest-wins by sequence** — a late write with an older
  sequence must not clobber. `InMemoryViewStore` is the reference; the
  adapters ([postgres](eventyr-store-postgres.md),
  [sqlite](eventyr-store-sqlite.md)) ship durable ones.
* **`ViewProjection`** — the `Projection` adapter that ties them: fold
  events, read the current row, apply, save. Idempotent by construction
  under at-least-once redelivery (the row's version gates the fold's
  effect).
* `view_store_contract` (behind `testing`) gates durable stores.

## Inline views (`inline.rs`, `inline` feature)

Views folded **inside the append transaction**, so a view row and its
event commit together — read-your-writes without a projector hop.

* `InlineView<E>` — `Event` + row-updating logic; `Inline<V, K>` is the
  serde-backed impl (value `V`, key `K`).
* `InlineViews<E> = Arc<[Arc<dyn InlineView<E>>]>` — the set a store
  carries; adapters expose `with_inline_views(..)` on their stores
  (Postgres, SQLite).
* The **JSON erasure boundary**: inside the transaction, rows move as
  `serde_json::Value` (`StoredRow{version, payload}`), keyed
  `RowKey = (String, String)`; `rows_touched` reports what folded;
  `fold_inline` is the shared fold. Adapter-specific failures surface as
  `InlineViewError`.
* The cost: the append transaction grows by the view folds. The bank and
  `inline_view` examples show the trade.

## Rebuilds (`rebuild.rs`)

Versioned re-projection after a schema change:

* `SchemaVersion(u64)` (display `"v7"`); `checkpoint_key(name, v)` →
  `"{name}@v{N}"` — a rebuild checkpoints under its own key, so the old
  projection's checkpoint is untouched and a rollback is possible.
* `SchemaCheckpointStore` — blanket impl over any `CheckpointStore`.
* `RebuildPlan<E>::new(name, version, chain)` — `.with_policy()` (forces
  `stop_at_catch_up`), `.checkpoint_key(..)` override, `.projector(source,
  checkpoints, projection)` — build the projector that re-projects from
  the beginning under the new key.

## Testing

`view_store_contract` for durable stores; the upcaster chain/registry
have exhaustive unit tests over the ladder rules; the
`ProjectionScenario` harness in
[eventyr-subscription](eventyr-subscription.md) covers the fold loops;
the `bank` example exercises upcasting end to end (a `v2` event read
through a chain).

## Limits

* No dead-letter for upcast failures — deliberately loud; if you need
  parking, park at the projection layer with
  [subscription](eventyr-subscription.md)'s `ParkedStore` around the whole
  projection.
* Inline views are the only write-path projection; there is no
  "background view with a lock" mode (inline's transaction *is* the lock).
* Roadmap 0.8's partitioned-projection work was superseded by slices
  (0.9) — rebuild plans are the interim tool for big re-projections.
