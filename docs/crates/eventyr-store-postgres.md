# eventyr-store-postgres

The durable, multi-node store: `PgStore<E>` over sqlx, its schema and
migrations, the `NOTIFY`-based commit signal, and the durable auxiliary
stores (snapshots, views, checkpoints, projector leases). This is the
adapter for the deployment where more than one process touches the log.

**Position:** adapter over core + store (and subscription/projection for
the auxiliary stores, behind features). Parent:
[ARCHITECTURE.md](../ARCHITECTURE.md); concurrency model in
[ARCHITECTURE.md §9](../ARCHITECTURE.md#9-concurrency-and-ordering-per-store).

## Features

* `time` — fill `Metadata::timestamp` from the database clock.
* `snapshots` — `PgSnapshotStore<S>`.
* `views` (pulls projection's `inline`) — `PgViewStore<V>` + inline views in
  the append transaction.
* `checkpoints` (pulls subscription) — `PgCheckpointStore`.
* `leases` (pulls subscription) — `PgLeaseStore`.
* `uuid` — uuid plumbing for lease holders.

sqlx is compiled with `runtime-tokio`, `tls-rustls`, `postgres`, `json`,
`migrate`, `macros` — the adapter is async-first; use the async drivers.

## The store

`PgStore<E>` — `new(pool)` (schema must already be migrated) or
`connect(url)` (runs migrations). `pool()` hands back the pool;
`with_inline_views(Vec<Arc<dyn InlineView<E>>>)` attaches transactional
view folding. Errors: `PgStoreError::{Payload, CorruptRow, Db(sqlx::Error),
Migrate}` — payload decode failures and corrupt rows are the store's own
taxonomy before crossing into `StoreError` (`CorruptRow`/`Payload` become
`Other`; `Db` errors that look like connection loss become
`Unavailable`).

Port support: `EventStore`, `StreamsAll` (with the filtered-read
**override**), `QueryAppend`, `StreamLifecycle`, `CommitSignal`. Single
`append` is **one autocommitted `append_events` procedure call** —
the shortest possible critical section — unless inline views are attached,
in which case it opens a transaction so rows and events commit together.
`append_batch` takes per-stream locks, then the commit-order lock, then
writes (below).

## Schema and migrations

Twelve migrations, `eventyr_` prefix, applied by `connect`/`migrate`:

| # | What it adds |
|---|---|
| 0001 | `events` table + `append_events` procedure |
| 0002 | snapshots table |
| 0003 | views table (`view_name, view_id, version, payload`) |
| 0004 | conflict hint carries the stream name |
| 0005 | index `(event_type, global_sequence)` — the DCB read prefilter |
| 0006 | commit-order advisory lock |
| 0007 | `pg_notify('eventyr_commits', …)` in the commit path |
| 0008 | idempotency key (8-arg procedure; old one dropped) |
| 0009 | stream lifecycle (`closed`, `first_kept`, `head`) |
| 0010 | helpers: `eventyr_lock_stream`, `eventyr_commit_order_lock`, `eventyr_check_open`, `eventyr_stream_head`, `eventyr_check_expected` |
| 0011 | checkpoints |
| 0012 | projector leases |

Key columns on `events`: `global_sequence`, `stream_id`,
`stream_version`, `event_type`, `payload` (JSONB), causation/correlation/
idempotency, `created_at`, unique `(stream_id, stream_version)`.

## Concurrency model

The invariants ([ARCHITECTURE.md §9](../ARCHITECTURE.md#9-concurrency-and-ordering-per-store)):
per-stream versions unique and gapless; sequences global and
append-ordered. The mechanism:

1. **Commit-order lock** — `eventyr_commit_order_lock()` wraps
   `pg_advisory_xact_lock(7300160413598463541)`. Every sequence
   allocation passes through it, so `global_sequence` is handed out in
   one place. Transaction-scoped, so it releases at commit — it
   serializes the allocation window, not the transaction.
2. **Per-stream locks first** — `eventyr_lock_stream(hashtext(stream_id))`
   for every stream in the batch, **acquired in sorted order** (the
   deadlock-avoidance rule), then the commit-order lock, then the writes.
3. **`append_if`** — same order: stream locks → commit-order lock →
   condition scan → writes, all in one transaction. No matching event can
   land between check and write.
4. **Reads** — `REPEATABLE READ READ ONLY` transactions: each page reads
   a consistent snapshot; whole append batches appear atomically (the
   visibility rule); no read locks.
5. **Lifecycle** — close/truncate under the stream lock, with head and
   `first_kept` maintained so appends continue and old-position reads
   answer `Truncated`.

Conflict signaling: `append_events` raises SQLSTATE `P0001` with a hint
`"{stream}:{version}"`, which the adapter maps to
`StoreError::Conflict{stream_id, current}`; the custom `EV001` maps to
`StreamClosed`. The text convention means a DBA reading a failure sees the
same data the machine will.

## Reads

* `stream(&id, from)` — one stream, pages via keyset on
  `(stream_id, stream_version)`, `QUERY_PAGE = 512`; truncation checked
  up front (`Truncated{first}`).
* `stream_all` — keyset on `global_sequence`, page 512, snapshot
  transaction per page.
* **Filtered override** — `stream_all_filtered` pushed into SQL: the
  event-type index (0005) prefilters; **tags match on the decoded event**
  (they're derived, not stored), so the type predicate narrows and the
  tag predicate is evaluated in Rust. Stream-prefix matching is
  byte-exact (`starts_with`/`substr` — never `LIKE`); the scan bound is
  pushed down so `FilteredRead.scanned` is the store's own account of how
  far it looked.
* `QueryAppend::read` — type-prefiltered pages, tag match decoded, same
  discipline as filtered reads.

## Auxiliary stores

* **`PgSnapshotStore<S>`** — snapshots table, newest-wins.
* **`PgViewStore<V>`** — views table; rows carry `version: Sequence`;
  newest-wins by sequence.
* **`PgCheckpointStore`** — `new(pool)` / `from_pool`; **blind upsert**
  (last-write-wins — CAS is 0.8.1; leases are the guard).
* **`PgLeaseStore`** — a **row per lease name, not an advisory lock**,
   because pool connections don't own sessions: `INSERT … ON CONFLICT …
   WHERE expired`; holder is a uuid, a `version` column guards renewals.
   `LeasePolicy{ttl 5s, grace 3, max_grace 12}` semantics are in
   [subscription](eventyr-subscription.md).
* **`PgCommitSignal`** — `LISTEN eventyr_commits` on one **dedicated
  pooled connection**; `NOTIFY` fires in the commit path (0007). It is a
  hint — the projector's idle poll is the fallback and the correctness.

## Operational contract

From the crate docs, the load-bearing caveats:

* **Own schema**: the store assumes its tables are the schema's; set
  `search_path` per pool if you share a database.
* **Postgres ≥ 11**; privileges only need the eventyr tables/procedures.
* **One writable primary** — the commit-order advisory lock is the serial
  point; async (streaming) replication is fine, and in-DB consumers
  rewind together through failover, but there is no multi-primary mode.
* **`LISTEN` doesn't fire on standbys** — wake signals are for nodes with
  their own primary connection.
* **Database-wide lock and channel**: the advisory-lock constant and
  `eventyr_commits` are per-*database*, not per-schema — two eventyr
  schemas contend and cross-notify (per-store identity is roadmap 0.8.2).
* **Parked events**: the durable parked store is SQLite today; a
  Postgres-backed parked store is 0.8.6 — run parking in-process aware
  that a crash drops in-memory parks.

## Testing

The full [contract suites](eventyr-store-testing.md) run against a real
Postgres 17 service in CI (`#[ignore]`d by default, `--test-threads=1`),
plus adapter-specific tests: lock ordering under concurrency, conflict
hint mapping, pagination, truncation behavior, notify round-trip, lease
expiry/takeover. `distributed` (umbrella) exercises the multi-node shape
against `EVENTYR_PG_URL`.

## Limits

* Advisory-lock identity is database-wide (0.8.2 fixes).
* Checkpoints blind-upsert (0.8.1 CAS).
* No parked store yet (0.8.6).
* The commit-order lock is the scale ceiling — [DESIGN.md
  §16](../../DESIGN.md#16-horizontal-scale-slices-shards-and-the-consistency-boundary)
  (slices/shards) is the roadmap answer; until then one primary is the
  shape.
