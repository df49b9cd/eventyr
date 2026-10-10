# eventyr-store-sqlite

The embedded SQL store and the workspace's **reference port**: the
smallest complete adapter, the one
[CONTRIBUTING.md](../../CONTRIBUTING.md) points at for what a new backend
must implement, and the contract suites' default durable target. One
process owns the file; the engine's single-writer discipline is the
concurrency story.

**Position:** adapter over core + store (+ subscription/projection for
auxiliary stores, behind features). Parent:
[ARCHITECTURE.md](../ARCHITECTURE.md).

## Features

* `snapshots` — `SqliteSnapshotStore`.
* `views` — `SqliteViewStore` + inline views in the append transaction.
* `checkpoints` — `SqliteCheckpointStore`.
* `shred` — `SqliteKeyStore` for
  [crypto-shredding](eventyr-shred.md).
* `parked` — `SqliteParkedStore` for poison events.

rusqlite is compiled with `bundled` SQLite. The store is
`Arc<Mutex<Connection>>` — one connection, writes serialized by the mutex
(engine-level single-writer plus a process-level guarantee).

## The store

`SqliteStore<E>`: `open(path)`, `open_in_memory()`, or
`from_connection(Connection)` — the escape hatch for hosts that need to
set their own pragmas; **WAL + `busy_timeout` are recommended for
multi-process use** and set via `from_connection`, since pragmas are
per-connection. `with_inline_views(..)` attaches transactional view
folding (same JSON-erasure boundary as Postgres, via projection's
`inline` machinery).

Schema:

```sql
events(global_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
       stream_id, stream_version, event_type, payload TEXT,
       causation_id, correlation_id, idempotency_key, created_at,
       UNIQUE(stream_id, stream_version))
```

plus `stream_lifecycle` (`closed`, `first_kept`, `head`) — the same
lifecycle columns Postgres carries.

Port support: `EventStore`, `StreamsAll` (with filtered **override**),
`QueryAppend`, `StreamLifecycle`, `CommitSignal`.

## Concurrency and transactions

* **`append_within`** — one `BEGIN IMMEDIATE` transaction per append
  (batch or single): lifecycle check, head check, expected-version check,
  insert, all inside. `IMMEDIATE` takes the write lock up front, so the
  check-then-insert is atomic against other writers even
  multi-process-on-WAL.
* **`append_if`** — `IMMEDIATE` transaction, latest-match scan for the
  condition, then writes: check and append can't interleave.
* **Truncation** — `IMMEDIATE` transaction updating `first_kept` and
  `head`; reads starting below the cut answer `Truncated{first}`.
* **Commit signal** — `LocalCommitSignal`: **same-process only**. A second
  process's commits wake nobody; idle poll covers it (the signal is a
  hint by contract).
* Pages: keyset on the appropriate key, `PAGE = 512`, same as Postgres.
* Filtered override: SQL-side event-type and byte-exact stream-prefix
  predicates (`starts_with`/`substr`, never `LIKE`), scan bound pushed
  down, tags matched on decoded events, `FilteredRead.scanned` reported by
  the store.

## Auxiliary stores

* **`SqliteSnapshotStore::new(&store)`** — snapshots beside the log.
* **`SqliteViewStore::new(&store)`** — views beside the log,
  newest-wins by sequence.
* **`SqliteCheckpointStore`** — `beside(&store)` / `from_connection`;
  last-write-wins like every checkpoint store today.
* **`SqliteKeyStore`** — subject keys for shred: `open(path)` /
  `from_connection` / `beside(&store)`. **Erased subject = key column
  NULL with `erased_at` set**: an erased subject can never mint a new key
  (the KMS contract, see [eventyr-shred](eventyr-shred.md)). The docs
  recommend a **separate key file** from the log — the threat model
  shredding answers is "the log leaked, the keys didn't".
* **`SqliteParkedStore`** — `beside(&store)` / `from_connection`;
  `parked_events` keyed `(subscription, global_sequence)` — the durable
  poison-event store (Postgres's is roadmap 0.8.6).

## Testing

The full [contract suites](eventyr-store-testing.md) run against both
`open_in_memory` and a file-backed database in CI — no external service
needed, which is why SQLite is the reference contract target. The
`inline_view` and `distributed` examples use it as the embedded side.

## Limits

* One writer at a time (engine + mutex); throughput is bounded by one
  connection, deliberately.
* Wake signals are in-process only.
* Multi-process is possible (WAL + busy_timeout) but contention is yours
  to live with; fjall is the single-process embedded alternative with
  different trade-offs.
