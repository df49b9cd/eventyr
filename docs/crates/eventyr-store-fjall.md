# eventyr-store-fjall

The LSM-backed embedded store: no server, no runtime, one process.
`FjallStore<E>` over a fjall `SingleWriterTxDatabase` — the choice when
the log must live beside the app with minimal ceremony and writes are
durable by construction.

**Position:** adapter over core + store. The pure-embedded alternative to
[sqlite](eventyr-store-sqlite.md); trades SQL familiarity and the
auxiliary-store ecosystem for LSM write throughput and a tighter,
transactional key-value core. Parent: [ARCHITECTURE.md](../ARCHITECTURE.md).

## Feature

* `snapshots` — `FjallSnapshotStore`; otherwise the crate is
  dependency-lean (fjall 3 is the only heavy dep).

## The store

`FjallStore<E>`: `open(path)` creates/opens the database;
`from_keyspace(SingleWriterTxDatabase)` adopts an existing one (the host
owns pragmas-equivalents — fjall's knobs — if any).

Keyspaces, each a purpose-named partition:

| Keyspace | Key → value |
|---|---|
| `streams` | `"{stream_id}\0{version:016}"` → serialized `StoredRow` (the event + metadata, JSON) |
| `heads` | `stream_id` → head version |
| `global` | `"{sequence:016}"` → stream key (the global log index) |
| `meta` | `"next"` → the sequence counter |

Zero-padded fixed-width keys make range scans the pagination: a "page" is
a `range(prefix..)` — no offset arithmetic, no `QUERY_PAGE` constant;
keyset pagination is the engine's native idiom.

## Concurrency and durability

* **Single writer by engine contract** — fjall's
  `SingleWriterTxDatabase`; the adapter adds no mutex because the engine
  already serializes.
* **`write_in` is two-pass**: collect all writes (assigning sequences and
  versions), then commit one transaction. Two-pass is what makes
  `append_batch` atomic: all streams' events land in one commit.
* **`PersistMode::SyncAll`** — durable by construction: the commit
  fsyncs. No "async durability" mode is exposed.
* **`append_if`** — one write transaction: latest-match scan over the
  condition range, then the writes; check and append can't interleave.
* **Reads** — read snapshots over the keyspaces; `snapshot_global` is a
  lazy iterator over the global keyspace (fold over the snapshot; gaps
  and tombstone-free semantics come from the same visibility discipline
  as the SQL stores: batches commit atomically, so a consumer never sees
  a torn batch).
* **Lifecycle** — closed-set and cuts in `meta`; append paths check them
  like every store (`StreamClosed` / `Truncated`).
* **Engine errors** — fjall `Io` and `Locked` map to
  `StoreError::Unavailable` (a locked database is another process's:
  retry, don't die). Everything else is `Other` with the engine error
  inside.

## Snapshot store

`FjallSnapshotStore::open(&keyspace)` — snapshots in their own keyspace;
load is a **reverse range scan** taking the first row at or below the
requested version and **drops rows ≤ the newest** it returns
(newest-wins enforced by read-side choice, matching the port's
newest-wins contract).

## Testing

The full [contract suites](eventyr-store-testing.md) run against a
`tempfile`-backed database — proving the LSM layout satisfies the same
contracts as the SQL stores is the crate's main test burden, and it runs
in CI without an external service.

## Limits

* **Single-process only** — the single-writer contract is the boundary;
  two processes on one directory get `Locked` → `Unavailable`.
* No checkpoint/view/parked/lease/key stores ship here — the embedded
  deployments this crate targets run those in
  [SQLite](eventyr-store-sqlite.md) or in-memory; nothing about the
  keyspaces prevents adding them.
* No commit signal: same-process wake comes from the caller
  (`LocalCommitSignal` works, since the store lives in the process);
  idle poll is the fallback.
* Sequence allocation is a `meta`-keyspace counter inside the write
  transaction — correct and serialized, but the counter is the hot key; a
  slice/partition story (DESIGN.md §16) would be the scale answer.
