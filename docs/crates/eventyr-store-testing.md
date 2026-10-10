# eventyr-store-testing

The behavioral conformance suites: one crate of contract functions that
every store adapter runs in its own test suite, so "implements
`EventStore`" means *behaves like every other store*, not just
type-checks. This is the workspace's portability mechanism — the reason a
machine can trust that `Conflict{current}` means the same thing from
Postgres, SQLite, fjall, and in-memory.

**Position:** a dev-only leaf — consumed as a dev-dependency by every
store adapter, by eventyr-store (for `InMemoryStore`), and by shred. It
is never a runtime dependency of anything. Parent:
[ARCHITECTURE.md](../ARCHITECTURE.md) (§13 testing architecture).

## Shape

Not a harness, not a framework: plain `#[test]`-callable **async contract
functions**, one suite per port, each taking the store (and whatever
auxiliary stores that suite needs). Runs without an async runtime — the
suites drive futures with `block_on` — so they work in any test
environment, tokio or not.

| Suite | Port exercised |
|---|---|
| `event_store_contract` | `EventStore`: append/stream, expected versions (`Any`/`Exact`/`Empty`), conflict shapes, idempotency-key dedupe → `AlreadyCommitted`, head/contiguity |
| `event_store_batch_contract` | `append_batch`: multi-stream atomicity, single-transaction conflict behavior |
| `streams_all_contract` | `StreamsAll`: global order, paging, visibility (no torn batches), `stop_at_catch_up`-style bounds |
| `filtered_read_contract` | `stream_all_filtered`: matches, **`scanned` accounting** (the scan bound is data), no `LIKE`-style prefix surprises (`ParityEvent` fixtures) |
| `lifecycle_contract` | `StreamLifecycle`: close → `StreamClosed` on every append path; truncate → `Truncated` reads, head preserved |
| `lifecycle_query_append_contract` | lifecycle × `QueryAppend` interplay |
| `query_append_contract` | `QueryAppend`: the `enrollment` DCB fixture — condition holds, `QueryConflict` on violation, `acknowledged` retry |
| `commit_signal_contract` | `CommitSignal`: commit → listener wakes (in-process signals; **now_or_never** driven, so only same-process signals qualify) |
| `snapshot_contract` | `SnapshotStore`: newest-wins, stale save doesn't clobber |

Event payloads are contract fixtures, not caller types:
`ContractEvent` is the small trait the suites need (`PayloadEvent` with
`{"Payload":{"value":N}}` JSON shape, `ParityEvent` even/odd, `Counted`
with a decode counter). The caller's own events need not participate —
the suites test the *store*, so the payloads only need to round-trip.

## How adapters use it

```rust
#[tokio::test] // or plain #[test] — the suites block_on themselves
fn contracts() {
    event_store_contract(&store, ...);
    event_store_batch_contract(&store, ...);
    // ... one call per suite, with the auxiliary stores each needs
}
```

In CI: in-memory and SQLite run on every build; the Postgres run is
`#[ignore]`d and enabled against a Postgres 17 service, serialized with
`--test-threads=1` (one database, one migration state). fjall runs on
`tempfile` directories. The shred crate runs the log suites through
`ShreddingStore` — the decorator must be transparent — plus its own
`cipher_contract`/`key_store_contract` (those live in
[eventyr-shred](eventyr-shred.md) behind its `testing` feature).

## What the contracts pin (the shared semantics)

The suites are where the cross-store guarantees in
[ARCHITECTURE.md §8](../ARCHITECTURE.md#8-delivery-idempotency-and-lifecycle-semantics)
become executable:

* Conflict taxonomy is uniform: `Conflict{stream_id, current}` carries
  the true current version; `QueryConflict{sequence}` for DCB.
* Idempotency keys dedupe per stream; replays return the original commit
  as `AlreadyCommitted`.
* Closed/truncated semantics are identical across adapters, including on
  batch and `append_if` paths.
* Global reads never expose a torn batch; filtered reads' `scanned` is
  the store's honest account of how far it looked.
* Snapshots are newest-wins and never regress a state.

A new adapter's bar (per [CONTRIBUTING.md](../../CONTRIBUTING.md)):
implement the ports, pass every suite, keep SQLite as the reference for
questions of "what does the contract mean".

## Limits

* The suites are deliberately *not* a load/soak harness — semantics only.
* `commit_signal_contract` can only assert in-process signals
  (`now_or_never`); Postgres's `NOTIFY` path is covered by its own
  adapter tests.
* Adding a suite is a workspace-wide event: every adapter's suite must
  grow with it, which is the point.
