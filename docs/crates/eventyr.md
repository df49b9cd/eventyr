# eventyr

The umbrella crate: one dependency to add, one feature list to choose from.
It contains no logic of its own — `src/lib.rs` is `pub use eventyr_core::*`,
feature-gated re-export modules for each sibling crate, and the crate docs
rendered from the root README.

**Position:** the top of the graph. Parent: [ARCHITECTURE.md](../ARCHITECTURE.md).

## What re-exports what

* `pub use eventyr_core::*` — unconditionally. The domain seam, the
  vocabulary, and the five machines are always in scope, plus `__private`.
* `store` — eventyr-store (ports, `InMemoryStore`, drivers, repository,
  snapshot stores, commit signals, metrics backend).
* `subscription` — eventyr-subscription (projector runner, checkpoints,
  leases, parked stores, sources, saga driving).
* `bus` — the §2 `EventBus`/`Subscription` live-push seam
  (eventyr-subscription's `bus` feature).
* `projection` — eventyr-projection (upcasters, views, inline views,
  rebuilds).
* `postgres`, `fjall`, `sqlite` — the store adapters.
* `shred`, `shred_aes_gcm`, `shred_chacha` — crypto-shredding and the
  cipher adapters.

## The feature graph

Default: `["time", "macros", "store"]` — the newcomer path works with no
choices made.

| Feature | Pulls in | Notes |
|---|---|---|
| `time` | core+store `time` | `Metadata::timestamp`; propagates into postgres when that store is on |
| `macros` | core `macros` | the derives, re-exported through core |
| `store` | eventyr-store | |
| `subscription` | store + eventyr-subscription | |
| `bus` | subscription + `eventyr-subscription/bus` | |
| `projection` | subscription + eventyr-projection | |
| `postgres` | store + eventyr-store-postgres | |
| `postgres_snapshots` | postgres + adapter `snapshots` | |
| `postgres_checkpoints` | postgres + subscription + adapter `checkpoints` | |
| `postgres_leases` | postgres + subscription + adapter `leases` | |
| `postgres_views` | postgres + projection + adapter `views` | inline views + `PgViewStore` |
| `snapshots` | `postgres_snapshots` | **legacy alias**, kept working |
| `fjall` | store + eventyr-store-fjall | |
| `fjall_snapshots` | fjall + adapter `snapshots` | |
| `sqlite` | store + eventyr-store-sqlite | |
| `sqlite_snapshots`, `sqlite_views`, `sqlite_checkpoints` | | each adds the named auxiliary store |
| `sqlite_shred` | sqlite + shred + adapter `shred` | SQLite's subject-key store |
| `sqlite_parked` | sqlite + subscription + adapter `parked` | durable poison-event parking |
| `shred` | store + eventyr-shred | `Sensitive`, `ShreddingStore`, `Cipher`/`KeyStore` seams |
| `shred_aes_gcm`, `shred_chacha` | shred + adapter crate | pick a cipher |
| `shred_parked` | shred + subscription | parked rejects are sealed before parking |
| `metrics` | store `tracing` | `TracingMetrics` |

The design principle: features map one-to-one onto *capabilities that drag
dependencies*. Nothing compiles sqlx unless `postgres` is on; nothing pulls
serde_json into your build unless a JSON-carrying feature is.

## Examples

Five shipped examples, each behind exactly the features it uses so
`cargo build --examples` under default features skips rather than fails:

| Example | Features | Demonstrates |
|---|---|---|
| `bank` | subscription, projection, shred_aes_gcm | the full surface: aggregate writes, projector, upcasters, views, shredding |
| `enrollment` | store (default) | the teaching example, DCB-style enrollment |
| `loan_eligibility` | store (default) | the *blocking* boundary driver |
| `inline_view` | sqlite_views | views folded inside the append transaction |
| `distributed` | sqlite_views, sqlite_checkpoints, postgres_checkpoints, postgres_leases, postgres_views | the multi-node shape: leases, durable checkpoints, `EVENTYR_PG_URL` |

`tests/bank_story.rs` mirrors the bank example as an integration test.

## Conventions

* Docs.rs builds with `all-features = true` and `--cfg docsrs`.
* The umbrella's own `README.md` is the crate docs (`#![doc =
  include_str!("../README.md")]`) and the crate/table features table is the
  user-facing map; every per-crate `README.md` in the workspace is a symlink to
  it.
* CI runs `cargo hack --each-feature --no-dev-deps` against this crate to
  prove every feature combination builds standalone.

## Limits

* The umbrella is the composition surface and version-0.1.0 like everything
  else; when the roadmap's breaking milestones (0.9 slices) land, the
  feature list is where renames surface.
* Adding a new sibling crate means adding a feature here — there is no
  "everything" escape hatch beyond `docs.rs`'s all-features build.
