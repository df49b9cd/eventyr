# Eventyr documentation map

| Document | Scope |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | The workspace as a whole: the machine/driver pattern, crate layering, the write and read paths, the port inventory, the consistency model, deployment topology, testing architecture |
| [crates/eventyr.md](crates/eventyr.md) | The umbrella crate and its feature graph |
| [crates/eventyr-core.md](crates/eventyr-core.md) | The pure core: `Aggregate`, protocol vocabulary, the five machines |
| [crates/eventyr-macros.md](crates/eventyr-macros.md) | `#[derive(Aggregate)]` / `#[derive(EventName)]` |
| [crates/eventyr-store.md](crates/eventyr-store.md) | The persistence ports, in-memory store, drivers, repository |
| [crates/eventyr-subscription.md](crates/eventyr-subscription.md) | The projector runner, checkpoints, leases, parking, sagas |
| [crates/eventyr-projection.md](crates/eventyr-projection.md) | Upcasting, views, inline views, rebuilds |
| [crates/eventyr-store-postgres.md](crates/eventyr-store-postgres.md) | The Postgres store and its migrations |
| [crates/eventyr-store-sqlite.md](crates/eventyr-store-sqlite.md) | The embedded SQLite reference port |
| [crates/eventyr-store-fjall.md](crates/eventyr-store-fjall.md) | The embedded fjall store |
| [crates/eventyr-store-testing.md](crates/eventyr-store-testing.md) | The store contract suites |
| [crates/eventyr-shred.md](crates/eventyr-shred.md) | Crypto-shredding |
| [crates/eventyr-shred-aes-gcm.md](crates/eventyr-shred-aes-gcm.md) | AES-256-GCM cipher adapter |
| [crates/eventyr-shred-chacha.md](crates/eventyr-shred-chacha.md) | XChaCha20-Poly1305 cipher adapter |

These documents describe the workspace **as built** (DESIGN.md milestone 0.7.9;
workspace version 0.1.0, pre-release). [DESIGN.md](../DESIGN.md) remains the
constitution — the argument, alternatives weighed, and the roadmap; these pages
are the as-built architecture record. [GUIDE.md](../GUIDE.md) is the
newcomer's path through the concepts.
