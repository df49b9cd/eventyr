# Contributing

## The lay of the land

- [DESIGN.md](DESIGN.md) is the constitution. Read §7 (machine modeling
  rules) and the §12 roadmap before proposing a subsystem; several
  tempting features are deliberate non-goals (§2), and the roadmap
  records what was tried and withdrawn with the reasons.
- Rust 1.99+, edition 2024, `#![forbid(unsafe_code)]` workspace-wide.
- One version of every shared dependency, set in the root
  `[workspace.dependencies]`; per-crate manifests inherit and add only
  features.
- Docs are part of the code: `missing_docs` warns at the workspace
  level, and every public item is documented. Match the surrounding
  voice — dense prose, `///` comments that explain why, no filler.

## Before you push

CI runs all of these; run them locally in this order:

```bash
cargo fmt --all --check
cargo check --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
cargo test --workspace --all-features
cargo run -p eventyr --example bank --all-features   # examples' asserts gate merges
```

The Postgres integration suites are `#[ignore]`d for casual runs and
need a database (CI provisions `postgres:17`; locally point
`EVENTYR_TEST_PG_URL` at one and run `cargo test -p eventyr-store-postgres
--features snapshots,time -- --ignored --test-threads=1`).

Feature powersets are checked with `cargo-hack`
(`cargo hack check --workspace --each-feature --no-dev-deps`), and miri
covers `eventyr-core` on nightly.

## Conventions that matter

- **Machines are pure** (§7): no clocks, randomness, or I/O inside;
  actions are data; bad input ends in `Done(Err(..))`, never a panic.
  A new multi-step interaction starts as a machine + a driver, with
  transition tests via `drive_scripted` — not an async loop.
- **Stores prove themselves with the contract suite** in
  `eventyr-store-testing`; a new store runs every contract tier it
  claims.
- **Examples are tests**: each `examples/*.rs` states its features and
  runs in CI with its asserts live.
- **Commit messages**: imperative subject line, no attribution footers.

## Adding a store adapter

`eventyr-store-sqlite` is the reference port: read it first. Implement
`EventStore` (+ `append_batch`) at minimum; `StreamsAll` for
subscriptions; the opt-in ports (`SnapshotStore`, `ViewStore`,
`CheckpointStore`, `ParkedStore`, `QueryAppend`, `StreamLifecycle`,
`CommitSignal`) as they apply. Gate each behind its own feature, and
wire the contract suites in `tests/`.

## Reporting issues

Bugs and design discussions go to the GitHub issue tracker. Security
issues go to [SECURITY.md](SECURITY.md) — never a public issue.
