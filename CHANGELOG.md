# Changelog

All notable changes to the eventyr workspace. The 0.x.y numbers are the
design milestones of [DESIGN.md §12](DESIGN.md#12-roadmap), tracked here
as they ship; the crates' semver versions begin with the first crates.io
release.

## Unreleased

Added (0.8 groundwork is still ahead; this is where the next milestone's
entries go):

- Documentation: the workspace's crates are packaged for publication —
  license texts (MIT/Apache-2.0), per-crate README, `repository`/
  `documentation`/`categories` metadata, docs.rs `all-features` builds
  with `doc_cfg` badges, and a publishable dependency graph (the two
  cyclic dev-dependencies are now path-only).
- `eventyr` (umbrella): `#[derive(Aggregate)]`/`#[derive(EventName)]`
  now resolve the crate they target from the caller's manifest, so the
  derives work through the umbrella with no `crate = "..."` attribute;
  only a renamed dependency needs it.
- `eventyr-core`: the at-least-once machine module is renamed
  `subscription_machine` (was `subscription`), so the umbrella's
  `eventyr::subscription` — the eventyr-subscription crate behind the
  `subscription` feature — no longer collides with core's module of the
  same name. The prelude re-exports are unchanged.
- `eventyr-store-postgres`: the crate docs now state the database setup
  (own schema for the migrations table, Postgres ≥ 11, required
  privileges) and the deployment contract (single writable primary,
  synchronous replication, what rewinds safely).
- `eventyr-subscription`: `LeasePolicy` and `run_leased`/`run_woken_leased`
  now document that `max_grace` caps every lease from acquire — a
  healthy run ends `LeaseLost` after `ttl × max_grace` and the caller
  loops, as the distributed example does.

Earlier milestones are documented in DESIGN.md's roadmap sections
(§12–§14), which record each plan and what actually shipped.
