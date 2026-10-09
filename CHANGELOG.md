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
- Documentation: DESIGN.md's 0.8–0.10 phasing is made consistent — the
  deployment contract (shipped with the Postgres docs above) is no
  longer listed as planned work, the HLC stamp is triggered ("ships
  when a user runs several stores") everywhere it is scheduled, the
  Postgres parked store 0.7.7 promised is scheduled as 0.8.6, and
  0.8.2's lock-key migration names its upgrade window (quiesce appends,
  or take both keys for one release). The Postgres crate docs no longer
  imply parked events rewind with the database, and the inline-view
  comment's section pointer follows per-tag locking to §16.6.
- `eventyr-subscription`: `LeasePolicy` and `run_leased`/`run_woken_leased`
  now document that `max_grace` caps every lease from acquire — a
  healthy run ends `LeaseLost` after `ttl × max_grace` and the caller
  loops, as the distributed example does.
- Documentation: a newcomer's guide (GUIDE.md — the concepts in
  learning order, linked from the README) and a Code of Conduct
  (Contributor Covenant v2.1, reports through the SECURITY.md channel).
  The port traits users implement themselves — `Projection`,
  `CheckpointStore`, `Upcaster`, `UpcasterChain`, `Cipher`, `KeyStore`,
  `EventBus` — now carry runnable doctests stating each contract in
  miniature (idempotent apply, the erase-stays-erased rule, the `aad`
  binding). Also fixed while there: `EventBus`'s module doc no longer
  says "until 0.3", `SubscriptionSource`'s doc no longer claims
  object safety, and the subscription crate example drops an unused
  import.

Earlier milestones are documented in DESIGN.md's roadmap sections
(§12–§14), which record each plan and what actually shipped.
