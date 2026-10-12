# Changelog

All notable changes to the eventyr workspace. The 0.x.y numbers are the
design milestones of [DESIGN.md §12](DESIGN.md#12-roadmap), tracked here
as they ship; the crates' semver versions begin with the first crates.io
release.

## Unreleased

Added (0.8 groundwork is still ahead; this is where the next milestone's
entries go):

- `eventyr` (examples): five new examples, each run by CI —
  `versioning` (upcaster ladders validated by `build()`,
  `VersionedSource` in a projector, a `RebuildPlan` schema bump, a
  poison payload stopping the projection), `embedded` (fjall with no
  runtime: the blocking write and projector drivers, snapshots on the
  same database, `Optional`, a reopened directory), `operations`
  (SQLite's key, parked, snapshot, and checkpoint stores surviving a
  restart; `TracingMetrics`; XChaCha20-Poly1305; erasure),
  `bus` (an `EventBus` transport waking a projector), and
  `hello_postgres` (the minimal Postgres deployment).
- `eventyr` (examples): `enrollment` defines its DCB domain inline
  instead of importing the contract-suite fixture, and runs the async
  `drive_boundary`; `bank` now shows the write erasure refuses (a
  rename sealing an erased subject's data fails at the store) and
  says plainly that its upcast is the upcaster alone, pointing at
  `versioning` for the read path.
- `eventyr` (tests): mirroring suites for `inline_view` and
  `loan_eligibility`, beside `bank_story`; the loan suite steps the
  boundary machine by hand to show a concurrent deposit committing
  without a retry and a concurrent withdrawal forcing one.
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
- Refactoring review (sixth round), build: CI pins its actions to
  commit SHAs, runs every cargo step `--locked`, sets per-job timeouts
  and a cancel-superseded concurrency group, and the packaging dry run
  now verifies — it builds every crate from its tarball against a
  throwaway registry of the packaged siblings; the previous
  `--no-verify` could not tell a publishable tarball from a broken
  one. A weekly `.github/workflows/audit.yml` runs `cargo audit` over
  the committed Cargo.lock; `CONTRIBUTING.md` documents it. Line
  endings are pinned (`.gitattributes`), so a CRLF checkout cannot
  desynchronize the embedded sqlx migration checksums, and `.claude/`
  local state is ignored. The workspace manifest's inert
  `eventyr-store-testing` entry is dropped: no consumer inherited it
  (each dev-dependency on it stays path-only, deliberately — a
  versioned dev-dependency would point the packaged manifests at a
  registry that does not carry the crate yet).
- Review fixes, `eventyr-core`: the load-continuity rule (same
  stream, contiguous sequence) is one shared check used by the write
  machine, the batch machine, and the repository fold, which had
  hand-rolled it three times with drifting messages; the `enrollment`
  DCB fixture moves beside `account` in `testing` (re-exported from
  `boundary` for the documented path); `NoFold` (no callers) is
  deleted.
- Review fixes, ports: `ProjectorLease::acquire`/`renew` take
  `LeasePolicy` instead of a raw `(ttl, grace, max_grace)` clump;
  `ViewStore` and `KeyStore` gain the blanket `&T`/`Arc<T>` delegation
  impls every other port ships; the upcasting source adapters preserve
  the inner source's scan bound instead of discarding it.
- Review fixes, stores: the in-memory lease store validates
  degenerate policies like Postgres's CHECKs do (a `grace: 0` literal
  no longer silently voids exclusivity); lease-window arithmetic is
  checked instead of panicking on representable extremes; SQLite's
  per-stream read pages under the connection mutex like the global
  read does, instead of holding it through the full collect and
  decode; the pg/sqlite `FilteredRead` scanned-bound derivation is
  one shared constructor; a corrupt parked row's negative `attempts`
  is a `CorruptRow` like every other malformed number in the crate,
  not a silent `u32::MAX`; fjall's lifecycle record validates its cut
  before `first - 1` can underflow, and the 16-digit key-layout bound
  is documented; the store prelude re-exports the four
  `*_with_metrics` drivers.
- Review fixes, subscriptions: the three projector drive prologues
  (plain, caught-up, leased) share one preparation path, so the
  park-refusal gate cannot drift between the plain and leased paths,
  and a leased run stops loading its checkpoint twice; a listener
  that fails to arm degrades to the timer like a listener that errors
  mid-run, instead of aborting the run; the lease grace window and
  all four lease metrics gain tests (the review-5 headline fix had
  zero coverage); `LeasedProjector::caught_up_on`'s pulse, `Fanout`'s
  at-least-once redelivery, and `drive_projector_blocking` gain
  tests; the dead `InMemoryLeaseStore::expire` seam and the
  zero-caller `into_inner`/`name` accessors are deleted; the three
  unbounded test waits take the timeout their siblings use; the
  `ParkedStore` vocabulary gap (the key the other ports call `name`)
  is documented at the trait until a breaking round can rename it.
- Review fixes, tests: `ShreddingStore` runs the query-append and
  filtered-read contracts, making its "whole store contract" claim
  true; the in-memory `ViewStore` reference runs its own contract;
  the shredded toy cipher is one copy behind the `testing` feature;
  the inline-view behavior tests share one contract between sqlite
  and pg; the pg `append_read_conflict_stream_all` eager test splits
  by scenario; the sqlite page-bound assertion pins the page size
  exactly; two macro error paths gain trybuild cases. The Postgres
  suites' shared schema registry is replaced by a per-test
  `SchemaScope` — a finishing test drops only its own schemas, so a
  parallel `-- --ignored` run is safe (verified against a live
  Postgres); CI keeps `--test-threads=1` for the contract suites'
  open transactions, not for schema safety.

Earlier milestones are documented in DESIGN.md's roadmap sections
(§12–§14), which record each plan and what actually shipped.
