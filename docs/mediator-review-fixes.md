# Mediator review fixes: state of play

2026-10-12 — the review was run on the mediator crates and the findings
were triaged into two batches:

1. **Landed on this branch, tested green** (commits `172c751` through
   `2a44ed2`):
   - The `__private` module (~910 lines) is split out to `src/private.rs`;
     lib.rs is about 700 lines now.
   - `StreamId::aggregate_id` lives where the mapping lives, beside
     `for_aggregate`.
   - `with_metadata` keeps an already-set `idempotency_key`.
   - `dispatch_failure` no longer claims the runner retries `Unavailable`.
   - `Retained` has `do_not_recommend`, so the hand-written Clone message
     surfaces on error-stage routes.
   - Nine more trybuild cases pin diagnostics that had none.
   - The custom-publisher test has its assertion.
   - A three-variant stream enum exercises the middle `Either` arm.
   - `LoadUntil` is routed through a mediator and asserted.
   - `Trace` is one helper in `tests/common/mod.rs`.
   - `eventyr-mediator-macros` is pinned `=0.1.0`.
   - The six `Apply*`/`Check*` pairs are one `Apply`/`Check` pair, keyed
     by the existing `Kind` markers (adding a kind drops to one line).
   - The stage-selection docs were tightened; what is "decided per
     message" now says what actually is.

2. **Found but deferred (discussed in the review):**
   - The clone regression: each retaining stage clones the request, so a
     route with a post-processor, a handler, an error handler and an
     action clones it four times. The fix needs `NextKept`/Arc-wrapped
     retained references — it was written as `NextKept` and reduced the
     count, but one test measured `5` under the old code and deferred it
     because it broke other bounds. Tracked with a note.
   - The `Request` trait is `Send`-only while `Notification` is `Send +
     Sync`, so a stage that reads `&Req` and awaits silently skips
     non-`Sync` requests. Deliberate in scope — `Validator` is how the
     tests are written — flagged as a follow-up.
   - `is-an-outcome-behavioural` stage-kind check on generic routes
     remains absent: a offers an `Authorize` behavior bounded on
     `Authorizable`, with `Get<Secret>: Authorizable` but no concrete
     `Get<_>` route, the earlier build "applies on the concrete routes
     but not the family" check passes. That is one of the remaining
     findings and needs a proper check route.
