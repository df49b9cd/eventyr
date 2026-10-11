# eventyr-macros

The proc-macro crate behind core's `macros` feature:
`#[derive(Aggregate)]` and `#[derive(EventName)]`. Depended on only via
eventyr-core (never directly — the derives are re-exported through the
core prelude), so application code has one import surface.

**Position:** a leaf crate used through core. Parent:
[ARCHITECTURE.md](../ARCHITECTURE.md); conventions in
[DESIGN.md §8](../../DESIGN.md#8-derive-macros-eventyr-macros).

## `#[derive(Aggregate)]`

Derives `Aggregate` for a **unit struct** from conventional pieces. The
conventions, given `struct Loan`:

* `NAME` = `"Loan"` (snake-cased), overridable with `#[eventyr(name =
  "…")]`.
* `Id` = `LoanId`, `Event` = `LoanEvent`, `Command` = `LoanCommand`,
  `Error` = `LoanError`, `State` = `Self` — each overridable
  (`#[eventyr(id = …)]`, `state = …`, `event = …`, `command = …`,
  `error = …`).
* `initial(id)` = `<State as Default>::default()` (or a free `initial(id)`
  fn; the attribute `initial` puts the id in scope for it).
* `apply(state, event)` / `decide(state, command)` call the free functions
  `apply` / `decide` in scope — overridable with
  `#[eventyr(apply = path)]`, `#[eventyr(decide = path)]`.
* With `#[eventyr(events(Opened, Closed, …))]`, the macro generates the
  `Event` enum itself: derives `Clone, Debug, PartialEq`, a
  `From<Payload>` per variant, and `EventName` (variant names are the
  stored event-type names; `event_derive(...)` and
  `event_attr("...")` add derives/attributes to the generated enum).
* `#[eventyr(crate = path)]` redirects the `eventyr_core` path — for
  renames and vendoring.
* The `eventyr_core` crate is resolved through `proc-macro-crate`
  (`default_core_crate`), so a renamed dependency still works without the
  attribute.

The unit-struct restriction is deliberate: the aggregate type itself is a
name-bearing handle, not data. Fielded structs and enums are rejected with
a UI-tested error (see trybuild below).

## `#[derive(EventName)]`

Implements `EventName` (the stored `event_type` string):

* **Enum form** — each variant's name is its event name, unless the
  variant carries `#[eventyr(name = "…")]`.
* **Struct form** — one event, one name.

Composing with `#[derive(Aggregate)]`'s generated enum is the usual
pattern: hand-write nothing, get `event_name()` everywhere the stores need
it.

## Testing

* **trybuild UI tests** pin the error messages: `aggregate_on_a_fielded_struct`,
  `aggregate_on_an_enum`, `decide_not_a_path`, `duplicate_attribute`,
  `duplicate_events`, and friends — the macro's diagnostics are part of
  its contract.
* The sibling crates' test suites exercise the happy paths: the `account`
  fixture in core, the bank/enrollment examples, the contract suites.
* Dev-dependencies include `eventyr` and `eventyr-core` themselves (path
  dev-deps, workspace-internal only) for integration-style tests.

## Limits

* Only the two derives ship; no `Saga`/`Projection` derives (the traits
  are small enough that hand impls read fine — a deliberate line).
* The generated `Event` enum derives are fixed to what projection paths
  need (`Clone, Debug, PartialEq`); richer derives go through
  `event_derive(...)`.
