//! Derive macros for eventyr: [`Aggregate`](derive@Aggregate) and
//! [`EventName`](derive@EventName).
//!
//! Depend on `eventyr-core` (with its `macros` feature) or on the `eventyr`
//! umbrella crate instead of this crate directly — both re-export the
//! derives. Generated code targets `::eventyr_core` paths by default; when
//! the derives come through the umbrella, point them back at it with
//! `#[eventyr(crate = "eventyr")]` (the serde `crate = "..."` pattern).

mod aggregate;
mod attrs;
mod event_name;
mod naming;

use proc_macro::TokenStream;

/// Convention wiring for the [`Aggregate`](derive@Aggregate) trait.
///
/// Derive it on the aggregate's marker struct and it wires the trait impl
/// by convention, delegating the domain logic to plain functions written
/// next to it. Everything the derive generates is writable by hand — it
/// is sugar, not the API.
///
/// # Conventions
///
/// For `#[derive(Aggregate)] struct Account;` the impl is wired as:
///
/// - `NAME` = `"account"` — the struct's name, snake_cased
/// - `Id` = `AccountId`, `Event` = `AccountEvent`, `Command` =
///   `AccountCommand`, `Error` = `AccountError` — the `{Ident}...`
///   position convention
/// - `State` = `Self` — so a unit struct is its own (empty) state; point
///   `state` at a state type for the usual marker-plus-state shape
/// - `initial` = `Default::default()`, or `Self` when a unit struct is
///   its own state
/// - `apply`/`decide` delegate to free functions named `apply`/`decide`
///   in the same module
///
/// # Overrides
///
/// Every convention is overridable with `#[eventyr(...)]` attributes:
///
/// - `name = "..."` — the aggregate type name
/// - `id = Path`, `state = Path`, `event = Path`, `command = Path`,
///   `error = Path` — the associated types
/// - `initial = expr` — the state before any event; the id is in scope
///   as `id`
/// - `apply = Path`, `decide = Path` — the delegate functions
/// - `crate = "..."` — the crate the generated code targets
///   (`eventyr-core` by default; `"eventyr"` when the derives come
///   through the umbrella crate)
/// - `event_derive(Serialize, ...)` — extra `#[derive(...)]` paths for
///   the generated event enum, and `event_attr("#[serde(...)]")` for
///   any other attribute on it (0.7.6+: a stored or shredded event
///   enum is a serde value)
///
/// # The event enum
///
/// `#[eventyr(events(Opened, Deposited))]` generates the event enum from
/// payload structs: an enum with one newtype variant per payload, a
/// `From<Payload>` conversion for building events in `decide`, and an
/// [`EventName`](derive@EventName) impl naming each variant after its
/// payload. Name it with `event_enum = BankEvent` (default:
/// `{Ident}Event`); the `Event` type follows the enum. The reverse
/// conversion is a `match` — the point of a concrete enum.
///
/// # Example
///
/// ```
/// use std::fmt;
/// use eventyr_macros::Aggregate;
/// use eventyr_core::aggregate::Aggregate as _;
/// use eventyr_core::event_name::EventName as _;
///
/// #[derive(Clone, PartialEq, Eq, Hash, Debug)]
/// struct CounterId(u64);
/// impl fmt::Display for CounterId {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "{}", self.0)
///     }
/// }
///
/// /// A domain event payload.
/// #[derive(Clone, Debug, PartialEq)]
/// struct Incremented {
///     by: u32,
/// }
///
/// /// A command.
/// #[derive(Debug)]
/// enum CounterCommand {
///     Increment(u32),
/// }
///
/// /// A domain rejection.
/// #[derive(Debug, PartialEq)]
/// enum CounterError {
///     TooHigh,
/// }
/// impl fmt::Display for CounterError {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         f.write_str("counter would go too high")
///     }
/// }
///
/// /// The folded state.
/// #[derive(Debug, Default)]
/// struct CounterState {
///     count: u32,
/// }
///
/// fn apply(state: &mut CounterState, event: &CounterEvent) {
///     match event {
///         CounterEvent::Incremented(Incremented { by }) => state.count += by,
///     }
/// }
///
/// fn decide(
///     state: &CounterState,
///     command: &CounterCommand,
/// ) -> Result<Vec<CounterEvent>, CounterError> {
///     match command {
///         CounterCommand::Increment(by) if state.count + by > 100 => {
///             Err(CounterError::TooHigh)
///         }
///         CounterCommand::Increment(by) => Ok(vec![Incremented { by: *by }.into()]),
///     }
/// }
///
/// #[derive(Aggregate)]
/// #[eventyr(state = CounterState, events(Incremented))]
/// struct Counter;
///
/// assert_eq!(Counter::NAME, "counter");
/// assert_eq!(
///     Counter::decide(&CounterState { count: 1 }, &CounterCommand::Increment(2)).unwrap(),
///     vec![CounterEvent::Incremented(Incremented { by: 2 })],
/// );
/// assert_eq!(
///     CounterEvent::Incremented(Incremented { by: 2 }).event_name(),
///     "Incremented",
/// );
/// ```
#[proc_macro_derive(Aggregate, attributes(eventyr))]
pub fn derive_aggregate(input: TokenStream) -> TokenStream {
    expand_or_error(aggregate::expand, input)
}

/// Stable storage names for event variants: implements `EventName` by
/// naming each variant after itself.
///
/// The name is what a store persists in its event-type column and what
/// the upcasting chain selects by — renaming a Rust variant renames the
/// event, so pin historical names with `#[eventyr(name = "...")]` on the
/// variant. On the struct form (one payload struct, one event type) the
/// name is the type's own, overridable with `#[eventyr(name = "...")]`
/// on the struct. `#[eventyr(crate = "...")]` retargets the generated
/// code, as on [`Aggregate`](derive@Aggregate).
///
/// # Example
///
/// ```
/// use eventyr_macros::EventName;
/// use eventyr_core::event_name::EventName as _;
///
/// #[derive(EventName)]
/// enum TransferEvent {
///     Started,
///     #[eventyr(name = "transfer.completed")]
///     Completed { amount: u64 },
/// }
///
/// assert_eq!(TransferEvent::Started.event_name(), "Started");
/// assert_eq!(
///     TransferEvent::Completed { amount: 1 }.event_name(),
///     "transfer.completed",
/// );
/// ```
#[proc_macro_derive(EventName, attributes(eventyr))]
pub fn derive_event_name(input: TokenStream) -> TokenStream {
    expand_or_error(event_name::expand, input)
}

/// Parses the derive input, expands it, and routes errors to
/// `compile_error!`.
fn expand_or_error(
    expand: fn(&syn::DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error>,
    input: TokenStream,
) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
