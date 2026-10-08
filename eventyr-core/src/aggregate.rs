//! The aggregate: two pure functions plus identity.
//!
//! An [`Aggregate`] is the consistency boundary of an event-sourced domain.
//! Its state is never mutated directly; it is *folded* from events by
//! [`apply`](Aggregate::apply), and commands are *decided* against that
//! state by [`decide`](Aggregate::decide). Both are pure: no I/O, no clocks,
//! no randomness — which is what makes domain logic testable with plain
//! table-driven tests.

use alloc::vec::Vec;
use core::fmt::{Debug, Display};

/// Marker trait for aggregate identifier types.
///
/// The supertraits are what the protocols need: [`Clone`] to carry the id,
/// [`Eq`] and [`Hash`] to key caches and deduplicate, [`Debug`] for logs,
/// and [`Display`] to build
/// [`StreamId`](crate::vocabulary::StreamId)s.
pub trait AggregateId: Clone + Eq + core::hash::Hash + Debug + Display {}

impl<T> AggregateId for T where T: Clone + Eq + core::hash::Hash + Debug + Display {}

/// A consistency boundary: decides events from commands, folds events into
/// state.
///
/// Implementors are usually unit structs or thin namespaces — the aggregate
/// *type* carries no state; state is rebuilt per interaction by folding.
pub trait Aggregate {
    /// The stable aggregate type name, used to build stream ids
    /// (`"{NAME}-{id}"`) and, in stores, table names.
    const NAME: &'static str;

    /// The aggregate instance's identifier.
    type Id: AggregateId;

    /// The state folded from the event history.
    type State;

    /// The domain events this aggregate produces. A plain enum.
    type Event;

    /// The commands this aggregate accepts. A plain enum or struct.
    type Command;

    /// The domain rejection reasons [`decide`](Aggregate::decide) can
    /// produce.
    type Error: Debug + Display;

    /// The state before any event.
    ///
    /// Aggregates whose instances "do not exist yet" use
    /// `State = Option<T>` and return `None` here.
    fn initial(id: &Self::Id) -> Self::State;

    /// Fold one event into state. **Pure and total: must never fail.**
    ///
    /// Events are facts. If an event cannot be applied, the model is wrong
    /// and the process should stop loudly — panic on violated invariants
    /// rather than corrupting state.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// Decide which events a command produces, given the current state.
    /// **Pure: no I/O, no clocks, no randomness.**
    ///
    /// Rejections are domain outcomes (e.g. "insufficient funds"), not
    /// failures. Anything the decision needs from the environment (time,
    /// catalogs) belongs in the command payload or a context type, not in
    /// the environment.
    ///
    /// # Errors
    ///
    /// The aggregate's own domain rejection — `Self::Error`, the
    /// command refused against the folded state, never a store or
    /// transport failure.
    fn decide(
        state: &Self::State,
        command: &Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error>;
}

/// Lift an aggregate's state into [`Option`]: `None` is "does not exist
/// yet".
///
/// Event-sourced aggregates whose instances come into being through an
/// event (an account on `Opened`, an order on `Placed`) have no honest
/// state before it: the natural `State` is `Option<T>`, with
/// `initial` returning `None`. [`Optional`] is the helper for that
/// shape — implement it for the *inner* state type and use
/// [`apply_state`](Optional::apply_state) as the aggregate's `apply`:
/// absent until the first event creates it, then plain delegation.
///
/// `initial` and `decide` are the aggregate's own: whether a command may
/// run against `None` ("only `Open` on a missing account") is the
/// domain's question, not the adapter's.
///
/// ```rust
/// use eventyr_core::aggregate::{Aggregate, Optional};
/// # use core::fmt;
/// # #[derive(Clone, PartialEq, Eq, Hash, Debug)]
/// # struct WidgetId(u64);
/// # impl fmt::Display for WidgetId { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) } }
///
/// #[derive(Default)]
/// struct WidgetState { count: u64 }
///
/// #[derive(Debug)]
/// enum WidgetEvent { Added }
/// # #[derive(Debug)] enum WidgetCommand { Add }
/// # #[derive(Debug)] enum WidgetError {}
/// # impl fmt::Display for WidgetError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { Ok(()) } }
///
/// impl Optional for WidgetState {
///     type Event = WidgetEvent;
///     fn apply(state: &mut Self, event: &WidgetEvent) {
///         match event { WidgetEvent::Added => state.count += 1 }
///     }
/// }
///
/// struct Widget;
/// impl Aggregate for Widget {
///     const NAME: &'static str = "widget";
///     type Id = WidgetId;
///     type State = Option<WidgetState>; // "no widget yet"
///     type Event = WidgetEvent;
///     type Command = WidgetCommand;
///     type Error = WidgetError;
///     fn initial(_id: &WidgetId) -> Option<WidgetState> { None }
///     fn apply(state: &mut Option<WidgetState>, event: &WidgetEvent) {
///         Optional::apply_state(state, event); // the bridge
///     }
///     fn decide(state: &Option<WidgetState>, command: &WidgetCommand)
///         -> Result<Vec<WidgetEvent>, WidgetError> {
///         match command { WidgetCommand::Add => Ok(vec![WidgetEvent::Added]) }
///     }
/// }
/// ```
pub trait Optional: Default {
    /// The domain event that brings the state into being, and every
    /// event folded after.
    type Event;
    /// Fold one event into an existing state (pure and total, like
    /// [`Aggregate::apply`]).
    fn apply(state: &mut Self, event: &Self::Event);

    /// Fold one event into an `Option<Self>` state: `None` becomes
    /// `Some(Self::default())` before delegating to
    /// [`apply`](Optional::apply). This is the body an `Aggregate`
    /// impl's `apply` writes.
    fn apply_state(state: &mut Option<Self>, event: &Self::Event) {
        let state = state.get_or_insert_with(Self::default);
        Self::apply(state, event);
    }
}
