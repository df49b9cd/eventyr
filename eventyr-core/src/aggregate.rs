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
    fn decide(state: &Self::State, command: &Self::Command)
        -> Result<Vec<Self::Event>, Self::Error>;
}
