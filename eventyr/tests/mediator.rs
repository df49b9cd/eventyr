//! The mediator through the umbrella: `eventyr::mediator` carries the
//! macro, the vocabulary, and the store integration, and a composition
//! in a crate that depends only on `eventyr` resolves it there.
#![cfg(feature = "mediator")]

use std::fmt;
use std::sync::Arc;

use eventyr::mediator::prelude::*;
use eventyr::mediator::store::{Execute, Load};
use eventyr::prelude::*;
use eventyr::store::prelude::{AggregateRepository, ExecutionOutcome, InMemoryStore};
use futures::executor::block_on;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TallyId(u64);

impl fmt::Display for TallyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Counted;

#[derive(Debug)]
struct Count;

#[derive(Debug)]
enum TallyError {}

impl fmt::Display for TallyError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

#[derive(Debug, Default)]
struct TallyState {
    total: u64,
}

fn apply(state: &mut TallyState, _: &TallyEvent) {
    state.total += 1;
}

fn decide(_: &TallyState, _: &Count) -> Result<Vec<TallyEvent>, TallyError> {
    Ok(vec![Counted.into()])
}

// The umbrella's own tests are in the `eventyr` package, where
// `proc-macro-crate` cannot tell them from the umbrella itself; a user's
// crate needs no `crate` attribute.
#[derive(Aggregate)]
#[eventyr(crate = "eventyr", state = TallyState, command = Count, events(Counted))]
struct Tally;

/// The edge's query, answered from the store through the mediator.
struct Total(u64);
impl Request for Total {
    type Response = u64;
}

#[derive(Default)]
struct TotalHandler;

impl<M: Sender<Load<Tally>>> RequestHandler<Total, M> for TotalHandler {
    async fn handle(&self, mediator: &M, Total(id): Total) -> u64 {
        match mediator.send(Load::<Tally>::new(TallyId(id))).await {
            Ok(loaded) => loaded.state.total,
            Err(_) => 0,
        }
    }
}

type Store = Arc<InMemoryStore<TallyEvent>>;

eventyr::mediator::mediator! {
    struct App {
        tallies: AggregateRepository<Tally, Store>,
    }

    command Execute<Tally> => self.tallies;
    query Load<Tally> => self.tallies;
    query Total => TotalHandler;
}

#[test]
fn a_composition_through_the_umbrella_reaches_the_store() {
    let store: Store = Arc::new(InMemoryStore::new());
    let app = App::new(AggregateRepository::new(store, RetryPolicy::default()));
    for _ in 0..3 {
        let outcome = block_on(app.send(Execute::<Tally>::new(TallyId(1), Count)));
        assert!(matches!(outcome, Ok(ExecutionOutcome::Committed { .. })));
    }
    assert_eq!(block_on(app.send(Total(1))), 3);
}
