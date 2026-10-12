//! Requests: routing, handler instances, sub-dispatch, the inherent API,
//! and pointers to a mediator.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use eventyr_mediator::prelude::*;
use futures::executor::block_on;

/// A command answering nothing.
struct Record(&'static str);
impl Request for Record {
    type Response = ();
}

/// A query answered from the mediator's state.
struct Total;
impl Request for Total {
    type Response = usize;
}

/// A query whose handler sends another request.
struct Echo(u32);
impl Request for Echo {
    type Response = u32;
}

/// The request `Echo` sub-dispatches.
struct Double(u32);
impl Request for Double {
    type Response = u32;
}

/// A handler held in a field: neither `Clone` nor `Default`, borrowed
/// for each call.
struct Journal {
    lines: Mutex<Vec<&'static str>>,
}

impl<M: Sync> RequestHandler<Record, M> for Journal {
    async fn handle(&self, _: &M, request: Record) {
        self.lines.lock().unwrap().push(request.0);
    }
}

impl<M: Sync> RequestHandler<Total, M> for Journal {
    async fn handle(&self, _: &M, _: Total) -> usize {
        self.lines.lock().unwrap().len()
    }
}

/// A handler named by type, constructed with `Default`.
#[derive(Default)]
struct Doubler;

impl<M: Sync> RequestHandler<Double, M> for Doubler {
    async fn handle(&self, _: &M, request: Double) -> u32 {
        request.0 * 2
    }
}

/// Sub-dispatch: the bound on `M` is the `ISender` injection.
#[derive(Default)]
struct Echoer;

impl<M: Sender<Double>> RequestHandler<Echo, M> for Echoer {
    async fn handle(&self, mediator: &M, request: Echo) -> u32 {
        mediator.send(Double(request.0)).await + 1
    }
}

eventyr_mediator::mediator! {
    /// The mediator under test.
    struct App {
        journal: Journal,
    }

    command Record => self.journal;
    query Total => self.journal;
    query Echo => Echoer;
    query Double => Doubler;
}

fn make() -> App {
    App::new(Journal {
        lines: Mutex::new(Vec::new()),
    })
}

#[test]
fn a_field_handler_is_borrowed_not_cloned() {
    let app = make();
    block_on(app.send(Record("a")));
    block_on(app.send(Record("b")));
    assert_eq!(block_on(app.send(Total)), 2);
    assert_eq!(*app.journal.lines.lock().unwrap(), vec!["a", "b"]);
}

/// Counts its own constructions: only the `once` test builds it.
static CONSTRUCTED: AtomicU32 = AtomicU32::new(0);

struct Counted;

impl Default for Counted {
    fn default() -> Self {
        CONSTRUCTED.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

struct Tick;
impl Request for Tick {
    type Response = ();
}

impl<M: Sync> RequestHandler<Tick, M> for Counted {
    async fn handle(&self, _: &M, _: Tick) {}
}

eventyr_mediator::mediator! {
    /// A mediator whose only handler is named by type.
    #[derive(Clone)]
    struct Once;

    command Tick => Counted;
}

#[test]
fn a_typed_handler_is_constructed_once_per_mediator() {
    let once = Once::new();
    assert_eq!(CONSTRUCTED.load(Ordering::SeqCst), 1);
    block_on(once.send(Tick));
    block_on(once.send(Tick));
    // A clone shares the instance.
    let clone = once.clone();
    block_on(clone.send(Tick));
    assert_eq!(CONSTRUCTED.load(Ordering::SeqCst), 1);
}

#[test]
fn a_handler_sends_through_its_mediator() {
    let app = make();
    assert_eq!(block_on(app.send(Echo(20))), 41);
}

#[test]
fn the_dispatch_traits_name_the_routes() {
    fn send<S: Sender<Double>>(sender: &S) -> u32 {
        block_on(sender.send(Double(21)))
    }
    let app = make();
    assert_eq!(send(&app), 42);
    // A pointer to a mediator dispatches like the mediator.
    assert_eq!(send(&&app), 42);
    assert_eq!(send(&Box::new(make())), 42);
    assert_eq!(send(&Arc::new(app)), 42);
}

#[test]
fn the_futures_are_send() {
    fn assert_send<T: Send>(_: T) {}
    let app = make();
    assert_send(app.send(Echo(1)));
    assert_send(app.send(Record("x")));
}
