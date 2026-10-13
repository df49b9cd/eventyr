//! Stream requests: handlers in fields and by type, stream behaviors
//! selected by impl bounds.

use eventyr_mediator::prelude::*;
use futures::StreamExt;
use futures::executor::block_on;

/// Counts up from a start.
struct CountFrom(u32);
impl StreamRequest for CountFrom {
    type Item = u32;
}

/// Lists names.
struct Names;
impl StreamRequest for Names {
    type Item = &'static str;
}

/// A stateful stream handler in a field: the stream borrows it.
struct Counter {
    limit: u32,
}

impl<M: Sync> StreamRequestHandler<CountFrom, M> for Counter {
    fn handle(&self, _: &M, request: CountFrom) -> impl Stream<Item = u32> + Send {
        futures::stream::iter(request.0..self.limit)
    }
}

/// A zero-sized stream handler named by type.
#[derive(Default)]
struct Roster;

impl<M: Sync> StreamRequestHandler<Names, M> for Roster {
    fn handle(&self, _: &M, _: Names) -> impl Stream<Item = &'static str> + Send {
        futures::stream::iter(["ada", "grace"])
    }
}

/// Applies to number streams only: doubles each item.
#[derive(Default)]
struct DoubleNumbers;

impl<Req: StreamRequest<Item = u32>, M: Sync> StreamPipelineBehavior<Req, M> for DoubleNumbers {
    fn handle<N: NextStream<Req>>(
        &self,
        _: &M,
        request: Req,
        next: N,
    ) -> impl Stream<Item = u32> + Send {
        next.run(request).map(|item| item * 2)
    }
}

/// Applies to every stream: keeps the first three items.
#[derive(Default)]
struct FirstThree;

impl<Req: StreamRequest, M: Sync> StreamPipelineBehavior<Req, M> for FirstThree {
    fn handle<N: NextStream<Req>>(
        &self,
        _: &M,
        request: Req,
        next: N,
    ) -> impl Stream<Item = Req::Item> + Send {
        next.run(request).take(3)
    }
}

eventyr_mediator::mediator! {
    struct Streams {
        counter: Counter,
    }

    stream CountFrom => self.counter;
    stream Names => Roster;

    stream_behaviors { FirstThree, DoubleNumbers }
}

#[test]
fn stream_behaviors_wrap_in_declared_order() {
    let app = Streams::new(Counter { limit: 10 });
    // FirstThree outermost: doubling happens inside, then three are kept.
    let items: Vec<u32> = block_on(app.create_stream(CountFrom(4)).collect());
    assert_eq!(items, [8, 10, 12]);
}

#[test]
fn a_stream_behavior_applies_where_its_bounds_hold() {
    let app = Streams::new(Counter { limit: 10 });
    // `DoubleNumbers` is for `u32` items only; `FirstThree` applies.
    let names: Vec<&str> = block_on(app.create_stream(Names).collect());
    assert_eq!(names, ["ada", "grace"]);
}

// -- three routes in one dispatch enum: the nested `Either` middle arm ---

/// A singleton stream.
struct Once(u32);
impl StreamRequest for Once {
    type Item = u32;
}

#[derive(Default)]
struct Singles;

impl<M: Sync> StreamRequestHandler<Once, M> for Singles {
    fn handle(&self, _: &M, request: Once) -> impl Stream<Item = u32> + Send {
        futures::stream::iter([request.0])
    }
}

eventyr_mediator::mediator! {
    struct ThreeVariant {
        counter: Counter,
    }

    stream CountFrom => self.counter;
    stream Names => Roster;
    stream Once => Singles;

    // Three arms nest: `Left`, `Right(Left(_))`, `Right(Right(_))`. The
    // middle arm is generated only for three or more routes.
    streams AnyThree -> #[derive(Debug, PartialEq)] AnyThreeItem;
}

#[test]
fn a_three_variant_stream_enum_dispatches_each_arm() {
    let app = ThreeVariant::new(Counter { limit: 5 });
    let count: Vec<AnyThreeItem> =
        block_on(app.create_stream(AnyThree::from(CountFrom(3))).collect());
    assert_eq!(
        count,
        [AnyThreeItem::CountFrom(3), AnyThreeItem::CountFrom(4)]
    );
    // The middle arm: `Right(Left(_))`.
    let once: Vec<AnyThreeItem> = block_on(app.create_stream(AnyThree::Once(Once(7))).collect());
    assert_eq!(once, [AnyThreeItem::Once(7)]);
    let names: Vec<AnyThreeItem> = block_on(app.create_stream(AnyThree::Names(Names)).collect());
    assert_eq!(
        names,
        [AnyThreeItem::Names("ada"), AnyThreeItem::Names("grace")]
    );
}
