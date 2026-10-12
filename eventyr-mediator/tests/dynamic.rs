//! Dispatch by value — MediatR's `Send(object)` and `Publish(object)` —
//! through the generated enums, and a generic mediator.

use std::convert::Infallible;

use eventyr_mediator::prelude::*;
use futures::StreamExt;
use futures::executor::block_on;

#[derive(Debug)]
struct Add(u32, u32);
impl Request for Add {
    type Response = u32;
}

#[derive(Debug)]
struct Greet(&'static str);
impl Request for Greet {
    type Response = String;
}

struct Digits(u32);
impl StreamRequest for Digits {
    type Item = u32;
}

struct Letters(&'static str);
impl StreamRequest for Letters {
    type Item = char;
}

struct Ping;
impl Notification for Ping {
    type Error = Infallible;
}

struct Fail;
impl Notification for Fail {
    type Error = &'static str;
}

#[derive(Default)]
struct Handlers;

impl<M: Sync> RequestHandler<Add, M> for Handlers {
    async fn handle(&self, _: &M, Add(a, b): Add) -> u32 {
        a + b
    }
}

impl<M: Sync> RequestHandler<Greet, M> for Handlers {
    async fn handle(&self, _: &M, Greet(name): Greet) -> String {
        format!("hello, {name}")
    }
}

impl<M: Sync> StreamRequestHandler<Digits, M> for Handlers {
    fn handle(&self, _: &M, Digits(n): Digits) -> impl Stream<Item = u32> + Send {
        futures::stream::iter(
            n.to_string()
                .chars()
                .filter_map(|c| c.to_digit(10))
                .collect::<Vec<_>>(),
        )
    }
}

impl<M: Sync> StreamRequestHandler<Letters, M> for Handlers {
    fn handle(&self, _: &M, Letters(word): Letters) -> impl Stream<Item = char> + Send {
        futures::stream::iter(word.chars())
    }
}

impl<M: Sync> NotificationHandler<Ping, M> for Handlers {
    async fn handle(&self, _: &M, _: &Ping) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<M: Sync> NotificationHandler<Fail, M> for Handlers {
    async fn handle(&self, _: &M, _: &Fail) -> Result<(), &'static str> {
        Err("failed")
    }
}

eventyr_mediator::mediator! {
    struct App;

    query Add => Handlers;
    query Greet as Hello => Handlers;
    stream Digits => Handlers;
    stream Letters => Handlers;
    event Ping => Handlers;
    event Fail => Handlers;

    #[derive(Debug)]
    requests AnyRequest -> #[derive(Debug, PartialEq)] AnyResponse;
    streams AnyStream -> #[derive(Debug, PartialEq)] AnyItem;
    notifications AnyNotification -> #[derive(Debug, PartialEq)] AnyNotificationError;
}

#[test]
fn a_request_enum_dispatches_by_value() {
    let app = App::new();
    let requests: Vec<AnyRequest> = vec![Add(2, 3).into(), Greet("ada").into()];
    let responses: Vec<AnyResponse> = requests
        .into_iter()
        .map(|request| block_on(app.send(request)))
        .collect();
    assert_eq!(
        responses,
        [AnyResponse::Add(5), AnyResponse::Hello("hello, ada".into())]
    );
}

#[test]
fn a_stream_enum_dispatches_by_value() {
    let app = App::new();
    let digits: Vec<AnyItem> = block_on(app.create_stream(AnyStream::from(Digits(42))).collect());
    assert_eq!(digits, [AnyItem::Digits(4), AnyItem::Digits(2)]);
    let letters: Vec<AnyItem> = block_on(
        app.create_stream(AnyStream::Letters(Letters("ok")))
            .collect(),
    );
    assert_eq!(letters, [AnyItem::Letters('o'), AnyItem::Letters('k')]);
}

#[test]
fn a_notification_enum_publishes_by_value() {
    let app = App::new();
    assert_eq!(block_on(app.publish(&AnyNotification::from(Ping))), Ok(()));
    assert_eq!(
        block_on(app.publish(&AnyNotification::Fail(Fail))),
        Err(AnyNotificationError::Fail("failed"))
    );
}

// A generic mediator: the composition's generics carry through to every
// route, and to the singletons.

trait Clock: Send + Sync + 'static {
    fn now(&self) -> u64;
}

struct Fixed(u64);
impl Clock for Fixed {
    fn now(&self) -> u64 {
        self.0
    }
}

struct Now;
impl Request for Now {
    type Response = u64;
}

struct ClockHandler<C>(C);

impl<C: Clock, M: Sync> RequestHandler<Now, M> for ClockHandler<C> {
    async fn handle(&self, _: &M, _: Now) -> u64 {
        self.0.now()
    }
}

eventyr_mediator::mediator! {
    struct Timed<C: Clock> {
        clock: ClockHandler<C>,
    }

    query Now => self.clock;
    query Add => Handlers;
}

#[test]
fn a_generic_mediator_routes() {
    let app = Timed::new(ClockHandler(Fixed(1_700_000_000)));
    assert_eq!(block_on(app.send(Now)), 1_700_000_000);
    assert_eq!(block_on(app.send(Add(1, 1))), 2);
}

// A generic route: one route for a family of requests — MediatR's
// generic request handlers, bounded like its open-generic constraints.

trait Entity: Send + Sync + 'static {
    const KIND: &'static str;
}

struct User;
impl Entity for User {
    const KIND: &'static str = "user";
}

struct Order;
impl Entity for Order {
    const KIND: &'static str = "order";
}

/// Fetches one entity by id.
struct Get<E>(u64, std::marker::PhantomData<E>);

impl<E: Entity> Request for Get<E> {
    type Response = String;
}

/// Serves every entity kind.
#[derive(Default)]
struct Repository;

impl<E: Entity, M: Sync> RequestHandler<Get<E>, M> for Repository {
    async fn handle(&self, _: &M, Get(id, _): Get<E>) -> String {
        format!("{} {id}", E::KIND)
    }
}

/// A behavior for the generic family too.
#[derive(Default)]
struct Upper;

impl<E: Entity, M: Sync> PipelineBehavior<Get<E>, M> for Upper {
    async fn handle<N: Next<Get<E>>>(&self, _: &M, request: Get<E>, next: N) -> String {
        next.run(request).await.to_uppercase()
    }
}

eventyr_mediator::mediator! {
    struct Entities;

    query<E: Entity> Get<E> => Repository;
    query Add => Handlers;

    behaviors { Upper }
}

#[test]
fn a_generic_route_serves_a_family_of_requests() {
    let app = Entities::new();
    let user = block_on(app.send(Get::<User>(1, std::marker::PhantomData)));
    let order = block_on(app.send(Get::<Order>(2, std::marker::PhantomData)));
    assert_eq!(user, "USER 1");
    assert_eq!(order, "ORDER 2");
    // The behavior's bounds hold for the generic family only.
    assert_eq!(block_on(app.send(Add(2, 2))), 4);
}
