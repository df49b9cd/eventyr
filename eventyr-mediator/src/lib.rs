#![cfg_attr(docsrs, feature(doc_cfg))]
#![no_std]

//! # eventyr-mediator
//!
//! MediatR's design for Rust, composed at compile time. MediatR resolves
//! handlers at runtime from a DI container; the .NET answer to that cost
//! is source generation (`martinothamar/Mediator` generates the dispatch
//! at build time). The Rust equivalent is a proc macro — [`mediator!`] —
//! and this crate is the vocabulary it composes: requests and their handlers,
//! stream requests, notifications, pipeline behaviors, pre- and
//! post-processors, error handlers and error actions, and publisher
//! strategies.
//!
//! There is no registry: no `TypeId` map, no `Box<dyn Any>`, no service
//! container. A composition names every handler, so a request nobody
//! handles is a compile error at the `send` call rather than a runtime
//! "handler not found", and every dispatch is static.
//!
//! ## The vocabulary
//!
//! | MediatR | eventyr-mediator |
//! |---|---|
//! | `IRequest<TResponse>` | [`Request`] (`type Response`) |
//! | `IRequestHandler<TRequest, TResponse>` | [`RequestHandler<Req, M>`] |
//! | `ISender.Send` | [`Sender<Req>::send`](Sender::send) |
//! | `ISender.Send(object)` | a generated request enum (`requests AnyRequest -> AnyResponse;`) |
//! | `IStreamRequest<T>` | [`StreamRequest`] (`type Item`) |
//! | `IStreamRequestHandler<TRequest, T>` | [`StreamRequestHandler<Req, M>`] |
//! | `ISender.CreateStream` | [`StreamSender<Req>::create_stream`](StreamSender::create_stream) |
//! | `INotification` | [`Notification`] (`type Error`) |
//! | `INotificationHandler<T>` | [`NotificationHandler<N, M>`] |
//! | `IPublisher.Publish` | [`Publisher<N>::publish`](Publisher::publish) |
//! | `IPublisher.Publish(object)` | a generated notification enum (`notifications AnyNotification -> AnyNotificationError;`) |
//! | `ForeachAwaitPublisher` / `TaskWhenAllPublisher` | `publish sequential;` / `publish concurrent;` |
//! | `IPipelineBehavior<TRequest, TResponse>` | [`PipelineBehavior<Req, M>`] |
//! | `IStreamPipelineBehavior<TRequest, T>` | [`StreamPipelineBehavior<Req, M>`] |
//! | `IRequestPreProcessor<TRequest>` | [`RequestPreProcessor<Req, M>`] |
//! | `IRequestPostProcessor<TRequest, TResponse>` | [`RequestPostProcessor<Req, M>`] |
//! | `IRequestExceptionHandler<…>` | [`RequestErrorHandler<Req, M>`] |
//! | `IRequestExceptionAction<…>` | [`RequestErrorAction<Req, M>`] |
//! | `RequestExceptionActionProcessorStrategy` | `error_action_strategy unhandled;` / `error_action_strategy all;` |
//! | `IMediator` (`ISender` + `IPublisher`) | the composed type: it implements [`Sender`], [`StreamSender`], and [`Publisher`] for exactly what it routes |
//! | `ISender` injected into a handler | a bound on the handler's mediator parameter: `impl<M: Sender<Other>> RequestHandler<Req, M>` |
//! | Assembly scanning, handler registration | the `mediator!` composition |
//! | Open-generic constraints on a behavior | the behavior's impl bounds, resolved per route at compile time |
//!
//! ## A composition
//!
//! ```rust
//! use eventyr_mediator::prelude::*;
//!
//! // Messages are ordinary types — derive what the edge needs (serde,
//! // validation) and say what each one answers.
//! pub struct Greet { pub name: String }
//! impl Request for Greet { type Response = String; }
//!
//! pub struct Greeted { pub name: String }
//! impl Notification for Greeted { type Error = core::convert::Infallible; }
//!
//! // Handlers are ordinary types, generic over the mediator running
//! // them. The bound on `M` is MediatR's `IPublisher` injection: this
//! // handler publishes through whichever mediator composes it.
//! #[derive(Default)]
//! pub struct Greeter;
//! impl<M: Publisher<Greeted>> RequestHandler<Greet, M> for Greeter {
//!     async fn handle(&self, mediator: &M, request: Greet) -> String {
//!         let greeted = Greeted { name: request.name.clone() };
//!         let Ok(()) = mediator.publish(&greeted).await;
//!         format!("hello, {}", request.name)
//!     }
//! }
//!
//! #[derive(Default)]
//! pub struct Audit;
//! impl<M: Sync> NotificationHandler<Greeted, M> for Audit {
//!     async fn handle(&self, _: &M, _: &Greeted) -> Result<(), core::convert::Infallible> {
//!         Ok(())
//!     }
//! }
//!
//! mediator! {
//!     pub struct App;
//!
//!     command Greet => Greeter;
//!     event Greeted => Audit;
//! }
//!
//! # futures::executor::block_on(async {
//! let app = App::new();
//! assert_eq!(app.send(Greet { name: "ada".into() }).await, "hello, ada");
//! # });
//! ```
//!
//! ## Handler instances
//!
//! A route names its handler either by *type* or as a *field* of the
//! mediator. A type is constructed once, with `Default`, when the
//! mediator is built, and shared by every clone of it — a DI container's
//! singleton. A field (`=> self.accounts`) is whatever the composition
//! root put there: a repository, a pool, a client — constructor
//! injection, done where the mediator is built. Either way the handler is
//! borrowed for the call; nothing is cloned or constructed per dispatch.
//! Behaviors and processors are named the same two ways.
//!
//! ## Pipelines and route selection
//!
//! A composition lists its behaviors, processors, and error stages once,
//! and every route runs the ones that *apply* to it. Whether one applies
//! is decided by its impl bounds — MediatR's open-generic constraint
//! filtering, resolved by the compiler instead of the container. A
//! behavior implemented for `Req: Request + Authorizable` wraps exactly
//! the routes whose request is `Authorizable`; a blanket impl wraps every
//! route; an impl for one concrete request wraps one route. A listed
//! stage that applies to none of the mediator's routes is a mistake — a
//! missing impl, or bounds no routed message meets — and the build fails
//! at the stage.
//!
//! A generic route is a family of messages, and its probes resolve once
//! for the whole family — deciding per member would be monomorphization,
//! which comes after macro expansion. So a stage wraps a generic route
//! only when its bounds hold for every member of the family; a stage
//! bounded on a property of *some* members (say `Req: Authorizable`
//! where only `Get<Secret>` is `Authorizable`) never runs for the
//! family, and the applies-somewhere check above fails the build at the
//! stage unless it also applies to a concrete route.
//!
//! A request's pipeline is MediatR's, from the outside in: the error
//! actions and error handlers (actions outermost, so they see what no
//! handler recovered — `error_action_strategy all;` swaps them), the
//! pre-processors, the post-processors (which run once everything
//! inside them has answered), the behaviors in declared order, and the
//! handler. Error handlers and error actions run in declared order. The
//! chain is a statically nested value built on the stack per call — no
//! boxing.
//!
//! ## Errors are values
//!
//! MediatR's exception pipeline catches thrown exceptions; Rust has none
//! worth catching. The [`Fallible`] adapter exposes a `Result`-shaped
//! response to [`RequestErrorHandler`] (which recovers by answering a
//! replacement response) and [`RequestErrorAction`] (which observes and
//! lets the error through). Panics are not intercepted.
//!
//! ## On an event-sourced system
//!
//! With the `store` feature, the `store` module puts eventyr's write
//! side behind the mediator: `Execute<A>` and `Load<A>` are requests an
//! `AggregateRepository` handles, `StampMetadata` fills the
//! interaction's correlation ids on every command, and `saga_dispatcher`
//! makes the mediator the dispatcher a `SagaProjection` sends its
//! commands through. Notifications stay in-process: a fact that must
//! survive a crash is an event in the log, and its reactions are
//! subscriptions and sagas.
//!
//! ## Cargo features
//!
//! - `store` — the `store` module: eventyr's repository behind the
//!   mediator. Off by default; the umbrella crate's `mediator` feature
//!   turns it on with its own `store` feature.
//! - `time` — `store::LoadUntil`, the repository's fold-to-an-instant
//!   read, as a request.

extern crate alloc;
extern crate self as eventyr_mediator;

use core::future::Future;

use futures::Stream;

pub use eventyr_mediator_macros::mediator;

#[cfg(feature = "store")]
#[cfg_attr(docsrs, doc(cfg(feature = "store")))]
pub mod store;

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// A request: one message, one handler, one response — MediatR's
/// `IRequest<TResponse>`.
///
/// Commands and queries are both requests; the composition's `command`
/// and `query` keywords record intent, and dispatch is identical. A
/// request with nothing to answer uses `type Response = ();`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a request",
    label = "routed as a request",
    note = "implement `Request` for it: `impl Request for {Self} {{ type Response = …; }}`"
)]
pub trait Request: Send {
    /// What the handler answers.
    type Response: Send;
}

/// Handles one request type — MediatR's `IRequestHandler<TRequest,
/// TResponse>`.
///
/// `M` is the mediator running the handler. Implement generically
/// (`impl<M: Sync> RequestHandler<Req, M> for H`) for a handler that
/// ignores it; bound it (`M: Sender<Other>`) to send or publish through
/// it — the compile-time form of injecting `ISender`. A handler bounded
/// that way works under any mediator that routes `Other`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not handle the request `{Req}` in the mediator `{M}`",
    label = "not a handler of `{Req}`",
    note = "implement `RequestHandler<{Req}, {M}>` for it, or check the bounds its impl puts on the mediator"
)]
pub trait RequestHandler<Req: Request, M>: Send + Sync {
    /// Handles `request`. The future is `Send`, so the mediator runs
    /// under any multi-threaded executor.
    fn handle(&self, mediator: &M, request: Req) -> impl Future<Output = Req::Response> + Send;
}

/// The mediator's request dispatch for one request type — MediatR's
/// `ISender.Send`.
///
/// Implemented by [`mediator!`] for exactly the requests the composition
/// routes, so a request without a handler is a compile error at the call
/// site. A reference, `Box`, or `Arc` to a mediator dispatches like it.
#[diagnostic::on_unimplemented(
    message = "the mediator `{Self}` has no handler for the request `{Req}`",
    label = "`{Req}` is not routed by this mediator",
    note = "route it in the composition: `command {Req} => SomeHandler;` (or `query`)"
)]
pub trait Sender<Req: Request>: Sync {
    /// Runs `request` through its pipeline to its handler.
    fn send(&self, request: Req) -> impl Future<Output = Req::Response> + Send;
}

// ---------------------------------------------------------------------------
// Stream requests
// ---------------------------------------------------------------------------

/// A request answered with a stream of items — MediatR's
/// `IStreamRequest<TResponse>`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a stream request",
    label = "routed as a stream request",
    note = "implement `StreamRequest` for it: `impl StreamRequest for {Self} {{ type Item = …; }}`"
)]
pub trait StreamRequest: Send {
    /// One element of the answer.
    type Item: Send;
}

/// Handles one stream request type — MediatR's
/// `IStreamRequestHandler<TRequest, TResponse>`.
///
/// The stream may borrow the handler and the mediator: both outlive it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not handle the stream request `{Req}` in the mediator `{M}`",
    label = "not a handler of `{Req}`",
    note = "implement `StreamRequestHandler<{Req}, {M}>` for it, or check the bounds its impl puts on the mediator"
)]
pub trait StreamRequestHandler<Req: StreamRequest, M>: Send + Sync {
    /// Opens the stream answering `request`.
    fn handle(&self, mediator: &M, request: Req) -> impl Stream<Item = Req::Item> + Send;
}

/// The mediator's stream dispatch for one stream request type —
/// MediatR's `ISender.CreateStream`.
#[diagnostic::on_unimplemented(
    message = "the mediator `{Self}` has no handler for the stream request `{Req}`",
    label = "`{Req}` is not routed by this mediator",
    note = "route it in the composition: `stream {Req} => SomeHandler;`"
)]
pub trait StreamSender<Req: StreamRequest>: Sync {
    /// Runs `request` through its stream pipeline to its handler.
    fn create_stream(&self, request: Req) -> impl Stream<Item = Req::Item> + Send;
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

/// A notification: one message, any number of handlers — MediatR's
/// `INotification`.
///
/// `Error` is what a handler may fail with; use
/// [`Infallible`](core::convert::Infallible) when handlers cannot fail.
/// Notifications are in-process: on an event-sourced system a durable
/// fact belongs in the log, its reactions in subscriptions and sagas.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a notification",
    label = "routed as a notification",
    note = "implement `Notification` for it: `impl Notification for {Self} {{ type Error = …; }}`"
)]
pub trait Notification: Send + Sync {
    /// The failure a handler can report.
    type Error: Send;
}

/// Handles one notification type — MediatR's
/// `INotificationHandler<TNotification>`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not handle the notification `{N}` in the mediator `{M}`",
    label = "not a handler of `{N}`",
    note = "implement `NotificationHandler<{N}, {M}>` for it, or check the bounds its impl puts on the mediator"
)]
pub trait NotificationHandler<N: Notification, M>: Send + Sync {
    /// Reacts to `notification`.
    fn handle(
        &self,
        mediator: &M,
        notification: &N,
    ) -> impl Future<Output = Result<(), N::Error>> + Send;
}

/// The mediator's publish for one notification type — MediatR's
/// `IPublisher.Publish`.
///
/// The composition's strategy decides delivery: `publish sequential;`
/// (MediatR's default `ForeachAwaitPublisher`) awaits each handler in
/// declared order and stops at the first error; `publish concurrent;`
/// (`TaskWhenAllPublisher`) runs every handler to completion and reports
/// the first error in declared order.
#[diagnostic::on_unimplemented(
    message = "the mediator `{Self}` does not publish the notification `{N}`",
    label = "`{N}` is not routed by this mediator",
    note = "route it in the composition: `event {N} => HandlerA, HandlerB;`"
)]
pub trait Publisher<N: Notification>: Sync {
    /// Delivers `notification` to its handlers.
    fn publish(&self, notification: &N) -> impl Future<Output = Result<(), N::Error>> + Send;
}

/// One handler's delivery of a notification, as a publish strategy sees
/// it — what MediatR's `NotificationHandlerExecutor` wraps.
pub trait Delivery<N: Notification>: Send {
    /// Runs the handler.
    fn deliver(self) -> impl Future<Output = Result<(), N::Error>> + Send;
}

/// The handlers of one notification, in declared order, as a publish
/// strategy receives them: `()` for none, or a handler's [`Delivery`]
/// and the rest.
pub trait Deliveries<N: Notification>: Send {
    /// Delivers to every handler in declared order, awaiting each one
    /// and stopping at the first error — MediatR's
    /// `ForeachAwaitPublisher`.
    fn sequential(self) -> impl Future<Output = Result<(), N::Error>> + Send;

    /// Delivers to every handler concurrently, running all to
    /// completion, and answers the first error in declared order —
    /// MediatR's `TaskWhenAllPublisher`.
    fn concurrent(self) -> impl Future<Output = Result<(), N::Error>> + Send;

    /// Delivers to every handler in declared order, running all to
    /// completion, and answers the first error — sequential delivery
    /// that does not stop at a failure.
    fn sequential_all(self) -> impl Future<Output = Result<(), N::Error>> + Send;
}

impl<N: Notification> Deliveries<N> for () {
    async fn sequential(self) -> Result<(), N::Error> {
        Ok(())
    }

    async fn concurrent(self) -> Result<(), N::Error> {
        Ok(())
    }

    async fn sequential_all(self) -> Result<(), N::Error> {
        Ok(())
    }
}

impl<N, D, R> Deliveries<N> for (D, R)
where
    N: Notification,
    D: Delivery<N>,
    R: Deliveries<N>,
{
    async fn sequential(self) -> Result<(), N::Error> {
        self.0.deliver().await?;
        self.1.sequential().await
    }

    async fn concurrent(self) -> Result<(), N::Error> {
        let (first, rest) = futures::future::join(self.0.deliver(), self.1.concurrent()).await;
        first.and(rest)
    }

    async fn sequential_all(self) -> Result<(), N::Error> {
        let first = self.0.deliver().await;
        let rest = self.1.sequential_all().await;
        first.and(rest)
    }
}

/// A publish strategy of the application's own — MediatR's
/// `INotificationPublisher`, named in a composition with
/// `publish with MyPublisher;`.
///
/// It receives a notification's handlers as [`Deliveries`] and decides
/// how to run them: the built-in strategies are the provided
/// combinators, and a publisher can wrap them (log, time) or order its
/// own. Deliveries are consumed on use, so a run happens exactly once —
/// to retry a failed delivery, publish again from the caller. Implement
/// it generically over `N` to serve every notification the mediator
/// routes.
pub trait NotificationPublisher<N: Notification>: Send + Sync {
    /// Runs `handlers`, in whatever order and concurrency this
    /// publisher chooses.
    fn publish<D: Deliveries<N>>(
        &self,
        notification: &N,
        handlers: D,
    ) -> impl Future<Output = Result<(), N::Error>> + Send;
}

// ---------------------------------------------------------------------------
// Pipeline behaviors
// ---------------------------------------------------------------------------

/// The rest of a request pipeline, handed to a [`PipelineBehavior`] —
/// MediatR's `RequestHandlerDelegate<TResponse>`.
pub trait Next<Req: Request>: Send {
    /// Continues the pipeline: the next stage, eventually the handler.
    fn run(self, request: Req) -> impl Future<Output = Req::Response> + Send;
}

/// Wraps request handling — MediatR's `IPipelineBehavior<TRequest,
/// TResponse>`.
///
/// The impl's bounds select the routes it wraps: `impl<Req: Request, M:
/// Sync>` wraps every route, `impl<Req: Request + Authorizable, M:
/// Sync>` only those whose request is `Authorizable`, `impl<M: Sync>
/// PipelineBehavior<Transfer, M>` exactly one. A behavior may change the
/// request, answer without calling `next` (short-circuit), or reshape
/// the response.
pub trait PipelineBehavior<Req: Request, M>: Send + Sync {
    /// Runs this behavior around `next`.
    fn handle<N: Next<Req>>(
        &self,
        mediator: &M,
        request: Req,
        next: N,
    ) -> impl Future<Output = Req::Response> + Send;
}

/// The rest of a stream pipeline, handed to a [`StreamPipelineBehavior`]
/// — MediatR's `StreamHandlerDelegate<TResponse>`.
pub trait NextStream<Req: StreamRequest>: Send {
    /// Continues the pipeline: the next stage, eventually the handler.
    fn run(self, request: Req) -> impl Stream<Item = Req::Item> + Send;
}

/// Wraps stream handling — MediatR's `IStreamPipelineBehavior<TRequest,
/// TResponse>`. Route selection by impl bounds, as for
/// [`PipelineBehavior`].
pub trait StreamPipelineBehavior<Req: StreamRequest, M>: Send + Sync {
    /// Runs this behavior around `next`: map, filter, or replace the
    /// stream the rest of the pipeline produces.
    fn handle<N: NextStream<Req>>(
        &self,
        mediator: &M,
        request: Req,
        next: N,
    ) -> impl Stream<Item = Req::Item> + Send;
}

// ---------------------------------------------------------------------------
// Processors, error handlers, error actions
// ---------------------------------------------------------------------------

/// Runs before the handler — MediatR's `IRequestPreProcessor<TRequest>`.
///
/// Route selection by impl bounds. Pre-processors see the request by
/// reference, in declared order, before the behaviors run.
pub trait RequestPreProcessor<Req: Request, M>: Send + Sync {
    /// Inspects `request` before it is handled.
    fn process(&self, mediator: &M, request: &Req) -> impl Future<Output = ()> + Send;
}

/// Runs after the handler — MediatR's `IRequestPostProcessor<TRequest,
/// TResponse>`.
///
/// Post-processors see the request and the response, in declared order,
/// once the behaviors and the handler have answered. The handler
/// consumes the request, so a route with a post-processor keeps a copy:
/// its request must be `Clone`.
pub trait RequestPostProcessor<Req: Request, M>: Send + Sync {
    /// Inspects `request` and its `response` after handling.
    fn process(
        &self,
        mediator: &M,
        request: &Req,
        response: &Req::Response,
    ) -> impl Future<Output = ()> + Send;
}

/// A `Result`-shaped response: what the error pipeline needs to look
/// inside one.
///
/// Implemented for every `Result<T, E>`; a response type with its own
/// error convention implements it to join the error pipeline.
pub trait Fallible: Send + Sized {
    /// The success value.
    type Ok: Send;
    /// The error value.
    type Error: Send;
    /// The response as a `Result`.
    fn into_result(self) -> Result<Self::Ok, Self::Error>;
    /// A response from a `Result`.
    fn from_result(result: Result<Self::Ok, Self::Error>) -> Self;
}

impl<T: Send, E: Send> Fallible for Result<T, E> {
    type Ok = T;
    type Error = E;

    #[inline]
    fn into_result(self) -> Self {
        self
    }

    #[inline]
    fn from_result(result: Self) -> Self {
        result
    }
}

/// Recovers from a failed request — MediatR's
/// `IRequestExceptionHandler`, with errors as values.
///
/// Receives the request (a copy taken before the rest of the pipeline
/// ran, so the route's request must be `Clone`) and the error, and
/// answers the response the caller sees: a fallback `Ok`, or a
/// translated error that the next error handler sees. Route selection by
/// impl bounds; a route whose response is not [`Fallible`] has no error
/// stages.
pub trait RequestErrorHandler<Req, M>: Send + Sync
where
    Req: Request,
    Req::Response: Fallible,
{
    /// The response replacing the failed one.
    fn handle(
        &self,
        mediator: &M,
        request: &Req,
        error: <Req::Response as Fallible>::Error,
    ) -> impl Future<Output = Req::Response> + Send;
}

/// Observes a failed request — MediatR's `IRequestExceptionAction`.
///
/// Runs for every error that reaches it; the error propagates unchanged.
/// Route selection by impl bounds.
pub trait RequestErrorAction<Req, M>: Send + Sync
where
    Req: Request,
    Req::Response: Fallible,
{
    /// Reacts to `error`: log it, count it, alert on it.
    fn execute(
        &self,
        mediator: &M,
        request: &Req,
        error: &<Req::Response as Fallible>::Error,
    ) -> impl Future<Output = ()> + Send;
}

// ---------------------------------------------------------------------------
// Pointer delegation
// ---------------------------------------------------------------------------

// A handler, behavior, or processor held by reference, `Box`, or `Arc`
// is one: a composition can name a field of any of those types.
macro_rules! delegate {
    ($($pointer:ty),*) => {$(
        #[diagnostic::do_not_recommend]
        impl<Req: Request, M, T: RequestHandler<Req, M> + ?Sized> RequestHandler<Req, M> for $pointer {
            #[inline]
            fn handle(&self, mediator: &M, request: Req) -> impl Future<Output = Req::Response> + Send {
                (**self).handle(mediator, request)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<Req: StreamRequest, M, T: StreamRequestHandler<Req, M> + ?Sized> StreamRequestHandler<Req, M> for $pointer {
            #[inline]
            fn handle(&self, mediator: &M, request: Req) -> impl Stream<Item = Req::Item> + Send {
                (**self).handle(mediator, request)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<N: Notification, M, T: NotificationHandler<N, M> + ?Sized> NotificationHandler<N, M> for $pointer {
            #[inline]
            fn handle(&self, mediator: &M, notification: &N) -> impl Future<Output = Result<(), N::Error>> + Send {
                (**self).handle(mediator, notification)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<Req: Request, M, T: PipelineBehavior<Req, M> + ?Sized> PipelineBehavior<Req, M> for $pointer {
            #[inline]
            fn handle<N: Next<Req>>(&self, mediator: &M, request: Req, next: N) -> impl Future<Output = Req::Response> + Send {
                (**self).handle(mediator, request, next)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<Req: StreamRequest, M, T: StreamPipelineBehavior<Req, M> + ?Sized> StreamPipelineBehavior<Req, M> for $pointer {
            #[inline]
            fn handle<N: NextStream<Req>>(&self, mediator: &M, request: Req, next: N) -> impl Stream<Item = Req::Item> + Send {
                (**self).handle(mediator, request, next)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<Req: Request, M, T: RequestPreProcessor<Req, M> + ?Sized> RequestPreProcessor<Req, M> for $pointer {
            #[inline]
            fn process(&self, mediator: &M, request: &Req) -> impl Future<Output = ()> + Send {
                (**self).process(mediator, request)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<Req: Request, M, T: RequestPostProcessor<Req, M> + ?Sized> RequestPostProcessor<Req, M> for $pointer {
            #[inline]
            fn process(&self, mediator: &M, request: &Req, response: &Req::Response) -> impl Future<Output = ()> + Send {
                (**self).process(mediator, request, response)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<Req, M, T> RequestErrorHandler<Req, M> for $pointer
        where
            Req: Request,
            Req::Response: Fallible,
            T: RequestErrorHandler<Req, M> + ?Sized,
        {
            #[inline]
            fn handle(&self, mediator: &M, request: &Req, error: <Req::Response as Fallible>::Error) -> impl Future<Output = Req::Response> + Send {
                (**self).handle(mediator, request, error)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<Req, M, T> RequestErrorAction<Req, M> for $pointer
        where
            Req: Request,
            Req::Response: Fallible,
            T: RequestErrorAction<Req, M> + ?Sized,
        {
            #[inline]
            fn execute(&self, mediator: &M, request: &Req, error: &<Req::Response as Fallible>::Error) -> impl Future<Output = ()> + Send {
                (**self).execute(mediator, request, error)
            }
        }
    )*};
}

delegate!(&T, alloc::boxed::Box<T>, alloc::sync::Arc<T>);

// A pointer to a mediator dispatches like the mediator, so an
// `Arc<App>` can go wherever an owned, cloneable sender is needed: a
// saga dispatcher, a spawned task.
macro_rules! delegate_dispatch {
    ($($pointer:ty $(: $extra:ident)?),*) => {$(
        impl<Req: Request, T: Sender<Req> + ?Sized $(+ $extra)?> Sender<Req> for $pointer {
            #[inline]
            fn send(&self, request: Req) -> impl Future<Output = Req::Response> + Send {
                (**self).send(request)
            }
        }

        impl<Req: StreamRequest, T: StreamSender<Req> + ?Sized $(+ $extra)?> StreamSender<Req> for $pointer {
            #[inline]
            fn create_stream(&self, request: Req) -> impl Stream<Item = Req::Item> + Send {
                (**self).create_stream(request)
            }
        }

        impl<N: Notification, T: Publisher<N> + ?Sized $(+ $extra)?> Publisher<N> for $pointer {
            #[inline]
            fn publish(&self, notification: &N) -> impl Future<Output = Result<(), N::Error>> + Send {
                (**self).publish(notification)
            }
        }
    )*};
}

delegate_dispatch!(&T, alloc::boxed::Box<T>, alloc::sync::Arc<T>: Send);

/// The surface a composition needs, in one import.
pub mod prelude {
    pub use crate::{
        Deliveries, Delivery, Fallible, Next, NextStream, Notification, NotificationHandler,
        NotificationPublisher, PipelineBehavior, Publisher, Request, RequestErrorAction,
        RequestErrorHandler, RequestHandler, RequestPostProcessor, RequestPreProcessor, Sender,
        StreamPipelineBehavior, StreamRequest, StreamRequestHandler, StreamSender, mediator,
    };
    pub use futures::Stream;
}

// ---------------------------------------------------------------------------
// What the generated code builds on. Not API: the shapes change with the
// macro, which is versioned in lockstep with this crate.
// ---------------------------------------------------------------------------

#[doc(hidden)]
mod private;

#[doc(hidden)]
pub mod __private {
    pub use crate::private::*;
}
