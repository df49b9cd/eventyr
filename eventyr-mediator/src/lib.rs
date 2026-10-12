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
//! stage that applies to no route is skipped silently, as in MediatR.
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
/// combinators, and a publisher can wrap them (log, time, retry) or
/// order its own. Implement it generically over `N` to serve every
/// notification the mediator routes.
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
pub mod __private {
    use core::cell::Cell;
    use core::marker::PhantomData;

    use super::{
        Fallible, Future, Next, NextStream, Notification, NotificationHandler, PipelineBehavior,
        Request, RequestErrorAction, RequestErrorHandler, RequestHandler, RequestPostProcessor,
        RequestPreProcessor, StreamPipelineBehavior, StreamRequest, StreamRequestHandler,
    };

    pub use alloc::sync::Arc;
    pub use futures::future::Either;
    pub use futures::stream::{Stream, StreamExt};

    // -- instances and the diagnostics around them ----------------------

    /// A handler, behavior, or processor the composition names by type:
    /// constructed once, when the mediator is built.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is named by type, so the mediator constructs it once with `Default` — it does not implement `Default`",
        label = "constructed when the mediator is built",
        note = "derive or implement `Default` for it, or put an instance in a field of the mediator and name the field: `self.field`"
    )]
    pub trait Singleton: Sized {
        fn construct() -> Self;
    }

    impl<T: core::default::Default> Singleton for T {
        #[inline]
        fn construct() -> Self {
            T::default()
        }
    }

    #[inline]
    pub fn singleton<T: Singleton>() -> T {
        T::construct()
    }

    // -- handlers: one assertion, one terminus ---------------------------
    //
    // A route checks its handler with one assertion (the bound on
    // `handles_*`), and builds its terminus through a `HandlerProbe`
    // that falls back to `Unreachable` when the handler does not apply:
    // a handler that does not handle its route fails the build at the
    // assertion, once, and nothing built on the terminus reports again.

    /// Asserts that `H` handles the request `Req` under the mediator `M`.
    #[inline]
    pub fn handles_request<Req: Request, M, H: RequestHandler<Req, M> + ?Sized>(_: &H) {}

    /// Asserts that `H` handles the stream request `Req` under `M`.
    #[inline]
    pub fn handles_stream<Req: StreamRequest, M, H: StreamRequestHandler<Req, M> + ?Sized>(_: &H) {}

    /// Asserts that `H` handles the notification `N` under `M`.
    #[inline]
    pub fn handles_notification<N: Notification, M, H: NotificationHandler<N, M> + ?Sized>(_: &H) {}

    /// One route's handler, before selection.
    pub struct HandlerProbe<'a, Req, M, H: ?Sized> {
        mediator: &'a M,
        handler: &'a H,
        _route: PhantomData<fn(Req)>,
    }

    impl<'a, Req, M, H: ?Sized> HandlerProbe<'a, Req, M, H> {
        #[inline]
        pub fn new(mediator: &'a M, handler: &'a H) -> Self {
            Self {
                mediator,
                handler,
                _route: PhantomData,
            }
        }
    }

    /// The request handler at the end of the route's pipeline.
    pub trait ApplyTerminus {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<'a, Req, M, H> ApplyTerminus for HandlerProbe<'a, Req, M, H>
    where
        Req: Request,
        H: RequestHandler<Req, M> + ?Sized,
    {
        type Output = Terminus<'a, M, &'a H>;

        #[inline]
        fn select(&self) -> Self::Output {
            Terminus {
                mediator: self.mediator,
                handler: self.handler,
            }
        }
    }

    /// The stream handler at the end of the route's pipeline.
    pub trait ApplyStreamTerminus {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<'a, Req, M, H> ApplyStreamTerminus for HandlerProbe<'a, Req, M, H>
    where
        Req: StreamRequest,
        H: StreamRequestHandler<Req, M>,
    {
        type Output = StreamTerminus<'a, M, H>;

        #[inline]
        fn select(&self) -> Self::Output {
            StreamTerminus {
                mediator: self.mediator,
                handler: self.handler,
            }
        }
    }

    /// One notification handler, ready to deliver.
    pub trait ApplyDelivery {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<'a, N, M, H> ApplyDelivery for HandlerProbe<'a, N, M, H>
    where
        N: Notification,
        H: NotificationHandler<N, M> + ?Sized,
    {
        type Output = Handled<'a, N, M, H>;

        #[inline]
        fn select(&self) -> Self::Output {
            Handled {
                mediator: self.mediator,
                handler: self.handler,
                _notification: PhantomData,
            }
        }
    }

    /// The fallback: the handler does not apply, and the build already
    /// failed at the route's assertion.
    pub trait SkipHandler {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<Req, M, H: ?Sized> SkipHandler for &HandlerProbe<'_, Req, M, H> {
        type Output = Unreachable<Req>;

        #[inline]
        fn select(&self) -> Self::Output {
            Unreachable(PhantomData)
        }
    }

    /// A notification handler and its mediator, before the notification
    /// is attached.
    pub struct Handled<'a, N, M, H: ?Sized> {
        mediator: &'a M,
        handler: &'a H,
        _notification: PhantomData<fn(N)>,
    }

    impl<'a, N, M, H: ?Sized> Handled<'a, N, M, H> {
        /// Attaches the notification: one handler's delivery.
        #[inline]
        pub fn to(self, notification: &'a N) -> Deliver<'a, N, M, H> {
            Deliver {
                mediator: self.mediator,
                handler: self.handler,
                notification,
            }
        }
    }

    /// One handler's delivery of one notification.
    pub struct Deliver<'a, N, M, H: ?Sized> {
        mediator: &'a M,
        handler: &'a H,
        notification: &'a N,
    }

    impl<N, M, H> crate::Delivery<N> for Deliver<'_, N, M, H>
    where
        N: Notification,
        M: Sync,
        H: NotificationHandler<N, M> + ?Sized,
    {
        #[inline]
        fn deliver(self) -> impl Future<Output = Result<(), N::Error>> + Send {
            self.handler.handle(self.mediator, self.notification)
        }
    }

    /// Stands in for a handler that does not handle its route. Only built
    /// in a composition whose route assertion already failed, so no
    /// program that runs it compiles.
    pub struct Unreachable<Req>(PhantomData<fn(Req)>);

    impl<Req: Request> Next<Req> for Unreachable<Req> {
        async fn run(self, _: Req) -> Req::Response {
            unreachable!("a route whose handler does not handle it does not compile")
        }
    }

    impl<Req: StreamRequest> NextStream<Req> for Unreachable<Req> {
        fn run(self, _: Req) -> impl Stream<Item = Req::Item> + Send {
            futures::stream::poll_fn(|_| {
                unreachable!("a route whose handler does not handle it does not compile")
            })
        }
    }

    impl<N> Unreachable<N> {
        /// Never runs: see the type.
        pub fn to(self, _: &N) -> Self {
            self
        }
    }

    impl<N: Notification> crate::Delivery<N> for Unreachable<N> {
        async fn deliver(self) -> Result<(), N::Error> {
            unreachable!("a route whose handler does not handle it does not compile")
        }
    }

    // -- the inherent dispatch, keyed on the message -------------------
    //
    // A mediator's inherent `send` is bounded on the request (`Req:
    // RequestRoute<App>`), not on the mediator (`App: Sender<Req>`): an
    // obligation whose self type is still an inference variable is not
    // used to infer it, so a mediator with a single route reports an
    // unrouted request as unrouted — instead of inferring the one route
    // and reporting a type mismatch.

    /// A request the mediator `M` routes; implemented by the composition.
    #[diagnostic::on_unimplemented(
        message = "the mediator `{M}` has no handler for the request `{Self}`",
        label = "`{Self}` is not routed by this mediator",
        note = "route it in the composition: `command {Self} => SomeHandler;` (or `query`)"
    )]
    pub trait RequestRoute<M>: Request {
        fn send(mediator: &M, request: Self) -> impl Future<Output = Self::Response> + Send;
    }

    /// A stream request the mediator `M` routes.
    #[diagnostic::on_unimplemented(
        message = "the mediator `{M}` has no handler for the stream request `{Self}`",
        label = "`{Self}` is not routed by this mediator",
        note = "route it in the composition: `stream {Self} => SomeHandler;`"
    )]
    pub trait StreamRoute<M>: StreamRequest {
        fn create_stream(mediator: &M, request: Self) -> impl Stream<Item = Self::Item> + Send;
    }

    /// A notification the mediator `M` publishes.
    #[diagnostic::on_unimplemented(
        message = "the mediator `{M}` does not publish the notification `{Self}`",
        label = "`{Self}` is not routed by this mediator",
        note = "route it in the composition: `event {Self} => HandlerA, HandlerB;`"
    )]
    pub trait NotificationRoute<M>: Notification {
        fn publish<'a>(
            mediator: &'a M,
            notification: &'a Self,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    }

    /// A request a pipeline stage keeps a copy of: post-processors and
    /// the error stages see the request after the handler consumed it.
    #[diagnostic::on_unimplemented(
        message = "the request `{Self}` must be `Clone`: a post-processor or an error stage applies to its route and keeps a copy",
        label = "kept past the handler",
        note = "derive `Clone` for the request, or narrow that processor's impl bounds so they exclude it"
    )]
    pub trait Retained: Sized {
        fn retain(&self) -> Self;
    }

    impl<T: Clone> Retained for T {
        #[inline]
        fn retain(&self) -> Self {
            self.clone()
        }
    }

    // -- request chain -------------------------------------------------

    /// A behavior around the rest of a request pipeline.
    pub struct Layer<'a, M, B, N> {
        mediator: &'a M,
        behavior: B,
        next: N,
    }

    impl<M, Req, B, N> Next<Req> for Layer<'_, M, B, N>
    where
        M: Sync,
        Req: Request,
        B: PipelineBehavior<Req, M>,
        N: Next<Req>,
    {
        #[inline]
        async fn run(self, request: Req) -> Req::Response {
            self.behavior
                .handle(self.mediator, request, self.next)
                .await
        }
    }

    /// The handler at the end of a request pipeline; selected by
    /// [`HandlerProbe`].
    pub struct Terminus<'a, M, H> {
        mediator: &'a M,
        handler: H,
    }

    impl<M, Req, H> Next<Req> for Terminus<'_, M, H>
    where
        M: Sync,
        Req: Request,
        H: RequestHandler<Req, M>,
    {
        #[inline]
        async fn run(self, request: Req) -> Req::Response {
            self.handler.handle(self.mediator, request).await
        }
    }

    /// The route's pre-processors, in order, before the rest.
    pub struct PreProcess<'a, M, P, N> {
        mediator: &'a M,
        processors: P,
        next: N,
    }

    impl<M, Req, P, N> Next<Req> for PreProcess<'_, M, P, N>
    where
        M: Sync,
        Req: Request + Sync,
        P: PreProcessors<Req, M>,
        N: Next<Req>,
    {
        #[inline]
        async fn run(self, request: Req) -> Req::Response {
            self.processors.process(self.mediator, &request).await;
            self.next.run(request).await
        }
    }

    /// The route's post-processors, in order, after the rest answers.
    pub struct PostProcess<'a, M, P, N> {
        mediator: &'a M,
        processors: P,
        next: N,
    }

    impl<M, Req, P, N> Next<Req> for PostProcess<'_, M, P, N>
    where
        M: Sync,
        Req: Request + Retained + Sync,
        Req::Response: Sync,
        P: PostProcessors<Req, M>,
        N: Next<Req>,
    {
        #[inline]
        async fn run(self, request: Req) -> Req::Response {
            let kept = request.retain();
            let response = self.next.run(request).await;
            self.processors
                .process(self.mediator, &kept, &response)
                .await;
            response
        }
    }

    /// An error handler around the rest: recovers its errors.
    pub struct Recover<'a, M, H, N> {
        mediator: &'a M,
        handler: H,
        next: N,
    }

    impl<M, Req, H, N> Next<Req> for Recover<'_, M, H, N>
    where
        M: Sync,
        Req: Request + Retained + Sync,
        Req::Response: Fallible,
        H: RequestErrorHandler<Req, M>,
        N: Next<Req>,
    {
        #[inline]
        async fn run(self, request: Req) -> Req::Response {
            let kept = request.retain();
            match self.next.run(request).await.into_result() {
                Ok(ok) => Fallible::from_result(Ok(ok)),
                Err(error) => self.handler.handle(self.mediator, &kept, error).await,
            }
        }
    }

    /// An error action around the rest: observes its errors.
    pub struct Observe<'a, M, A, N> {
        mediator: &'a M,
        action: A,
        next: N,
    }

    impl<M, Req, A, N> Next<Req> for Observe<'_, M, A, N>
    where
        M: Sync,
        Req: Request + Retained + Sync,
        Req::Response: Fallible,
        <Req::Response as Fallible>::Error: Sync,
        A: RequestErrorAction<Req, M>,
        N: Next<Req>,
    {
        #[inline]
        async fn run(self, request: Req) -> Req::Response {
            let kept = request.retain();
            let response = self.next.run(request).await.into_result();
            if let Err(error) = &response {
                self.action.execute(self.mediator, &kept, error).await;
            }
            Fallible::from_result(response)
        }
    }

    /// A processor list: `Processors(p, Processors(q, ()))`.
    pub struct Processors<H, T>(pub H, pub T);

    /// Runs a processor list as pre-processors.
    pub trait PreProcessors<Req: Request, M>: Send {
        fn process(&self, mediator: &M, request: &Req) -> impl Future<Output = ()> + Send;
    }

    impl<Req: Request, M> PreProcessors<Req, M> for () {
        #[inline]
        fn process(&self, _: &M, _: &Req) -> impl Future<Output = ()> + Send {
            core::future::ready(())
        }
    }

    impl<Req, M, H, T> PreProcessors<Req, M> for Processors<H, T>
    where
        Req: Request + Sync,
        M: Sync,
        H: RequestPreProcessor<Req, M>,
        T: PreProcessors<Req, M> + Sync,
    {
        #[inline]
        async fn process(&self, mediator: &M, request: &Req) {
            self.0.process(mediator, request).await;
            self.1.process(mediator, request).await;
        }
    }

    /// Runs a processor list as post-processors.
    pub trait PostProcessors<Req: Request, M>: Send {
        fn process(
            &self,
            mediator: &M,
            request: &Req,
            response: &Req::Response,
        ) -> impl Future<Output = ()> + Send;
    }

    impl<Req: Request, M> PostProcessors<Req, M> for () {
        #[inline]
        fn process(&self, _: &M, _: &Req, _: &Req::Response) -> impl Future<Output = ()> + Send {
            core::future::ready(())
        }
    }

    impl<Req, M, H, T> PostProcessors<Req, M> for Processors<H, T>
    where
        Req: Request + Sync,
        Req::Response: Sync,
        M: Sync,
        H: RequestPostProcessor<Req, M>,
        T: PostProcessors<Req, M> + Sync,
    {
        #[inline]
        async fn process(&self, mediator: &M, request: &Req, response: &Req::Response) {
            self.0.process(mediator, request, response).await;
            self.1.process(mediator, request, response).await;
        }
    }

    /// Wraps the rest of a pipeline in a processor stage only when the
    /// route's list is non-empty.
    pub trait Stage<'a, M: 'a, N>: Sized {
        type Pre;
        type Post;
        fn pre(self, mediator: &'a M, next: N) -> Self::Pre;
        fn post(self, mediator: &'a M, next: N) -> Self::Post;
    }

    impl<'a, M: 'a, N> Stage<'a, M, N> for () {
        type Pre = N;
        type Post = N;

        #[inline]
        fn pre(self, _: &'a M, next: N) -> N {
            next
        }

        #[inline]
        fn post(self, _: &'a M, next: N) -> N {
            next
        }
    }

    impl<'a, M: 'a, N, H, T> Stage<'a, M, N> for Processors<H, T> {
        type Pre = PreProcess<'a, M, Self, N>;
        type Post = PostProcess<'a, M, Self, N>;

        #[inline]
        fn pre(self, mediator: &'a M, next: N) -> Self::Pre {
            PreProcess {
                mediator,
                processors: self,
                next,
            }
        }

        #[inline]
        fn post(self, mediator: &'a M, next: N) -> Self::Post {
            PostProcess {
                mediator,
                processors: self,
                next,
            }
        }
    }

    // -- stream chain --------------------------------------------------

    /// A behavior around the rest of a stream pipeline. The stream the
    /// behavior returns may borrow it for `'a`.
    pub struct StreamLayer<'a, M, B, N> {
        mediator: &'a M,
        behavior: &'a B,
        next: N,
    }

    impl<'a, M, Req, B, N> NextStream<Req> for StreamLayer<'a, M, B, N>
    where
        M: Sync,
        Req: StreamRequest,
        B: StreamPipelineBehavior<Req, M>,
        N: NextStream<Req>,
    {
        #[inline]
        fn run(self, request: Req) -> impl Stream<Item = Req::Item> + Send {
            self.behavior.handle(self.mediator, request, self.next)
        }
    }

    /// The handler at the end of a stream pipeline; selected by
    /// [`HandlerProbe`].
    pub struct StreamTerminus<'a, M, H> {
        mediator: &'a M,
        handler: &'a H,
    }

    impl<'a, M, Req, H> NextStream<Req> for StreamTerminus<'a, M, H>
    where
        M: Sync,
        Req: StreamRequest,
        H: StreamRequestHandler<Req, M>,
    {
        #[inline]
        fn run(self, request: Req) -> impl Stream<Item = Req::Item> + Send {
            self.handler.handle(self.mediator, request)
        }
    }

    // -- route selection: open-generic filtering at compile time --------
    //
    // Each listed stage is wrapped in a `Probe` for each route, and
    // `(&probe).select()` resolves by autoref specialization: the
    // applying impl (on `Probe`) is found first when the stage's trait
    // bounds hold for the route; otherwise the fallback (on `&Probe`)
    // returns the rest of the chain unchanged. A stage whose bounds
    // exclude a route is skipped for that route instead of failing the
    // build. The kind marker keeps a type that is, say, both a behavior
    // and a pre-processor from making two applying impls ambiguous.

    /// Probe kind: a pipeline behavior.
    pub struct BehaviorKind;
    /// Probe kind: a stream pipeline behavior.
    pub struct StreamBehaviorKind;
    /// Probe kind: a pre-processor.
    pub struct PreKind;
    /// Probe kind: a post-processor.
    pub struct PostKind;
    /// Probe kind: an error handler.
    pub struct RecoverKind;
    /// Probe kind: an error action.
    pub struct ObserveKind;

    /// One candidate stage for one route, and the rest of its chain.
    pub struct Probe<'a, K, Req, M, C, N> {
        mediator: &'a M,
        slot: Cell<Option<(C, N)>>,
        _kind: PhantomData<fn(K, Req)>,
    }

    impl<'a, K, Req, M, C, N> Probe<'a, K, Req, M, C, N> {
        #[inline]
        pub fn new(mediator: &'a M, candidate: C, next: N) -> Self {
            Self {
                mediator,
                slot: Cell::new(Some((candidate, next))),
                _kind: PhantomData,
            }
        }

        #[inline]
        fn take(&self) -> (C, N) {
            match self.slot.take() {
                Some(pair) => pair,
                None => unreachable!("generated code selects each probe once"),
            }
        }
    }

    /// Applies a pipeline behavior.
    pub trait ApplyBehavior {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<'a, Req, M, B, N> ApplyBehavior for Probe<'a, BehaviorKind, Req, M, B, N>
    where
        Req: Request,
        B: PipelineBehavior<Req, M>,
    {
        type Output = Layer<'a, M, B, N>;

        #[inline]
        fn select(&self) -> Self::Output {
            let (behavior, next) = self.take();
            Layer {
                mediator: self.mediator,
                behavior,
                next,
            }
        }
    }

    /// Applies a stream pipeline behavior.
    pub trait ApplyStreamBehavior {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<'a, Req, M, B, N> ApplyStreamBehavior for Probe<'a, StreamBehaviorKind, Req, M, &'a B, N>
    where
        Req: StreamRequest,
        B: StreamPipelineBehavior<Req, M>,
    {
        type Output = StreamLayer<'a, M, B, N>;

        #[inline]
        fn select(&self) -> Self::Output {
            let (behavior, next) = self.take();
            StreamLayer {
                mediator: self.mediator,
                behavior,
                next,
            }
        }
    }

    /// Applies a pre-processor: prepends it to the route's list.
    pub trait ApplyPre {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<Req, M, P, T> ApplyPre for Probe<'_, PreKind, Req, M, P, T>
    where
        Req: Request,
        P: RequestPreProcessor<Req, M>,
    {
        type Output = Processors<P, T>;

        #[inline]
        fn select(&self) -> Self::Output {
            let (processor, rest) = self.take();
            Processors(processor, rest)
        }
    }

    /// Applies a post-processor: prepends it to the route's list.
    pub trait ApplyPost {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<Req, M, P, T> ApplyPost for Probe<'_, PostKind, Req, M, P, T>
    where
        Req: Request,
        P: RequestPostProcessor<Req, M>,
    {
        type Output = Processors<P, T>;

        #[inline]
        fn select(&self) -> Self::Output {
            let (processor, rest) = self.take();
            Processors(processor, rest)
        }
    }

    /// Applies an error handler around the rest of the chain.
    pub trait ApplyRecover {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<'a, Req, M, H, N> ApplyRecover for Probe<'a, RecoverKind, Req, M, H, N>
    where
        Req: Request,
        Req::Response: Fallible,
        H: RequestErrorHandler<Req, M>,
    {
        type Output = Recover<'a, M, H, N>;

        #[inline]
        fn select(&self) -> Self::Output {
            let (handler, next) = self.take();
            Recover {
                mediator: self.mediator,
                handler,
                next,
            }
        }
    }

    /// Applies an error action around the rest of the chain.
    pub trait ApplyObserve {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<'a, Req, M, A, N> ApplyObserve for Probe<'a, ObserveKind, Req, M, A, N>
    where
        Req: Request,
        Req::Response: Fallible,
        A: RequestErrorAction<Req, M>,
    {
        type Output = Observe<'a, M, A, N>;

        #[inline]
        fn select(&self) -> Self::Output {
            let (action, next) = self.take();
            Observe {
                mediator: self.mediator,
                action,
                next,
            }
        }
    }

    /// The fallback for every probe: the candidate does not apply to
    /// this route, and the rest of the chain passes through unchanged.
    /// One autoref further than the applying impls, so it is chosen only
    /// when they do not hold.
    pub trait Skip {
        type Output;
        fn select(&self) -> Self::Output;
    }

    impl<K, Req, M, C, N> Skip for &Probe<'_, K, Req, M, C, N> {
        type Output = N;

        #[inline]
        fn select(&self) -> N {
            self.take().1
        }
    }

    // -- every listed stage applies somewhere -------------------------
    //
    // A stage listed in a composition but applying to none of its routes
    // is a mistake — a missing impl, or bounds no routed message meets —
    // so the composition checks each listed stage against its routes,
    // with the same autoref selection the chains use, and fails the
    // build at the list when none applies.

    /// The stage applies to the route probed.
    pub struct Applies;
    /// The stage does not apply to the route probed.
    pub struct Skips;

    /// One stage against one route, by type only.
    pub struct Check<K, Req, M, C>(PhantomData<fn(K, Req, M, C)>);

    impl<K, Req, M, C> Check<K, Req, M, C> {
        #[inline]
        pub fn new(_: &M, _: &C) -> Self {
            Self(PhantomData)
        }
    }

    pub trait CheckBehavior {
        #[inline]
        fn check(&self) -> Applies {
            Applies
        }
    }

    impl<Req: Request, M, B: PipelineBehavior<Req, M>> CheckBehavior
        for Check<BehaviorKind, Req, M, B>
    {
    }

    pub trait CheckStreamBehavior {
        #[inline]
        fn check(&self) -> Applies {
            Applies
        }
    }

    impl<Req: StreamRequest, M, B: StreamPipelineBehavior<Req, M>> CheckStreamBehavior
        for Check<StreamBehaviorKind, Req, M, B>
    {
    }

    pub trait CheckPre {
        #[inline]
        fn check(&self) -> Applies {
            Applies
        }
    }

    impl<Req: Request, M, P: RequestPreProcessor<Req, M>> CheckPre for Check<PreKind, Req, M, P> {}

    pub trait CheckPost {
        #[inline]
        fn check(&self) -> Applies {
            Applies
        }
    }

    impl<Req: Request, M, P: RequestPostProcessor<Req, M>> CheckPost for Check<PostKind, Req, M, P> {}

    pub trait CheckRecover {
        #[inline]
        fn check(&self) -> Applies {
            Applies
        }
    }

    impl<Req, M, H> CheckRecover for Check<RecoverKind, Req, M, H>
    where
        Req: Request,
        Req::Response: Fallible,
        H: RequestErrorHandler<Req, M>,
    {
    }

    pub trait CheckObserve {
        #[inline]
        fn check(&self) -> Applies {
            Applies
        }
    }

    impl<Req, M, A> CheckObserve for Check<ObserveKind, Req, M, A>
    where
        Req: Request,
        Req::Response: Fallible,
        A: RequestErrorAction<Req, M>,
    {
    }

    /// The fallback: the stage does not apply to the route probed.
    pub trait CheckSkip {
        #[inline]
        fn check(&self) -> Skips {
            Skips
        }
    }

    impl<K, Req, M, C> CheckSkip for &Check<K, Req, M, C> {}

    /// Some route in the list applies.
    #[diagnostic::on_unimplemented(
        message = "this pipeline stage applies to none of the mediator's routes",
        label = "applies to no route",
        note = "a stage applies to the routes whose messages its impl's bounds accept: check that it implements its stage trait (`PipelineBehavior`, `RequestPreProcessor`, `RequestErrorHandler`, …) for a request this mediator routes — or take it out of the list"
    )]
    pub trait AppliesSomewhere {}

    #[diagnostic::do_not_recommend]
    impl<T> AppliesSomewhere for (Applies, T) {}

    #[diagnostic::do_not_recommend]
    impl<T: AppliesSomewhere> AppliesSomewhere for (Skips, T) {}

    #[inline]
    pub fn applies_somewhere<T: AppliesSomewhere>(_: T) {}
}
