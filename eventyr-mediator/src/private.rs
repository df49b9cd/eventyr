//! What the generated code builds on. Not API: the shapes change with
//! the macro, which is versioned in lockstep with this crate.

use core::cell::Cell;
use core::marker::PhantomData;

use crate::{
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

#[diagnostic::do_not_recommend]
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

/// One `Check<Kind, …>` applies its stage when the stage's trait bounds
/// hold for the route: the marker that "some route applies" looks for.
/// Each stage kind gets one trait and one impl, written out by the macro
/// so adding a kind is one line here, not a trait-impl pair by hand.
macro_rules! check_stage {
    ($check:ident, $kind:ident, $candidate:ident : $($bounds:tt)*) => {
        pub trait $check {
            #[inline]
            fn check(&self) -> Applies {
                Applies
            }
        }

        impl<Req, M, $candidate> $check for Check<$kind, Req, M, $candidate>
        where
            $($bounds)*
        {
        }
    };
}

check_stage!(CheckBehavior, BehaviorKind, B: Req: Request, B: PipelineBehavior<Req, M>);
check_stage!(CheckStreamBehavior, StreamBehaviorKind, B: Req: StreamRequest, B: StreamPipelineBehavior<Req, M>);
check_stage!(CheckPre, PreKind, P: Req: Request, P: RequestPreProcessor<Req, M>);
check_stage!(CheckPost, PostKind, P: Req: Request, P: RequestPostProcessor<Req, M>);
check_stage!(CheckRecover, RecoverKind, H: Req: Request, Req::Response: Fallible, H: RequestErrorHandler<Req, M>);
check_stage!(CheckObserve, ObserveKind, A: Req: Request, Req::Response: Fallible, A: RequestErrorAction<Req, M>);

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
