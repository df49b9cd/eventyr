//! eventyr's write side behind the mediator.
//!
//! The mediator is the seam between an edge (HTTP, a queue consumer, a
//! scheduler) and the domain: the edge sends requests and never names an
//! aggregate repository, a store, or a retry policy. This module supplies
//! the requests that reach eventyr's repository, the handler that serves
//! them, and the two pieces of glue an event-sourced system needs around
//! them:
//!
//! - [`Execute<A>`] runs a command against an aggregate instance —
//!   load, fold, decide, append, retry on conflict — and answers the
//!   repository's outcome. [`Load<A>`] and [`LoadAt<A>`] fold an
//!   instance's state without a command (and [`LoadUntil<A>`] to an
//!   instant, behind the `time` feature). An `AggregateRepository<A, …>`
//!   handles all of them — plain or snapshotting, held directly or in an
//!   `Arc` — so route it as a field of the mediator:
//!   `command Execute<Account> => self.accounts;`.
//! - [`StampMetadata`] is a pipeline behavior filling an interaction's
//!   metadata — correlation and causation ids — on every request that
//!   carries it ([`HasMetadata`]), so the events a command appends can be
//!   traced back to the edge request that caused them.
//! - [`saga_dispatcher`] turns a mediator into the dispatcher a
//!   [`SagaProjection`](https://docs.rs/eventyr-subscription) sends its
//!   commands through: each saga command becomes an [`Execute`] request
//!   carrying the saga's idempotency key, so a redelivered event cannot
//!   execute its commands twice.
//!
//! The store remains the bus. A command's outcome is facts in the log;
//! reactions to them are subscriptions and sagas, which survive a crash.
//! In-process notifications ([`Notification`](crate::Notification)) are
//! for what may be lost.

use alloc::string::String;
use core::fmt;
use core::future::Future;

use eventyr_core::aggregate::Aggregate;
use eventyr_core::envelope::Metadata;
use eventyr_core::error::StoreError;
use eventyr_core::saga::SagaCommand;
use eventyr_core::vocabulary::{StreamId, Version};
use eventyr_store::repository::{
    AggregateRepository, ExecutionError, ExecutionOutcome, Loaded, Repository,
};

use crate::{Next, PipelineBehavior, Request, RequestHandler, Sender};

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// Executes `command` against the aggregate instance `id` — the write
/// path (load, fold, decide, append, retry) as a request.
///
/// `metadata` is stamped on every event the command appends. Its
/// [`idempotency_key`](Metadata::idempotency_key) makes the command
/// idempotent: a repeat finds the earlier commit in the stream and
/// answers [`ExecutionOutcome::AlreadyCommitted`] without deciding
/// again.
///
/// The response is the repository's: the committed events, or a domain
/// rejection (`ExecutionError::Domain`), or a store failure
/// (`ExecutionError::Store`). A route maps it to its edge's status —
/// `Committed` to 201, `AlreadyCommitted` to 200, a domain rejection to
/// 422, an exhausted conflict to 409, `Unavailable` to 503.
pub struct Execute<A: Aggregate> {
    /// The aggregate instance.
    pub id: A::Id,
    /// What to decide against its state.
    pub command: A::Command,
    /// Stamped on every appended event.
    pub metadata: Metadata,
}

impl<A: Aggregate> Execute<A> {
    /// `command` against `id`, with empty metadata.
    pub fn new(id: A::Id, command: A::Command) -> Self {
        Self {
            id,
            command,
            metadata: Metadata::default(),
        }
    }

    /// Builder-style: the metadata to stamp on the appended events.
    ///
    /// An [`idempotency_key`](Metadata::idempotency_key) the command
    /// already carries is kept — it is what a redelivered saga command
    /// matches the earlier commit with, and an interaction's metadata
    /// must not clobber it. The rest of `metadata` replaces the
    /// command's.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Metadata {
            idempotency_key: metadata.idempotency_key.or(self.metadata.idempotency_key),
            ..metadata
        };
        self
    }

    /// Builder-style: makes the command idempotent under `key` — a
    /// repeat answers the earlier commit instead of deciding again.
    #[must_use]
    pub fn idempotent(mut self, key: impl Into<String>) -> Self {
        self.metadata.idempotency_key = Some(key.into());
        self
    }
}

impl<A: Aggregate> Clone for Execute<A>
where
    A::Id: Clone,
    A::Command: Clone,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            command: self.command.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

impl<A: Aggregate> fmt::Debug for Execute<A>
where
    A::Command: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Execute")
            .field("aggregate", &A::NAME)
            .field("id", &self.id)
            .field("command", &self.command)
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[diagnostic::do_not_recommend]
impl<A> Request for Execute<A>
where
    A: Aggregate,
    A::Id: Send,
    A::Command: Send,
    A::Event: Send,
    A::Error: Send,
{
    type Response = Result<ExecutionOutcome<A::Event>, ExecutionError<A>>;
}

/// Folds the aggregate instance `id` to its current state — the read
/// path, as a request. An unknown instance loads its initial state at
/// `Version::EMPTY`.
pub struct Load<A: Aggregate> {
    /// The aggregate instance.
    pub id: A::Id,
}

impl<A: Aggregate> Load<A> {
    /// The current state of `id`.
    pub fn new(id: A::Id) -> Self {
        Self { id }
    }
}

impl<A: Aggregate> Clone for Load<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
        }
    }
}

impl<A: Aggregate> fmt::Debug for Load<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Load")
            .field("aggregate", &A::NAME)
            .field("id", &self.id)
            .finish()
    }
}

#[diagnostic::do_not_recommend]
impl<A> Request for Load<A>
where
    A: Aggregate,
    A::Id: Send,
    A::State: Send,
{
    type Response = Result<Loaded<A::State>, StoreError>;
}

/// Folds the aggregate instance `id` up to `version`, inclusive — its
/// state as of that point in its history.
pub struct LoadAt<A: Aggregate> {
    /// The aggregate instance.
    pub id: A::Id,
    /// The last version to fold.
    pub version: Version,
}

impl<A: Aggregate> LoadAt<A> {
    /// The state of `id` as of `version`.
    pub fn new(id: A::Id, version: Version) -> Self {
        Self { id, version }
    }
}

impl<A: Aggregate> Clone for LoadAt<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            version: self.version,
        }
    }
}

impl<A: Aggregate> fmt::Debug for LoadAt<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadAt")
            .field("aggregate", &A::NAME)
            .field("id", &self.id)
            .field("version", &self.version)
            .finish()
    }
}

#[diagnostic::do_not_recommend]
impl<A> Request for LoadAt<A>
where
    A: Aggregate,
    A::Id: Send,
    A::State: Send,
{
    type Response = Result<Loaded<A::State>, StoreError>;
}

/// Folds the aggregate instance `id` up to `timestamp`, inclusive — its
/// state as of that instant. Needs a store that records commit times.
#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
pub struct LoadUntil<A: Aggregate> {
    /// The aggregate instance.
    pub id: A::Id,
    /// The last instant to fold.
    pub timestamp: time::OffsetDateTime,
}

#[cfg(feature = "time")]
impl<A: Aggregate> LoadUntil<A> {
    /// The state of `id` as of `timestamp`.
    pub fn new(id: A::Id, timestamp: time::OffsetDateTime) -> Self {
        Self { id, timestamp }
    }
}

#[cfg(feature = "time")]
impl<A: Aggregate> Clone for LoadUntil<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            timestamp: self.timestamp,
        }
    }
}

#[cfg(feature = "time")]
impl<A: Aggregate> fmt::Debug for LoadUntil<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadUntil")
            .field("aggregate", &A::NAME)
            .field("id", &self.id)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

#[cfg(feature = "time")]
#[diagnostic::do_not_recommend]
impl<A> Request for LoadUntil<A>
where
    A: Aggregate,
    A::Id: Send,
    A::State: Send,
{
    type Response = Result<Loaded<A::State>, StoreError>;
}

// ---------------------------------------------------------------------------
// The repository is their handler
// ---------------------------------------------------------------------------

// An `AggregateRepository` handles the requests above whichever snapshot
// channel it carries: the impls go through eventyr-store's `Repository`
// trait, which picks the plain or the snapshot path. A repository held by
// reference, `Box`, or `Arc` handles them through the crate's pointer
// delegation.

#[diagnostic::do_not_recommend]
impl<A, S, SS, M> RequestHandler<Execute<A>, M> for AggregateRepository<A, S, SS>
where
    A: Aggregate,
    A::Id: Send,
    A::Command: Send,
    A::Event: Send,
    A::Error: Send,
    Self: Repository<A>,
    M: Sync,
{
    fn handle(
        &self,
        _: &M,
        request: Execute<A>,
    ) -> impl Future<Output = Result<ExecutionOutcome<A::Event>, ExecutionError<A>>> + Send {
        Repository::execute_with_metadata(self, request.id, request.command, request.metadata)
    }
}

#[diagnostic::do_not_recommend]
impl<A, S, SS, M> RequestHandler<Load<A>, M> for AggregateRepository<A, S, SS>
where
    A: Aggregate,
    A::Id: Send,
    A::State: Send,
    Self: Repository<A>,
    M: Sync,
{
    fn handle(
        &self,
        _: &M,
        request: Load<A>,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        Repository::load(self, request.id)
    }
}

#[diagnostic::do_not_recommend]
impl<A, S, SS, M> RequestHandler<LoadAt<A>, M> for AggregateRepository<A, S, SS>
where
    A: Aggregate,
    A::Id: Send,
    A::State: Send,
    Self: Repository<A>,
    M: Sync,
{
    fn handle(
        &self,
        _: &M,
        request: LoadAt<A>,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        Repository::load_at(self, request.id, request.version)
    }
}

/// Snapshots record versions, not instants, so a load to an instant
/// folds the stream from the start on either kind of repository.
#[cfg(feature = "time")]
#[diagnostic::do_not_recommend]
impl<A, S, SS, M> RequestHandler<LoadUntil<A>, M> for AggregateRepository<A, S, SS>
where
    A: Aggregate,
    A::Id: Send + Sync,
    A::State: Send,
    A::Event: Send + Sync,
    S: eventyr_store::store::EventStore<Event = A::Event> + Send + Sync,
    SS: Send + Sync,
    M: Sync,
{
    fn handle(
        &self,
        _: &M,
        request: LoadUntil<A>,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        self.load_until(request.id, request.timestamp)
    }
}

// ---------------------------------------------------------------------------
// Interaction metadata
// ---------------------------------------------------------------------------

/// A request that carries interaction metadata — [`Execute`] does, and
/// application requests that reach a repository through a handler can
/// too. [`StampMetadata`] wraps exactly the routes whose request
/// implements it.
pub trait HasMetadata {
    /// The interaction's metadata, to fill in.
    fn metadata_mut(&mut self) -> &mut Metadata;
}

impl<A: Aggregate> HasMetadata for Execute<A> {
    fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
}

/// Fills an interaction's correlation ids on every request that carries
/// metadata ([`HasMetadata`]) — the edge's request id as the
/// correlation id, and as the causation id when the request has none —
/// without overwriting what the caller already set.
///
/// The ids come from a [`CorrelationSource`]: a per-request value the
/// edge sets, or a generator. List it among the composition's behaviors;
/// it wraps the routes whose request carries metadata and no others.
#[derive(Clone, Copy, Debug, Default)]
pub struct StampMetadata<C>(pub C);

/// Where [`StampMetadata`] takes an interaction's correlation id from.
pub trait CorrelationSource: Send + Sync {
    /// The correlation id for the request being dispatched, if there is
    /// one.
    fn correlation_id(&self) -> Option<String>;
}

impl<F: Fn() -> Option<String> + Send + Sync> CorrelationSource for F {
    fn correlation_id(&self) -> Option<String> {
        self()
    }
}

impl<Req, M, C> PipelineBehavior<Req, M> for StampMetadata<C>
where
    Req: Request + HasMetadata,
    M: Sync,
    C: CorrelationSource,
{
    fn handle<N: Next<Req>>(
        &self,
        _: &M,
        mut request: Req,
        next: N,
    ) -> impl Future<Output = Req::Response> + Send {
        if let Some(id) = self.0.correlation_id() {
            let metadata = request.metadata_mut();
            if metadata.correlation_id.is_none() {
                metadata.correlation_id = Some(id.clone());
            }
            if metadata.causation_id.is_none() {
                metadata.causation_id = Some(id);
            }
        }
        next.run(request)
    }
}

// ---------------------------------------------------------------------------
// The mediator as a saga dispatcher
// ---------------------------------------------------------------------------

/// Why a saga's command did not commit, as the dispatcher answers it.
///
/// A store failure keeps its classification; a domain rejection becomes
/// [`StoreError::other`] naming the aggregate and the refusal. Either
/// way the projection's `apply` fails, and the runner wraps that as its
/// own `StoreError::other` — a dispatch failure is never `Unavailable`
/// to the runner, so it is parked or halted by policy, not retried.
fn dispatch_failure<A: Aggregate>(error: ExecutionError<A>) -> StoreError {
    match error {
        ExecutionError::Store(error) => error,
        ExecutionError::Domain(rejection) => StoreError::other(alloc::format!(
            "{} rejected the saga command: {rejection}",
            A::NAME
        )),
    }
}

/// The aggregate instance a saga command targets: its stream id is
/// `"{A::NAME}-{id}"` (see `StreamId::for_aggregate`), and
/// [`StreamId::aggregate_id`] reads the id back.
fn target_id<A: Aggregate>(
    target: &StreamId,
    parse: &impl Fn(&str) -> Option<A::Id>,
) -> Result<A::Id, StoreError> {
    match target.aggregate_id::<A>(parse) {
        Some(id) => Ok(id),
        None => Err(StoreError::other(alloc::format!(
            "the saga targeted {target}, which is not a {} stream",
            A::NAME
        ))),
    }
}

/// Makes `mediator` the dispatcher of a `SagaProjection` over aggregate
/// `A`: each `SagaCommand` the saga issues becomes an [`Execute<A>`]
/// request — targeting the instance its stream id names, read back by
/// `parse_id` — sent through the mediator's pipeline.
///
/// The saga's metadata travels with the command, idempotency key
/// included: if the projection crashes between dispatch and ack, the
/// redelivered event re-issues the command under the same key, and the
/// target stream answers the earlier commit instead of executing twice.
/// A domain rejection fails the dispatch, so the projection's failure
/// policy (halt, or park) decides what happens to the event.
///
/// `mediator` is anything that dispatches `Execute<A>`: an
/// `Arc<App>`, or a `&App` that outlives the projection.
pub fn saga_dispatcher<A, S, P>(mediator: S, parse_id: P) -> impl SagaDispatcher<A::Command>
where
    A: Aggregate,
    A::Id: Send,
    A::Command: Send,
    A::Event: Send,
    A::Error: Send,
    S: Sender<Execute<A>> + Clone + Send,
    P: Fn(&str) -> Option<A::Id> + Clone + Send,
{
    move |command: SagaCommand<A::Command>| {
        let mediator = mediator.clone();
        let parse_id = parse_id.clone();
        async move {
            let id = target_id::<A>(&command.target, &parse_id)?;
            let request = Execute::<A>::new(id, command.command).with_metadata(command.metadata);
            mediator
                .send(request)
                .await
                .map(drop)
                .map_err(dispatch_failure::<A>)
        }
    }
}

/// A saga dispatcher: what `SagaProjection::new` takes — a function from
/// a saga's command to the future dispatching it. Named, so a function
/// can return one.
pub trait SagaDispatcher<C>:
    FnMut(SagaCommand<C>) -> <Self as SagaDispatcher<C>>::Future + Send
{
    /// The future dispatching one command.
    type Future: Future<Output = Result<(), StoreError>> + Send;
}

impl<C, F, Fut> SagaDispatcher<C> for F
where
    F: FnMut(SagaCommand<C>) -> Fut + Send,
    Fut: Future<Output = Result<(), StoreError>> + Send,
{
    type Future = Fut;
}

/// Parses an aggregate id from the part of a stream id after
/// `"{NAME}-"`, for ids that implement `FromStr` — the usual
/// `parse_id` for [`saga_dispatcher`].
pub fn parse_id<I: core::str::FromStr>(raw: &str) -> Option<I> {
    raw.parse().ok()
}
