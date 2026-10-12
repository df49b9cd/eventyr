# eventyr-mediator

MediatR's design for Rust, composed at compile time: requests and their
handlers, stream requests, notifications, pipeline behaviors, pre- and
post-processors, error handlers and error actions, and publish
strategies. The `mediator!` macro (from `eventyr-mediator-macros`)
composes them into one concrete type that implements `Sender`,
`StreamSender`, and `Publisher` for exactly the messages it routes. With
the `store` feature, eventyr's `AggregateRepository` is a handler like
any other, and the mediator can dispatch a saga's commands.

**Position:** an edge crate. It depends on core and the store side
(behind `store`); nothing in the workspace depends on it. Parent:
[ARCHITECTURE.md](../ARCHITECTURE.md); the scope decision is
[DESIGN.md §2](../../DESIGN.md#2-non-goals) and §15.1.

## Why a macro, not a registry

MediatR resolves handlers at runtime from a DI container that assembly
scanning filled. The .NET answer to that cost is source generation:
`martinothamar/Mediator` keeps MediatR's API and generates the dispatch
at build time. (MediatR itself, at 14.2, still dispatches through the
container.) The Rust equivalent is a proc macro. A composition names
every handler, so:

- dispatch is a static, monomorphized call chain — no `TypeId` map, no
  `Box<dyn Any>`, no allocation per send;
- a request no route handles is a compile error at the `send` call,
  instead of a runtime "handler not found";
- every stage of a pipeline is checked against every route when the
  mediator is compiled.

## The vocabulary

| MediatR | eventyr-mediator |
|---|---|
| `IRequest<TResponse>` | `Request` (`type Response`) |
| `IRequestHandler<TRequest, TResponse>` | `RequestHandler<Req, M>` |
| `ISender.Send` | `Sender<Req>::send`; the inherent `App::send` |
| `ISender.Send(object)` | a generated request enum: `requests AnyRequest -> AnyResponse;` |
| `IStreamRequest<T>` / `IStreamRequestHandler` | `StreamRequest` / `StreamRequestHandler<Req, M>` |
| `ISender.CreateStream` | `StreamSender<Req>::create_stream` |
| `INotification` / `INotificationHandler<T>` | `Notification` (`type Error`) / `NotificationHandler<N, M>` |
| `IPublisher.Publish` | `Publisher<N>::publish` |
| `IPublisher.Publish(object)` | a generated notification enum: `notifications AnyNotification -> AnyError;` |
| `INotificationPublisher` | `publish sequential;` (`ForeachAwaitPublisher`), `publish concurrent;` (`TaskWhenAllPublisher`), or `publish with P;` for a `NotificationPublisher` of your own |
| `IPipelineBehavior<TRequest, TResponse>` | `PipelineBehavior<Req, M>` |
| `IStreamPipelineBehavior` | `StreamPipelineBehavior<Req, M>` |
| `IRequestPreProcessor` / `IRequestPostProcessor` | `RequestPreProcessor<Req, M>` / `RequestPostProcessor<Req, M>` |
| `IRequestExceptionHandler` / `IRequestExceptionAction` | `RequestErrorHandler<Req, M>` / `RequestErrorAction<Req, M>`, over a `Fallible` (`Result`-shaped) response |
| `RequestExceptionActionProcessorStrategy` | `error_action_strategy unhandled;` (default) / `all;` |
| Generic request handlers | generic routes: `query<E: Entity> Get<E> => Repository;` |
| `ISender` injected into a handler | a bound on the handler's mediator parameter: `impl<M: Sender<Other>> RequestHandler<Req, M>` |
| Open-generic constraints on a behavior | the behavior's impl bounds, resolved per route at compile time |
| Assembly scanning, service lifetimes | the composition: a stage named by type is constructed once, a stage named `self.field` is the composition root's instance |

One deliberate difference: there are no exceptions to catch. Errors
are values, the error pipeline works on `Result`-shaped responses
through `Fallible`, and panics are not intercepted.

## A composition

```rust
mediator! {
    /// Attributes and docs go on the mediator struct.
    #[derive(Clone)]
    pub struct App {
        // Fields are the composition root's dependencies; `App::new`
        // takes them in declared order.
        accounts: Arc<AggregateRepository<Account, Store>>,
        stamp: StampMetadata<fn() -> Option<String>>,
    }

    // A route names a message and its handler: a type (constructed
    // once with `Default`) or a field (`self.accounts`, borrowed).
    command Execute<Account> as ExecuteAccount => self.accounts;
    query Load<Account> as LoadAccount => self.accounts;
    command OpenAccount => OpenAccountHandler;
    query<E: Entity> Get<E> => Lookup;
    stream History => HistoryHandler;
    event Opened => AuditHandler, NotifyHandler;
    event Closed;                       // routed, nobody handles it yet

    // Each stage list appears once. A stage wraps the routes its impl
    // bounds accept and is skipped on the others; a stage that
    // applies to no route fails the build.
    behaviors { Tracing, self.stamp }   // outermost first
    stream_behaviors { RateLimit }
    pre_processors { Validate }
    post_processors { Metrics }
    error_handlers { Fallback }
    error_actions { Alert }
    error_action_strategy unhandled;    // or `all`
    publish concurrent;                 // `sequential` (default), or `with self.publisher`

    // Dispatch by value; attributes before the clause go on the
    // message enum, after `->` on the output enum.
    #[derive(Debug)]
    requests AnyRequest -> AnyResponse;
    streams AnyStream -> AnyItem;
    notifications AnyNotification -> AnyNotificationError;
}
```

The composition generates:

- the struct, a `new` taking its fields in order, and inherent `send`,
  `create_stream`, and `publish`;
- one `Sender`, `StreamSender`, or `Publisher` impl per route, each
  building its pipeline as a nested value on the stack per call;
- a hidden `Arc` holding the stages named by type, constructed once by
  `new` and shared by clones of the mediator;
- the dispatch enums, with `From` for each routed message.

A request's pipeline runs, from the outside in: the error actions and
the error handlers (actions outermost under `unhandled`, so they see what
no handler recovered), the pre-processors, the post-processors, the
behaviors in declared order, and the handler. That is MediatR's
registration order.

## On eventyr (`store` feature)

The mediator is the seam between an edge and the domain. The edge sends
requests and never names an aggregate, a repository, a store, or a
retry policy:

- `Execute<A>` runs a command through the repository's write path: load,
  fold, decide, append, and retry on conflict. Its response is the
  repository's `Result<ExecutionOutcome, ExecutionError>`; an edge maps
  it to statuses (committed 201, already committed 200, domain rejection
  422, exhausted conflict 409, unavailable 503). `Execute::idempotent(key)`
  makes a repeat answer the earlier commit.
- `Load<A>`, `LoadAt<A>`, and with `time` `LoadUntil<A>` read an
  instance's folded state.
- `AggregateRepository<A, S, SS>` handles all of them, plain or
  snapshotting, held directly or in an `Arc`. Routing goes through
  `eventyr_store::Repository`, the trait both repository kinds share.
- `StampMetadata` is a behavior. It fills an interaction's correlation
  and causation ids on every request that carries `Metadata`
  (`HasMetadata`), so appended events trace back to the edge request.
- `saga_dispatcher::<A, _, _>(mediator, parse_id)` is the dispatcher a
  `SagaProjection` takes. Each saga command becomes an `Execute<A>` sent
  through the mediator's pipeline, carrying the saga's idempotency key,
  so a redelivered event cannot execute its commands twice.

Handlers that translate edge requests into aggregate commands
sub-dispatch: `impl<M: Sender<Execute<Wallet>>> RequestHandler<Deposit, M>`
sends the command through the same mediator, behaviors included.

The store remains the bus. Notifications are in-process and lost on a
crash. A fact that must survive is an event in the log, and its
reactions are subscriptions and sagas.

## Features

- `store`: the `store` module (requests, the repository handler
  impls, `StampMetadata`, `saga_dispatcher`). Pulls in eventyr-core and
  eventyr-store.
- `time`: `LoadUntil<A>`.

The umbrella's `mediator` feature turns on `store` and re-exports the
crate as `eventyr::mediator`.

## Testing

- Integration suites, one per area: `requests`, `pipeline`, `errors`,
  `notifications`, `streams`, `dynamic` (dispatch enums, generic routes,
  generic mediators), and `store` (a derived aggregate end to end:
  routing, metadata stamping, idempotency, a snapshotting repository, a
  saga dispatching through the mediator).
- trybuild UI tests pin every composition diagnostic: unrouted request,
  duplicate route, a second handler on a request, a route without a
  handler, a handler that does not handle its route, a message that is
  not a request, a typed handler without `Default`, a post-processor on
  a non-`Clone` request, a stage that applies nowhere, empty and clashing
  dispatch enums, and an unknown clause.
- The umbrella's `tests/mediator.rs` composes through `eventyr::mediator`
  against an in-memory store.

## Limits

- A generic route is a family, so it is not a variant of a dispatch
  enum, and the stages of its kind are not checked for applying
  somewhere: whether a stage applies to a member of the family is
  decided per message, at each `send`. (For concrete routes the check
  does run, and a stage that applies to no route fails the build.)
- A stage or handler named by type is a singleton. Per-dispatch state
  belongs in the request, and shared mutable state behind `Mutex` or
  `Arc` in a field.
- A stream handler or stream behavior named by type is held by the
  mediator, so the streams it returns borrow the mediator.
