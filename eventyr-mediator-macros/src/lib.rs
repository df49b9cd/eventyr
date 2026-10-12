#![cfg_attr(docsrs, feature(doc_cfg))]

//! The composition macro behind
//! [`eventyr-mediator`](https://docs.rs/eventyr-mediator).
//!
//! Depended on through `eventyr-mediator` (or the `eventyr` umbrella's
//! `mediator` feature), which re-exports [`mediator!`] beside the
//! vocabulary it composes. The expansion names that crate's paths, so
//! this crate is never used on its own.

use proc_macro::TokenStream;

mod attrs;
mod expand;
mod parse;

/// Composes a mediator: a struct that implements `Sender`,
/// `StreamSender`, and `Publisher` for exactly the messages it routes,
/// each through a pipeline fixed at compile time.
///
/// ```ignore
/// mediator! {
///     /// Attributes and docs go on the mediator struct.
///     #[derive(Clone)]
///     pub struct App {
///         // Fields are the composition root's dependencies: a
///         // repository, a pool, a client. `App::new` takes them in
///         // declared order.
///         accounts: AggregateRepository<Account, Store>,
///         audit: AuditLog,
///     }
///
///     // A route names a message and its handler. A handler named by
///     // type is constructed once with `Default` when the mediator is
///     // built; `self.field` borrows a field instead.
///     command Execute<Account> as OpenOrDeposit => self.accounts;
///     query Load<Account> => self.accounts;
///     query Balance => BalanceHandler;
///     stream History => HistoryHandler;
///     event Deposited => self.audit, Notify;    // any number of handlers
///     event Closed;                             // nobody handles it yet
///
///     // Pipeline stages, each listed once. A stage wraps the routes
///     // its impl bounds accept and is skipped on the others.
///     behaviors { Tracing, Authorize }          // outermost first
///     stream_behaviors { RateLimit }
///     pre_processors { Validate }
///     post_processors { Metrics }
///     error_handlers { Fallback }
///     error_actions { Alert }
///     error_action_strategy unhandled;          // or `all`
///     publish concurrent;                       // or `sequential` (the default)
///
///     // Dispatch by value, MediatR's `Send(object)`: an enum of every
///     // routed request (stream request, notification) and one of
///     // their answers. Attributes before the clause go on the first
///     // enum, attributes after `->` on the second.
///     #[derive(Debug)]
///     requests AnyRequest -> #[derive(Debug)] AnyResponse;
///     streams AnyStream -> AnyItem;
///     notifications AnyNotification -> AnyNotificationError;
///
///     // Only to override where the runtime crate is found.
///     crate = ::vendored::eventyr_mediator;
/// }
/// ```
///
/// A message routed twice, a route without a handler, or a second
/// handler on a request is a compile error at the composition. A
/// request sent to a mediator that does not route it is a compile error
/// at the `send` call.
#[proc_macro]
pub fn mediator(input: TokenStream) -> TokenStream {
    let composition = syn::parse_macro_input!(input as parse::Composition);
    let krate = composition
        .krate
        .clone()
        .unwrap_or_else(attrs::mediator_crate);
    expand::expand(&composition, &krate)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
