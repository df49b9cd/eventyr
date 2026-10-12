//! Parses the `mediator!` composition.

use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Attribute, Field, Generics, Ident, Member, Path, Token, Type, Visibility, braced};

mod kw {
    syn::custom_keyword!(command);
    syn::custom_keyword!(query);
    syn::custom_keyword!(stream);
    syn::custom_keyword!(event);
    syn::custom_keyword!(behaviors);
    syn::custom_keyword!(stream_behaviors);
    syn::custom_keyword!(pre_processors);
    syn::custom_keyword!(post_processors);
    syn::custom_keyword!(error_handlers);
    syn::custom_keyword!(error_actions);
    syn::custom_keyword!(error_action_strategy);
    syn::custom_keyword!(unhandled);
    syn::custom_keyword!(all);
    syn::custom_keyword!(publish);
    syn::custom_keyword!(sequential);
    syn::custom_keyword!(concurrent);
    syn::custom_keyword!(with);
    syn::custom_keyword!(requests);
    syn::custom_keyword!(streams);
    syn::custom_keyword!(notifications);
}

/// What a route dispatches.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteKind {
    /// `command`: a request that changes state.
    Command,
    /// `query`: a request that reads.
    Query,
    /// `stream`: a stream request.
    Stream,
    /// `event`: a notification.
    Event,
}

impl RouteKind {
    pub(crate) fn keyword(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Query => "query",
            Self::Stream => "stream",
            Self::Event => "event",
        }
    }
}

/// A handler, behavior, or processor instance.
pub(crate) enum Instance {
    /// Named by type: default-constructed when the mediator is built,
    /// and owned by it.
    Type(Box<Type>),
    /// A field of the mediator, `self.a.b`, borrowed per call.
    Field {
        /// The member path after `self`.
        members: Vec<Member>,
        /// The whole `self.a.b`, for errors.
        span: Span,
    },
}

impl Instance {
    pub(crate) fn span(&self) -> Span {
        match self {
            Self::Type(ty) => ty.span(),
            Self::Field { span, .. } => *span,
        }
    }
}

impl Parse for Instance {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if !input.peek(Token![self]) {
            return Ok(Self::Type(Box::new(input.parse()?)));
        }
        let receiver: Token![self] = input.parse()?;
        let mut span = receiver.span;
        let mut members = Vec::new();
        while input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            let member: Member = input.parse()?;
            span = span.join(member.span()).unwrap_or(span);
            members.push(member);
        }
        if members.is_empty() {
            return Err(syn::Error::new(
                receiver.span,
                "name a field of the mediator: `self.field`",
            ));
        }
        Ok(Self::Field { members, span })
    }
}

/// One route: `command Req [as Variant] => Handler;`, or a generic
/// route over a family of messages: `query<T: Bound> Get<T> => Handler;`.
pub(crate) struct Route {
    pub kind: RouteKind,
    /// The route's own generic parameters and where clause; empty for a
    /// route of one concrete message.
    pub generics: Generics,
    /// The message type.
    pub message: Type,
    /// The variant name in the generated enum, from `as`.
    pub variant: Option<Ident>,
    /// The handlers: exactly one for requests and streams, any number
    /// for events.
    pub handlers: Vec<Instance>,
}

/// A generated dispatch enum: `requests AnyRequest -> AnyResponse;`.
pub(crate) struct Enumeration {
    /// Attributes for the message enum.
    pub attrs: Vec<Attribute>,
    /// The message enum.
    pub name: Ident,
    /// Attributes for the output enum.
    pub output_attrs: Vec<Attribute>,
    /// The output enum: responses, items, or notification errors.
    pub output: Ident,
    /// The clause keyword, for errors.
    pub keyword: Span,
}

/// When error actions run relative to error handlers — MediatR's
/// `RequestExceptionActionProcessorStrategy`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionStrategy {
    /// `ApplyForUnhandledExceptions`, the default: actions see the
    /// errors no error handler recovered.
    Unhandled,
    /// `ApplyForAllExceptions`: actions see every error, before the
    /// error handlers do.
    All,
}

/// How a notification reaches its handlers — MediatR's
/// `INotificationPublisher`.
pub(crate) enum PublishStrategy {
    /// `ForeachAwaitPublisher`, the default.
    Sequential,
    /// `TaskWhenAllPublisher`.
    Concurrent,
    /// A publisher of the application's own: `publish with P;`.
    Custom(Instance),
}

/// The whole composition.
pub(crate) struct Composition {
    pub attrs: Vec<Attribute>,
    pub vis: Visibility,
    pub name: Ident,
    pub generics: Generics,
    pub fields: Vec<Field>,
    pub routes: Vec<Route>,
    pub behaviors: Vec<Instance>,
    pub stream_behaviors: Vec<Instance>,
    pub pre_processors: Vec<Instance>,
    pub post_processors: Vec<Instance>,
    pub error_handlers: Vec<Instance>,
    pub error_actions: Vec<Instance>,
    pub action_strategy: ActionStrategy,
    pub publish: PublishStrategy,
    pub requests: Option<Enumeration>,
    pub streams: Option<Enumeration>,
    pub notifications: Option<Enumeration>,
    pub krate: Option<Path>,
}

impl Parse for Composition {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let vis: Visibility = input.parse()?;
        input.parse::<Token![struct]>().map_err(|error| {
            syn::Error::new(
                error.span(),
                "a composition starts with the mediator's struct: `pub struct App { … }` or `pub struct App;`",
            )
        })?;
        let name: Ident = input.parse()?;
        let mut generics: Generics = input.parse()?;
        generics.where_clause = input.parse()?;
        let fields = if input.peek(Token![;]) {
            input.parse::<Token![;]>()?;
            Vec::new()
        } else if input.peek(syn::token::Brace) {
            let content;
            braced!(content in input);
            content
                .parse_terminated(Field::parse_named, Token![,])?
                .into_iter()
                .collect()
        } else {
            return Err(input.error(
                "expected the mediator's fields `{ … }`, or `;` for a mediator without fields",
            ));
        };

        let mut composition = Self {
            attrs,
            vis,
            name,
            generics,
            fields,
            routes: Vec::new(),
            behaviors: Vec::new(),
            stream_behaviors: Vec::new(),
            pre_processors: Vec::new(),
            post_processors: Vec::new(),
            error_handlers: Vec::new(),
            error_actions: Vec::new(),
            action_strategy: ActionStrategy::Unhandled,
            publish: PublishStrategy::Sequential,
            requests: None,
            streams: None,
            notifications: None,
            krate: None,
        };
        let mut seen = Seen::default();
        while !input.is_empty() {
            composition.clause(input, &mut seen)?;
        }
        Ok(composition)
    }
}

/// Which once-only clauses have appeared.
#[derive(Default)]
struct Seen {
    behaviors: bool,
    stream_behaviors: bool,
    pre_processors: bool,
    post_processors: bool,
    error_handlers: bool,
    error_actions: bool,
    action_strategy: bool,
    publish: bool,
}

/// Rejects a second occurrence of a once-only clause.
fn once(seen: &mut bool, keyword: Span, name: &str) -> syn::Result<()> {
    if core::mem::replace(seen, true) {
        return Err(syn::Error::new(
            keyword,
            format!("`{name}` appears twice; list everything in one clause"),
        ));
    }
    Ok(())
}

impl Composition {
    fn clause(&mut self, input: ParseStream<'_>, seen: &mut Seen) -> syn::Result<()> {
        let attrs = input.call(Attribute::parse_outer)?;
        let lookahead = input.lookahead1();

        // The enum clauses carry attributes onto the generated enums;
        // anywhere else only doc comments are accepted (and dropped).
        if lookahead.peek(kw::requests) {
            let keyword = input.parse::<kw::requests>()?.span;
            let enumeration = enumeration(input, attrs, keyword)?;
            return set_enum(&mut self.requests, enumeration, "requests");
        }
        if lookahead.peek(kw::streams) {
            let keyword = input.parse::<kw::streams>()?.span;
            let enumeration = enumeration(input, attrs, keyword)?;
            return set_enum(&mut self.streams, enumeration, "streams");
        }
        if lookahead.peek(kw::notifications) {
            let keyword = input.parse::<kw::notifications>()?.span;
            let enumeration = enumeration(input, attrs, keyword)?;
            return set_enum(&mut self.notifications, enumeration, "notifications");
        }
        if let Some(attr) = attrs.iter().find(|attr| !attr.path().is_ident("doc")) {
            return Err(syn::Error::new_spanned(
                attr,
                "attributes are accepted only on the `requests`, `streams`, and `notifications` clauses",
            ));
        }

        if lookahead.peek(kw::command) {
            input.parse::<kw::command>()?;
            self.routes.push(route(input, RouteKind::Command)?);
        } else if lookahead.peek(kw::query) {
            input.parse::<kw::query>()?;
            self.routes.push(route(input, RouteKind::Query)?);
        } else if lookahead.peek(kw::stream) {
            input.parse::<kw::stream>()?;
            self.routes.push(route(input, RouteKind::Stream)?);
        } else if lookahead.peek(kw::event) {
            input.parse::<kw::event>()?;
            self.routes.push(route(input, RouteKind::Event)?);
        } else if lookahead.peek(kw::behaviors) {
            let keyword = input.parse::<kw::behaviors>()?.span;
            once(&mut seen.behaviors, keyword, "behaviors")?;
            self.behaviors = list(input)?;
        } else if lookahead.peek(kw::stream_behaviors) {
            let keyword = input.parse::<kw::stream_behaviors>()?.span;
            once(&mut seen.stream_behaviors, keyword, "stream_behaviors")?;
            self.stream_behaviors = list(input)?;
        } else if lookahead.peek(kw::pre_processors) {
            let keyword = input.parse::<kw::pre_processors>()?.span;
            once(&mut seen.pre_processors, keyword, "pre_processors")?;
            self.pre_processors = list(input)?;
        } else if lookahead.peek(kw::post_processors) {
            let keyword = input.parse::<kw::post_processors>()?.span;
            once(&mut seen.post_processors, keyword, "post_processors")?;
            self.post_processors = list(input)?;
        } else if lookahead.peek(kw::error_handlers) {
            let keyword = input.parse::<kw::error_handlers>()?.span;
            once(&mut seen.error_handlers, keyword, "error_handlers")?;
            self.error_handlers = list(input)?;
        } else if lookahead.peek(kw::error_actions) {
            let keyword = input.parse::<kw::error_actions>()?.span;
            once(&mut seen.error_actions, keyword, "error_actions")?;
            self.error_actions = list(input)?;
        } else if lookahead.peek(kw::error_action_strategy) {
            let keyword = input.parse::<kw::error_action_strategy>()?.span;
            once(&mut seen.action_strategy, keyword, "error_action_strategy")?;
            let choice = input.lookahead1();
            self.action_strategy = if choice.peek(kw::unhandled) {
                input.parse::<kw::unhandled>()?;
                ActionStrategy::Unhandled
            } else if choice.peek(kw::all) {
                input.parse::<kw::all>()?;
                ActionStrategy::All
            } else {
                return Err(choice.error());
            };
            input.parse::<Token![;]>()?;
        } else if lookahead.peek(kw::publish) {
            let keyword = input.parse::<kw::publish>()?.span;
            once(&mut seen.publish, keyword, "publish")?;
            let choice = input.lookahead1();
            self.publish = if choice.peek(kw::sequential) {
                input.parse::<kw::sequential>()?;
                PublishStrategy::Sequential
            } else if choice.peek(kw::concurrent) {
                input.parse::<kw::concurrent>()?;
                PublishStrategy::Concurrent
            } else if choice.peek(kw::with) {
                input.parse::<kw::with>()?;
                PublishStrategy::Custom(input.parse()?)
            } else {
                return Err(choice.error());
            };
            input.parse::<Token![;]>()?;
        } else if lookahead.peek(Token![crate]) {
            let keyword = input.parse::<Token![crate]>()?.span;
            if self.krate.is_some() {
                return Err(syn::Error::new(keyword, "`crate` appears twice"));
            }
            input.parse::<Token![=]>()?;
            self.krate = Some(input.call(Path::parse_mod_style)?);
            input.parse::<Token![;]>()?;
        } else {
            return Err(lookahead.error());
        }
        Ok(())
    }
}

/// `[<generics>] Message [as Variant] [where …] => Handler;` — for
/// events `=> A, B;`, or a bare `;` for an event nothing handles yet.
fn route(input: ParseStream<'_>, kind: RouteKind) -> syn::Result<Route> {
    let mut generics: Generics = if input.peek(Token![<]) {
        input.parse()?
    } else {
        Generics::default()
    };
    let message: Type = input.parse()?;
    let variant = if input.peek(Token![as]) {
        let keyword = input.parse::<Token![as]>()?;
        if !generics.params.is_empty() {
            return Err(syn::Error::new(
                keyword.span,
                "a generic route is not a variant of a dispatch enum, so it takes no `as` name",
            ));
        }
        Some(input.parse()?)
    } else {
        None
    };
    if input.peek(Token![where]) {
        if generics.params.is_empty() {
            return Err(input.error(
                "a `where` clause needs the route's own generics: `query<T> Get<T> where T: Bound => …;`",
            ));
        }
        generics.where_clause = Some(input.parse()?);
    }

    let mut handlers = Vec::new();
    if kind == RouteKind::Event {
        if input.peek(Token![=>]) {
            input.parse::<Token![=>]>()?;
            loop {
                handlers.push(input.parse()?);
                if !input.peek(Token![,]) {
                    break;
                }
                input.parse::<Token![,]>()?;
                if input.peek(Token![;]) {
                    break;
                }
            }
        }
    } else {
        if !input.peek(Token![=>]) {
            return Err(input.error(format!(
                "expected `=>` and the handler: `{} {} => SomeHandler;`",
                kind.keyword(),
                quote::ToTokens::to_token_stream(&message),
            )));
        }
        input.parse::<Token![=>]>()?;
        handlers.push(input.parse()?);
        if input.peek(Token![,]) {
            return Err(input.error(format!(
                "a `{}` route has exactly one handler; only an `event` has several",
                kind.keyword()
            )));
        }
    }
    input.parse::<Token![;]>()?;
    Ok(Route {
        kind,
        generics,
        message,
        variant,
        handlers,
    })
}

/// `{ A, B, self.c }`, then an optional `;`.
fn list(input: ParseStream<'_>) -> syn::Result<Vec<Instance>> {
    let content;
    braced!(content in input);
    let items = content
        .parse_terminated(Instance::parse, Token![,])?
        .into_iter()
        .collect();
    if input.peek(Token![;]) {
        input.parse::<Token![;]>()?;
    }
    Ok(items)
}

/// `Name -> [#[attr]] Output;`.
fn enumeration(
    input: ParseStream<'_>,
    attrs: Vec<Attribute>,
    keyword: Span,
) -> syn::Result<Enumeration> {
    let name: Ident = input.parse()?;
    input.parse::<Token![->]>()?;
    let output_attrs = input.call(Attribute::parse_outer)?;
    let output: Ident = input.parse()?;
    input.parse::<Token![;]>()?;
    Ok(Enumeration {
        attrs,
        name,
        output_attrs,
        output,
        keyword,
    })
}

fn set_enum(slot: &mut Option<Enumeration>, value: Enumeration, name: &str) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new(
            value.keyword,
            format!("`{name}` appears twice; a composition generates one such enum"),
        ));
    }
    *slot = Some(value);
    Ok(())
}
