//! Expands a parsed composition into the mediator: its struct, the
//! singletons it owns, one dispatch impl per route, and the dispatch
//! enums.

use std::collections::BTreeMap;

use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{GenericParam, Ident, Path, Type};

use crate::parse::{
    ActionStrategy, Composition, Enumeration, Instance, PublishStrategy, Route, RouteKind,
};

/// Expands `composition`, naming the runtime crate `krate`.
pub(crate) fn expand(composition: &Composition, krate: &Path) -> syn::Result<TokenStream> {
    validate(composition)?;
    let expansion = Expansion::new(composition, krate);
    let mut out = expansion.mediator();
    for route in &composition.routes {
        out.extend(match route.kind {
            RouteKind::Command | RouteKind::Query => expansion.request(route),
            RouteKind::Stream => expansion.stream(route),
            RouteKind::Event => expansion.event(route),
        });
    }
    out.extend(expansion.checks());
    if let Some(enumeration) = &composition.requests {
        out.extend(expansion.requests(enumeration)?);
    }
    if let Some(enumeration) = &composition.streams {
        out.extend(expansion.streams(enumeration)?);
    }
    if let Some(enumeration) = &composition.notifications {
        out.extend(expansion.notifications(enumeration)?);
    }
    Ok(out)
}

/// The mediator's generics with a route's own merged in: the impl
/// generics of that route's dispatch impls.
fn merged(mediator: &syn::Generics, route: &syn::Generics) -> syn::Generics {
    let mut merged = mediator.clone();
    merged.params.extend(route.params.iter().cloned());
    if let Some(clause) = &route.where_clause {
        merged
            .make_where_clause()
            .predicates
            .extend(clause.predicates.iter().cloned());
    }
    merged
}

/// The message key duplicates are found by: the type's tokens.
fn key(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

/// The type as written, for docs and messages.
fn display(ty: &Type) -> String {
    key(ty).replace(' ', "")
}

/// Which route kinds share one namespace (and one dispatch enum).
fn class(kind: RouteKind) -> u8 {
    match kind {
        RouteKind::Command | RouteKind::Query => 0,
        RouteKind::Stream => 1,
        RouteKind::Event => 2,
    }
}

/// A route's variant in a dispatch enum: `as Variant`, or the last
/// segment of the message's path.
fn variant(route: &Route) -> syn::Result<Ident> {
    if let Some(variant) = &route.variant {
        return Ok(variant.clone());
    }
    if let Type::Path(path) = &route.message
        && path.qself.is_none()
        && let Some(segment) = path.path.segments.last()
    {
        return Ok(segment.ident.clone());
    }
    Err(syn::Error::new_spanned(
        &route.message,
        "name this route's variant in the dispatch enum: `… as Variant => …`",
    ))
}

/// Rejects what no expansion can make sense of: a message routed twice,
/// and dispatch enums with clashing or missing variants.
fn validate(composition: &Composition) -> syn::Result<()> {
    let mut errors: Option<syn::Error> = None;
    let mut push = |error: syn::Error| match &mut errors {
        Some(all) => all.combine(error),
        None => errors = Some(error),
    };

    let mut seen: BTreeMap<(u8, String), &Route> = BTreeMap::new();
    for route in &composition.routes {
        let slot = (class(route.kind), key(&route.message));
        if let Some(first) = seen.get(&slot) {
            let message = match route.kind {
                RouteKind::Event => format!(
                    "`{}` is routed twice; list all its handlers in one route: `event {} => A, B;`",
                    display(&route.message),
                    display(&route.message),
                ),
                _ => format!(
                    "`{}` is routed twice; a {} has exactly one handler",
                    display(&route.message),
                    if route.kind == RouteKind::Stream {
                        "stream request"
                    } else {
                        "request"
                    },
                ),
            };
            push(syn::Error::new_spanned(&route.message, message));
            push(syn::Error::new_spanned(&first.message, "first routed here"));
        } else {
            seen.insert(slot, route);
        }
    }

    let enums = [
        (&composition.requests, 0, "request (`command` or `query`)"),
        (&composition.streams, 1, "`stream`"),
        (&composition.notifications, 2, "`event`"),
    ];
    for (enumeration, wanted, what) in enums {
        let Some(enumeration) = enumeration else {
            continue;
        };
        let mut variants: BTreeMap<String, &Route> = BTreeMap::new();
        let mut any = false;
        for route in composition
            .routes
            .iter()
            .filter(|route| class(route.kind) == wanted)
        {
            if !route.generics.params.is_empty() {
                // A family of messages is not one variant: it is sent by
                // its concrete types, not through the enum.
                continue;
            }
            any = true;
            match variant(route) {
                Ok(name) => {
                    if let Some(first) = variants.get(&name.to_string()) {
                        push(syn::Error::new_spanned(
                            &route.message,
                            format!(
                                "`{}` and `{}` would both be the variant `{name}` of `{}`; name one with `as`: `… as OtherName => …`",
                                display(&first.message),
                                display(&route.message),
                                enumeration.name,
                            ),
                        ));
                    } else {
                        variants.insert(name.to_string(), route);
                    }
                }
                Err(error) => push(error),
            }
        }
        if !any {
            push(syn::Error::new(
                enumeration.keyword,
                format!(
                    "`{}` would be empty: the composition routes no {what} messages",
                    enumeration.name
                ),
            ));
        }
    }

    match errors {
        Some(errors) => Err(errors),
        None => Ok(()),
    }
}

/// One expansion: the composition, the crate path, and the singletons
/// (stages named by type) the mediator owns.
struct Expansion<'c> {
    c: &'c Composition,
    krate: &'c Path,
    /// Field name and type of every singleton, in first-named order.
    singletons: Vec<(Ident, &'c Type)>,
    /// Singleton field by type key.
    index: BTreeMap<String, Ident>,
    /// The singletons struct.
    holder: Ident,
}

impl<'c> Expansion<'c> {
    fn new(c: &'c Composition, krate: &'c Path) -> Self {
        let mut singletons = Vec::new();
        let mut index = BTreeMap::new();
        let named = c
            .routes
            .iter()
            .flat_map(|route| &route.handlers)
            .chain(&c.behaviors)
            .chain(&c.stream_behaviors)
            .chain(&c.pre_processors)
            .chain(&c.post_processors)
            .chain(&c.error_handlers)
            .chain(&c.error_actions)
            .chain(match &c.publish {
                PublishStrategy::Custom(publisher) => Some(publisher),
                _ => None,
            });
        for instance in named {
            if let Instance::Type(ty) = instance
                && let std::collections::btree_map::Entry::Vacant(slot) = index.entry(key(ty))
            {
                let field = format_ident!("h{}", singletons.len());
                slot.insert(field.clone());
                singletons.push((field, &**ty));
            }
        }
        Self {
            c,
            krate,
            singletons,
            index,
            holder: format_ident!("__{}Singletons", c.name),
        }
    }

    /// A stage or handler as a reference expression: into the
    /// singletons, or to the named field.
    fn instance(&self, instance: &Instance) -> TokenStream {
        match instance {
            Instance::Type(ty) => {
                let field = &self.index[&key(ty)];
                quote_spanned!(ty.span()=> &self.__mediator_singletons.#field)
            }
            Instance::Field { members, span } => quote_spanned!(*span=> &self #(.#members)*),
        }
    }

    /// The struct, its singletons, and its inherent API.
    fn mediator(&self) -> TokenStream {
        let Composition {
            attrs,
            vis,
            name,
            generics,
            fields,
            ..
        } = self.c;
        let krate = self.krate;
        let holder = &self.holder;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let (hidden_field, hidden_init, holder_items) = if self.singletons.is_empty() {
            (quote!(), quote!(), quote!())
        } else {
            let names: Vec<_> = self.singletons.iter().map(|(field, _)| field).collect();
            let types: Vec<_> = self.singletons.iter().map(|(_, ty)| *ty).collect();
            let constructs = types
                .iter()
                .map(|ty| quote_spanned!(ty.span()=> #krate::__private::singleton::<#ty>()));
            let phantoms = generics.params.iter().filter_map(|param| match param {
                GenericParam::Type(param) => {
                    let ident = &param.ident;
                    Some(quote!(#ident))
                }
                GenericParam::Lifetime(param) => {
                    let lifetime = &param.lifetime;
                    Some(quote!(&#lifetime ()))
                }
                GenericParam::Const(_) => None,
            });
            let label = format!("{name} singletons");
            (
                quote! {
                    #[doc(hidden)]
                    __mediator_singletons: #krate::__private::Arc<#holder #ty_generics>,
                },
                quote! {
                    __mediator_singletons: #krate::__private::Arc::new(<#holder #ty_generics>::__new()),
                },
                quote! {
                    #[doc(hidden)]
                    #[allow(non_camel_case_types)]
                    struct #holder #generics #where_clause {
                        #(#names: #types,)*
                        __marker: ::core::marker::PhantomData<fn() -> (#(#phantoms,)*)>,
                    }

                    impl #impl_generics #holder #ty_generics #where_clause {
                        fn __new() -> Self {
                            Self {
                                #(#names: #constructs,)*
                                __marker: ::core::marker::PhantomData,
                            }
                        }
                    }

                    impl #impl_generics ::core::default::Default for #holder #ty_generics #where_clause {
                        fn default() -> Self {
                            Self::__new()
                        }
                    }

                    impl #impl_generics ::core::fmt::Debug for #holder #ty_generics #where_clause {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                            f.write_str(#label)
                        }
                    }
                },
            )
        };

        let field_names: Vec<_> = fields.iter().map(|field| &field.ident).collect();
        let field_types: Vec<_> = fields.iter().map(|field| &field.ty).collect();

        quote! {
            #(#attrs)*
            #vis struct #name #generics #where_clause {
                #(#fields,)*
                #hidden_field
            }

            #holder_items

            #[allow(dead_code, clippy::new_without_default, clippy::too_many_arguments)]
            impl #impl_generics #name #ty_generics #where_clause {
                /// Builds the mediator from its fields, in declared order.
                /// The handlers and stages the composition names by type
                /// are constructed here, once.
                pub fn new(#(#field_names: #field_types),*) -> Self {
                    Self {
                        #(#field_names,)*
                        #hidden_init
                    }
                }

                /// Sends `request` through its pipeline to its handler —
                /// MediatR's `ISender.Send`.
                #[inline]
                pub fn send<'__mediator, __Request>(
                    &'__mediator self,
                    request: __Request,
                ) -> impl ::core::future::Future<Output = <__Request as #krate::Request>::Response>
                       + ::core::marker::Send
                       + '__mediator
                where
                    __Request: #krate::__private::RequestRoute<Self> + '__mediator,
                {
                    <__Request as #krate::__private::RequestRoute<Self>>::send(self, request)
                }

                /// Opens the stream answering `request` — MediatR's
                /// `ISender.CreateStream`.
                #[inline]
                pub fn create_stream<'__mediator, __Request>(
                    &'__mediator self,
                    request: __Request,
                ) -> impl #krate::__private::Stream<Item = <__Request as #krate::StreamRequest>::Item>
                       + ::core::marker::Send
                       + '__mediator
                where
                    __Request: #krate::__private::StreamRoute<Self> + '__mediator,
                {
                    <__Request as #krate::__private::StreamRoute<Self>>::create_stream(self, request)
                }

                /// Delivers `notification` to its handlers — MediatR's
                /// `IPublisher.Publish`.
                #[inline]
                pub fn publish<'__mediator, __Notification>(
                    &'__mediator self,
                    notification: &'__mediator __Notification,
                ) -> impl ::core::future::Future<
                       Output = ::core::result::Result<(), <__Notification as #krate::Notification>::Error>,
                   >
                       + ::core::marker::Send
                       + '__mediator
                where
                    __Notification: #krate::__private::NotificationRoute<Self> + '__mediator,
                {
                    <__Notification as #krate::__private::NotificationRoute<Self>>::publish(self, notification)
                }
            }
        }
    }

    /// One check per listed stage: it applies to at least one route of
    /// its kind, or the build fails at the stage.
    ///
    /// A generic route is a family whose stages are selected once, in
    /// the family's generic context: per member is not decidable (that
    /// is monomorphization, after macro expansion). So each generic
    /// route is checked in its own generic context — a stage must apply
    /// to the whole family, or to one of the concrete routes of its
    /// kind, to pass there.
    fn checks(&self) -> TokenStream {
        let krate = self.krate;
        let name = &self.c.name;
        let (impl_generics, ty_generics, where_clause) = self.c.generics.split_for_impl();

        /// The routes of one kind: the concrete ones' messages, and the
        /// generic ones (families) whole.
        struct KindRoutes<'r> {
            concrete: Vec<&'r Type>,
            generic: Vec<&'r Route>,
        }
        let routes_of = |wanted: &dyn Fn(RouteKind) -> bool| -> KindRoutes<'_> {
            let mut concrete = Vec::new();
            let mut generic = Vec::new();
            for route in self.c.routes.iter().filter(|route| wanted(route.kind)) {
                if route.generics.params.is_empty() {
                    concrete.push(&route.message);
                } else {
                    generic.push(route);
                }
            }
            KindRoutes { concrete, generic }
        };
        let requests = routes_of(&|kind| class(kind) == 0);
        let streams = routes_of(&|kind| kind == RouteKind::Stream);

        /// A stage list, its probe kind, and the routes it is checked
        /// against.
        type StageList<'s> = (&'s [Instance], Ident, &'s KindRoutes<'s>);
        let lists: [StageList<'_>; 6] = [
            (&self.c.behaviors, format_ident!("BehaviorKind"), &requests),
            (
                &self.c.stream_behaviors,
                format_ident!("StreamBehaviorKind"),
                &streams,
            ),
            (&self.c.pre_processors, format_ident!("PreKind"), &requests),
            (
                &self.c.post_processors,
                format_ident!("PostKind"),
                &requests,
            ),
            (
                &self.c.error_handlers,
                format_ident!("RecoverKind"),
                &requests,
            ),
            (
                &self.c.error_actions,
                format_ident!("ObserveKind"),
                &requests,
            ),
        ];

        // One stage's probes against `messages`, as a nested tuple.
        let probe = |kind: &Ident, stage: &Instance, messages: &[&Type]| -> TokenStream {
            let stage_expr = self.instance(stage);
            let mut tuple = quote!(());
            for message in messages.iter().rev() {
                tuple = quote! {
                    (
                        (&#krate::__private::StageCheck::<#krate::__private::#kind, #message, Self, _>::new(self, #stage_expr)).check(),
                        #tuple,
                    )
                };
            }
            quote_spanned! {stage.span()=>
                #krate::__private::applies_somewhere({
                    #[allow(unused_imports)]
                    use #krate::__private::{Check as _, CheckSkip as _};
                    #tuple
                });
            }
        };

        let mut checks = Vec::new();
        let mut family_checks = Vec::new();
        for (stages, kind, routes) in lists {
            if routes.generic.is_empty() {
                // All routes of the kind are concrete: one check method
                // in the mediator's own context.
                for stage in stages {
                    checks.push(probe(&kind, stage, &routes.concrete));
                }
                continue;
            }
            // A kind with generic routes checks in each family's own
            // context, one generic method per family; the concrete
            // routes probe anywhere.
            for (index, family) in routes.generic.iter().enumerate() {
                let mut messages = vec![&family.message];
                messages.extend(routes.concrete.iter().copied());
                let probes: Vec<TokenStream> = stages
                    .iter()
                    .map(|stage| probe(&kind, stage, &messages))
                    .collect();
                if probes.is_empty() {
                    continue;
                }
                let method = format_ident!("__mediator_stages_apply_route_{index}");
                let (family_generics, _, family_where_clause) = family.generics.split_for_impl();
                family_checks.push(quote! {
                    /// Never called: type-checks that every listed stage
                    /// of one kind applies to this family (whose bounds
                    /// must hold for the whole family) or to one of the
                    /// kind's concrete routes.
                    #[doc(hidden)]
                    #[allow(dead_code, clippy::needless_borrow, clippy::let_unit_value)]
                    fn #method #family_generics (&self) #family_where_clause {
                        #(#probes)*
                    }
                });
            }
        }
        if checks.is_empty() && family_checks.is_empty() {
            return TokenStream::new();
        }

        quote! {
            impl #impl_generics #name #ty_generics #where_clause {
                /// Never called: type-checks that every listed stage
                /// applies to one of the routes.
                #[doc(hidden)]
                #[allow(dead_code, clippy::needless_borrow, clippy::let_unit_value)]
                fn __mediator_stages_apply(&self) {
                    #(#checks)*
                }

                #(#family_checks)*
            }
        }
    }

    /// `let #rest = (&Probe::<Kind, Msg, Self, _, _>::new(self, stage, #rest)).select();`
    fn probe(&self, kind: &str, message: &Type, stage: &Instance, rest: &str) -> TokenStream {
        let krate = self.krate;
        let kind = format_ident!("{kind}");
        let rest = format_ident!("{rest}");
        let stage_expr = self.instance(stage);
        quote_spanned! {stage.span()=>
            let #rest = (&#krate::__private::Probe::<#krate::__private::#kind, #message, Self, _, _>::new(
                self, #stage_expr, #rest,
            ))
            .select();
        }
    }

    /// `impl Sender<Req>`: the request pipeline, inside out.
    fn request(&self, route: &Route) -> TokenStream {
        let krate = self.krate;
        let message = &route.message;
        let name = &self.c.name;
        let generics = merged(&self.c.generics, &route.generics);
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        let (_, ty_generics, _) = self.c.generics.split_for_impl();
        let handler = &route.handlers[0];
        let handler_expr = self.instance(handler);

        let mut chain = vec![
            quote_spanned! {handler.span()=>
                #krate::__private::handles_request::<#message, Self, _>(#handler_expr);
            },
            quote! {
                let __next = (&#krate::__private::HandlerProbe::<#message, Self, _>::new(self, #handler_expr)).select();
            },
        ];
        for behavior in self.c.behaviors.iter().rev() {
            chain.push(self.probe("BehaviorKind", message, behavior, "__next"));
        }
        chain.push(quote!(let __list = ();));
        for processor in self.c.post_processors.iter().rev() {
            chain.push(self.probe("PostKind", message, processor, "__list"));
        }
        chain.push(quote!(let __next = #krate::__private::Stage::post(__list, self, __next);));
        chain.push(quote!(let __list = ();));
        for processor in self.c.pre_processors.iter().rev() {
            chain.push(self.probe("PreKind", message, processor, "__list"));
        }
        chain.push(quote!(let __next = #krate::__private::Stage::pre(__list, self, __next);));
        let (inner, inner_kind, outer, outer_kind) = match self.c.action_strategy {
            ActionStrategy::Unhandled => (
                &self.c.error_handlers,
                "RecoverKind",
                &self.c.error_actions,
                "ObserveKind",
            ),
            ActionStrategy::All => (
                &self.c.error_actions,
                "ObserveKind",
                &self.c.error_handlers,
                "RecoverKind",
            ),
        };
        for stage in inner {
            chain.push(self.probe(inner_kind, message, stage, "__next"));
        }
        for stage in outer {
            chain.push(self.probe(outer_kind, message, stage, "__next"));
        }

        quote_spanned! {message.span()=>
            impl #impl_generics #krate::Sender<#message> for #name #ty_generics #where_clause {
                #[inline]
                #[allow(
                    unused_imports,
                    clippy::needless_borrow,
                    clippy::let_unit_value,
                    clippy::unit_arg
                )]
                fn send(
                    &self,
                    request: #message,
                ) -> impl ::core::future::Future<Output = <#message as #krate::Request>::Response>
                       + ::core::marker::Send {
                    use #krate::__private::{
                        Apply as _, ApplyTerminus as _, Skip as _, SkipHandler as _,
                    };
                    #(#chain)*
                    <_ as #krate::Next<#message>>::run(__next, request)
                }
            }

            impl #impl_generics #krate::__private::RequestRoute<#name #ty_generics> for #message #where_clause {
                #[inline]
                fn send(
                    mediator: &#name #ty_generics,
                    request: Self,
                ) -> impl ::core::future::Future<Output = <Self as #krate::Request>::Response>
                       + ::core::marker::Send {
                    <#name #ty_generics as #krate::Sender<Self>>::send(mediator, request)
                }
            }
        }
    }

    /// `impl StreamSender<Req>`: the stream pipeline, inside out.
    fn stream(&self, route: &Route) -> TokenStream {
        let krate = self.krate;
        let message = &route.message;
        let name = &self.c.name;
        let generics = merged(&self.c.generics, &route.generics);
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        let (_, ty_generics, _) = self.c.generics.split_for_impl();
        let handler = &route.handlers[0];
        let handler_expr = self.instance(handler);

        let mut chain = vec![
            quote_spanned! {handler.span()=>
                #krate::__private::handles_stream::<#message, Self, _>(#handler_expr);
            },
            quote! {
                let __next = (&#krate::__private::HandlerProbe::<#message, Self, _>::new(self, #handler_expr)).select();
            },
        ];
        for behavior in self.c.stream_behaviors.iter().rev() {
            chain.push(self.probe("StreamBehaviorKind", message, behavior, "__next"));
        }

        quote_spanned! {message.span()=>
            impl #impl_generics #krate::StreamSender<#message> for #name #ty_generics #where_clause {
                #[inline]
                #[allow(unused_imports, clippy::needless_borrow)]
                fn create_stream(
                    &self,
                    request: #message,
                ) -> impl #krate::__private::Stream<Item = <#message as #krate::StreamRequest>::Item>
                       + ::core::marker::Send {
                    use #krate::__private::{
                        Apply as _, ApplyStreamTerminus as _, Skip as _, SkipHandler as _,
                    };
                    #(#chain)*
                    <_ as #krate::NextStream<#message>>::run(__next, request)
                }
            }

            impl #impl_generics #krate::__private::StreamRoute<#name #ty_generics> for #message #where_clause {
                #[inline]
                fn create_stream(
                    mediator: &#name #ty_generics,
                    request: Self,
                ) -> impl #krate::__private::Stream<Item = <Self as #krate::StreamRequest>::Item>
                       + ::core::marker::Send {
                    <#name #ty_generics as #krate::StreamSender<Self>>::create_stream(mediator, request)
                }
            }
        }
    }

    /// `impl Publisher<N>`: the handlers, by the publish strategy.
    fn event(&self, route: &Route) -> TokenStream {
        let krate = self.krate;
        let message = &route.message;
        let name = &self.c.name;
        let generics = merged(&self.c.generics, &route.generics);
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        let (_, ty_generics, _) = self.c.generics.split_for_impl();

        // The handlers' deliveries as a list, `(first, (second, ()))`:
        // what a publish strategy runs.
        let mut deliveries = quote!(());
        for handler in route.handlers.iter().rev() {
            let handler_expr = self.instance(handler);
            deliveries = quote! {
                (
                    (&#krate::__private::HandlerProbe::<#message, Self, _>::new(self, #handler_expr))
                        .select()
                        .to(notification),
                    #deliveries,
                )
            };
        }

        let assertions: Vec<TokenStream> = route
            .handlers
            .iter()
            .map(|handler| {
                let handler_expr = self.instance(handler);
                quote_spanned! {handler.span()=>
                    #krate::__private::handles_notification::<#message, Self, _>(#handler_expr);
                }
            })
            .collect();

        let publish = match &self.c.publish {
            PublishStrategy::Sequential => quote! {
                #krate::Deliveries::<#message>::sequential(__deliveries)
            },
            PublishStrategy::Concurrent => quote! {
                #krate::Deliveries::<#message>::concurrent(__deliveries)
            },
            PublishStrategy::Custom(publisher) => {
                let publisher_expr = self.instance(publisher);
                quote_spanned! {publisher.span()=>
                    #krate::NotificationPublisher::<#message>::publish(
                        #publisher_expr,
                        notification,
                        __deliveries,
                    )
                }
            }
        };

        quote_spanned! {message.span()=>
            impl #impl_generics #krate::Publisher<#message> for #name #ty_generics #where_clause {
                #[inline]
                #[allow(clippy::needless_borrow, clippy::unit_arg)]
                fn publish(
                    &self,
                    notification: &#message,
                ) -> impl ::core::future::Future<
                       Output = ::core::result::Result<(), <#message as #krate::Notification>::Error>,
                   >
                       + ::core::marker::Send {
                    #[allow(unused_imports)]
                    use #krate::__private::{ApplyDelivery as _, SkipHandler as _};
                    #(#assertions)*
                    // Built inside the future: each delivery borrows the
                    // mediator and the notification for one lifetime,
                    // which only the future's body can name.
                    async move {
                        let _ = notification;
                        let __deliveries = #deliveries;
                        #publish.await
                    }
                }
            }

            impl #impl_generics #krate::__private::NotificationRoute<#name #ty_generics> for #message #where_clause {
                #[inline]
                fn publish<'__mediator>(
                    mediator: &'__mediator #name #ty_generics,
                    notification: &'__mediator Self,
                ) -> impl ::core::future::Future<
                       Output = ::core::result::Result<(), <Self as #krate::Notification>::Error>,
                   >
                       + ::core::marker::Send {
                    <#name #ty_generics as #krate::Publisher<Self>>::publish(mediator, notification)
                }
            }
        }
    }

    /// The routes of one class with their variants (validated).
    fn variants(&self, wanted: u8) -> syn::Result<(Vec<&Type>, Vec<Ident>, Vec<String>)> {
        let mut messages = Vec::new();
        let mut variants = Vec::new();
        let mut docs = Vec::new();
        for route in self
            .c
            .routes
            .iter()
            .filter(|route| class(route.kind) == wanted && route.generics.params.is_empty())
        {
            messages.push(&route.message);
            variants.push(variant(route)?);
            docs.push(format!("The `{}` route.", display(&route.message)));
        }
        Ok((messages, variants, docs))
    }

    /// `requests AnyRequest -> AnyResponse;`
    fn requests(&self, enumeration: &Enumeration) -> syn::Result<TokenStream> {
        let krate = self.krate;
        let (messages, variants, docs) = self.variants(0)?;
        let Enumeration {
            attrs,
            name: enum_name,
            output_attrs,
            output,
            ..
        } = enumeration;
        let vis = &self.c.vis;
        let name = &self.c.name;
        let (impl_generics, ty_generics, where_clause) = self.c.generics.split_for_impl();
        let enum_doc = format!(
            "Every request `{name}` routes, as one value — MediatR's `ISender.Send(object)`. Sending it answers `{output}`."
        );
        let output_doc = format!("The answer to an `{enum_name}`, in its request's variant.");

        Ok(quote! {
            #[doc = #enum_doc]
            #(#attrs)*
            #[allow(clippy::large_enum_variant)]
            #vis enum #enum_name {
                #(#[doc = #docs] #variants(#messages),)*
            }

            #[doc = #output_doc]
            #(#output_attrs)*
            #[allow(clippy::large_enum_variant)]
            #vis enum #output {
                #(#[doc = #docs] #variants(<#messages as #krate::Request>::Response),)*
            }

            impl #krate::Request for #enum_name {
                type Response = #output;
            }

            #(
                impl ::core::convert::From<#messages> for #enum_name {
                    #[inline]
                    fn from(request: #messages) -> Self {
                        Self::#variants(request)
                    }
                }
            )*

            impl #impl_generics #krate::__private::RequestRoute<#name #ty_generics> for #enum_name #where_clause {
                #[inline]
                fn send(
                    mediator: &#name #ty_generics,
                    request: Self,
                ) -> impl ::core::future::Future<Output = #output> + ::core::marker::Send {
                    <#name #ty_generics as #krate::Sender<Self>>::send(mediator, request)
                }
            }

            impl #impl_generics #krate::Sender<#enum_name> for #name #ty_generics #where_clause {
                fn send(
                    &self,
                    request: #enum_name,
                ) -> impl ::core::future::Future<Output = #output> + ::core::marker::Send {
                    async move {
                        match request {
                            #(
                                #enum_name::#variants(request) => #output::#variants(
                                    <Self as #krate::Sender<#messages>>::send(self, request).await,
                                ),
                            )*
                        }
                    }
                }
            }
        })
    }

    /// `streams AnyStream -> AnyItem;`
    fn streams(&self, enumeration: &Enumeration) -> syn::Result<TokenStream> {
        let krate = self.krate;
        let (messages, variants, docs) = self.variants(1)?;
        let Enumeration {
            attrs,
            name: enum_name,
            output_attrs,
            output,
            ..
        } = enumeration;
        let vis = &self.c.vis;
        let name = &self.c.name;
        let (impl_generics, ty_generics, where_clause) = self.c.generics.split_for_impl();
        let enum_doc = format!(
            "Every stream request `{name}` routes, as one value. Its stream yields `{output}`."
        );
        let output_doc = format!("An item of an `{enum_name}`'s stream, in its request's variant.");

        // Each arm is a different stream type: nest them in `Either`s.
        let count = messages.len();
        let arms = messages
            .iter()
            .zip(&variants)
            .enumerate()
            .map(|(index, (message, variant))| {
                let mut stream = quote! {
                    #krate::__private::StreamExt::map(
                        <Self as #krate::StreamSender<#message>>::create_stream(self, request),
                        #output::#variant,
                    )
                };
                if index + 1 < count {
                    stream = quote!(#krate::__private::Either::Left(#stream));
                }
                for _ in 0..index {
                    stream = quote!(#krate::__private::Either::Right(#stream));
                }
                quote!(#enum_name::#variant(request) => #stream,)
            });

        Ok(quote! {
            #[doc = #enum_doc]
            #(#attrs)*
            #[allow(clippy::large_enum_variant)]
            #vis enum #enum_name {
                #(#[doc = #docs] #variants(#messages),)*
            }

            #[doc = #output_doc]
            #(#output_attrs)*
            #[allow(clippy::large_enum_variant)]
            #vis enum #output {
                #(#[doc = #docs] #variants(<#messages as #krate::StreamRequest>::Item),)*
            }

            impl #krate::StreamRequest for #enum_name {
                type Item = #output;
            }

            #(
                impl ::core::convert::From<#messages> for #enum_name {
                    #[inline]
                    fn from(request: #messages) -> Self {
                        Self::#variants(request)
                    }
                }
            )*

            impl #impl_generics #krate::__private::StreamRoute<#name #ty_generics> for #enum_name #where_clause {
                #[inline]
                fn create_stream(
                    mediator: &#name #ty_generics,
                    request: Self,
                ) -> impl #krate::__private::Stream<Item = #output> + ::core::marker::Send {
                    <#name #ty_generics as #krate::StreamSender<Self>>::create_stream(mediator, request)
                }
            }

            impl #impl_generics #krate::StreamSender<#enum_name> for #name #ty_generics #where_clause {
                fn create_stream(
                    &self,
                    request: #enum_name,
                ) -> impl #krate::__private::Stream<Item = #output> + ::core::marker::Send {
                    match request {
                        #(#arms)*
                    }
                }
            }
        })
    }

    /// `notifications AnyNotification -> AnyNotificationError;`
    fn notifications(&self, enumeration: &Enumeration) -> syn::Result<TokenStream> {
        let krate = self.krate;
        let (messages, variants, docs) = self.variants(2)?;
        let Enumeration {
            attrs,
            name: enum_name,
            output_attrs,
            output,
            ..
        } = enumeration;
        let vis = &self.c.vis;
        let name = &self.c.name;
        let (impl_generics, ty_generics, where_clause) = self.c.generics.split_for_impl();
        let enum_doc = format!(
            "Every notification `{name}` publishes, as one value — MediatR's `IPublisher.Publish(object)`. A handler's failure is `{output}`."
        );
        let output_doc =
            format!("A handler's failure on an `{enum_name}`, in its notification's variant.");

        Ok(quote! {
            #[doc = #enum_doc]
            #(#attrs)*
            #[allow(clippy::large_enum_variant)]
            #vis enum #enum_name {
                #(#[doc = #docs] #variants(#messages),)*
            }

            #[doc = #output_doc]
            #(#output_attrs)*
            #[allow(clippy::large_enum_variant)]
            #vis enum #output {
                #(#[doc = #docs] #variants(<#messages as #krate::Notification>::Error),)*
            }

            impl #krate::Notification for #enum_name {
                type Error = #output;
            }

            #(
                impl ::core::convert::From<#messages> for #enum_name {
                    #[inline]
                    fn from(notification: #messages) -> Self {
                        Self::#variants(notification)
                    }
                }
            )*

            impl #impl_generics #krate::__private::NotificationRoute<#name #ty_generics> for #enum_name #where_clause {
                #[inline]
                fn publish<'__mediator>(
                    mediator: &'__mediator #name #ty_generics,
                    notification: &'__mediator Self,
                ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #output>>
                       + ::core::marker::Send {
                    <#name #ty_generics as #krate::Publisher<Self>>::publish(mediator, notification)
                }
            }

            impl #impl_generics #krate::Publisher<#enum_name> for #name #ty_generics #where_clause {
                fn publish(
                    &self,
                    notification: &#enum_name,
                ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #output>>
                       + ::core::marker::Send {
                    async move {
                        match notification {
                            #(
                                #enum_name::#variants(notification) => {
                                    <Self as #krate::Publisher<#messages>>::publish(self, notification)
                                        .await
                                        .map_err(#output::#variants)
                                }
                            )*
                        }
                    }
                }
            }
        })
    }
}
