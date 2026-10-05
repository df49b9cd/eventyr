//! `#[derive(Aggregate)]` — convention wiring for the `Aggregate` trait.
//!
//! The derive sits on the aggregate *marker* struct (usually a unit
//! struct) and wires the trait impl by convention, delegating the domain
//! logic to plain functions written next to it. Everything the derive
//! generates is writable by hand — it is sugar, not the API.
//!
//! ## Conventions
//!
//! For `#[derive(Aggregate)] struct Account;` the impl is wired as:
//!
//! - `NAME` = `"account"` (the struct name, snake_cased)
//! - `Id` = `AccountId`, `Event` = `AccountEvent`, `Command` =
//!   `AccountCommand`, `Error` = `AccountError` (the `{Ident}...`
//!   position convention)
//! - `State` = `Self` — so a unit struct is its own (empty) state; point
//!   `state` at a state type for the usual marker-plus-state shape
//! - `initial` = `Default::default()`, or `Self` when a unit struct is
//!   its own state
//! - `apply`/`decide` delegate to free functions named `apply`/`decide`
//!   in the same module
//!
//! Every convention is overridable with `#[eventyr(...)]` attributes:
//!
//! ```ignore
//! #[derive(Aggregate)]
//! #[eventyr(
//!     name = "bank_account",
//!     id = BankAccountId,
//!     state = BankAccountState,
//!     event = BankEvent,
//!     command = BankCommand,
//!     error = BankError,
//!     initial = new_account(id),
//!     apply = fold_event,
//!     decide = decide_command,
//!     crate = "eventyr",
//! )]
//! struct BankAccount;
//! ```
//!
//! `crate` names the crate the generated code targets — `eventyr-core`
//! by default, or the umbrella `eventyr` crate if you depend on that
//! instead (the serde `crate = "..."` pattern).
//!
//! ## The event enum
//!
//! `#[eventyr(events(Opened, Deposited))]` generates the event enum from
//! payload structs (the sourcery pattern): an enum with one newtype
//! variant per payload, a `From<Payload>` conversion for building
//! events in `decide`, and an `EventName` impl naming each variant
//! after its payload. Name it with `#[eventyr(event_enum = BankEvent)]`
//! (default: `{Ident}Event`); the `Event` type follows the enum. The
//! reverse conversion is a `match` — the point of a concrete enum.
//!
//! The generated enum is plain Rust — clone it, match it, derive on it.
//! Storage-bound enums opt into their codecs with
//! `#[eventyr(event_derive(...))]` (extra `#[derive(...)]` paths) and
//! `#[eventyr(event_attr("..."))]` (any other attribute, e.g. a
//! `#[serde(...)]` container attribute).

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Expr, Fields, Ident, Path};

use crate::attrs::{self, absolutize, assign, path_list};
use crate::naming::to_snake_case;

/// Everything `#[derive(Aggregate)]` was told, with conventions filled
/// in where the attributes were silent.
#[derive(Default)]
struct Wiring {
    name: Option<String>,
    id: Option<Path>,
    state: Option<Path>,
    event: Option<Path>,
    command: Option<Path>,
    error: Option<Path>,
    initial: Option<Expr>,
    apply: Option<Path>,
    decide: Option<Path>,
    proc_crate: Option<Path>,
    events: Option<Vec<Path>>,
    event_enum_name: Option<Ident>,
    /// Extra `#[derive(...)]` paths for the generated event enum.
    event_derive: Vec<Path>,
    /// Any other attribute for the generated event enum, parsed from
    /// the string (a `#[serde(...)]` container attribute, say).
    event_attr: Vec<syn::Attribute>,
}

/// `decide` is a function path, like `apply`'s: accepting any
/// expression would let it drift (`decide = f(x)`) into shapes the
/// impl can't wire.
struct DecidedFn(Path);

impl DecidedFn {
    fn into_path(self) -> Path {
        self.0
    }
}

impl syn::parse::Parse for DecidedFn {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        match input.parse::<Expr>()? {
            Expr::Path(path) => Ok(Self(path.path)),
            other => Err(syn::Error::new(
                other.span(),
                "eventyr decide expects a function path",
            )),
        }
    }
}

/// Newtype over the tokens of `event_attr("...")`: a string of one or
/// more outer attributes, parsed as attributes.
struct EventAttrs(Vec<syn::Attribute>);

impl syn::parse::Parse for EventAttrs {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(input.call(syn::Attribute::parse_outer)?))
    }
}

/// The attribute keys `parse_wiring` accepts, in the order the
/// unknown-attribute error lists them. One list: the error and the
/// parser cannot drift apart.
const KEYS: &[&str] = &[
    "name",
    "id",
    "state",
    "event",
    "event_enum",
    "command",
    "error",
    "initial",
    "apply",
    "decide",
    "crate",
    "events",
    "event_derive",
    "event_attr",
];

/// Parses `#[eventyr(...)]` attributes into a [`Wiring`], keeping only
/// what the attributes actually said (conventions are applied later, so
/// the error can point at the right span).
fn parse_wiring(input: &DeriveInput) -> Result<Wiring, syn::Error> {
    let mut wiring = Wiring::default();

    for attr in &input.attrs {
        if !attr.path().is_ident("eventyr") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            // The uniform `key = value` arms.
            if attrs::assign_parsed_lit(&mut wiring.name, "name", &meta)?
                || attrs::assign_parsed(&mut wiring.id, "id", &meta)?
                || attrs::assign_parsed(&mut wiring.state, "state", &meta)?
                || attrs::assign_parsed(&mut wiring.event, "event", &meta)?
                || attrs::assign_parsed(&mut wiring.event_enum_name, "event_enum", &meta)?
                || attrs::assign_parsed(&mut wiring.command, "command", &meta)?
                || attrs::assign_parsed(&mut wiring.error, "error", &meta)?
                || attrs::assign_parsed(&mut wiring.initial, "initial", &meta)?
                || attrs::assign_parsed(&mut wiring.apply, "apply", &meta)?
                || attrs::assign_parsed_map(&mut wiring.decide, "decide", DecidedFn::into_path, &meta)?
            {
                return Ok(());
            }
            if meta.path.is_ident("crate") {
                let value = meta.value()?.parse::<syn::LitStr>()?;
                let path: Path = value.parse()?;
                assign(&mut wiring.proc_crate, "crate", &meta, absolutize(path))
            } else if meta.path.is_ident("events") {
                let list = path_list(&meta)?;
                if list.is_empty() {
                    return Err(syn::Error::new(
                        meta.path.span(),
                        "eventyr events(...) lists the payload types: events(Opened, Deposited)",
                    ));
                }
                assign(&mut wiring.events, "events", &meta, list)
            } else if meta.path.is_ident("event_derive") {
                // Repeatable: two enums rarely want the same extra set
                // twice, and merging keeps `derive` order predictable.
                wiring.event_derive.extend(path_list(&meta)?);
                Ok(())
            } else if meta.path.is_ident("event_attr") {
                // One attribute in a string: writing several
                // `event_attr(...)` keys them apart, and `#[serde(tag
                // = "t", content = "c")]` is several tokens.
                let content;
                if !meta.input.peek(syn::token::Paren) {
                    return Err(syn::Error::new_spanned(
                        &meta.path,
                        r##"expected a parenthesized string, like event_attr("#[serde(tag = "kind")]")"##,
                    ));
                }
                syn::parenthesized!(content in meta.input);
                let value = content.parse::<syn::LitStr>()?;
                wiring
                    .event_attr
                    .extend(syn::parse_str::<EventAttrs>(&value.value())?.0);
                Ok(())
            } else {
                Err(syn::Error::new(
                    meta.path.span(),
                    format!(
                        "unknown eventyr attribute; expected one of {}",
                        KEYS.join(", ")
                    ),
                ))
            }
        })?;
    }

    Ok(wiring)
}

/// Everything the impl needs, conventions resolved. Produced by
/// [`Wiring::resolve`]; emission reads only this.
struct Resolved {
    core: Path,
    name: String,
    id: Path,
    state: TokenStream,
    event: Path,
    command: Path,
    error: Path,
    initial: TokenStream,
    apply: Path,
    decide: Path,
    events: Option<Vec<Path>>,
    enum_ident: Ident,
    event_derive: Vec<Path>,
    event_attr: Vec<syn::Attribute>,
}

impl Wiring {
    /// Apply the conventions: the `{Ident}...` position defaults, a
    /// unit struct as its own state, `Default` for the initial state,
    /// the free `apply`/`decide` functions. Kept apart from
    /// [`expand`]: validation happens there, defaulting here, emission
    /// in [`emit`] — so emission never branches on what was given and
    /// parsing never branches on what is emitted.
    fn resolve(self, ident: &Ident) -> Resolved {
        let core = self
            .proc_crate
            .unwrap_or_else(crate::attrs::default_core_crate);
        let name = self
            .name
            .unwrap_or_else(|| to_snake_case(&ident.to_string()));
        // The `{Ident}...` position convention, as paths.
        let default_path = |suffix| -> Path { format_ident!("{ident}{suffix}").into() };
        let id = self.id.unwrap_or_else(|| default_path("Id"));
        let command = self.command.unwrap_or_else(|| default_path("Command"));
        let error = self.error.unwrap_or_else(|| default_path("Error"));
        // With `events(...)` the generated enum *is* the event type, so
        // the `Event` convention follows it — `event = X` overrides, and
        // `event_enum = X` renames the enum `Event` follows.
        let enum_ident = self
            .event_enum_name
            .unwrap_or_else(|| default_path("Event").segments[0].ident.clone());
        let event = self.event.unwrap_or_else(|| enum_ident.clone().into());
        let state = self
            .state
            .as_ref()
            .map_or_else(|| quote! { Self }, |state| quote! { #state });
        let initial = self.initial.as_ref().map_or_else(
            || {
                if self.state.is_some() {
                    quote! { ::core::default::Default::default() }
                } else {
                    // A unit struct is its own state: `Self`.
                    quote! { Self }
                }
            },
            |initial| quote! { #initial },
        );
        let apply = self.apply.unwrap_or_else(|| default_fn("apply"));
        let decide = self.decide.unwrap_or_else(|| default_fn("decide"));
        Resolved {
            core,
            name,
            id,
            state,
            event,
            command,
            error,
            initial,
            apply,
            decide,
            events: self.events,
            enum_ident,
            event_derive: self.event_derive,
            event_attr: self.event_attr,
        }
    }
}
/// Expands `#[derive(Aggregate)]` into the `Aggregate` impl (and the
/// event enum, when `events(...)` was given).
pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    // The aggregate marker struct must be a unit struct — the aggregate
    // type is a namespace, not a state holder.
    match &input.data {
        Data::Struct(data) if matches!(data.fields, Fields::Unit) => {}
        Data::Struct(_) => {
            return Err(syn::Error::new(
                input.ident.span(),
                "Aggregate must be derived on a unit struct — the aggregate type is a \
                 namespace, not a state holder; put the fields on the state type",
            ));
        }
        Data::Enum(_) | Data::Union(_) => {
            return Err(syn::Error::new(
                input.ident.span(),
                "Aggregate must be derived on a unit struct",
            ));
        }
    }
    emit(input, parse_wiring(input)?.resolve(&input.ident))
}

/// The tokens: the event enum (when `events(...)` was given) and the
/// `Aggregate` impl wiring conventions to delegates.
fn emit(input: &DeriveInput, resolved: Resolved) -> Result<TokenStream, syn::Error> {
    let Resolved {
        core,
        name,
        id,
        state,
        event,
        command,
        error,
        initial,
        apply,
        decide,
        events,
        enum_ident,
        event_derive,
        event_attr,
    } = resolved;
    let ident = &input.ident;

    let event_enum = match &events {
        Some(events) => Some(generate_event_enum(
            events,
            &enum_ident,
            &input.vis,
            &core,
            &event_derive,
            &event_attr,
        )?),
        None => None,
    };

    Ok(quote! {
        #event_enum

        impl #core::aggregate::Aggregate for #ident {
            const NAME: &'static ::core::primitive::str = #name;

            type Id = #id;
            type State = #state;
            type Event = #event;
            type Command = #command;
            type Error = #error;

            #[inline]
            fn initial(id: &Self::Id) -> Self::State {
                #initial
            }

            #[inline]
            fn apply(state: &mut Self::State, event: &Self::Event) {
                #apply(state, event)
            }

            #[inline]
            fn decide(
                state: &Self::State,
                command: &Self::Command,
            ) -> ::core::result::Result<#core::__private::Vec<Self::Event>, Self::Error> {
                #decide(state, command)
            }
        }
    })
}

/// Generates the event enum from payload structs (the sourcery pattern):
/// one newtype variant per payload, a `From<Payload>` conversion, and
/// an `EventName` impl naming each variant after its payload.
///
/// The enum follows the aggregate marker's visibility, so private
/// payload structs don't leak through a `pub` enum.
fn generate_event_enum(
    events: &[Path],
    enum_ident: &Ident,
    visibility: &syn::Visibility,
    core: &Path,
    extra_derives: &[Path],
    extra_attrs: &[syn::Attribute],
) -> Result<TokenStream, syn::Error> {
    // `events(Opened, Deposited)` — the variant name is the payload's
    // own name (the last path segment), so `module::Opened` still
    // becomes the `Opened` variant.
    let variant_idents: Vec<Ident> = events
        .iter()
        .map(|payload| {
            payload
                .segments
                .last()
                .map(|segment| segment.ident.clone())
                .ok_or_else(|| {
                    syn::Error::new_spanned(
                        payload,
                        "eventyr events(...) expects a type path, like events(Opened)",
                    )
                })
        })
        .collect::<Result<_, _>>()?;
    let variants = events
        .iter()
        .zip(&variant_idents)
        .map(|(payload, variant)| {
            quote! {
                /// A domain event payload.
                #variant(#payload)
            }
        });
    let from_impls = events
        .iter()
        .zip(&variant_idents)
        .map(|(payload, variant)| {
            quote! {
                impl ::core::convert::From<#payload> for #enum_ident {
                    #[inline]
                    fn from(payload: #payload) -> Self {
                        Self::#variant(payload)
                    }
                }
            }
        });
    let event_name_arms = variant_idents.iter().map(|variant| {
        quote! {
            Self::#variant(_) => stringify!(#variant),
        }
    });

    Ok(quote! {
        /// The aggregate's domain events, generated from payload
        /// structs by `#[derive(Aggregate)]`.
        #[derive(Clone, Debug, PartialEq, #(#extra_derives),*)]
        #(#extra_attrs)*
        #visibility enum #enum_ident {
            #(#variants,)*
        }

        #(#from_impls)*

        impl #core::event_name::EventName for #enum_ident {
            #[inline]
            fn event_name(&self) -> &'static ::core::primitive::str {
                match self {
                    #(#event_name_arms)*
                }
            }
        }
    })
}

/// The default free-function path for `apply`/`decide`.
///
/// The name becomes an [`Ident`](proc_macro2::Ident) first: interpolating
/// a `&str` into `parse_quote!` would produce a string literal, not a
/// path.
fn default_fn(name: &str) -> Path {
    let ident = Ident::new(name, proc_macro2::Span::call_site());
    syn::parse_quote!(#ident)
}
