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
//! Serde glue arrives with the Postgres store (0.2); until then the
//! enum is domain-only.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Expr, Fields, Ident, Path};

use crate::attrs::{absolutize, assign, path_list};
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
}

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
            if meta.path.is_ident("name") {
                let value = meta.value()?.parse::<syn::LitStr>()?;
                assign(&mut wiring.name, "name", &meta, value.value())
            } else if meta.path.is_ident("id") {
                let value = meta.value()?.parse::<Path>()?;
                assign(&mut wiring.id, "id", &meta, value)
            } else if meta.path.is_ident("state") {
                let value = meta.value()?.parse::<Path>()?;
                assign(&mut wiring.state, "state", &meta, value)
            } else if meta.path.is_ident("event") {
                let value = meta.value()?.parse::<Path>()?;
                assign(&mut wiring.event, "event", &meta, value)
            } else if meta.path.is_ident("event_enum") {
                let value = meta.value()?.parse::<syn::Ident>()?;
                assign(&mut wiring.event_enum_name, "event_enum", &meta, value)
            } else if meta.path.is_ident("command") {
                let value = meta.value()?.parse::<Path>()?;
                assign(&mut wiring.command, "command", &meta, value)
            } else if meta.path.is_ident("error") {
                let value = meta.value()?.parse::<Path>()?;
                assign(&mut wiring.error, "error", &meta, value)
            } else if meta.path.is_ident("initial") {
                let value = meta.value()?.parse::<Expr>()?;
                assign(&mut wiring.initial, "initial", &meta, value)
            } else if meta.path.is_ident("apply") {
                let value = meta.value()?.parse::<Path>()?;
                assign(&mut wiring.apply, "apply", &meta, value)
            } else if meta.path.is_ident("decide") {
                let value = match meta.value()?.parse::<Expr>()? {
                    Expr::Path(path) => path.path,
                    other => {
                        return Err(syn::Error::new(
                            other.span(),
                            "eventyr decide expects a function path",
                        ));
                    }
                };
                assign(&mut wiring.decide, "decide", &meta, value)
            } else if meta.path.is_ident("crate") {
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
            } else {
                Err(syn::Error::new(
                    meta.path.span(),
                    "unknown eventyr attribute; expected one of name, id, state, event, \
                     event_enum, command, error, initial, apply, decide, crate, events",
                ))
            }
        })?;
    }

    Ok(wiring)
}

/// Expands `#[derive(Aggregate)]` into the `Aggregate` impl (and the
/// event enum, when `events(...)` was given).
pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    let wiring = parse_wiring(input)?;

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

    let ident = &input.ident;
    let default_core = crate::attrs::default_core_crate();
    let core = wiring.proc_crate.as_ref().unwrap_or(&default_core);
    let name = wiring
        .name
        .clone()
        .unwrap_or_else(|| to_snake_case(&ident.to_string()));
    // The `{Ident}...` position convention, as paths.
    let default_id: Path = format_ident!("{}Id", ident).into();
    let default_command: Path = format_ident!("{}Command", ident).into();
    let default_error: Path = format_ident!("{}Error", ident).into();
    let id = wiring.id.as_ref().unwrap_or(&default_id);
    let command = wiring.command.as_ref().unwrap_or(&default_command);
    let error = wiring.error.as_ref().unwrap_or(&default_error);
    // With `events(...)` the generated enum *is* the event type, so the
    // `Event` convention follows it — `event = X` overrides, and
    // `event_enum = X` renames the enum `Event` follows.
    let enum_ident: Ident = wiring
        .event_enum_name
        .clone()
        .unwrap_or_else(|| format_ident!("{}Event", ident));
    let default_event: Path = if wiring.events.is_some() {
        enum_ident.clone().into()
    } else {
        format_ident!("{}Event", ident).into()
    };
    let event = wiring.event.as_ref().unwrap_or(&default_event);
    let state = wiring
        .state
        .as_ref()
        .map_or_else(|| quote! { Self }, |state| quote! { #state });
    let initial = wiring.initial.as_ref().map_or_else(
        || {
            if wiring.state.is_some() {
                quote! { ::core::default::Default::default() }
            } else {
                // A unit struct is its own state: `Self`.
                quote! { Self }
            }
        },
        |initial| quote! { #initial },
    );
    let default_apply = default_fn("apply");
    let default_decide = default_fn("decide");
    let apply = wiring.apply.as_ref().unwrap_or(&default_apply);
    let decide = wiring.decide.as_ref().unwrap_or(&default_decide);

    let event_enum = match &wiring.events {
        Some(events) => Some(generate_event_enum(events, &enum_ident, &input.vis, core)?),
        None => None,
    };

    let expanded = quote! {
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
    };

    Ok(expanded)
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
        #[derive(Clone, Debug, PartialEq)]
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
