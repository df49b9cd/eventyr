//! `#[derive(EventName)]` — stable storage names for event variants.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Fields, Path};

use crate::attrs::{absolutize, assign, default_core_crate};

/// What `#[derive(EventName)]` was told at the struct/enum level.
struct Meta {
    name: Option<String>,
    proc_crate: Option<Path>,
}

/// Expands `#[derive(EventName)]` into an `EventName` impl that names
/// each variant after itself (or its `#[eventyr(name = "...")]`).
pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    let meta = parse_meta(&input.attrs)?;
    let core = meta.proc_crate.unwrap_or_else(default_core_crate);
    let ident = &input.ident;

    let body = match &input.data {
        Data::Enum(data) => {
            if meta.name.is_some() {
                return Err(syn::Error::new(
                    ident.span(),
                    "eventyr name belongs on the variants of an enum, not the enum itself",
                ));
            }
            let arms = data
                .variants
                .iter()
                .map(|variant| {
                    let name = variant_name(variant)?;
                    let ident = &variant.ident;
                    Ok::<_, syn::Error>(match &variant.fields {
                        Fields::Unit => quote! { Self::#ident => #name, },
                        _ => quote! { Self::#ident { .. } => #name, },
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            quote! { match self { #(#arms)* } }
        }
        // The struct form: one payload struct, one event type. The name
        // is the type's own, overridable on the struct.
        Data::Struct(_) => {
            let name = meta.name.unwrap_or_else(|| ident.to_string());
            quote! { #name }
        }
        Data::Union(_) => {
            return Err(syn::Error::new(
                ident.span(),
                "EventName must be derived on an enum or a struct",
            ));
        }
    };

    let expanded = quote! {
        impl #core::event_name::EventName for #ident {
            #[inline]
            fn event_name(&self) -> &'static ::core::primitive::str {
                #body
            }
        }
    };

    Ok(expanded)
}
/// Parses the struct/enum-level `#[eventyr(...)]`: `name` and `crate`.
fn parse_meta(attrs: &[Attribute]) -> Result<Meta, syn::Error> {
    let mut meta = Meta {
        name: None,
        proc_crate: None,
    };
    for attr in attrs {
        if !attr.path().is_ident("eventyr") {
            continue;
        }
        attr.parse_nested_meta(|nested| {
            if nested.path.is_ident("name") {
                let value = nested.value()?.parse::<syn::LitStr>()?;
                assign(&mut meta.name, "name", &nested, value.value())
            } else if nested.path.is_ident("crate") {
                let value = nested.value()?.parse::<syn::LitStr>()?;
                let path: Path = value.parse()?;
                assign(&mut meta.proc_crate, "crate", &nested, absolutize(path))
            } else {
                Err(syn::Error::new(
                    nested.path.span(),
                    "unknown eventyr attribute; expected `name` or `crate`",
                ))
            }
        })?;
    }
    Ok(meta)
}

/// The stored name of one variant: its `#[eventyr(name = "...")]`, else
/// the variant's own name.
fn variant_name(variant: &syn::Variant) -> Result<String, syn::Error> {
    let mut name = None;
    for attr in &variant.attrs {
        if !attr.path().is_ident("eventyr") {
            continue;
        }
        attr.parse_nested_meta(|nested| {
            if nested.path.is_ident("name") {
                let value = nested.value()?.parse::<syn::LitStr>()?;
                assign(&mut name, "name", &nested, value.value())
            } else {
                Err(syn::Error::new(
                    nested.path.span(),
                    "unknown eventyr attribute; expected `name`",
                ))
            }
        })?;
    }
    Ok(name.unwrap_or_else(|| variant.ident.to_string()))
}
