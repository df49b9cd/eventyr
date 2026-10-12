//! Which crate the generated code names.

use proc_macro_crate::FoundCrate;
use syn::Path;

/// The path to `eventyr-mediator` from the invoking crate.
///
/// Resolved from the invoking crate's manifest, the `eventyr-macros`
/// pattern: a direct `eventyr-mediator` dependency first (under its
/// local name, so a renamed dependency works), then the umbrella's
/// `eventyr::mediator` re-export. Inside either crate itself — its
/// doctests and examples, which link it under its own name — the
/// absolute path still resolves. A `crate = path;` clause in the
/// composition overrides all of this.
pub(crate) fn mediator_crate() -> Path {
    if let Ok(found) = proc_macro_crate::crate_name("eventyr-mediator") {
        return match found {
            FoundCrate::Itself => syn::parse_quote!(::eventyr_mediator),
            FoundCrate::Name(name) => {
                let ident = proc_macro2::Ident::new(&name, proc_macro2::Span::call_site());
                syn::parse_quote!(::#ident)
            }
        };
    }
    if let Ok(found) = proc_macro_crate::crate_name("eventyr") {
        return match found {
            FoundCrate::Itself => syn::parse_quote!(::eventyr::mediator),
            FoundCrate::Name(name) => {
                let ident = proc_macro2::Ident::new(&name, proc_macro2::Span::call_site());
                syn::parse_quote!(::#ident::mediator)
            }
        };
    }
    syn::parse_quote!(::eventyr_mediator)
}
