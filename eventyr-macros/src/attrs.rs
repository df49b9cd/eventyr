//! Shared `#[eventyr(...)]` parsing helpers.

use syn::meta::ParseNestedMeta;
use syn::punctuated::Punctuated;
use syn::{Error, Path, Token};

/// Records `value` in `slot`, rejecting a repeated attribute.
pub(crate) fn assign<T>(
    slot: &mut Option<T>,
    key: &str,
    meta: &ParseNestedMeta<'_>,
    value: T,
) -> Result<(), Error> {
    if slot.is_some() {
        return Err(Error::new_spanned(
            &meta.path,
            format!("duplicate eventyr attribute `{key}`"),
        ));
    }
    *slot = Some(value);
    Ok(())
}

/// The uniform attribute arm — `key = <value parsed as T>`, recorded in
/// `slot`. Returns `Ok(false)` when the attribute is not `key`, so callers
/// can chain the special-cased and unknown-attribute arms after it.
pub(crate) fn assign_parsed<T: syn::parse::Parse>(
    slot: &mut Option<T>,
    key: &str,
    meta: &ParseNestedMeta<'_>,
) -> Result<bool, Error> {
    if !meta.path.is_ident(key) {
        return Ok(false);
    }
    let value = meta.value()?.parse::<T>()?;
    assign(slot, key, meta, value)?;
    Ok(true)
}

/// [`assign_parsed`] with a conversion: the attribute still reads
/// `key = <T>` and lands in the slot as `U` (a restricted shape, e.g.
/// `decide` takes a function path but not "any expression").
pub(crate) fn assign_parsed_map<T: syn::parse::Parse, U>(
    slot: &mut Option<U>,
    key: &str,
    map: impl FnOnce(T) -> U,
    meta: &ParseNestedMeta<'_>,
) -> Result<bool, Error> {
    if !meta.path.is_ident(key) {
        return Ok(false);
    }
    let value = map(meta.value()?.parse::<T>()?);
    assign(slot, key, meta, value)?;
    Ok(true)
}

/// The uniform `key = "..."` (string literal) arm — like
/// [`assign_parsed`], but stores the literal's unquoted value.
pub(crate) fn assign_parsed_lit(
    slot: &mut Option<String>,
    key: &str,
    meta: &ParseNestedMeta<'_>,
) -> Result<bool, Error> {
    if !meta.path.is_ident(key) {
        return Ok(false);
    }
    let value = meta.value()?.parse::<syn::LitStr>()?;
    assign(slot, key, meta, value.value())?;
    Ok(true)
}

/// Parses the comma-separated paths inside `key(...)`.
pub(crate) fn path_list(meta: &ParseNestedMeta<'_>) -> Result<Vec<Path>, Error> {
    if !meta.input.peek(syn::token::Paren) {
        return Err(Error::new_spanned(
            &meta.path,
            "expected a parenthesized list, like events(Opened, Deposited)",
        ));
    }
    let content;
    syn::parenthesized!(content in meta.input);
    let list = Punctuated::<Path, Token![,]>::parse_terminated(&content)?;
    Ok(list.into_iter().collect())
}

/// The crate the generated code targets by default.
///
/// Resolved from the caller's manifest, the serde `crate = "..."`
/// convention: the umbrella when the user depends on `eventyr` (the
/// common case — its prelude carries the derives), `eventyr_core` when
/// they depend on core directly, and the found name either way, so a
/// renamed dependency works without the attribute. Only when the
/// manifest can't be read (e.g. rustdoc walking a remote source) does
/// this fall back to plain `eventyr_core` — and an explicit
/// `#[eventyr(crate = "...")]` always wins.
pub(crate) fn default_core_crate() -> Path {
    use proc_macro_crate::FoundCrate;

    let found = proc_macro_crate::crate_name("eventyr")
        .or_else(|_| proc_macro_crate::crate_name("eventyr-core"));
    match found {
        // The invoking crate *is* the target — the umbrella's or
        // core's own tests, examples, and doctests. Bare `crate` paths
        // resolve to the invoking crate root, where the glob
        // re-exports carry `aggregate`/`event_name`/`__private`.
        Ok(FoundCrate::Itself) => syn::parse_quote!(crate),
        // The name the caller's manifest gives the dependency, so a
        // renamed `eventyr_core = { package = "..." }` needs no
        // attribute either.
        Ok(FoundCrate::Name(name)) => {
            let ident = proc_macro2::Ident::new(&name, proc_macro2::Span::call_site());
            syn::parse_quote!(::#ident)
        }
        Err(_) => syn::parse_quote!(::eventyr_core),
    }
}

/// Makes `path` absolute (`::name`) so a local module cannot shadow the
/// target crate. `crate`/`self`/`super`-relative paths pass through.
pub(crate) fn absolutize(path: Path) -> Path {
    let relative = path.leading_colon.is_none()
        && !matches!(
            path.segments.first(),
            Some(segment) if segment.ident == "crate"
                || segment.ident == "self"
                || segment.ident == "super"
        );
    if relative {
        syn::parse_quote!(::#path)
    } else {
        path
    }
}
