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
pub(crate) fn default_core_crate() -> Path {
    syn::parse_quote!(::eventyr_core)
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
