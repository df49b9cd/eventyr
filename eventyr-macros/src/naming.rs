//! The `NAME` default: the struct name in snake_case.

/// Converts an identifier to snake_case: `BankAccount` becomes
/// `bank_account`, `HTTPServer` becomes `http_server`.
pub(crate) fn to_snake_case(ident: &str) -> String {
    let chars: Vec<char> = ident.chars().collect();
    let mut out = String::with_capacity(ident.len() + 4);
    for (index, &c) in chars.iter().enumerate() {
        if !c.is_uppercase() {
            out.push(c);
            continue;
        }
        let previous = index.checked_sub(1).and_then(|i| chars.get(i));
        let next = chars.get(index + 1);
        let insert_underscore = match previous {
            // The first character, or one already preceded by `_`.
            None | Some('_') => false,
            Some(previous) => {
                previous.is_lowercase()
                    || previous.is_ascii_digit()
                    || next.is_some_and(|n| n.is_lowercase())
            }
        };
        if insert_underscore {
            out.push('_');
        }
        out.extend(c.to_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::to_snake_case;

    #[test]
    fn conventional_names() {
        assert_eq!(to_snake_case("Account"), "account");
        assert_eq!(to_snake_case("BankAccount"), "bank_account");
        assert_eq!(to_snake_case("HTTPServer"), "http_server");
        assert_eq!(to_snake_case("V2Ray"), "v2_ray");
        assert_eq!(to_snake_case("_Internal"), "_internal");
        assert_eq!(to_snake_case("A"), "a");
    }
}
