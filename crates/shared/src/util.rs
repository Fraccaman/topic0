//! Small chain-neutral helpers shared across crates (identifier validation, hex).

/// The `receipts` aux table: subordinate to its transaction (FK on `(chain_id,
/// tx_id)`), so it carries no own block-position `idx`.
pub const RECEIPTS_TABLE: &str = "receipts";

/// True if a typed table carries the block-position `idx` meta column — the
/// sort/keyset key. Every table except [`RECEIPTS_TABLE`] does.
pub fn table_has_idx(table: &str) -> bool {
    table != RECEIPTS_TABLE
}

/// True if `name` is a safe SQL identifier (lowercase/digit/underscore, no leading
/// digit, ≤63 chars). Guards dynamically-built table/column names.
pub fn is_valid_ident(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !name.as_bytes()[0].is_ascii_digit()
}

/// Normalize an ABI identifier to a snake_case SQL column name
/// (`actionTreeRoot` -> `action_tree_root`): inserts `_` at camel/Pascal
/// boundaries, lowercases, collapses non-alphanumerics, trims edge underscores.
pub fn to_snake_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() {
            let prev_boundary = i > 0
                && (chars[i - 1].is_ascii_lowercase()
                    || chars[i - 1].is_ascii_digit()
                    || (chars[i - 1].is_ascii_uppercase()
                        && i + 1 < chars.len()
                        && chars[i + 1].is_ascii_lowercase()));
            if prev_boundary && !out.ends_with('_') {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else if c.is_ascii_alphanumeric() {
            out.push(c);
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

/// Decode a hex string (optional `0x` prefix) into bytes. `None` if malformed.
pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Encode bytes as a lowercase hex string (no `0x` prefix).
pub fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrips_and_strips_prefix() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let enc = hex_encode(&bytes);
        assert_eq!(enc, "deadbeef");
        assert_eq!(hex_decode(&enc).unwrap(), bytes);
        // `0x` prefix accepted on decode.
        assert_eq!(hex_decode("0xdeadbeef").unwrap(), bytes);
    }

    #[test]
    fn hex_decode_rejects_malformed() {
        assert!(hex_decode("abc").is_none()); // odd length
        assert!(hex_decode("zz").is_none()); // non-hex digit
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn snake_case_normalizes_abi_names() {
        assert_eq!(to_snake_case("actionTreeRoot"), "action_tree_root");
        assert_eq!(to_snake_case("value"), "value");
        assert_eq!(to_snake_case("from"), "from");
        assert_eq!(to_snake_case("actionID"), "action_id");
        assert_eq!(to_snake_case("URLValue"), "url_value");
        assert_eq!(to_snake_case("already_snake"), "already_snake");
        assert_eq!(to_snake_case("amount0"), "amount0");
        // result is always a valid SQL identifier
        assert!(is_valid_ident(&to_snake_case("actionTreeRoot")));
    }

    #[test]
    fn only_receipts_lacks_idx() {
        assert!(!table_has_idx(RECEIPTS_TABLE));
        assert!(table_has_idx("transactions"));
        assert!(table_has_idx("evt_token_transfer"));
    }

    #[test]
    fn valid_idents_accept_safe_names() {
        assert!(is_valid_ident("evt_token_transfer"));
        assert!(is_valid_ident("a1"));
        assert!(is_valid_ident("amount0"));
    }

    #[test]
    fn invalid_idents_rejected() {
        assert!(!is_valid_ident("")); // empty
        assert!(!is_valid_ident("1abc")); // leading digit
        assert!(!is_valid_ident("evt-token")); // dash
        assert!(!is_valid_ident("Transfer")); // uppercase
        assert!(!is_valid_ident("drop table")); // space / injection
        assert!(!is_valid_ident(&"x".repeat(64))); // >63 chars
    }
}
