//! Keyset cursor codec. Pages are ordered by `(height, idx)` — stable regardless of
//! the active filters — and the cursor is just that pair as `"height:idx"`.

use crate::QueryError;
use shared::Cursor;

/// `(height, idx)` → cursor string.
pub(crate) fn format_cursor(height: i64, idx: i64) -> Cursor {
    Cursor(format!("{height}:{idx}"))
}

/// Cursor string → `(height, idx)`.
pub(crate) fn parse_cursor(c: &Cursor) -> Result<(i64, i64), QueryError> {
    let bad = || QueryError::Invalid("bad cursor".into());
    let (h, i) = c.0.split_once(':').ok_or_else(bad)?;
    Ok((h.parse().map_err(|_| bad())?, i.parse().map_err(|_| bad())?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        assert_eq!(format_cursor(123, 45).0, "123:45");
        assert_eq!(parse_cursor(&Cursor("123:45".into())).unwrap(), (123, 45));
    }

    #[test]
    fn rejects_malformed() {
        assert!(parse_cursor(&Cursor("123".into())).is_err()); // no separator
        assert!(parse_cursor(&Cursor("abc:45".into())).is_err()); // non-integer height
        assert!(parse_cursor(&Cursor("123:xy".into())).is_err()); // non-integer index
    }
}
