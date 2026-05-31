//! The single `schema::ColumnType` → Postgres mapping, shared by the SQL backends
//! (`migrator` DDL, `db-query` projection + filter casts). Pure, no DB. Adding a
//! `ColumnType` variant is caught here once instead of in every backend.

use schema::ColumnType;

/// DDL column type.
pub fn pg_type(t: ColumnType) -> &'static str {
    match t {
        ColumnType::Address | ColumnType::Bytes => "bytea",
        ColumnType::UInt(_) | ColumnType::Int(_) => "numeric(78,0)",
        ColumnType::Bool => "boolean",
        ColumnType::Utf8 => "text",
        ColumnType::Json => "jsonb",
        ColumnType::Int64 => "bigint",
        ColumnType::Timestamp => "timestamptz",
    }
}

/// Cast suffix a filter placeholder needs (numerics/timestamps bind as text and cast
/// in SQL to keep full precision). Empty for types bound natively. `Json` returns
/// `::jsonb` for completeness but is never filtered (jsonb columns aren't filterable).
pub fn cast_suffix(t: ColumnType) -> &'static str {
    match t {
        ColumnType::UInt(_) | ColumnType::Int(_) => "::numeric",
        ColumnType::Timestamp => "::timestamptz",
        ColumnType::Json => "::jsonb",
        _ => "",
    }
}

/// SQL expression projecting a column to its lossless JSON form: bytea → `0x`-hex,
/// numeric → text (keeps full 78-digit precision; a raw JSON number would round to
/// f64), everything else as-is.
pub fn json_projection(alias: &str, name: &str, t: ColumnType) -> String {
    match t {
        ColumnType::Address | ColumnType::Bytes => {
            format!("'0x' || encode({alias}.\"{name}\", 'hex')")
        }
        ColumnType::UInt(_) | ColumnType::Int(_) => format!("{alias}.\"{name}\"::text"),
        _ => format!("{alias}.\"{name}\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_type_maps_every_variant() {
        assert_eq!(pg_type(ColumnType::Address), "bytea");
        assert_eq!(pg_type(ColumnType::Bytes), "bytea");
        assert_eq!(pg_type(ColumnType::UInt(256)), "numeric(78,0)");
        assert_eq!(pg_type(ColumnType::Int(8)), "numeric(78,0)");
        assert_eq!(pg_type(ColumnType::Bool), "boolean");
        assert_eq!(pg_type(ColumnType::Utf8), "text");
        assert_eq!(pg_type(ColumnType::Json), "jsonb");
        assert_eq!(pg_type(ColumnType::Int64), "bigint");
        assert_eq!(pg_type(ColumnType::Timestamp), "timestamptz");
    }

    #[test]
    fn cast_suffix_only_for_text_bound_types() {
        assert_eq!(cast_suffix(ColumnType::UInt(256)), "::numeric");
        assert_eq!(cast_suffix(ColumnType::Int(64)), "::numeric");
        assert_eq!(cast_suffix(ColumnType::Timestamp), "::timestamptz");
        assert_eq!(cast_suffix(ColumnType::Int64), "");
        assert_eq!(cast_suffix(ColumnType::Bool), "");
        assert_eq!(cast_suffix(ColumnType::Address), "");
        assert_eq!(cast_suffix(ColumnType::Utf8), "");
    }

    #[test]
    fn json_projection_encodes_losslessly() {
        assert_eq!(
            json_projection("t", "from", ColumnType::Address),
            "'0x' || encode(t.\"from\", 'hex')"
        );
        assert_eq!(
            json_projection("t", "value", ColumnType::UInt(256)),
            "t.\"value\"::text"
        );
        assert_eq!(json_projection("t", "ok", ColumnType::Bool), "t.\"ok\"");
    }
}
