//! SQL construction for the read path: column-type resolution, lossless JSON
//! projection, identifier/operator/value validation, and typed bind application.
//! The logical `ColumnType` → Postgres mapping (projection + bind casts) lives in
//! `sql_pg` (shared with `migrator`). Everything is pure (no DB) and unit-tested.

use crate::QueryError;
use schema::ColumnType;
use shared::FilterOp;

/// Meta columns present on every typed table (filterable without an ABI column).
/// `chain_id`/`height`/`idx` are bigint; `block_time` is timestamptz; the rest bytea.
pub(crate) fn meta_col_type(name: &str) -> Option<ColumnType> {
    match name {
        "chain_id" | "height" | "idx" => Some(ColumnType::Int64),
        "block_time" => Some(ColumnType::Timestamp),
        "block_hash" | "tx_id" => Some(ColumnType::Bytes),
        _ => None,
    }
}

/// The two aux tables joined into every event row as nested `transaction`/`receipt`.
pub(crate) const AUX_TABLES: [&str; 2] = ["transactions", "receipts"];

/// `jsonb_build_object(...)` for one table: the shared meta columns plus its decoded
/// columns, each losslessly encoded. `has_idx` controls the block-position `idx` meta
/// column — absent on the receipts FK extension.
pub(crate) fn row_json(alias: &str, cols: &[(String, ColumnType)], has_idx: bool) -> String {
    let mut parts = vec![
        format!("'chain_id', {alias}.chain_id"),
        format!("'height', {alias}.height"),
        format!("'block_hash', '0x' || encode({alias}.block_hash, 'hex')"),
        format!("'block_time', {alias}.block_time"),
        format!("'tx_id', '0x' || encode({alias}.tx_id, 'hex')"),
    ];
    if has_idx {
        parts.push(format!("'idx', {alias}.idx"));
    }
    for (name, ty) in cols {
        parts.push(format!(
            "'{name}', {}",
            sql_pg::json_projection(alias, name, *ty)
        ));
    }
    format!("jsonb_build_object({})", parts.join(", "))
}

/// A typed bind value, applied in placeholder order.
pub(crate) enum Bind {
    Int(i64),
    Bytes(Vec<u8>),
    Bool(bool),
    /// numeric / text / timestamptz — bound as text, cast in SQL.
    Text(String),
}

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

/// Bind every value in placeholder order. Centralizes the `Bind` → `q.bind(..)` match.
pub(crate) fn apply_binds<'q>(mut q: PgQuery<'q>, binds: &'q [Bind]) -> PgQuery<'q> {
    for b in binds {
        q = match b {
            Bind::Int(v) => q.bind(*v),
            Bind::Bytes(v) => q.bind(v.as_slice()),
            Bind::Bool(v) => q.bind(*v),
            Bind::Text(v) => q.bind(v.as_str()),
        };
    }
    q
}

/// Validate a table identifier.
pub(crate) fn safe_table(name: &str) -> Result<&str, QueryError> {
    if shared::is_valid_ident(name) {
        Ok(name)
    } else {
        Err(QueryError::Invalid(format!("bad table name: {name}")))
    }
}

pub(crate) fn op_sql(op: FilterOp) -> &'static str {
    match op {
        FilterOp::Eq => "=",
        FilterOp::Ne => "<>",
        FilterOp::Gt => ">",
        FilterOp::Gte => ">=",
        FilterOp::Lt => "<",
        FilterOp::Lte => "<=",
    }
}

/// Convert a raw filter value to a typed bind + the SQL cast suffix its placeholder
/// needs (e.g. `::numeric`). Bytea values are `0x`-hex; numerics stay text to keep
/// full 78-digit precision. Jsonb columns are not filterable.
pub(crate) fn bind_value(ty: ColumnType, raw: &str) -> Result<(&'static str, Bind), QueryError> {
    let err = |m: &str| QueryError::Invalid(m.to_string());
    // Cast suffix (e.g. `::numeric`) owned by `sql_pg`; the `Bind` variant (parse +
    // encode) is read-path-specific and stays here.
    let cast = sql_pg::cast_suffix(ty);
    Ok(match ty {
        ColumnType::Int64 => (
            cast,
            Bind::Int(raw.parse().map_err(|_| err("expected an integer"))?),
        ),
        ColumnType::Bool => (
            cast,
            Bind::Bool(raw.parse().map_err(|_| err("expected a boolean"))?),
        ),
        ColumnType::Address | ColumnType::Bytes => (
            cast,
            Bind::Bytes(shared::hex_decode(raw).ok_or_else(|| err("expected 0x-hex bytes"))?),
        ),
        // Bound as text, cast in SQL — preserves numeric(78,0) precision.
        ColumnType::UInt(_) | ColumnType::Int(_) | ColumnType::Timestamp | ColumnType::Utf8 => {
            (cast, Bind::Text(raw.to_string()))
        }
        ColumnType::Json => return Err(err("jsonb columns are not filterable")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_sql_maps_every_operator() {
        assert_eq!(op_sql(FilterOp::Eq), "=");
        assert_eq!(op_sql(FilterOp::Ne), "<>");
        assert_eq!(op_sql(FilterOp::Gt), ">");
        assert_eq!(op_sql(FilterOp::Gte), ">=");
        assert_eq!(op_sql(FilterOp::Lt), "<");
        assert_eq!(op_sql(FilterOp::Lte), "<=");
    }

    #[test]
    fn safe_table_accepts_valid_rejects_injection() {
        assert_eq!(
            safe_table("evt_token_transfer").unwrap(),
            "evt_token_transfer"
        );
        assert!(safe_table("evt; DROP TABLE x").is_err());
        assert!(safe_table("Evt").is_err()); // uppercase
        assert!(safe_table("").is_err());
    }

    #[test]
    fn bind_value_types_and_casts() {
        // Int64 / Bool / Utf8 carry no cast suffix.
        assert!(matches!(
            bind_value(ColumnType::Int64, "42"),
            Ok(("", Bind::Int(42)))
        ));
        assert!(matches!(
            bind_value(ColumnType::Bool, "true"),
            Ok(("", Bind::Bool(true)))
        ));
        // UInt / Int / Timestamp bind as text with a SQL cast.
        assert!(matches!(
            bind_value(ColumnType::UInt(256), "12345678901234567890"),
            Ok(("::numeric", Bind::Text(_)))
        ));
        assert!(matches!(
            bind_value(ColumnType::Timestamp, "2024-01-01T00:00:00Z"),
            Ok(("::timestamptz", Bind::Text(_)))
        ));
        // Address / Bytes accept 0x-hex.
        assert!(matches!(
            bind_value(ColumnType::Bytes, "0xdeadbeef"),
            Ok(("", Bind::Bytes(_)))
        ));
    }

    #[test]
    fn bind_value_rejects_bad_input() {
        assert!(bind_value(ColumnType::Int64, "notint").is_err());
        assert!(bind_value(ColumnType::Bool, "maybe").is_err());
        assert!(bind_value(ColumnType::Bytes, "0xZZ").is_err());
        assert!(bind_value(ColumnType::Json, "{}").is_err()); // not filterable
    }

    #[test]
    fn row_json_projects_meta_and_columns() {
        let sql = row_json("t", &[("value".into(), ColumnType::UInt(256))], true);
        assert!(sql.starts_with("jsonb_build_object("));
        assert!(sql.contains("'tx_id', '0x' || encode(t.tx_id, 'hex')"));
        assert!(sql.contains("'block_hash', '0x' || encode(t.block_hash, 'hex')"));
        assert!(sql.contains("'idx', t.idx"));
        assert!(sql.contains("'value', t.\"value\"::text"));
        assert!(sql.contains("'chain_id', t.chain_id"));
    }

    #[test]
    fn row_json_omits_idx_when_absent() {
        // Receipts (FK extension) project no `idx` meta column.
        let sql = row_json("rc", &[("status".into(), ColumnType::Bool)], false);
        assert!(!sql.contains("idx"));
        assert!(sql.contains("'tx_id', '0x' || encode(rc.tx_id, 'hex')"));
        assert!(sql.contains("'status', rc.\"status\""));
    }

    #[test]
    fn meta_columns_are_filterable() {
        assert_eq!(meta_col_type("chain_id"), Some(ColumnType::Int64));
        assert_eq!(meta_col_type("height"), Some(ColumnType::Int64));
        assert_eq!(meta_col_type("tx_id"), Some(ColumnType::Bytes));
        assert_eq!(meta_col_type("block_time"), Some(ColumnType::Timestamp));
        assert_eq!(meta_col_type("nonexistent"), None);
    }
}
