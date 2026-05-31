//! Read side: `PgEventQueryRepository` — parameterized SQL from a `QuerySpec`,
//! keyset-paginated on `(height, idx)`, rows returned as JSON text.
//!
//! - `builder`     — pure SQL construction (column types, JSON projection, binds).
//! - `pagination`  — keyset cursor codec.

mod builder;
mod cache;
mod pagination;

pub use cache::CachingEventQueryRepository;

use async_trait::async_trait;
use builder::{
    apply_binds, bind_value, meta_col_type, op_sql, row_json, safe_table, Bind, AUX_TABLES,
};
use domain::ports::repository::EventQueryRepository;
use pagination::{format_cursor, parse_cursor};
use schema::{ColumnType, EventSchema};
use shared::{DomainError, Page, QuerySpec};
use sqlx::{PgPool, Row};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("invalid query: {0}")]
    Invalid(String),
}

impl From<QueryError> for DomainError {
    fn from(e: QueryError) -> Self {
        match e {
            QueryError::Invalid(m) => DomainError::Invalid(m),
            other => DomainError::Storage(other.to_string()),
        }
    }
}

pub struct PgEventQueryRepository {
    pool: PgPool,
    /// table name → its decoded columns (ordered), for validation, typed binding,
    /// and explicit JSON projection.
    tables: HashMap<String, Vec<(String, ColumnType)>>,
}

impl PgEventQueryRepository {
    /// `schemas` = every event + aux table the API may serve (from `AbiIndex`).
    pub fn new(pool: PgPool, schemas: &[EventSchema]) -> Self {
        let tables = schemas
            .iter()
            .map(|s| {
                let cols = s.columns.iter().map(|c| (c.name.clone(), c.ty)).collect();
                (s.table.clone(), cols)
            })
            .collect();
        Self { pool, tables }
    }

    /// Resolve a filterable column's type: meta columns first, then the table's
    /// decoded columns. `None` → unknown column (rejected).
    fn col_type(&self, table: &str, col: &str) -> Option<ColumnType> {
        meta_col_type(col).or_else(|| {
            self.tables
                .get(table)?
                .iter()
                .find(|(n, _)| n == col)
                .map(|(_, t)| *t)
        })
    }

    /// Validate `spec.table`: a safe identifier, known to the schema, and self-orderable
    /// (receipts have no `idx` and are queried via their transaction). Returns the table.
    fn validate_table<'a>(&self, spec: &'a QuerySpec) -> Result<&'a str, DomainError> {
        let table = safe_table(&spec.table).map_err(DomainError::from)?;
        if !shared::table_has_idx(table) {
            return Err(DomainError::Invalid(
                "receipts are queried via their transaction, not standalone".into(),
            ));
        }
        if !self.tables.contains_key(table) {
            return Err(DomainError::Invalid(format!("unknown table '{table}'")));
        }
        Ok(table)
    }

    /// Build the `WHERE` conditions + ordered binds for `spec.filters` (no keyset, no
    /// joins). Shared by `query` and `count` so their filter semantics can't drift.
    /// Placeholders start at `$1`; the caller may append more starting at `binds.len()+1`.
    fn build_filters(
        &self,
        table: &str,
        filters: &[shared::Filter],
    ) -> Result<(Vec<String>, Vec<Bind>), DomainError> {
        let mut conds = Vec::new();
        let mut binds = Vec::new();
        for f in filters {
            let ty = self.col_type(table, &f.column).ok_or_else(|| {
                DomainError::Invalid(format!("unknown filter column '{}'", f.column))
            })?;
            let op = op_sql(f.op);
            let (cast, bind) = bind_value(ty, &f.value).map_err(DomainError::from)?;
            conds.push(format!(
                "t.\"{}\" {op} ${}{cast}",
                f.column,
                binds.len() + 1
            ));
            binds.push(bind);
        }
        Ok((conds, binds))
    }
}

#[async_trait]
impl EventQueryRepository for PgEventQueryRepository {
    async fn query(&self, spec: &QuerySpec) -> Result<Page<String>, DomainError> {
        let table = self.validate_table(spec)?;
        let cols = &self.tables[table];
        let first = spec.first.clamp(1, 1000) as i64;

        // Validate + bind the filter predicates ($1..); the keyset adds two more below.
        let (mut conds, mut binds) = self.build_filters(table, &spec.filters)?;

        // Keyset: rows past the cursor's (height, idx). Direction follows the sort —
        // `>` ascending, `<` descending — and the tuple compare keeps the page boundary
        // exact across rows that share a height.
        let after = spec
            .after
            .as_ref()
            .map(parse_cursor)
            .transpose()
            .map_err(DomainError::from)?;
        if let Some((b, l)) = after {
            let idx = binds.len() + 1;
            let cmp = if spec.descending { "<" } else { ">" };
            conds.push(format!("(t.height, t.idx) {cmp} (${idx}, ${})", idx + 1));
            binds.push(Bind::Int(b));
            binds.push(Bind::Int(l));
        }

        // Project explicit, lossless JSON. For event tables, LEFT JOIN the aux tables
        // (1:1 on (chain_id, tx_id) — indexed) and embed them as nested objects, null
        // when absent — but only the ones the query selected (`include_*`). Aux tables
        // themselves return flat rows (no self-join).
        let is_aux = AUX_TABLES.contains(&table);
        // Main table always has idx here (receipts rejected by validate_table).
        let mut row = row_json("t", cols, true);
        let mut joins = String::new();
        if !is_aux {
            let mut nested = Vec::new();
            for (alias, aux, include) in [
                ("tx", "transactions", spec.include_tx),
                ("rc", "receipts", spec.include_receipt),
            ] {
                if !include {
                    continue;
                }
                let Some(aux_cols) = self.tables.get(aux) else {
                    continue;
                };
                joins.push_str(&format!(
                    " LEFT JOIN {aux} {alias} ON {alias}.chain_id = t.chain_id AND {alias}.tx_id = t.tx_id"
                ));
                let key = if aux == "transactions" {
                    "transaction"
                } else {
                    "receipt"
                };
                nested.push(format!(
                    "'{key}', CASE WHEN {alias}.tx_id IS NULL THEN NULL ELSE {} END",
                    row_json(alias, aux_cols, shared::table_has_idx(aux))
                ));
            }
            if !nested.is_empty() {
                row = format!("{row} || jsonb_build_object({})", nested.join(", "));
            }
        }

        let order = if spec.descending {
            "t.height DESC, t.idx DESC"
        } else {
            "t.height, t.idx"
        };
        let mut sql = format!("SELECT ({row})::text AS j, t.height, t.idx FROM {table} t{joins}");
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        sql.push_str(&format!(" ORDER BY {order} LIMIT "));
        sql.push_str(&(first + 1).to_string()); // +1 row signals has_next

        let sql_start = std::time::Instant::now();
        let rows = apply_binds(sqlx::query(&sql), &binds)
            .fetch_all(&self.pool)
            .await
            .map_err(QueryError::from)
            .map_err(DomainError::from)?;
        metrics::histogram!("query_sql_duration_seconds", "table" => table.to_string())
            .record(sql_start.elapsed().as_secs_f64());

        let has_next = rows.len() as i64 > first;
        let mut items = Vec::new();
        let mut end_cursor = None;
        for r in rows.into_iter().take(first as usize) {
            let j: String = r.get(0);
            let bn: i64 = r.get(1);
            let li: i64 = r.get(2);
            end_cursor = Some(format_cursor(bn, li));
            items.push(j);
        }

        Ok(Page {
            items,
            end_cursor: if has_next { end_cursor } else { None },
            has_next,
        })
    }

    async fn count(&self, spec: &QuerySpec) -> Result<u64, DomainError> {
        let table = self.validate_table(spec)?;
        // Same filter WHERE as `query`, but no keyset/limit/joins — the full filtered set.
        let (conds, binds) = self.build_filters(table, &spec.filters)?;
        let mut sql = format!("SELECT count(*) FROM {table} t");
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        let row = apply_binds(sqlx::query(&sql), &binds)
            .fetch_one(&self.pool)
            .await
            .map_err(QueryError::from)
            .map_err(DomainError::from)?;
        let n: i64 = row.get(0);
        Ok(n.max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn first_clamps_to_bounds() {
        assert_eq!(0u32.clamp(1, 1000), 1);
        assert_eq!(5000u32.clamp(1, 1000), 1000);
        assert_eq!(50u32.clamp(1, 1000), 50);
    }
}
