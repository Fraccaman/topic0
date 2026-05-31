use crate::error::DbError;
use crate::repositories::safe_ident;
use async_trait::async_trait;
use domain::ports::repository::EventRepository;
use shared::{ChainId, DomainError, EventRow, EventValue, Height};
use sqlx::{Postgres, QueryBuilder};
use std::collections::BTreeMap;

pub struct PgEventRepository {
    pool: sqlx::PgPool,
}

impl PgEventRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventRepository for PgEventRepository {
    async fn upsert_batch(&self, table: &str, rows: &[EventRow]) -> Result<u64, DomainError> {
        safe_ident(table)?;
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        let n = upsert_batch_tx(&mut tx, table, rows).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(n)
    }

    async fn delete_from_height(
        &self,
        table: &str,
        chain: ChainId,
        from: Height,
    ) -> Result<u64, DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        let n = delete_from_height_tx(&mut tx, table, chain, from).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(n)
    }
}

/// Delete rows at/above `from` for one table in a transaction (shared with the
/// UnitOfWork reorg rollback).
pub(crate) async fn delete_from_height_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    chain: ChainId,
    from: Height,
) -> Result<u64, DbError> {
    safe_ident(table)?;
    let sql = format!("DELETE FROM {table} WHERE chain_id = $1 AND height >= $2");
    let r = sqlx::query(&sql)
        .bind(chain.get() as i64)
        .bind(from.get() as i64)
        .execute(&mut **tx)
        .await?;
    Ok(r.rows_affected())
}

/// Batch-upsert decoded rows for one table in a transaction (shared with the
/// UnitOfWork). Rows are grouped by column-signature (optional fields vary the
/// column set), each group written as one multi-row `INSERT … ON CONFLICT DO NOTHING`.
pub(crate) async fn upsert_batch_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    rows: &[EventRow],
) -> Result<u64, DbError> {
    safe_ident(table)?;
    // Receipts are an FK extension of transactions and carry no own `idx`.
    let has_idx = shared::table_has_idx(table);
    let mut groups: BTreeMap<Vec<&str>, Vec<&EventRow>> = BTreeMap::new();
    for row in rows {
        let sig: Vec<&str> = row.event.fields.iter().map(|(n, _)| n.as_str()).collect();
        groups.entry(sig).or_default().push(row);
    }

    let mut n = 0;
    for (sig, group) in groups {
        // meta cols (5, or 6 with idx) + field cols; stay under the 65535 bind cap.
        let cols = if has_idx { 6 } else { 5 } + sig.len();
        let chunk = crate::repositories::chunk_rows(cols);
        for part in group.chunks(chunk) {
            let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("INSERT INTO ");
            qb.push(table);
            qb.push(" (chain_id, height, block_hash, block_time, tx_id");
            if has_idx {
                qb.push(", idx");
            }
            for name in &sig {
                qb.push(", \"");
                qb.push(safe_ident(name)?);
                qb.push("\"");
            }
            qb.push(") VALUES ");
            // Manual VALUES tuples: rows carry SQL expressions (to_timestamp/CAST).
            let mut first = true;
            for row in part {
                if !first {
                    qb.push(", ");
                }
                first = false;
                qb.push("(");
                qb.push_bind(row.chain_id.get() as i64);
                qb.push(", ").push_bind(row.height.get() as i64);
                qb.push(", ").push_bind(&row.block_hash.0);
                match row.block_time {
                    Some(secs) => {
                        qb.push(", to_timestamp(").push_bind(secs).push(")");
                    }
                    None => {
                        qb.push(", NULL");
                    }
                }
                qb.push(", ").push_bind(&row.tx_id.0);
                if has_idx {
                    qb.push(", ").push_bind(row.index.get() as i64);
                }
                for (_, value) in &row.event.fields {
                    qb.push(", ");
                    bind_value(&mut qb, value);
                }
                qb.push(")");
            }
            qb.push(" ON CONFLICT DO NOTHING");
            let res = qb.build().execute(&mut **tx).await?;
            n += res.rows_affected();
        }
    }
    Ok(n)
}

/// Bind a decoded value. The `numeric`/`jsonb` cast type names mirror the
/// `ColumnType`→Postgres mapping owned by `sql_pg`; here the cast is keyed on the
/// runtime `EventValue` (not a `ColumnType`), so it stays a local match.
fn bind_value<'a>(qb: &mut QueryBuilder<'a, Postgres>, v: &'a EventValue) {
    match v {
        EventValue::Address(b) | EventValue::Bytes(b) | EventValue::Hash(b) => {
            qb.push_bind(b.as_slice());
        }
        EventValue::Bool(b) => {
            qb.push_bind(*b);
        }
        EventValue::Uint(s) | EventValue::Int(s) => {
            qb.push("CAST(").push_bind(s.as_str()).push(" AS numeric)");
        }
        EventValue::String(s) => {
            qb.push_bind(s.as_str());
        }
        EventValue::Json(s) => {
            qb.push("CAST(").push_bind(s.as_str()).push(" AS jsonb)");
        }
    }
}
