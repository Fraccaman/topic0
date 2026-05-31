use crate::error::DbError;
use async_trait::async_trait;
use domain::ports::repository::CursorRepository;
use shared::{ChainId, DomainError, Hash, Height};
use sqlx::{PgPool, Row};

pub struct PgCursorRepository {
    pool: PgPool,
}

impl PgCursorRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CursorRepository for PgCursorRepository {
    async fn get(&self, chain: ChainId) -> Result<Option<(Height, Hash)>, DomainError> {
        let row = sqlx::query("SELECT last_height, last_hash FROM cursors WHERE chain_id = $1")
            .bind(chain.get() as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::from)?;
        Ok(row.map(|r| {
            let lh: i64 = r.get(0);
            let hash: Vec<u8> = r.get(1);
            (Height(lh as u64), Hash(hash))
        }))
    }

    async fn advance(
        &self,
        chain: ChainId,
        last: Height,
        last_hash: Hash,
    ) -> Result<(), DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        advance_in_tx(&mut tx, chain, last, last_hash).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(())
    }

    async fn rewind(&self, chain: ChainId, to: Height) -> Result<(), DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        rewind_in_tx(&mut tx, chain, to).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(())
    }
}

/// Rewind the cursor to `to` in a transaction (shared with the UnitOfWork reorg
/// rollback). Clears the stale hash; re-set when ingest re-reaches `to`.
pub(crate) async fn rewind_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain: ChainId,
    to: Height,
) -> Result<(), DbError> {
    sqlx::query("UPDATE cursors SET last_height = $2, last_hash = '' WHERE chain_id = $1")
        .bind(chain.get() as i64)
        .bind(to.get() as i64)
        .execute(&mut **tx)
        .await?;
    metrics::gauge!("cursor_height_block", "chain_id" => chain.to_string()).set(to.get() as f64);
    Ok(())
}

pub(crate) async fn advance_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain: ChainId,
    last: Height,
    last_hash: Hash,
) -> Result<(), DbError> {
    sqlx::query(
        "INSERT INTO cursors (chain_id, last_height, last_hash) VALUES ($1,$2,$3) \
         ON CONFLICT (chain_id) DO UPDATE SET last_height = EXCLUDED.last_height, \
           last_hash = EXCLUDED.last_hash",
    )
    .bind(chain.get() as i64)
    .bind(last.get() as i64)
    .bind(last_hash.0)
    .execute(&mut **tx)
    .await?;
    metrics::counter!("cursor_advances_total", "chain_id" => chain.to_string()).increment(1);
    metrics::gauge!("cursor_height_block", "chain_id" => chain.to_string()).set(last.get() as f64);
    Ok(())
}
