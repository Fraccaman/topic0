use crate::error::DbError;
use async_trait::async_trait;
use domain::ports::repository::BlockRepository;
use shared::{BlockMeta, ChainId, DomainError, Hash, Height, TxCalldata};
use sqlx::{PgPool, QueryBuilder, Row};

/// 5 columns/row (chunked under the 65535 bind-param cap via `chunk_rows`).
const BLOCK_COLS: usize = 5;
/// tx_ids per `= ANY($2)` query.
const CALLDATA_CHUNK: usize = 10_000;

pub struct PgBlockRepository {
    pool: PgPool,
}

impl PgBlockRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BlockRepository for PgBlockRepository {
    async fn upsert_batch(&self, blocks: &[BlockMeta]) -> Result<u64, DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        let n = upsert_batch_tx(&mut tx, blocks).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(n)
    }

    async fn get(&self, chain: ChainId, height: Height) -> Result<Option<BlockMeta>, DomainError> {
        let row = sqlx::query(
            "SELECT hash, parent_hash, time FROM blocks WHERE chain_id = $1 AND height = $2",
        )
        .bind(chain.get() as i64)
        .bind(height.get() as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::from)?;

        Ok(row.map(|r| {
            let hash: Vec<u8> = r.get(0);
            let parent: Vec<u8> = r.get(1);
            let time: i64 = r.get(2);
            BlockMeta {
                chain_id: chain,
                height,
                hash: Hash(hash),
                parent_hash: Hash(parent),
                time,
            }
        }))
    }

    async fn times(
        &self,
        chain: ChainId,
        from: Height,
        to: Height,
    ) -> Result<Vec<(Height, i64)>, DomainError> {
        let rows = sqlx::query(
            "SELECT height, time FROM blocks WHERE chain_id = $1 AND height BETWEEN $2 AND $3",
        )
        .bind(chain.get() as i64)
        .bind(from.get() as i64)
        .bind(to.get() as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::from)?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let h: i64 = r.get(0);
                let t: i64 = r.get(1);
                (Height(h as u64), t)
            })
            .collect())
    }

    async fn max_height(&self, chain: ChainId) -> Result<Option<Height>, DomainError> {
        let row = sqlx::query("SELECT max(height) FROM blocks WHERE chain_id = $1")
            .bind(chain.get() as i64)
            .fetch_one(&self.pool)
            .await
            .map_err(DbError::from)?;
        let max: Option<i64> = row.get(0);
        Ok(max.map(|n| Height(n as u64)))
    }

    async fn delete_from_height(&self, chain: ChainId, from: Height) -> Result<u64, DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        let n = delete_from_height_tx(&mut tx, chain, from).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(n)
    }

    async fn calldata(
        &self,
        chain: ChainId,
        tx_ids: &[Hash],
    ) -> Result<Vec<TxCalldata>, DomainError> {
        let mut out = Vec::with_capacity(tx_ids.len());
        for chunk in tx_ids.chunks(CALLDATA_CHUNK) {
            let ids: Vec<Vec<u8>> = chunk.iter().map(|h| h.0.clone()).collect();
            let rows = sqlx::query(
                "SELECT height, block_hash, tx_id, to_addr, idx::bigint, input \
                 FROM transactions WHERE chain_id = $1 AND tx_id = ANY($2)",
            )
            .bind(chain.get() as i64)
            .bind(&ids)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::from)?;

            for r in rows {
                let height: i64 = r.get(0);
                let block_hash: Vec<u8> = r.get(1);
                let tx_id: Vec<u8> = r.get(2);
                let to_addr: Option<Vec<u8>> = r.get(3);
                let tx_index: Option<i64> = r.get(4);
                let input: Vec<u8> = r.get(5);
                out.push(TxCalldata {
                    chain_id: chain,
                    height: Height(height as u64),
                    block_hash: Hash(block_hash),
                    tx_id: Hash(tx_id),
                    to_addr: to_addr.unwrap_or_default(),
                    tx_index: tx_index.unwrap_or(0) as u64,
                    input,
                });
            }
        }
        Ok(out)
    }
}

/// Delete block metas at/above `from` in a transaction (shared with the UnitOfWork
/// reorg rollback).
pub(crate) async fn delete_from_height_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain: ChainId,
    from: Height,
) -> Result<u64, DbError> {
    let r = sqlx::query("DELETE FROM blocks WHERE chain_id = $1 AND height >= $2")
        .bind(chain.get() as i64)
        .bind(from.get() as i64)
        .execute(&mut **tx)
        .await?;
    Ok(r.rows_affected())
}

/// Batch-upsert block metas in a transaction (shared with the UnitOfWork).
pub(crate) async fn upsert_batch_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    blocks: &[BlockMeta],
) -> Result<u64, DbError> {
    let mut n = 0;
    for chunk in blocks.chunks(crate::repositories::chunk_rows(BLOCK_COLS)) {
        let mut qb: QueryBuilder<sqlx::Postgres> =
            QueryBuilder::new("INSERT INTO blocks (chain_id, height, hash, parent_hash, time) ");
        qb.push_values(chunk, |mut b, blk| {
            b.push_bind(blk.chain_id.get() as i64)
                .push_bind(blk.height.get() as i64)
                .push_bind(&blk.hash.0)
                .push_bind(&blk.parent_hash.0)
                .push_bind(blk.time);
        });
        qb.push(
            " ON CONFLICT (chain_id, height) DO UPDATE SET hash = EXCLUDED.hash, \
              parent_hash = EXCLUDED.parent_hash, time = EXCLUDED.time",
        );
        let res = qb.build().execute(&mut **tx).await?;
        n += res.rows_affected();
    }
    Ok(n)
}
