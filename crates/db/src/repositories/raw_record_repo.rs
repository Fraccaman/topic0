use crate::encode::{frame_selectors, unframe_selectors};
use crate::error::DbError;
use async_trait::async_trait;
use domain::ports::repository::RawRecordRepository;
use shared::{AddressBytes, ChainId, DomainError, Hash, Height, RawRecord, RecordIndex};
use sqlx::{PgPool, QueryBuilder, Row};

/// 10 columns/row (chunked under the 65535 bind-param cap via `chunk_rows`).
const RAW_COLS: usize = 10;

pub struct PgRawRecordRepository {
    pool: PgPool,
}

impl PgRawRecordRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RawRecordRepository for PgRawRecordRepository {
    async fn insert_batch(&self, records: &[RawRecord]) -> Result<u64, DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        let n = insert_batch_tx(&mut tx, records).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(n)
    }

    async fn range(
        &self,
        chain: ChainId,
        from: Height,
        to: Height,
    ) -> Result<Vec<RawRecord>, DomainError> {
        let rows = sqlx::query(
            "SELECT height, idx, block_hash, address, selectors, data, tx_id, tx_index, inner_index \
             FROM raw_records WHERE chain_id = $1 AND height BETWEEN $2 AND $3 \
             ORDER BY height, idx",
        )
        .bind(chain.get() as i64)
        .bind(from.get() as i64)
        .bind(to.get() as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::from)?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let height: i64 = r.get(0);
            let idx: i64 = r.get(1);
            let block_hash: Vec<u8> = r.get(2);
            let address: Vec<u8> = r.get(3);
            let selectors: Vec<u8> = r.get(4);
            let data: Vec<u8> = r.get(5);
            let tx_id: Vec<u8> = r.get(6);
            let tx_index: i64 = r.get(7);
            let inner: Option<i64> = r.get(8);
            out.push(RawRecord {
                chain_id: chain,
                height: Height(height as u64),
                block_hash: Hash(block_hash),
                index: RecordIndex(idx as u64),
                address: AddressBytes(address),
                selectors: unframe_selectors(&selectors),
                data,
                tx_id: Hash(tx_id),
                tx_index: tx_index as u64,
                inner_index: inner.map(|v| v as u64),
            });
        }
        Ok(out)
    }

    async fn delete_from_height(&self, chain: ChainId, from: Height) -> Result<u64, DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        let n = delete_from_height_tx(&mut tx, chain, from).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(n)
    }
}

/// Delete raw records at/above `from` in a transaction (shared with the UnitOfWork
/// reorg rollback).
pub(crate) async fn delete_from_height_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain: ChainId,
    from: Height,
) -> Result<u64, DbError> {
    let r = sqlx::query("DELETE FROM raw_records WHERE chain_id = $1 AND height >= $2")
        .bind(chain.get() as i64)
        .bind(from.get() as i64)
        .execute(&mut **tx)
        .await?;
    Ok(r.rows_affected())
}

/// Batch-insert raw records in a transaction (shared with the UnitOfWork).
/// Chunked under the bind-param cap.
pub(crate) async fn insert_batch_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    records: &[RawRecord],
) -> Result<u64, DbError> {
    let mut n = 0;
    for chunk in records.chunks(crate::repositories::chunk_rows(RAW_COLS)) {
        let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO raw_records \
             (chain_id, height, idx, block_hash, address, selectors, data, tx_id, tx_index, inner_index) ",
        );
        qb.push_values(chunk, |mut b, r| {
            b.push_bind(r.chain_id.get() as i64)
                .push_bind(r.height.get() as i64)
                .push_bind(r.index.get() as i64)
                .push_bind(&r.block_hash.0)
                .push_bind(&r.address.0)
                .push_bind(frame_selectors(&r.selectors))
                .push_bind(&r.data)
                .push_bind(&r.tx_id.0)
                .push_bind(r.tx_index as i64)
                .push_bind(r.inner_index.map(|v| v as i64));
        });
        qb.push(" ON CONFLICT (chain_id, height, idx) DO NOTHING");
        let res = qb.build().execute(&mut **tx).await?;
        n += res.rows_affected();
    }
    Ok(n)
}
