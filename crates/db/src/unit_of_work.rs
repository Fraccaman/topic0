//! `PgUnitOfWork` — commits an ingested range atomically (raw records, block metas,
//! enrichment rows, enqueue, cursor advance) in one transaction.

use crate::error::DbError;
use crate::repositories::{block_repo, cursor_repo, event_repo, queue_repo, raw_record_repo};
use async_trait::async_trait;
use domain::ports::unit_of_work::{IngestBatch, UnitOfWork};
use shared::{ChainId, DomainError, Height};

pub struct PgUnitOfWork {
    pool: sqlx::PgPool,
}

impl PgUnitOfWork {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UnitOfWork for PgUnitOfWork {
    async fn commit_ingest(&self, batch: IngestBatch) -> Result<(), DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;

        raw_record_repo::insert_batch_tx(&mut tx, &batch.raw_records).await?;
        block_repo::upsert_batch_tx(&mut tx, &batch.block_metas).await?;
        // Group enrichment rows by aux table, then batch-upsert each.
        let mut by_table: std::collections::BTreeMap<&str, Vec<shared::EventRow>> =
            std::collections::BTreeMap::new();
        for row in &batch.enrichment {
            by_table
                .entry(row.event.table.as_str())
                .or_default()
                .push(row.clone());
        }
        // FK receipts → transactions: write the parent (and every non-receipts
        // table) before receipts so the referenced tx row already exists.
        let mut tables: Vec<&&str> = by_table.keys().collect();
        tables.sort_by_key(|t| **t == shared::RECEIPTS_TABLE);
        for table in tables {
            event_repo::upsert_batch_tx(&mut tx, table, &by_table[*table]).await?;
        }
        if let Some(item) = &batch.enqueue {
            queue_repo::push_in_tx(&mut tx, item).await?;
        }
        if let Some((chain, last, last_hash)) = batch.advance_cursor {
            cursor_repo::advance_in_tx(&mut tx, chain, last, last_hash).await?;
        }

        tx.commit().await.map_err(DbError::from)?;
        Ok(())
    }

    async fn rollback_to(
        &self,
        chain: ChainId,
        from: Height,
        tables: &[String],
    ) -> Result<(), DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        for table in tables {
            event_repo::delete_from_height_tx(&mut tx, table, chain, from).await?;
        }
        raw_record_repo::delete_from_height_tx(&mut tx, chain, from).await?;
        block_repo::delete_from_height_tx(&mut tx, chain, from).await?;
        // from = fork + 1, so the cursor rewinds to the fork (one below `from`).
        let fork = Height(from.get().saturating_sub(1));
        cursor_repo::rewind_in_tx(&mut tx, chain, fork).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(())
    }
}
