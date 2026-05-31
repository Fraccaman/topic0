use async_trait::async_trait;
use shared::{BlockMeta, ChainId, DomainError, EventRow, Hash, Height, RawRecord, WorkItem};

/// All writes for one ingested range, committed atomically (exactly-once enqueue).
#[derive(Debug, Default, Clone)]
pub struct IngestBatch {
    pub raw_records: Vec<RawRecord>,
    pub block_metas: Vec<BlockMeta>,
    /// Aux rows (e.g. EVM transactions/receipts) keyed by their `event.table`.
    pub enrichment: Vec<EventRow>,
    pub enqueue: Option<WorkItem>,
    /// (chain, last_height, last_hash) cursor advance, applied in the same tx.
    pub advance_cursor: Option<(ChainId, Height, Hash)>,
}

/// Atomic multi-repo transaction boundary for the ingest write path.
#[async_trait]
pub trait UnitOfWork: Send + Sync {
    async fn commit_ingest(&self, batch: IngestBatch) -> Result<(), DomainError>;

    /// Reorg rollback, atomically in one transaction: delete rows at/above `from`
    /// from every typed `table` + raw records + block metas, and rewind the cursor to
    /// `from - 1`. All-or-nothing — a crash mid-rollback leaves the DB unchanged.
    async fn rollback_to(
        &self,
        chain: ChainId,
        from: Height,
        tables: &[String],
    ) -> Result<(), DomainError>;
}
