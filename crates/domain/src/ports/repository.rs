use async_trait::async_trait;
use shared::{
    BlockMeta, ChainId, DomainError, EventRow, Hash, Height, Page, QuerySpec, RawRecord, TxCalldata,
};

/// Write side: decoded rows for any typed table (events + aux).
#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn upsert_batch(&self, table: &str, rows: &[EventRow]) -> Result<u64, DomainError>;
    /// Reorg rollback: delete rows at/above `from` for a chain.
    async fn delete_from_height(
        &self,
        table: &str,
        chain: ChainId,
        from: Height,
    ) -> Result<u64, DomainError>;
}

/// Write side: immutable raw records (the re-decode artifact).
#[async_trait]
pub trait RawRecordRepository: Send + Sync {
    async fn insert_batch(&self, records: &[RawRecord]) -> Result<u64, DomainError>;
    async fn range(
        &self,
        chain: ChainId,
        from: Height,
        to: Height,
    ) -> Result<Vec<RawRecord>, DomainError>;
    async fn delete_from_height(&self, chain: ChainId, from: Height) -> Result<u64, DomainError>;
}

/// Write side: block metadata cache (timestamps + reorg detection).
#[async_trait]
pub trait BlockRepository: Send + Sync {
    async fn upsert_batch(&self, blocks: &[BlockMeta]) -> Result<u64, DomainError>;
    async fn get(&self, chain: ChainId, height: Height) -> Result<Option<BlockMeta>, DomainError>;
    /// Height → unix-seconds for a range (decoder block_time enrichment).
    async fn times(
        &self,
        chain: ChainId,
        from: Height,
        to: Height,
    ) -> Result<Vec<(Height, i64)>, DomainError>;
    /// Highest stored height (reorg anchor).
    async fn max_height(&self, chain: ChainId) -> Result<Option<Height>, DomainError>;
    async fn delete_from_height(&self, chain: ChainId, from: Height) -> Result<u64, DomainError>;
    /// Stored transactions' calldata, by tx_id (PK lookup).
    /// For decoding function calls of already-captured txs.
    async fn calldata(
        &self,
        chain: ChainId,
        tx_ids: &[Hash],
    ) -> Result<Vec<TxCalldata>, DomainError>;
}

/// Checkpoint cursor per chain.
#[async_trait]
pub trait CursorRepository: Send + Sync {
    async fn get(&self, chain: ChainId) -> Result<Option<(Height, Hash)>, DomainError>;
    async fn advance(
        &self,
        chain: ChainId,
        last: Height,
        last_hash: Hash,
    ) -> Result<(), DomainError>;
    async fn rewind(&self, chain: ChainId, to: Height) -> Result<(), DomainError>;
}

/// Read side (CQRS) — JSON rows, generic over dynamic tables.
#[async_trait]
pub trait EventQueryRepository: Send + Sync {
    async fn query(&self, spec: &QuerySpec) -> Result<Page<String>, DomainError>;
    /// Total rows matching `spec`'s filters, ignoring pagination (`after`/`first`) and
    /// joins. Resolved only when the API selects `totalCount` (a full `count(*)`).
    async fn count(&self, spec: &QuerySpec) -> Result<u64, DomainError>;
}
