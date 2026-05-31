use crate::ports::cost_model::CostModel;
use async_trait::async_trait;
use futures::stream::BoxStream;
use shared::{
    BlockMeta, ChainCaps, ChainId, DomainError, EventRow, Hash, Height, PlanProfile, RawRecord,
    RecordFilter, TipLog,
};

/// Block metadata + enrichment rows for one ingested range, fetched together.
#[derive(Debug, Default, Clone)]
pub struct AuxData {
    pub block_metas: Vec<BlockMeta>,
    /// Rows for adapter-declared aux tables (EVM: transactions/receipts).
    pub enrichment: Vec<EventRow>,
}

/// Per-chain fetch seam. Bound to one chain; stamps `chain_id` on its records.
#[async_trait]
pub trait ChainSource: Send + Sync {
    /// The chain this source serves.
    fn chain_id(&self) -> ChainId;

    /// Current chain head (block number / slot).
    async fn head(&self) -> Result<Height, DomainError>;

    /// Filtered records over `[from, to]`. Provider range/result caps handled internally.
    async fn fetch_records(
        &self,
        filter: &RecordFilter,
        from: Height,
        to: Height,
    ) -> Result<Vec<RawRecord>, DomainError>;

    /// Block metadata + chain-specific enrichment for the matched records.
    async fn fetch_aux(&self, records: &[RawRecord]) -> Result<AuxData, DomainError>;

    /// Canonical block hash at a height (cheap; for reorg detection).
    async fn block_hash(&self, height: Height) -> Result<Option<Hash>, DomainError>;

    /// Live tip subscription. Items carry the chain's `removed` flag so the tip loop
    /// can cheaply detect reorgs without probing block hashes every tick.
    async fn subscribe(
        &self,
        filter: &RecordFilter,
    ) -> Result<BoxStream<'static, Result<TipLog, DomainError>>, DomainError>;

    fn cost_model(&self) -> &dyn CostModel;
    fn plan(&self) -> &PlanProfile;
    fn caps(&self) -> ChainCaps;
}
