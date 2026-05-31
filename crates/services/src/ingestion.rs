//! IngestionService: fetch filtered records + aux, write everything atomically.

use domain::ports::chain_source::{AuxData, ChainSource};
use domain::ports::unit_of_work::{IngestBatch, UnitOfWork};
use shared::{DomainError, Epoch, Hash, Height, RawRecord, RecordFilter, WorkItem, WorkKind};

pub struct IngestionService {
    source: Box<dyn ChainSource>,
    uow: Box<dyn UnitOfWork>,
}

#[derive(Debug, Clone)]
pub struct IngestOutcome {
    pub records: usize,
    pub blocks: usize,
    pub enrichment: usize,
    /// Highest matched block (height, hash) for the cursor hash; `None` if no matches.
    pub last_block: Option<(Height, Hash)>,
}

/// Fetched-but-unwritten range: the pure-RPC output of `fetch_range`, handed to a
/// separate writer (`commit_range`) so RPC and DB run on independent concurrency.
#[derive(Debug)]
pub struct FetchedRange {
    pub from: Height,
    pub to: Height,
    pub records: Vec<RawRecord>,
    pub aux: AuxData,
    /// Highest matched block (height, hash); `None` if no matches.
    pub last_block: Option<(Height, Hash)>,
}

impl IngestionService {
    pub fn new(source: Box<dyn ChainSource>, uow: Box<dyn UnitOfWork>) -> Self {
        Self { source, uow }
    }

    /// Pure-RPC half: fetch filtered records + aux for one range. No DB writes, so many
    /// ranges run concurrently without holding a connection. Pair with `commit_range`.
    pub async fn fetch_range(
        &self,
        filter: &RecordFilter,
        from: Height,
        to: Height,
    ) -> Result<FetchedRange, DomainError> {
        let records = self.source.fetch_records(filter, from, to).await?;
        metrics::counter!("ingest_records_fetched_total", "chain_id" => self.source.chain_id().to_string())
            .increment(records.len() as u64);
        let aux = if records.is_empty() {
            AuxData::default()
        } else {
            self.source.fetch_aux(&records).await?
        };
        let last_block = aux
            .block_metas
            .iter()
            .max_by_key(|b| b.height.get())
            .map(|b| (b.height, b.hash.clone()));
        Ok(FetchedRange {
            from,
            to,
            records,
            aux,
            last_block,
        })
    }

    /// DB half: write a fetched range atomically (raw + aux + enqueue). Never advances
    /// the cursor — the pipeline driver advances the contiguous prefix once committed.
    pub async fn commit_range(
        &self,
        fetched: FetchedRange,
        epoch: Epoch,
    ) -> Result<IngestOutcome, DomainError> {
        self.commit(fetched, epoch, false).await
    }

    /// Fetch + write one range in one shot. `advance_cursor` persists `to` as the
    /// checkpoint in the same tx; set false when ranges run concurrently (caller
    /// advances the prefix). Convenience over `fetch_range` + `commit_range`.
    pub async fn ingest_range(
        &self,
        filter: &RecordFilter,
        from: Height,
        to: Height,
        epoch: Epoch,
        advance_cursor: bool,
    ) -> Result<IngestOutcome, DomainError> {
        let fetched = self.fetch_range(filter, from, to).await?;
        self.commit(fetched, epoch, advance_cursor).await
    }

    /// Build the atomic write batch from a fetched range and commit it. Always enqueues
    /// a decode `WorkItem` when there are records; `advance_cursor` additionally stamps
    /// `to` + the highest matched block's hash as the cursor checkpoint, in the same tx.
    async fn commit(
        &self,
        fetched: FetchedRange,
        epoch: Epoch,
        advance_cursor: bool,
    ) -> Result<IngestOutcome, DomainError> {
        let chain = self.source.chain_id();
        let FetchedRange {
            from,
            to,
            records,
            aux,
            last_block,
        } = fetched;

        let cursor = advance_cursor.then(|| {
            let hash = last_block
                .as_ref()
                .map(|(_, h)| h.clone())
                .unwrap_or(Hash(Vec::new()));
            (chain, to, hash)
        });
        let enqueue = (!records.is_empty()).then_some(WorkItem {
            chain_id: chain,
            from,
            to,
            kind: WorkKind::Backfill,
            epoch,
        });
        let outcome = IngestOutcome {
            records: records.len(),
            blocks: aux.block_metas.len(),
            enrichment: aux.enrichment.len(),
            last_block,
        };

        let cid = chain.to_string();
        let start = std::time::Instant::now();
        self.uow
            .commit_ingest(IngestBatch {
                raw_records: records,
                block_metas: aux.block_metas,
                enrichment: aux.enrichment,
                enqueue,
                advance_cursor: cursor,
            })
            .await?;
        metrics::histogram!("ingest_commit_duration_seconds", "chain_id" => cid.clone())
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("ingest_records_written_total", "chain_id" => cid.clone())
            .increment(outcome.records as u64);
        metrics::counter!("ingest_enrichment_rows_total", "chain_id" => cid.clone(), "row_type" => "block")
            .increment(outcome.blocks as u64);
        metrics::counter!("ingest_enrichment_rows_total", "chain_id" => cid, "row_type" => "enrichment")
            .increment(outcome.enrichment as u64);
        Ok(outcome)
    }

    pub fn source(&self) -> &dyn ChainSource {
        &*self.source
    }

    pub async fn head(&self) -> Result<Height, DomainError> {
        self.source.head().await
    }
}
