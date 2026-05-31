//! Reorg safety (gated on `caps().supports_reorg`). On each tick, compare the
//! canonical hash of the highest stored block against the recorded one; on
//! mismatch roll back a `confirmations`-deep window (delete rows + raw + blocks
//! at/above the fork, rewind the cursor) and let the follow loop reindex. Correct
//! while reorg depth ≤ confirmations.

use domain::ports::chain_source::ChainSource;
use domain::ports::repository::BlockRepository;
use domain::ports::unit_of_work::UnitOfWork;
use shared::{DomainError, Height};

pub struct ReorgService {
    blocks: Box<dyn BlockRepository>,
    /// Atomic rollback boundary (delete events+raw+blocks ≥ fork + rewind cursor).
    uow: Box<dyn UnitOfWork>,
    /// Typed tables to roll back (event + aux).
    tables: Vec<String>,
}

impl ReorgService {
    pub fn new(
        blocks: Box<dyn BlockRepository>,
        uow: Box<dyn UnitOfWork>,
        tables: Vec<String>,
    ) -> Self {
        Self {
            blocks,
            uow,
            tables,
        }
    }

    /// Detect a tip reorg and roll back. `Some(reindex_from)` on rollback, else `None`.
    pub async fn check(
        &self,
        source: &dyn ChainSource,
        confirmations: u64,
    ) -> Result<Option<Height>, DomainError> {
        if !source.caps().supports_reorg {
            return Ok(None);
        }
        let chain = source.chain_id();
        let Some(tip) = self.blocks.max_height(chain).await? else {
            return Ok(None);
        };
        let Some(stored) = self.blocks.get(chain, tip).await? else {
            return Ok(None);
        };
        let Some(canon) = source.block_hash(tip).await? else {
            return Ok(None);
        };
        if canon == stored.hash {
            return Ok(None);
        }

        let fork = Height(tip.get().saturating_sub(confirmations));
        let from = Height(fork.get() + 1);
        tracing::warn!(
            chain = chain.get(),
            tip = tip.get(),
            rollback_to = fork.get(),
            "reorg detected; rolling back"
        );
        let cid = chain.to_string();
        metrics::counter!("reorg_detected_total", "chain_id" => cid.clone()).increment(1);
        metrics::histogram!("reorg_rollback_depth_blocks", "chain_id" => cid.clone())
            .record((tip.get() - fork.get()) as f64);

        let start = std::time::Instant::now();
        self.uow.rollback_to(chain, from, &self.tables).await?;
        metrics::histogram!("reorg_rollback_duration_seconds", "chain_id" => cid)
            .record(start.elapsed().as_secs_f64());
        Ok(Some(from))
    }
}
