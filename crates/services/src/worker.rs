//! Queue consumer. Pulls a work item (any chain, one in-flight per chain), decodes
//! its range with that chain's `Decoder`, upserts, acks. Competing consumers;
//! decode is idempotent so retries are safe.

use crate::decoding::decode_range;
use domain::ports::decoder::Decoder;
use domain::ports::queue_repo::QueueRepository;
use domain::ports::repository::{BlockRepository, EventRepository, RawRecordRepository};
use shared::{ChainId, DomainError};
use std::collections::HashMap;
use std::sync::Arc;

pub struct DecodeWorker {
    queue: Box<dyn QueueRepository>,
    events: Box<dyn EventRepository>,
    raw: Box<dyn RawRecordRepository>,
    blocks: Box<dyn BlockRepository>,
    /// One decoder per configured chain.
    decoders: HashMap<ChainId, Arc<dyn Decoder>>,
}

impl DecodeWorker {
    pub fn new(
        queue: Box<dyn QueueRepository>,
        events: Box<dyn EventRepository>,
        raw: Box<dyn RawRecordRepository>,
        blocks: Box<dyn BlockRepository>,
        decoders: HashMap<ChainId, Arc<dyn Decoder>>,
    ) -> Self {
        Self {
            queue,
            events,
            raw,
            blocks,
            decoders,
        }
    }

    /// Pull one item, decode its range, ack. Rows written, or `None` if queue empty.
    pub async fn run_once(&self) -> Result<Option<usize>, DomainError> {
        let Some(lease) = self.queue.pull_any().await? else {
            return Ok(None);
        };
        let item = &lease.item;
        let chain = item.chain_id.to_string();
        metrics::counter!("decode_items_total", "chain_id" => chain.clone()).increment(1);

        let written = match self.decoders.get(&item.chain_id) {
            Some(decoder) => {
                let start = std::time::Instant::now();
                let r = decode_range(
                    decoder.as_ref(),
                    self.raw.as_ref(),
                    self.blocks.as_ref(),
                    self.events.as_ref(),
                    item.chain_id,
                    item.from,
                    item.to,
                )
                .await;
                metrics::histogram!("decode_duration_seconds", "chain_id" => chain.clone())
                    .record(start.elapsed().as_secs_f64());
                match r {
                    Ok(w) => w,
                    Err(e) => {
                        metrics::counter!("decode_errors_total", "chain_id" => chain).increment(1);
                        return Err(e);
                    }
                }
            }
            None => {
                // No decoder for this chain; ack to avoid a stuck item. Range stays
                // in raw, resyncable later.
                tracing::warn!(chain = item.chain_id.get(), "no decoder for chain; acking");
                0
            }
        };

        self.queue.ack(lease.id, lease.lease_seq).await?;
        Ok(Some(written))
    }

    /// Reclaim leases abandoned by crashed workers.
    pub async fn reclaim(&self, older_than_secs: u64) -> Result<u64, DomainError> {
        self.queue.reclaim_expired(older_than_secs).await
    }

    pub async fn depth(&self) -> Result<u64, DomainError> {
        self.queue.depth().await
    }
}
