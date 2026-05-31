//! `RpcLogSource` — EVM JSON-RPC `domain::ChainSource`. Owns the alloy provider(s),
//! limiter, plan, and cost model; delegates JSON-RPC batching to `batch`, retry to
//! `retry`, log/filter mapping to `map`, and aux row assembly to `enrichment`.

use crate::enrichment;
use crate::error::SourceError;
use crate::limiter::Limiter;
use crate::map::{base_filter, map_log};
use crate::metric::{method_name, CallGuard};
use crate::retry::{is_range_cap_error, split_range, with_retry};
use alloy::eips::BlockNumberOrTag;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::{Block, Log, TransactionReceipt};
use async_trait::async_trait;
use domain::ports::chain_source::{AuxData, ChainSource};
use domain::CostModel;
use futures::stream::{BoxStream, StreamExt};
use pricing::SpendLedger;
use shared::{
    ChainCaps, ChainId, DomainError, Hash, Height, PlanProfile, RawRecord, RecordFilter, RpcCall,
    TipLog,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct RpcLogSource {
    chain_id: ChainId,
    pub(crate) provider: DynProvider,
    ws: Option<DynProvider>,
    pub(crate) limiter: Limiter,
    pub(crate) plan: PlanProfile,
    pub(crate) cost_model: Box<dyn CostModel>,
    /// Spend accounting; recorded once per real RPC sub-call (see `meter`).
    pub(crate) ledger: Arc<SpendLedger>,
    /// Max concurrent block/receipt RPCs in `fetch_aux`.
    pub(crate) aux_concurrency: usize,
    /// Observed log density (logs/block ×1000, EWMA). Seeds the getLogs sub-span so a
    /// dense contract fetches under the result cap instead of paying an over-cap
    /// round-trip before halving. 0 = unknown → seed at `max_getlogs_blocks`.
    density_milli: AtomicU64,
}

impl RpcLogSource {
    pub async fn connect(
        chain_id: ChainId,
        http_url: &str,
        ws_url: Option<&str>,
        plan: PlanProfile,
        cost_model: Box<dyn CostModel>,
        ledger: Arc<SpendLedger>,
        aux_concurrency: usize,
    ) -> Result<Self, SourceError> {
        let url = http_url
            .parse()
            .map_err(|e| SourceError::Transport(format!("bad url: {e}")))?;
        let provider = ProviderBuilder::new().connect_http(url).erased();
        let ws = match ws_url {
            Some(u) if !u.is_empty() => Some(
                ProviderBuilder::new()
                    .connect_ws(WsConnect::new(u.to_string()))
                    .await
                    .map_err(|e| SourceError::Transport(format!("ws connect: {e}")))?
                    .erased(),
            ),
            _ => None,
        };
        if ws.is_some() {
            tracing::info!(chain_id = %chain_id, "websocket connected");
        } else {
            tracing::info!(chain_id = %chain_id, "no websocket configured; http only");
        }
        let limiter = Limiter::from_plan(&plan);
        Ok(Self {
            chain_id,
            provider,
            ws,
            limiter,
            plan,
            cost_model,
            ledger,
            aux_concurrency: aux_concurrency.max(1),
            density_milli: AtomicU64::new(0),
        })
    }

    fn cid(&self) -> String {
        self.chain_id.to_string()
    }

    /// Accumulate a real RPC sub-call's cost into the ledger. Called at each true call
    /// site (one getLogs page, one block/receipt batch, …), so ledger totals match the
    /// actual call count, not the outer-trait boundary.
    pub(crate) fn meter(&self, call: &RpcCall) {
        let (units, money) = self.cost_model.cost(call);
        self.ledger.record(units, money);
    }

    /// Meter a real RPC sub-call and return a guard that tracks its call count,
    /// in-flight concurrency, and latency until dropped (i.e. until the call returns).
    pub(crate) fn enter(&self, call: &RpcCall) -> CallGuard {
        self.meter(call);
        CallGuard::new(self.cid(), method_name(call))
    }

    /// Block span to seed a getLogs sub-span at, from the observed density: aim for
    /// ~80% of the result cap. Capped at the provider block ceiling; falls back to it
    /// when density/cap are unknown.
    fn suggested_span(&self) -> u64 {
        let max_blocks = u64::from(self.plan.max_getlogs_blocks.max(1));
        let cap = u64::from(self.plan.max_getlogs_results);
        let d = self.density_milli.load(Ordering::Relaxed);
        if cap == 0 || d == 0 {
            return max_blocks;
        }
        (cap.saturating_mul(800) / d).max(1).min(max_blocks)
    }

    /// Fold a clean page's logs/block into the density EWMA (1/8 weight).
    fn record_density(&self, blocks: u64, logs: usize) {
        if blocks == 0 {
            return;
        }
        let sample = (logs as u64).saturating_mul(1000) / blocks;
        let old = self.density_milli.load(Ordering::Relaxed);
        let new = if old == 0 {
            sample
        } else {
            (old * 7 + sample) / 8
        };
        self.density_milli.store(new, Ordering::Relaxed);
        metrics::gauge!("log_density", "chain_id" => self.cid()).set(new as f64 / 1000.0);
    }
}

#[async_trait]
impl ChainSource for RpcLogSource {
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    async fn head(&self) -> Result<Height, DomainError> {
        self.limiter
            .acquire_cu(self.cu_for(&RpcCall::BlockNumber))
            .await;
        self.limiter.acquire().await;
        let _g = self.enter(&RpcCall::BlockNumber);
        let n = self
            .provider
            .get_block_number()
            .await
            .map_err(|e| SourceError::Transport(e.to_string()))?;
        Ok(Height(n))
    }

    async fn fetch_records(
        &self,
        filter: &RecordFilter,
        from: Height,
        to: Height,
    ) -> Result<Vec<RawRecord>, DomainError> {
        let base = base_filter(filter);
        let cap = self.plan.max_getlogs_results as u64;
        // Seed the work stack with density-sized sub-spans of [from,to] so a dense
        // contract starts under the result cap. A sub-span that still overflows the cap
        // (explicit error, or a page that hits the cap and may be silently truncated) is
        // split in half and each half retried, down to a single block — transient, the
        // caller's range/cursor are untouched. Clean pages feed the density EWMA.
        let mut pending: Vec<(u64, u64)> = {
            let span = self.suggested_span();
            let (mut s, hi) = (from.get(), to.get());
            let mut chunks = Vec::new();
            while s <= hi {
                let e = (s + span - 1).min(hi);
                chunks.push((s, e));
                s = e + 1;
            }
            chunks.reverse(); // LIFO: lower ranges fetched first
            chunks
        };
        let mut logs: Vec<Log> = Vec::new();
        while let Some((lo, hi)) = pending.pop() {
            let f = base.clone().from_block(lo).to_block(hi);
            let res = with_retry("eth_getLogs", || async {
                let call = RpcCall::GetLogs {
                    blocks: hi - lo + 1,
                    results: 0,
                };
                self.limiter.acquire_cu(self.cu_for(&call)).await;
                self.limiter.acquire().await;
                let _g = self.enter(&call);
                self.provider.get_logs(&f).await.map_err(|e| e.to_string())
            })
            .await;
            let overflow = match res {
                Ok(page) if lo < hi && cap > 0 && page.len() as u64 >= cap => true,
                Ok(page) => {
                    let blocks = hi - lo + 1;
                    self.record_density(blocks, page.len());
                    metrics::histogram!("getlogs_range_blocks", "chain_id" => self.cid())
                        .record(blocks as f64);
                    logs.extend(page);
                    false
                }
                Err(SourceError::Transport(e)) if lo < hi && is_range_cap_error(&e) => true,
                Err(e) => return Err(e.into()),
            };
            if overflow {
                metrics::counter!("getlogs_range_halves_total", "chain_id" => self.cid(), "reason" => "result_cap").increment(1);
                let (left, right) = split_range(lo, hi);
                pending.push(right); // LIFO: lower range fetched first
                pending.push(left);
            }
        }
        let mut out = Vec::with_capacity(logs.len());
        for log in &logs {
            out.push(map_log(self.chain_id, log)?);
        }
        Ok(out)
    }

    async fn fetch_aux(&self, records: &[RawRecord]) -> Result<AuxData, DomainError> {
        let heights: BTreeSet<u64> = records.iter().map(|r| r.height.get()).collect();
        let tx_ids: BTreeSet<Vec<u8>> = records.iter().map(|r| r.tx_id.0.clone()).collect();
        // Cross-contract cache: `touched` = every log's block/tx reference, `fetched` =
        // distinct ones actually RPC'd. The gap is the dedup saving (hit ratio derived).
        let chain = self.cid();
        let touched = records.len() as u64;
        metrics::counter!("rpc_aux_touched_total", "chain_id" => chain.clone(), "kind" => "block")
            .increment(touched);
        metrics::counter!("rpc_aux_touched_total", "chain_id" => chain.clone(), "kind" => "tx")
            .increment(touched);
        metrics::counter!("rpc_aux_fetched_total", "chain_id" => chain.clone(), "kind" => "block")
            .increment(heights.len() as u64);
        metrics::counter!("rpc_aux_fetched_total", "chain_id" => chain, "kind" => "tx")
            .increment(tx_ids.len() as u64);
        // tx_id → (height, block_hash) for receipt rows.
        let mut tx_loc: BTreeMap<Vec<u8>, (Height, Hash)> = BTreeMap::new();
        for r in records {
            tx_loc
                .entry(r.tx_id.0.clone())
                .or_insert((r.height, r.block_hash.clone()));
        }

        // Blocks (full=true → header + txs) and receipts are independent fetches; run
        // them concurrently so aux latency is the slower of the two, not the sum.
        let blocks_fut = self.batched::<u64, _, Option<Block>, _>(
            heights.into_iter().collect(),
            "eth_getBlockByNumber",
            |&h| (BlockNumberOrTag::Number(h), true),
        );
        let receipts_fut = self.batched::<Vec<u8>, _, Option<TransactionReceipt>, _>(
            tx_ids.iter().cloned().collect(),
            "eth_getTransactionReceipt",
            |id| (alloy_primitives::B256::from_slice(id),),
        );
        let (blocks, receipts) = tokio::try_join!(blocks_fut, receipts_fut)?;

        let mut aux = AuxData::default();
        enrichment::block_aux(self.chain_id, blocks, &tx_ids, &mut aux)?;
        enrichment::receipt_aux(self.chain_id, receipts, &tx_loc, &mut aux);
        Ok(aux)
    }

    async fn block_hash(&self, height: Height) -> Result<Option<Hash>, DomainError> {
        let blk = with_retry("eth_getBlockByNumber", || async {
            let call = RpcCall::BlockByNumber {
                count: 1,
                full: false,
            };
            self.limiter.acquire_cu(self.cu_for(&call)).await;
            self.limiter.acquire().await;
            let _g = self.enter(&call);
            self.provider
                .get_block_by_number(BlockNumberOrTag::Number(height.get()))
                .await
                .map_err(|e| e.to_string())
        })
        .await?;
        Ok(blk.map(|b| Hash(b.header.hash.as_slice().to_vec())))
    }

    async fn subscribe(
        &self,
        filter: &RecordFilter,
    ) -> Result<BoxStream<'static, Result<TipLog, DomainError>>, DomainError> {
        let ws = self
            .ws
            .clone()
            .ok_or_else(|| SourceError::Unsupported("no ws url configured".into()))?;
        let f = base_filter(filter);
        let chain_id = self.chain_id;
        self.meter(&RpcCall::LogSubscription);
        let sub = ws
            .subscribe_logs(&f)
            .await
            .map_err(|e| SourceError::Transport(format!("subscribe: {e}")))?;
        let stream = sub.into_stream().map(move |log| {
            let _keep_alive = &ws;
            let removed = log.removed;
            map_log(chain_id, &log)
                .map(|record| TipLog { record, removed })
                .map_err(DomainError::from)
        });
        Ok(stream.boxed())
    }

    fn cost_model(&self) -> &dyn CostModel {
        self.cost_model.as_ref()
    }
    fn plan(&self) -> &PlanProfile {
        &self.plan
    }
    fn caps(&self) -> ChainCaps {
        ChainCaps {
            supports_reorg: true,
            supports_subscribe: self.ws.is_some(),
        }
    }
}
