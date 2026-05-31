//! EVM adapter registry: builds a `(ChainSource, Decoder)` pair from config,
//! dispatching on `chain.kind`. EVM is the only kind today. The decoder itself lives
//! in `decoder`; this module wires the cost model, plan limits, limiter, and spend ledger.

use crate::client::RpcLogSource;
use crate::decoder::build_evm_decoder;
use crate::error::SourceError;
use config::ChainCfg;
use domain::ports::chain_source::ChainSource;
use domain::ports::decoder::Decoder;
use pricing::SpendLedger;
use shared::{ChainId, PlanProfile};
use std::path::Path;
use std::sync::Arc;

/// A built chain: source + decoder + ledger.
pub struct BuiltChain {
    pub source: Box<dyn ChainSource>,
    pub decoder: Box<dyn Decoder>,
    pub ledger: Arc<SpendLedger>,
}

/// Build only the decoder for a chain (no network) — used by `migrate`/`resync`.
pub fn build_decoder(cfg: &ChainCfg, base_dir: &Path) -> Result<Box<dyn Decoder>, SourceError> {
    match cfg.kind.as_str() {
        "evm" => Ok(Box::new(build_evm_decoder(cfg, base_dir)?)),
        other => Err(SourceError::UnknownKind(other.to_string())),
    }
}

/// Build the full adapter (source + decoder); connects HTTP/WS.
/// `aux_concurrency` caps concurrent block/receipt RPCs in `fetch_aux`.
pub async fn build_chain(
    cfg: &ChainCfg,
    base_dir: &Path,
    aux_concurrency: usize,
) -> Result<BuiltChain, SourceError> {
    match cfg.kind.as_str() {
        "evm" => build_evm(cfg, base_dir, aux_concurrency).await,
        other => Err(SourceError::UnknownKind(other.to_string())),
    }
}

async fn build_evm(
    cfg: &ChainCfg,
    base_dir: &Path,
    aux_concurrency: usize,
) -> Result<BuiltChain, SourceError> {
    let decoder = build_evm_decoder(cfg, base_dir)?;

    // Cost model + plan limits (caps from config `[limits]`).
    let cost_model = pricing::for_kind(&cfg.source.kind)
        .ok_or_else(|| SourceError::UnknownKind(cfg.source.kind.clone()))?;
    let mut plan = PlanProfile::default();
    apply_overrides(&mut plan, cfg);

    let ledger = Arc::new(SpendLedger::new(plan.monthly_quota, cost_model.rate()));
    let rpc = RpcLogSource::connect(
        ChainId(cfg.id),
        &cfg.source.http,
        cfg.source.ws.as_deref(),
        plan,
        cost_model,
        ledger.clone(),
        aux_concurrency,
    )
    .await?;

    Ok(BuiltChain {
        source: Box::new(rpc),
        decoder: Box::new(decoder),
        ledger,
    })
}

fn apply_overrides(plan: &mut PlanProfile, chain: &ChainCfg) {
    let Some(l) = &chain.source.limits else {
        return;
    };
    if let Some(v) = l.max_rps {
        plan.max_rps = v;
    }
    if let Some(v) = l.max_cu_per_sec {
        plan.max_cu_per_sec = Some(v);
    }
    if let Some(v) = l.max_batch {
        plan.max_batch = v;
    }
    if let Some(v) = l.max_getlogs_blocks {
        plan.max_getlogs_blocks = v;
    }
    if let Some(v) = l.max_getlogs_results {
        plan.max_getlogs_results = v;
    }
    if let Some(v) = l.monthly_quota_cu {
        plan.monthly_quota = Some(v);
    }
}
