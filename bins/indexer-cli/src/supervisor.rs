//! Per-chain ingest supervision: build the chain (connects HTTP/WS) and run its ingest
//! loop, restarting on error or panic with capped exponential backoff so a chain never
//! silently dies.

use crate::ingest::ingest_loop;
use crate::wiring::{build_reorg, new_ingest};
use anyhow::Result;
use config::ChainCfg;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Supervise one chain's ingest loop: restart on error (rebuilding the source so a
/// dropped websocket/provider recovers) or on panic, with capped exponential backoff.
/// Returns only on cancellation — a chain never silently dies.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn supervised_ingest(
    chain: ChainCfg,
    base: PathBuf,
    pool: db::PgPool,
    tables: Vec<String>,
    aux_concurrency: usize,
    range_concurrency: usize,
    interval: u64,
    token: CancellationToken,
) {
    let mut backoff = 1u64;
    while !token.is_cancelled() {
        // Inner spawn so a panic (not just an Err) is caught and restarted.
        let attempt = tokio::spawn(run_chain_once(
            chain.clone(),
            base.clone(),
            pool.clone(),
            tables.clone(),
            aux_concurrency,
            range_concurrency,
            interval,
            token.clone(),
        ));
        match attempt.await {
            Ok(Ok(())) => return, // clean shutdown (cancelled)
            Ok(Err(e)) => {
                tracing::error!(chain = chain.id, error = %e, "ingest loop failed; restarting");
            }
            Err(j) if j.is_panic() => {
                tracing::error!(chain = chain.id, "ingest loop panicked; restarting");
            }
            Err(_) => return, // task aborted
        }
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(backoff)) => {}
        }
        backoff = (backoff * 2).min(30);
    }
}

/// Build the chain (connects HTTP/WS) and run its ingest loop once.
#[allow(clippy::too_many_arguments)]
async fn run_chain_once(
    chain: ChainCfg,
    base: PathBuf,
    pool: db::PgPool,
    tables: Vec<String>,
    aux_concurrency: usize,
    range_concurrency: usize,
    interval: u64,
    token: CancellationToken,
) -> Result<()> {
    let built = source::build_chain(&chain, &base, aux_concurrency).await?;
    let filter = built.decoder.record_filter();
    let ledger = built.ledger.clone();
    let ingest = new_ingest(&pool, built.source);
    let reorg = build_reorg(&pool, tables);
    let cursors = db::PgCursorRepository::new(pool.clone());
    let step = ingest.source().plan().max_getlogs_blocks.max(1) as u64;
    let start = chain.start_block().expect("validated: chain has a start");
    ingest_loop(
        ingest,
        reorg,
        cursors,
        filter,
        ledger,
        chain.confirmations,
        start,
        step,
        range_concurrency,
        interval,
        token,
    )
    .await
}
