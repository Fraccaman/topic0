//! Per-chain live tip loop. The tip state machine lives in `tip`, the range planning +
//! fetch→write pipeline in `plan`, and the restart supervisor in `supervisor`.

use crate::plan::{fetch_target, run_pipeline};
use crate::tip::TipState;
use anyhow::Result;
use domain::ports::repository::CursorRepository;
use futures::stream::StreamExt;
use shared::{ChainId, Hash, Height, RecordFilter};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// On the WS tip path, probe block hashes for a reorg at most once every this many loop
/// ticks (a `removed` log triggers an immediate probe in between). Bounds worst-case
/// reorg-detection latency when no `removed` event is delivered.
const REORG_PROBE_TICKS: u64 = 16;

/// Per-chain ingest loop: resume, reorg-check, catch up to head−confirmations, enqueue
/// ranges (workers drain the queue). Tip is driven off pushed WS logs when available,
/// else interval poll — see `TipState`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ingest_loop(
    ingest: services::IngestionService,
    reorg: services::ReorgService,
    cursors: db::PgCursorRepository,
    filter: RecordFilter,
    ledger: Arc<pricing::SpendLedger>,
    confirmations: u64,
    start_block: u64,
    step: u64,
    range_concurrency: usize,
    interval: u64,
    token: CancellationToken,
) -> Result<()> {
    let chain = ingest.source().chain_id();
    let mut next = match cursors.get(chain).await? {
        Some((last, _)) => last.get() + 1,
        None => start_block.max(1),
    };
    tracing::info!(
        chain = chain.get(),
        resume_from = next,
        range_concurrency,
        "ingest loop started"
    );

    let mut state = TipState::new(ingest.source().caps().supports_subscribe);
    let mut tick: u64 = 0;

    loop {
        if token.is_cancelled() || ledger.quota_exhausted() {
            break;
        }

        state.ensure_subscribed(&ingest, &filter, chain).await;

        // Reorg: probe block hashes only when needed — on a WS `removed` signal, every
        // tick on the poll path (no cheap signal there), or periodically on the WS path
        // as a safety net. Idle WS ticks issue no `block_hash`.
        tick = tick.wrapping_add(1);
        let probe = state.take_reorg_signal().is_some()
            || !state.ws_enabled
            || tick.is_multiple_of(REORG_PROBE_TICKS);
        if probe {
            if let Some(reindex) = reorg.check(ingest.source(), confirmations).await? {
                next = reindex.get();
                state.reconcile = true; // re-fetch the rolled-back range
                tracing::warn!(chain = chain.get(), reindex_from = next, "reorg rollback");
            }
        }

        // Catch up to the safe tip when we might act (idle WS ticks skip the head poll).
        if state.should_poll_head() {
            let head = ingest.head().await?.get();
            metrics::gauge!("chain_head_block", "chain_id" => chain.to_string()).set(head as f64);
            let safe = head.saturating_sub(confirmations);
            if safe >= next {
                let caught = catch_up(
                    &ingest,
                    &cursors,
                    &filter,
                    &ledger,
                    &token,
                    &mut state,
                    chain,
                    next,
                    safe,
                    step,
                    range_concurrency,
                )
                .await?;
                if caught.shutdown {
                    break;
                }
                next = caught.next;
                // Bounded batch still behind the safe tip → fetch the next batch now.
                if caught.behind {
                    continue;
                }
            }
        }

        // Wake on a pushed WS log, shutdown, or the poll interval.
        let wait = state.wait(interval);
        tokio::select! {
            _ = token.cancelled() => break,
            rec = async { state.sub.as_mut().unwrap().next().await }, if state.sub.is_some() => {
                state.on_ws_event(rec, chain);
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
    Ok(())
}

/// Wall-clock seconds since the Unix epoch, for the cursor-staleness watchdog gauge.
fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Outcome of one [`catch_up`] window.
struct CaughtUp {
    /// Block to resume from next iteration.
    next: u64,
    /// Shutdown was requested mid-fetch.
    shutdown: bool,
    /// Still behind the safe tip (bounded batch) — caller should loop again immediately.
    behind: bool,
}

/// Fetch + commit one bounded window `[next..=fetch_target(safe)]`, advancing the cursor
/// over the committed prefix. On the WS no-fetch path (stream guarantees the window had
/// no matching logs) it checkpoints forward with zero RPC instead.
#[allow(clippy::too_many_arguments)]
async fn catch_up(
    ingest: &services::IngestionService,
    cursors: &db::PgCursorRepository,
    filter: &RecordFilter,
    ledger: &Arc<pricing::SpendLedger>,
    token: &CancellationToken,
    state: &mut TipState,
    chain: ChainId,
    next: u64,
    safe: u64,
    step: u64,
    range_concurrency: usize,
) -> Result<CaughtUp> {
    let must_fetch = state.must_fetch(safe);
    let target = fetch_target(safe, next, step, range_concurrency, must_fetch);
    metrics::gauge!("tip_mode", "chain_id" => chain.to_string())
        .set(f64::from(u8::from(state.ws_enabled)));

    if !must_fetch {
        cursors
            .advance(chain, Height(target), Hash(Vec::new()))
            .await?;
        metrics::gauge!("last_advance_timestamp_seconds", "chain_id" => chain.to_string())
            .set(now_secs());
        let next = target + 1;
        tracing::info!(chain = chain.get(), to = target, "checkpoint advanced");
        return Ok(CaughtUp {
            next,
            shutdown: false,
            behind: next <= safe,
        });
    }

    let out = run_pipeline(
        ingest,
        cursors,
        chain,
        filter,
        next,
        target,
        step,
        range_concurrency,
        ledger,
        token,
    )
    .await?;
    let start = next;
    let next = out.advanced_to.map_or(next, |adv| adv + 1);
    if next > start {
        metrics::gauge!("last_advance_timestamp_seconds", "chain_id" => chain.to_string())
            .set(now_secs());
        metrics::histogram!("tip_reconcile_backfill_blocks", "chain_id" => chain.to_string())
            .record((next - start) as f64);
    }
    if out.shutdown {
        return Ok(CaughtUp {
            next,
            shutdown: true,
            behind: false,
        });
    }

    // WS only covers blocks arriving after subscribe, never the historical gap — keep
    // sweeping until caught up to `safe` before trusting the stream.
    state.reconcile = next <= safe;
    state.clear_through(next.saturating_sub(1));
    let tip = if state.ws_enabled { "ws" } else { "poll" };
    tracing::info!(
        chain = chain.get(),
        to = next.saturating_sub(1),
        records = out.records,
        tip,
        "caught up"
    );
    Ok(CaughtUp {
        next,
        shutdown: false,
        behind: next <= safe,
    })
}
