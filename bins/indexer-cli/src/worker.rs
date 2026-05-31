//! Decode worker pool: N competing-consumer tasks draining the work queue, plus a
//! reclaim ticker that frees leases abandoned by crashed workers.

use crate::wiring::new_worker;
use domain::ports::decoder::Decoder;
use shared::ChainId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type Decoders = HashMap<ChainId, Arc<dyn Decoder>>;

/// One worker: pull → decode → ack, backing off `poll_idle_ms` when idle vs `poll_ms`
/// when busy. Decode errors are logged and treated as idle (retry next tick).
fn spawn_decode_worker(
    pool: &db::PgPool,
    decoders: &Decoders,
    id: usize,
    poll_ms: u64,
    poll_idle_ms: u64,
    token: CancellationToken,
) -> JoinHandle<()> {
    let w = new_worker(pool, decoders.clone());
    tokio::spawn(async move {
        loop {
            if token.is_cancelled() {
                break;
            }
            let idle = match w.run_once().await {
                Ok(Some(rows)) => {
                    tracing::debug!(worker = id, rows, "decoded item");
                    false
                }
                Ok(None) => true,
                Err(e) => {
                    tracing::error!(worker = id, error = %e, "decode error");
                    true
                }
            };
            let wait = if idle { poll_idle_ms } else { poll_ms };
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(wait)) => {}
            }
        }
    })
}

/// Free leases whose holding worker crashed (lease older than the visibility timeout).
fn spawn_reclaim_ticker(
    pool: &db::PgPool,
    decoders: &Decoders,
    token: CancellationToken,
) -> JoinHandle<()> {
    let reclaimer = new_worker(pool, decoders.clone());
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if let Ok(n) = reclaimer.reclaim(60).await {
                        if n > 0 { tracing::warn!(reclaimed = n, "reclaimed expired leases"); }
                    }
                }
            }
        }
    })
}

/// Spawn a pool of decode workers + a reclaim ticker. Returns their join handles.
pub(crate) fn spawn_worker_pool(
    pool: &db::PgPool,
    decoders: &Decoders,
    workers: usize,
    poll_ms: u64,
    poll_idle_ms: u64,
    token: CancellationToken,
) -> Vec<JoinHandle<()>> {
    metrics::gauge!("decode_workers_active").set(workers as f64);
    let mut handles: Vec<JoinHandle<()>> = (0..workers)
        .map(|id| spawn_decode_worker(pool, decoders, id, poll_ms, poll_idle_ms, token.clone()))
        .collect();
    handles.push(spawn_reclaim_ticker(pool, decoders, token));
    handles
}
