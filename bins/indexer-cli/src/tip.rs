//! Live-tip state machine for the ingest loop. `pending` holds WS-signaled heights not
//! yet confirmed+indexed; `reconcile` forces a full getLogs sweep over the confirmed
//! gap on startup and after every (re)subscribe, since WS can miss logs across a
//! disconnect.

use futures::stream::BoxStream;
use shared::{ChainId, DomainError, RecordFilter, TipLog};
use std::collections::BTreeSet;
use std::time::Duration;

/// Short recheck delay (secs) when a WS-pushed log is awaiting confirmation, so it
/// is indexed promptly instead of idling the full poll interval.
const PENDING_RECHECK_SECS: u64 = 2;

pub(crate) type LogStream = BoxStream<'static, Result<TipLog, DomainError>>;

pub(crate) struct TipState {
    pub(crate) ws_enabled: bool,
    pub(crate) sub: Option<LogStream>,
    pending: BTreeSet<u64>,
    pub(crate) reconcile: bool,
    /// Lowest height a WS `removed` log was seen at since last consumed — a cheap
    /// reorg signal that lets the loop probe block hashes only when needed.
    reorg_at: Option<u64>,
}

impl TipState {
    pub(crate) fn new(ws_enabled: bool) -> Self {
        Self {
            ws_enabled,
            sub: None,
            pending: BTreeSet::new(),
            reconcile: true,
            reorg_at: None,
        }
    }

    /// (Re)subscribe when WS is available but we have no live stream; a fresh stream
    /// only covers blocks after subscribe, so force a reconcile sweep.
    pub(crate) async fn ensure_subscribed(
        &mut self,
        ingest: &services::IngestionService,
        filter: &RecordFilter,
        chain: ChainId,
    ) {
        if !self.ws_enabled || self.sub.is_some() {
            return;
        }
        match ingest.source().subscribe(filter).await {
            Ok(s) => {
                self.sub = Some(s);
                self.reconcile = true;
                metrics::counter!("ws_reconnects_total", "chain_id" => chain.to_string())
                    .increment(1);
                tracing::info!(chain = chain.get(), "tip via websocket");
            }
            Err(e) => {
                tracing::warn!(chain = chain.get(), error = %e, "ws subscribe failed; retrying");
            }
        }
    }

    /// Fetch only when we must: no WS, a pending reconcile, or a WS-signaled log now
    /// confirmed (`<= safe`). Otherwise WS guarantees the confirmed range had no
    /// matching logs → the caller advances the cursor with zero getLogs.
    pub(crate) fn must_fetch(&self, safe: u64) -> bool {
        !self.ws_enabled || self.reconcile || self.pending.first().is_some_and(|&lo| lo <= safe)
    }

    /// Drop pending heights covered by a sweep up to `target`.
    pub(crate) fn clear_through(&mut self, target: u64) {
        self.pending.retain(|&h| h > target);
    }

    /// Whether the loop should poll head and attempt a catch-up this tick: always on the
    /// poll path, and on the WS path only while reconciling a gap or confirming a pushed
    /// log. Idle WS ticks return `false` → no head RPC, just wait on the stream.
    pub(crate) fn should_poll_head(&self) -> bool {
        !self.ws_enabled || self.reconcile || !self.pending.is_empty()
    }

    /// Consume the reorg signal: returns and clears the lowest `removed` height seen.
    pub(crate) fn take_reorg_signal(&mut self) -> Option<u64> {
        self.reorg_at.take()
    }

    /// A pending log confirms within ~confirmations blocks, so recheck soon instead of
    /// idling the full interval; an idle chain (no pending) keeps the full interval.
    pub(crate) fn wait(&self, interval: u64) -> Duration {
        if self.pending.is_empty() {
            Duration::from_secs(interval)
        } else {
            Duration::from_secs(interval).min(Duration::from_secs(PENDING_RECHECK_SECS))
        }
    }

    /// Fold a WS stream event: a `removed` log raises the reorg signal; a normal log
    /// marks its block pending; an error/end drops the stream so the next loop
    /// resubscribes.
    pub(crate) fn on_ws_event(&mut self, rec: Option<Result<TipLog, DomainError>>, chain: ChainId) {
        match rec {
            Some(Ok(t)) if t.removed => {
                metrics::counter!("ws_removed_logs_total", "chain_id" => chain.to_string())
                    .increment(1);
                let h = t.record.height.get();
                self.reorg_at = Some(self.reorg_at.map_or(h, |cur| cur.min(h)));
            }
            Some(Ok(t)) => {
                metrics::counter!("ws_logs_received_total", "chain_id" => chain.to_string())
                    .increment(1);
                self.pending.insert(t.record.height.get());
            }
            Some(Err(e)) => {
                tracing::warn!(chain = chain.get(), error = %e, "ws stream error; will resubscribe");
                self.sub = None;
            }
            None => {
                tracing::warn!(chain = chain.get(), "ws stream ended; will resubscribe");
                self.sub = None;
            }
        }
    }
}
