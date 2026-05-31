//! Range planning ([`plan_ranges`]/[`fetch_target`]), contiguous-prefix cursor tracking
//! ([`PrefixTracker`]), and the streaming fetch→write pipeline ([`run_pipeline`]) that
//! ties them together. Kept separate from the loop orchestration so the pure pieces are
//! unit-tested in isolation.

use anyhow::Result;
use domain::ports::repository::CursorRepository;
use futures::stream::{self, StreamExt};
use shared::{ChainId, Hash, Height, RecordFilter};
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

/// Upper bound for one fetch iteration. The no-fetch WS path jumps to the whole safe
/// tip (zero RPC); a real fetch is bounded to one pipeline's worth of ranges so the
/// cursor checkpoints incrementally (a restart mid-backfill resumes near where it
/// stopped instead of re-scanning from `start_block`).
pub(crate) fn fetch_target(
    safe: u64,
    next: u64,
    step: u64,
    range_concurrency: usize,
    must_fetch: bool,
) -> u64 {
    if must_fetch {
        safe.min(next + step * range_concurrency as u64 - 1)
    } else {
        safe
    }
}

/// Chunk `[next..=target]` into provider-sized `[lo, hi]` ranges.
pub(crate) fn plan_ranges(next: u64, target: u64, step: u64) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut s = next;
    while s <= target {
        let e = (s + step - 1).min(target);
        ranges.push((s, e));
        s = e + 1;
    }
    ranges
}

/// Tracks committed ranges and yields the gap-free prefix the cursor may advance to.
///
/// Concurrent fetches commit out of order, but the cursor must only ever move over a
/// contiguous run from the starting block — that way a mid-run failure leaves a gap-free
/// checkpoint and the next loop re-fetches exactly the missing tail. Each committed
/// range is held until the ranges before it arrive and close the gap.
struct PrefixTracker {
    /// Start of the next range that would extend the contiguous prefix.
    frontier: u64,
    /// Committed ranges waiting on an earlier gap, keyed by start → (end, last_block).
    waiting: BTreeMap<u64, (u64, Option<(Height, Hash)>)>,
    /// Highest matched block (height, hash) within the prefix so far. The cursor stamps
    /// this hash so reorg detection has a real block hash, never one above the cursor.
    best: Option<(u64, Hash)>,
    /// Highest block the prefix has reached (the last height written to the cursor).
    advanced_to: Option<u64>,
}

impl PrefixTracker {
    fn new(start: u64) -> Self {
        Self {
            frontier: start,
            waiting: BTreeMap::new(),
            best: None,
            advanced_to: None,
        }
    }

    /// Record a committed range `[from, to]`. Returns the new cursor checkpoint
    /// `(height, hash)` when the contiguous prefix grew, or `None` when the range landed
    /// ahead of a still-open gap.
    fn record(
        &mut self,
        from: u64,
        to: u64,
        last_block: Option<(Height, Hash)>,
    ) -> Option<(Height, Hash)> {
        self.waiting.insert(from, (to, last_block));
        let mut moved = false;
        while let Some((end, last_block)) = self.waiting.remove(&self.frontier) {
            if let Some((height, hash)) = last_block {
                let h = height.get();
                if self.best.as_ref().is_none_or(|(best_h, _)| h >= *best_h) {
                    self.best = Some((h, hash));
                }
            }
            self.frontier = end + 1;
            self.advanced_to = Some(end);
            moved = true;
        }
        moved.then(|| (Height(self.advanced_to.unwrap()), self.best_hash()))
    }

    /// Hash to stamp on the cursor: the highest matched block's, or empty if the prefix
    /// matched nothing.
    fn best_hash(&self) -> Hash {
        self.best
            .as_ref()
            .map(|(_, hash)| hash.clone())
            .unwrap_or(Hash(Vec::new()))
    }
}

/// Result of one `run_pipeline` drive.
pub(crate) struct PipelineOutcome {
    /// Highest block the cursor was advanced to (the contiguous prefix), or `None` if
    /// nothing committed. The caller resumes at `advanced_to + 1`.
    pub advanced_to: Option<u64>,
    pub records: usize,
    /// Set when shutdown was requested mid-drive (in-flight ranges drop cleanly).
    pub shutdown: bool,
}

/// Stream `[next..=target]` through a decoupled fetch→write pipeline: up to
/// `range_concurrency` `fetch_range` calls run concurrently (pure RPC), feeding a
/// bounded channel drained by a single writer that `commit_range`s each batch and
/// advances the cursor over the contiguous prefix (see [`PrefixTracker`]). RPC never
/// blocks on a DB commit, and commits overlap fetches.
///
/// A mid-drive error or shutdown leaves the cursor at a gap-free checkpoint; the next
/// loop re-fetches the remainder idempotently (ON CONFLICT).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_pipeline(
    ingest: &services::IngestionService,
    cursors: &db::PgCursorRepository,
    chain: ChainId,
    filter: &RecordFilter,
    next: u64,
    target: u64,
    step: u64,
    range_concurrency: usize,
    ledger: &pricing::SpendLedger,
    token: &CancellationToken,
) -> Result<PipelineOutcome> {
    let conc = range_concurrency.max(1);
    let ranges = plan_ranges(next, target, step);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<services::FetchedRange>(conc * 2);

    let producer = fetch_producer(ingest, filter, ranges, conc, ledger, token, tx);
    let consumer = commit_consumer(ingest, cursors, chain, next, &mut rx);

    let (shutdown, (advanced_to, records)) = tokio::try_join!(producer, consumer)?;
    Ok(PipelineOutcome {
        advanced_to,
        records,
        shutdown,
    })
}

/// Producer half: run the planned ranges through `fetch_range` concurrently (pure RPC)
/// and push each result to the writer. Stops feeding on shutdown or a hit free-quota
/// ceiling — returns `true` then — so the writer drains what is already in flight rather
/// than tearing a commit. A fetch error aborts the whole drive via `?`.
async fn fetch_producer(
    ingest: &services::IngestionService,
    filter: &RecordFilter,
    ranges: Vec<(u64, u64)>,
    concurrency: usize,
    ledger: &pricing::SpendLedger,
    token: &CancellationToken,
    tx: tokio::sync::mpsc::Sender<services::FetchedRange>,
) -> Result<bool> {
    let mut fetches = stream::iter(ranges)
        .map(|(lo, hi)| ingest.fetch_range(filter, Height(lo), Height(hi)))
        .buffer_unordered(concurrency);
    let mut cancelled = false;
    while let Some(res) = fetches.next().await {
        if token.is_cancelled() || ledger.quota_exhausted() {
            cancelled = true;
            break;
        }
        if tx.send(res?).await.is_err() {
            break; // writer gone
        }
    }
    drop(tx); // close the channel so the consumer's recv loop ends
    Ok(cancelled)
}

/// Consumer half: commit each fetched range and advance the cursor over the contiguous
/// prefix. Returns `(highest committed prefix block, total records)`.
async fn commit_consumer(
    ingest: &services::IngestionService,
    cursors: &db::PgCursorRepository,
    chain: ChainId,
    next: u64,
    rx: &mut tokio::sync::mpsc::Receiver<services::FetchedRange>,
) -> Result<(Option<u64>, usize)> {
    let mut prefix = PrefixTracker::new(next);
    let mut records = 0usize;
    while let Some(fetched) = rx.recv().await {
        let (from, to, last_block) = (
            fetched.from.get(),
            fetched.to.get(),
            fetched.last_block.clone(),
        );
        records += ingest
            .commit_range(fetched, shared::Epoch(0))
            .await?
            .records;
        if let Some((height, hash)) = prefix.record(from, to, last_block) {
            cursors.advance(chain, height, hash).await?;
            tracing::debug!(
                chain = chain.get(),
                cursor = height.get(),
                "prefix advanced"
            );
        }
    }
    Ok((prefix.advanced_to, records))
}

#[cfg(test)]
mod tests {
    use super::{fetch_target, plan_ranges, PrefixTracker};
    use shared::{Hash, Height};

    fn block(h: u64) -> Option<(Height, Hash)> {
        Some((Height(h), Hash(vec![h as u8; 32])))
    }

    #[test]
    fn prefix_advances_each_contiguous_range() {
        let mut p = PrefixTracker::new(1);
        assert_eq!(
            p.record(1, 10, block(10)),
            Some((Height(10), Hash(vec![10; 32])))
        );
        assert_eq!(
            p.record(11, 20, block(20)),
            Some((Height(20), Hash(vec![20; 32])))
        );
        assert_eq!(p.advanced_to, Some(20));
    }

    #[test]
    fn prefix_holds_out_of_order_range_until_gap_closes() {
        let mut p = PrefixTracker::new(1);
        // Range arrives ahead of an open gap → no advance yet.
        assert_eq!(p.record(11, 20, block(20)), None);
        assert_eq!(p.advanced_to, None);
        // The missing first range closes the gap → cursor jumps over both at once.
        let advance = p.record(1, 10, block(10));
        assert_eq!(advance, Some((Height(20), Hash(vec![20; 32]))));
        assert_eq!(p.advanced_to, Some(20));
    }

    #[test]
    fn prefix_advances_over_empty_range_with_empty_hash() {
        let mut p = PrefixTracker::new(1);
        assert_eq!(p.record(1, 10, None), Some((Height(10), Hash(Vec::new()))));
    }

    #[test]
    fn prefix_hash_is_highest_matched_block() {
        let mut p = PrefixTracker::new(1);
        // Second range matched a higher block; cursor hash tracks the highest.
        p.record(1, 10, block(7));
        let advance = p.record(11, 20, block(15));
        assert_eq!(advance, Some((Height(20), Hash(vec![15; 32]))));
        // A later empty range doesn't lower the stamped hash.
        let advance = p.record(21, 30, None);
        assert_eq!(advance, Some((Height(30), Hash(vec![15; 32]))));
    }

    #[test]
    fn plan_ranges_chunks_inclusive() {
        assert_eq!(plan_ranges(1, 1, 10), vec![(1, 1)]);
        assert_eq!(plan_ranges(1, 10, 10), vec![(1, 10)]);
        assert_eq!(plan_ranges(1, 25, 10), vec![(1, 10), (11, 20), (21, 25)]);
        assert!(plan_ranges(10, 5, 10).is_empty()); // next > target
    }

    #[test]
    fn fetch_target_bounds_pipeline_then_jumps() {
        // must_fetch: bounded to next + step*concurrency - 1, capped at safe.
        assert_eq!(fetch_target(1000, 1, 10, 4, true), 40);
        assert_eq!(fetch_target(20, 1, 10, 4, true), 20); // capped at safe
                                                          // no-fetch WS path jumps straight to safe.
        assert_eq!(fetch_target(1000, 1, 10, 4, false), 1000);
    }
}
