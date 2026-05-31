//! Transport resilience for the RPC client: jittered retry, result-cap error
//! classification, and the adaptive-range split. All pure / self-free.

use crate::error::SourceError;
use crate::metric::error_reason;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

/// Decorrelated jitter so concurrent retries (one per in-flight range) don't retry in
/// lockstep and re-trip the provider. Cheap LCG/SplitMix — no `rand` dependency.
fn jitter_below(max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    let x = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z % max
}

/// Retry a transport call on transient errors (429 / 5xx) with jittered backoff.
/// `op` must acquire the limiter itself so each retry is also gated. `method` labels
/// the retry/failure metrics.
pub(crate) async fn with_retry<T, F, Fut>(method: &'static str, mut op: F) -> Result<T, SourceError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let mut delay_ms = 250u64;
    for attempt in 0..6 {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let el = e.to_ascii_lowercase();
                let transient = el.contains("429")
                    || el.contains("limit")
                    || el.contains("too many requests")
                    || el.contains("timeout")
                    || el.contains("timed out")
                    || el.contains("503")
                    || el.contains("502")
                    || el.contains("connection");
                let reason = error_reason(&e);
                if !transient || attempt == 5 {
                    metrics::counter!("rpc_failures_total", "method" => method, "reason" => reason)
                        .increment(1);
                    return Err(SourceError::Transport(e));
                }
                metrics::counter!("rpc_retries_total", "method" => method, "reason" => reason)
                    .increment(1);
                // Sleep in [delay/2, delay] to spread herd retries.
                let half = delay_ms / 2;
                tokio::time::sleep(Duration::from_millis(half + jitter_below(half + 1))).await;
                delay_ms = (delay_ms * 2).min(4000);
            }
        }
    }
    unreachable!()
}

/// True if a `get_logs` error signals the range/result cap was exceeded (provider
/// wording varies: Alchemy "query returned more than N results", QuickNode/others
/// "block range is too large" / "response size exceeded"). Such an error is a signal
/// to split the range, not to give up.
pub(crate) fn is_range_cap_error(e: &str) -> bool {
    let e = e.to_ascii_lowercase();
    e.contains("more than")
        || e.contains("too large")
        || e.contains("response size")
        || e.contains("range is too")
        || e.contains("query timeout")
}

/// Halve an inclusive `[lo, hi]` (caller ensures `lo < hi`) into contiguous left/right
/// sub-ranges. Pushed right-then-left onto a LIFO stack so the lower range is fetched
/// first.
pub(crate) fn split_range(lo: u64, hi: u64) -> ((u64, u64), (u64, u64)) {
    let mid = lo + (hi - lo) / 2;
    ((lo, mid), (mid + 1, hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_cap_errors_classified() {
        assert!(is_range_cap_error("query returned more than 10000 results"));
        assert!(is_range_cap_error("block range is too large"));
        assert!(is_range_cap_error("Log response size exceeded the limit"));
        // Unrelated transport errors are not a split signal.
        assert!(!is_range_cap_error("connection reset"));
        assert!(!is_range_cap_error("429 too many requests"));
    }

    #[test]
    fn split_range_halves_contiguously() {
        assert_eq!(split_range(0, 1), ((0, 0), (1, 1)));
        let (l, r) = split_range(0, 2000);
        assert_eq!(l, (0, 1000));
        assert_eq!(r, (1001, 2000));
        assert_eq!(l.1 + 1, r.0); // no gap, no overlap
    }
}
