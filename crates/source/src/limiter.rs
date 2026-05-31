//! Token-bucket rate limiter seeded from a `PlanProfile`: a request-rate bucket
//! (`max_rps`) plus an optional compute-unit bucket (`max_cu_per_sec`). The CU bucket
//! is what keeps CU/credit-metered providers (Alchemy, QuickNode) under their
//! per-second compute cap — without it a burst of cheap-by-count but CU-heavy calls
//! trips provider 429s.

use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use shared::PlanProfile;
use std::num::NonZeroU32;

type Bucket = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub struct Limiter {
    rps: Bucket,
    /// CU bucket + its burst capacity, when the plan declares `max_cu_per_sec`.
    cu: Option<(Bucket, u32)>,
}

impl Limiter {
    pub fn from_plan(plan: &PlanProfile) -> Self {
        let rps = NonZeroU32::new(plan.max_rps.max(1)).unwrap();
        let cu = plan
            .max_cu_per_sec
            .and_then(NonZeroU32::new)
            .map(|n| (RateLimiter::direct(Quota::per_second(n)), n.get()));
        Self {
            rps: RateLimiter::direct(Quota::per_second(rps)),
            cu,
        }
    }

    /// Await one request permit (rps-gated).
    pub async fn acquire(&self) {
        let start = std::time::Instant::now();
        self.rps.until_ready().await;
        record_wait("rps", start.elapsed());
    }

    /// Await `units` compute-unit permits (no-op when the plan has no CU cap). A
    /// single call's units can exceed the per-second burst (e.g. a 100-block batch),
    /// which `until_n_ready` rejects, so drain in capacity-sized sub-bursts.
    pub async fn acquire_cu(&self, units: u32) {
        let Some((bucket, cap)) = &self.cu else {
            return;
        };
        let start = std::time::Instant::now();
        let mut remaining = units.max(1);
        while remaining > 0 {
            let take = remaining.min(*cap);
            if let Some(nz) = NonZeroU32::new(take) {
                // `take <= cap`, so this never returns InsufficientCapacity.
                let _ = bucket.until_n_ready(nz).await;
            }
            remaining -= take;
        }
        record_wait("cu", start.elapsed());
    }
}

/// Record token-bucket wait time; count a throttle when the wait was non-trivial
/// (the limiter actually blocked rather than handing out a ready permit).
fn record_wait(bucket: &'static str, waited: std::time::Duration) {
    metrics::histogram!("rate_limiter_wait_seconds", "bucket" => bucket)
        .record(waited.as_secs_f64());
    if waited.as_millis() >= 1 {
        metrics::counter!("rate_limiter_throttled_total", "bucket" => bucket).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::Limiter;
    use shared::PlanProfile;
    use std::time::Duration;

    fn plan(cu: Option<u32>) -> PlanProfile {
        PlanProfile {
            max_rps: 1000,
            max_cu_per_sec: cu,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn acquire_cu_is_noop_without_cap() {
        let l = Limiter::from_plan(&plan(None));
        tokio::time::timeout(Duration::from_millis(100), l.acquire_cu(1_000_000))
            .await
            .expect("acquire_cu must not block when the plan has no CU cap");
    }

    #[tokio::test]
    async fn acquire_cu_splits_over_burst_request() {
        // A request above the per-second burst capacity must split into capacity-sized
        // sub-bursts and still complete — never InsufficientCapacity, never a deadlock.
        let l = Limiter::from_plan(&plan(Some(10_000)));
        tokio::time::timeout(Duration::from_secs(5), l.acquire_cu(10_050))
            .await
            .expect("over-burst acquire_cu must complete");
    }
}
