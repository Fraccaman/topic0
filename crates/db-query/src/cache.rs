//! In-process TTL cache over an `EventQueryRepository`. A decorator: it implements the
//! same read port, memoizing `query`/`count` results keyed by the `QuerySpec`, and
//! delegates misses to the inner repo. Entries expire after a fixed TTL — event tables
//! are append-only (+ reorg deletes), so a short TTL bounds staleness without any
//! invalidation coupling to the write side. Errors are never cached.

use async_trait::async_trait;
use moka::future::Cache;
use shared::{DomainError, Page, QuerySpec};
use std::sync::Arc;
use std::time::Duration;

const MAX_ENTRIES: u64 = 10_000;

pub struct CachingEventQueryRepository {
    inner: Arc<dyn domain::ports::repository::EventQueryRepository>,
    queries: Cache<String, Page<String>>,
    counts: Cache<String, u64>,
}

impl CachingEventQueryRepository {
    /// Wrap `inner`; cached entries live for `ttl`.
    pub fn new(
        inner: Arc<dyn domain::ports::repository::EventQueryRepository>,
        ttl: Duration,
    ) -> Self {
        Self {
            inner,
            queries: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(MAX_ENTRIES)
                .build(),
            counts: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(MAX_ENTRIES)
                .build(),
        }
    }
}

/// Full spec → key: pagination, joins, and order all change the payload.
fn query_key(spec: &QuerySpec) -> String {
    serde_json::to_string(spec).unwrap_or_default()
}

/// `count` depends only on the table + filters (no pagination/joins/order), so key on
/// just those — different pages of the same filter share one count entry.
fn count_key(spec: &QuerySpec) -> String {
    serde_json::to_string(&(&spec.table, &spec.filters)).unwrap_or_default()
}

#[async_trait]
impl domain::ports::repository::EventQueryRepository for CachingEventQueryRepository {
    async fn query(&self, spec: &QuerySpec) -> Result<Page<String>, DomainError> {
        let key = query_key(spec);
        if let Some(page) = self.queries.get(&key).await {
            return Ok(page);
        }
        let page = self.inner.query(spec).await?;
        self.queries.insert(key, page.clone()).await;
        Ok(page)
    }

    async fn count(&self, spec: &QuerySpec) -> Result<u64, DomainError> {
        let key = count_key(spec);
        if let Some(n) = self.counts.get(&key).await {
            return Ok(n);
        }
        let n = self.inner.count(spec).await?;
        self.counts.insert(key, n).await;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::ports::repository::EventQueryRepository;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts how many times the inner repo is actually hit.
    #[derive(Default)]
    struct CountingInner {
        queries: AtomicUsize,
        counts: AtomicUsize,
    }

    #[async_trait]
    impl EventQueryRepository for CountingInner {
        async fn query(&self, _: &QuerySpec) -> Result<Page<String>, DomainError> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            Ok(Page {
                items: vec!["{}".into()],
                end_cursor: None,
                has_next: false,
            })
        }
        async fn count(&self, _: &QuerySpec) -> Result<u64, DomainError> {
            self.counts.fetch_add(1, Ordering::SeqCst);
            Ok(7)
        }
    }

    fn spec(table: &str, first: u32) -> QuerySpec {
        QuerySpec {
            table: table.into(),
            first,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn identical_query_hits_cache() {
        let inner = Arc::new(CountingInner::default());
        let cache = CachingEventQueryRepository::new(inner.clone(), Duration::from_secs(60));
        let s = spec("evt_x", 10);

        cache.query(&s).await.unwrap();
        cache.query(&s).await.unwrap();
        // Second call served from cache → inner hit exactly once.
        assert_eq!(inner.queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn distinct_specs_miss_separately() {
        let inner = Arc::new(CountingInner::default());
        let cache = CachingEventQueryRepository::new(inner.clone(), Duration::from_secs(60));

        cache.query(&spec("evt_x", 10)).await.unwrap();
        cache.query(&spec("evt_x", 20)).await.unwrap(); // different `first`
        cache.query(&spec("evt_y", 10)).await.unwrap(); // different table
        assert_eq!(inner.queries.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn count_caches_across_pages() {
        let inner = Arc::new(CountingInner::default());
        let cache = CachingEventQueryRepository::new(inner.clone(), Duration::from_secs(60));

        // Same table + filters, different pagination → one underlying count.
        assert_eq!(cache.count(&spec("evt_x", 10)).await.unwrap(), 7);
        assert_eq!(cache.count(&spec("evt_x", 50)).await.unwrap(), 7);
        assert_eq!(inner.counts.load(Ordering::SeqCst), 1);
    }
}
