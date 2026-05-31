use async_trait::async_trait;
use shared::{DomainError, Lease, WorkItem};

/// Work queue port (Postgres `work_queue` table behind it).
#[async_trait]
pub trait QueueRepository: Send + Sync {
    /// Enqueue an item. Runs inside a `UnitOfWork` tx with the raw write.
    async fn push(&self, item: &WorkItem) -> Result<(), DomainError>;

    /// Lease the next ready item across all chains via `FOR UPDATE SKIP LOCKED`.
    async fn pull_any(&self) -> Result<Option<Lease<WorkItem>>, DomainError>;

    /// Ack (delete) a completed lease. `lease_seq` fences a stale ack (re-leased → no-op).
    async fn ack(&self, lease_id: i64, lease_seq: i64) -> Result<(), DomainError>;

    /// Reclaim leases whose visibility timeout elapsed (crash recovery).
    async fn reclaim_expired(&self, older_than_secs: u64) -> Result<u64, DomainError>;

    /// Number of un-acked items (backpressure / lag metric).
    async fn depth(&self) -> Result<u64, DomainError>;
}
