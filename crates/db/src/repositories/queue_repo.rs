use crate::error::DbError;
use async_trait::async_trait;
use domain::ports::queue_repo::QueueRepository;
use shared::{ChainId, DomainError, Epoch, Height, Lease, WorkItem, WorkKind};
use sqlx::{PgPool, Row};

pub struct PgQueueRepository {
    pool: PgPool,
}

impl PgQueueRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn kind_str(k: WorkKind) -> &'static str {
    match k {
        WorkKind::Backfill => "backfill",
        WorkKind::Tip => "tip",
        WorkKind::Reorg => "reorg",
    }
}

fn parse_kind(s: &str) -> WorkKind {
    match s {
        "tip" => WorkKind::Tip,
        "reorg" => WorkKind::Reorg,
        _ => WorkKind::Backfill,
    }
}

#[async_trait]
impl QueueRepository for PgQueueRepository {
    async fn push(&self, item: &WorkItem) -> Result<(), DomainError> {
        let mut tx = self.pool.begin().await.map_err(DbError::from)?;
        push_in_tx(&mut tx, item).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(())
    }

    async fn pull_any(&self) -> Result<Option<Lease<WorkItem>>, DomainError> {
        // Lease the oldest ready item whose chain has nothing in flight: serial
        // within a chain (a Reorg item drains before the later redecode of the same
        // range), parallel across chains. FOR UPDATE SKIP LOCKED = competing
        // consumers. Decode is idempotent (upserts on PK); `lease_seq` bumped so a
        // stale ack after reclaim no-ops.
        let start = std::time::Instant::now();
        let row = sqlx::query(
            "UPDATE work_queue SET locked_at = now(), lease_seq = lease_seq + 1 WHERE id = ( \
                SELECT id FROM work_queue \
                WHERE locked_at IS NULL \
                  AND chain_id NOT IN (SELECT chain_id FROM work_queue WHERE locked_at IS NOT NULL) \
                ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1 \
             ) RETURNING id, lease_seq, chain_id, from_height, to_height, kind, epoch",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::from)?;
        metrics::histogram!("queue_pull_duration_seconds").record(start.elapsed().as_secs_f64());

        Ok(row.map(|r| {
            let id: i64 = r.get(0);
            let lease_seq: i64 = r.get(1);
            let chain: i64 = r.get(2);
            let from: i64 = r.get(3);
            let to: i64 = r.get(4);
            let kind: String = r.get(5);
            let epoch: i64 = r.get(6);
            Lease {
                id,
                lease_seq,
                item: WorkItem {
                    chain_id: ChainId(chain as u64),
                    from: Height(from as u64),
                    to: Height(to as u64),
                    kind: parse_kind(&kind),
                    epoch: Epoch(epoch as u64),
                },
            }
        }))
    }

    async fn depth(&self) -> Result<u64, DomainError> {
        let row = sqlx::query("SELECT count(*) FROM work_queue")
            .fetch_one(&self.pool)
            .await
            .map_err(DbError::from)?;
        let n: i64 = row.get(0);
        metrics::gauge!("queue_depth").set(n as f64);
        Ok(n as u64)
    }

    async fn ack(&self, lease_id: i64, lease_seq: i64) -> Result<(), DomainError> {
        // Only delete if this lease is still current (reclaim bumps lease_seq).
        sqlx::query("DELETE FROM work_queue WHERE id = $1 AND lease_seq = $2")
            .bind(lease_id)
            .bind(lease_seq)
            .execute(&self.pool)
            .await
            .map_err(DbError::from)?;
        metrics::counter!("queue_acks_total").increment(1);
        Ok(())
    }

    async fn reclaim_expired(&self, older_than_secs: u64) -> Result<u64, DomainError> {
        // Bump lease_seq so the crashed worker's eventual ack (old seq) no-ops.
        let r = sqlx::query(
            "UPDATE work_queue SET locked_at = NULL, lease_seq = lease_seq + 1 \
             WHERE locked_at IS NOT NULL AND locked_at < now() - make_interval(secs => $1)",
        )
        .bind(older_than_secs as f64)
        .execute(&self.pool)
        .await
        .map_err(DbError::from)?;
        if r.rows_affected() > 0 {
            metrics::counter!("queue_leases_reclaimed_total").increment(r.rows_affected());
        }
        Ok(r.rows_affected())
    }
}

pub(crate) async fn push_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    item: &WorkItem,
) -> Result<(), DbError> {
    sqlx::query(
        "INSERT INTO work_queue (chain_id, from_height, to_height, kind, epoch) \
         VALUES ($1,$2,$3,$4,$5) \
         ON CONFLICT (chain_id, from_height, to_height, kind) DO NOTHING",
    )
    .bind(item.chain_id.get() as i64)
    .bind(item.from.get() as i64)
    .bind(item.to.get() as i64)
    .bind(kind_str(item.kind))
    .bind(item.epoch.0 as i64)
    .execute(&mut **tx)
    .await?;
    metrics::counter!("queue_enqueued_total", "kind" => kind_str(item.kind)).increment(1);
    Ok(())
}
