//! Fixed support tables. Typed event/aux tables are created by `migrator`.

use crate::error::DbError;

const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS blocks (
    chain_id     bigint NOT NULL,
    height       bigint NOT NULL,
    hash         bytea  NOT NULL,
    parent_hash  bytea  NOT NULL,
    time         bigint NOT NULL,
    PRIMARY KEY (chain_id, height)
);

CREATE TABLE IF NOT EXISTS cursors (
    chain_id    bigint NOT NULL PRIMARY KEY,
    last_height bigint NOT NULL,
    last_hash   bytea  NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_records (
    chain_id     bigint NOT NULL,
    height       bigint NOT NULL,
    idx          bigint NOT NULL,
    block_hash   bytea  NOT NULL,
    address      bytea  NOT NULL,
    selectors    bytea  NOT NULL,
    data         bytea  NOT NULL,
    tx_id        bytea  NOT NULL,
    tx_index     bigint NOT NULL,
    inner_index  bigint,
    PRIMARY KEY (chain_id, height, idx)
);
CREATE INDEX IF NOT EXISTS raw_records_tx ON raw_records (chain_id, height, tx_index);

CREATE TABLE IF NOT EXISTS work_queue (
    id          bigserial PRIMARY KEY,
    chain_id    bigint NOT NULL,
    from_height bigint NOT NULL,
    to_height   bigint NOT NULL,
    kind        text   NOT NULL,
    epoch       bigint NOT NULL,
    locked_at   timestamptz,
    lease_seq   bigint NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now()
);
-- pull_any leases the oldest ready item (ORDER BY id) whose chain has nothing in
-- flight: one partial index serves the ordered ready scan, another the in-flight
-- NOT IN sub-scan. (Old composite (chain_id, id) didn't serve the unqualified id sort.)
DROP INDEX IF EXISTS work_queue_ready;
CREATE INDEX IF NOT EXISTS work_queue_ready_id ON work_queue (id) WHERE locked_at IS NULL;
CREATE INDEX IF NOT EXISTS work_queue_inflight ON work_queue (chain_id) WHERE locked_at IS NOT NULL;
-- Dedup identical pending items: a window re-fetched after a transient error must
-- not enqueue the same decode work twice. (Rows are deleted on ack, so an identical
-- range can be re-enqueued once the prior one drains.)
CREATE UNIQUE INDEX IF NOT EXISTS work_queue_item_uniq ON work_queue (chain_id, from_height, to_height, kind);
"#;

/// Create the fixed support tables (idempotent).
pub async fn init(pool: &sqlx::PgPool) -> Result<(), DbError> {
    sqlx::raw_sql(DDL).execute(pool).await?;
    Ok(())
}
