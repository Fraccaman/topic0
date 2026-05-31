//! Read raw records, decode via a chain `Decoder`, upsert rows.
//! `decode_range` is the inline path (resync) and the body of `DecodeWorker` (queue).

use domain::ports::decoder::Decoder;
use domain::ports::repository::{BlockRepository, EventRepository, RawRecordRepository};
use shared::{ChainId, DomainError, EventRow, Hash, Height};
use std::collections::{HashMap, HashSet};

/// Decode every raw record in `[from, to]` and upsert into typed tables.
/// Re-runnable from the raw store (no RPC).
pub async fn decode_range(
    decoder: &dyn Decoder,
    raw_repo: &dyn RawRecordRepository,
    blocks: &dyn BlockRepository,
    events: &dyn EventRepository,
    chain: ChainId,
    from: Height,
    to: Height,
) -> Result<usize, DomainError> {
    let raw = raw_repo.range(chain, from, to).await?;
    let times: HashMap<u64, i64> = blocks
        .times(chain, from, to)
        .await?
        .into_iter()
        .map(|(h, t)| (h.get(), t))
        .collect();

    // Stamp the block time and bucket a decoded row by its target table.
    fn collect(
        by_table: &mut HashMap<String, Vec<EventRow>>,
        times: &HashMap<u64, i64>,
        mut row: EventRow,
    ) {
        row.block_time = times.get(&row.height.get()).copied();
        by_table
            .entry(row.event.table.clone())
            .or_default()
            .push(row);
    }

    let mut by_table: HashMap<String, Vec<EventRow>> = HashMap::new();

    // Events from raw logs.
    for rec in &raw {
        if let Some(row) = decoder.decode(rec)? {
            collect(&mut by_table, &times, row);
        }
    }

    // Calldata of the distinct txs behind matched logs, fetched by tx_id.
    if decoder.has_calls() {
        let tx_ids: Vec<Hash> = raw
            .iter()
            .map(|r| r.tx_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        for tx in &blocks.calldata(chain, &tx_ids).await? {
            if let Some(row) = decoder.decode_call(tx)? {
                collect(&mut by_table, &times, row);
            }
        }
    }

    let mut written = 0;
    for (table, rows) in by_table {
        let n = events.upsert_batch(&table, &rows).await? as usize;
        metrics::counter!("decode_rows_total", "chain_id" => chain.to_string(), "table" => table)
            .increment(n as u64);
        written += n;
    }
    Ok(written)
}
