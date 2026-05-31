//! EVM aux tables (transactions / receipts): their schemas (declared by the EVM
//! decoder) and the row builders `fetch_aux` populates. Schema column names and the
//! row field names live together here so they can't drift apart.

use crate::error::SourceError;
use alloy::consensus::Transaction as ConsensusTx;
use alloy::network::TransactionResponse;
use alloy::rpc::types::{Block, TransactionReceipt};
use domain::ports::chain_source::AuxData;
use schema::{ColumnDef, ColumnType, EventSchema};
use shared::{
    BlockMeta, ChainId, DecodedEvent, DomainError, EventRow, EventValue, Hash, Height, RecordIndex,
};
use std::collections::{BTreeMap, BTreeSet};

fn col(name: &str, ty: ColumnType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        indexed_hash: false,
    }
}

fn aux_pk() -> Vec<String> {
    vec!["chain_id".into(), "tx_id".into()]
}

/// The two EVM aux tables. Fields are nullable; absent ones omitted from a row.
pub fn aux_schemas() -> Vec<EventSchema> {
    vec![
        EventSchema {
            table: "transactions".into(),
            event: "Transaction".into(),
            topic0: None,
            columns: vec![
                col("from_addr", ColumnType::Address),
                col("to_addr", ColumnType::Address),
                col("value", ColumnType::UInt(256)),
                col("input", ColumnType::Bytes),
                col("gas", ColumnType::UInt(256)),
                col("gas_price", ColumnType::UInt(256)),
                col("nonce", ColumnType::UInt(256)),
            ],
            indexed_positions: vec![],
            pk_columns: aux_pk(),
        },
        EventSchema {
            table: "receipts".into(),
            event: "Receipt".into(),
            topic0: None,
            columns: vec![
                col("status", ColumnType::Bool),
                col("gas_used", ColumnType::UInt(256)),
                col("effective_gas_price", ColumnType::UInt(256)),
                col("contract_address", ColumnType::Address),
            ],
            indexed_positions: vec![],
            pk_columns: aux_pk(),
        },
    ]
}

/// Build an aux `EventRow` (no block_time — aux rows key on tx_id). `idx` is the
/// transaction's block position for `transactions`; ignored for `receipts` (no idx
/// column).
fn row(
    chain_id: ChainId,
    table: &str,
    height: Height,
    block_hash: Hash,
    tx_id: Hash,
    idx: u64,
    fields: Vec<(String, EventValue)>,
) -> EventRow {
    EventRow {
        chain_id,
        height,
        block_hash,
        block_time: None,
        tx_id,
        index: RecordIndex(idx),
        event: DecodedEvent {
            table: table.into(),
            fields,
        },
    }
}

/// One matched transaction's enrichment values.
pub struct TxFields {
    pub from: Vec<u8>,
    pub tx_index: u64,
    pub value: String,
    pub input: Vec<u8>,
    pub gas: String,
    pub nonce: String,
    pub to: Option<Vec<u8>>,
    pub gas_price: Option<String>,
}

/// Build a `transactions` row. Field names mirror the schema above.
pub fn transaction_row(
    chain_id: ChainId,
    height: Height,
    block_hash: Hash,
    tx_id: Hash,
    f: TxFields,
) -> EventRow {
    let mut fields = vec![
        ("from_addr".into(), EventValue::Address(f.from)),
        ("value".into(), EventValue::Uint(f.value)),
        ("input".into(), EventValue::Bytes(f.input)),
        ("gas".into(), EventValue::Uint(f.gas)),
        ("nonce".into(), EventValue::Uint(f.nonce)),
    ];
    if let Some(to) = f.to {
        fields.push(("to_addr".into(), EventValue::Address(to)));
    }
    if let Some(gp) = f.gas_price {
        fields.push(("gas_price".into(), EventValue::Uint(gp)));
    }
    // idx = transaction_index → block inclusion order (sort/keyset key).
    row(
        chain_id,
        "transactions",
        height,
        block_hash,
        tx_id,
        f.tx_index,
        fields,
    )
}

/// One matched transaction's receipt enrichment values.
pub struct ReceiptFields {
    pub status: bool,
    pub gas_used: String,
    pub effective_gas_price: String,
    pub contract_address: Option<Vec<u8>>,
}

/// Build a `receipts` row. Field names mirror the schema above.
pub fn receipt_row(
    chain_id: ChainId,
    height: Height,
    block_hash: Hash,
    tx_id: Hash,
    f: ReceiptFields,
) -> EventRow {
    let mut fields = vec![
        ("status".into(), EventValue::Bool(f.status)),
        ("gas_used".into(), EventValue::Uint(f.gas_used)),
        (
            "effective_gas_price".into(),
            EventValue::Uint(f.effective_gas_price),
        ),
    ];
    if let Some(ca) = f.contract_address {
        fields.push(("contract_address".into(), EventValue::Address(ca)));
    }
    // Receipts have no own idx (FK to transactions); 0 is unused on write.
    row(chain_id, "receipts", height, block_hash, tx_id, 0, fields)
}

/// Assemble block metas + matched-tx rows from fetched full blocks into `aux`.
pub fn block_aux(
    chain_id: ChainId,
    blocks: Vec<(u64, Option<Block>)>,
    tx_ids: &BTreeSet<Vec<u8>>,
    aux: &mut AuxData,
) -> Result<(), DomainError> {
    for (h, blk) in &blocks {
        let blk = blk
            .as_ref()
            .ok_or_else(|| SourceError::Malformed(format!("block {h} not found")))?;
        let header = &blk.header;
        let bhash = Hash(header.hash.as_slice().to_vec());
        aux.block_metas.push(BlockMeta {
            chain_id,
            height: Height(header.number),
            hash: bhash.clone(),
            parent_hash: Hash(header.parent_hash.as_slice().to_vec()),
            time: header.timestamp as i64,
        });
        for (tx_index, tx) in blk.transactions.txns().enumerate() {
            let txid = tx.tx_hash().as_slice().to_vec();
            if !tx_ids.contains(&txid) {
                continue; // only matched txs
            }
            let fields = TxFields {
                from: tx.from().as_slice().to_vec(),
                tx_index: tx_index as u64,
                value: tx.value().to_string(),
                input: tx.input().to_vec(),
                gas: tx.gas_limit().to_string(),
                nonce: tx.nonce().to_string(),
                to: tx.to().map(|a| a.as_slice().to_vec()),
                gas_price: ConsensusTx::gas_price(tx).map(|gp| gp.to_string()),
            };
            aux.enrichment.push(transaction_row(
                chain_id,
                Height(header.number),
                bhash.clone(),
                Hash(txid),
                fields,
            ));
        }
    }
    Ok(())
}

/// Assemble receipt rows (one per distinct tx) into `aux`, locating each tx's block via
/// `tx_loc` (built from the matched records).
pub fn receipt_aux(
    chain_id: ChainId,
    receipts: Vec<(Vec<u8>, Option<TransactionReceipt>)>,
    tx_loc: &BTreeMap<Vec<u8>, (Height, Hash)>,
    aux: &mut AuxData,
) {
    for (txid, rcpt) in receipts {
        let Some(rcpt) = rcpt else {
            continue;
        };
        let (height, bhash) = tx_loc
            .get(&txid)
            .cloned()
            .unwrap_or((Height(0), Hash(vec![])));
        let fields = ReceiptFields {
            status: rcpt.status(),
            gas_used: rcpt.gas_used.to_string(),
            effective_gas_price: rcpt.effective_gas_price.to_string(),
            contract_address: rcpt.contract_address.map(|ca| ca.as_slice().to_vec()),
        };
        aux.enrichment
            .push(receipt_row(chain_id, height, bhash, Hash(txid), fields));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_idx_is_tx_index_no_transaction_index_column() {
        let f = TxFields {
            from: vec![0u8; 20],
            tx_index: 7,
            value: "0".into(),
            input: vec![],
            gas: "21000".into(),
            nonce: "0".into(),
            to: Some(vec![1u8; 20]),
            gas_price: Some("1".into()),
        };
        let r = transaction_row(
            ChainId(1),
            Height(100),
            Hash(vec![0u8; 32]),
            Hash(vec![9u8; 32]),
            f,
        );
        // idx carries the block inclusion order.
        assert_eq!(r.index, RecordIndex(7));
        // transaction_index is no longer a decoded field.
        assert!(r.event.fields.iter().all(|(n, _)| n != "transaction_index"));

        let tx = aux_schemas()
            .into_iter()
            .find(|s| s.table == "transactions")
            .unwrap();
        assert!(tx.columns.iter().all(|c| c.name != "transaction_index"));
    }
}
