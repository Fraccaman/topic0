//! Alloy ↔ canonical model mapping: a matched `Log` → `RawRecord`, and a
//! `RecordFilter` → the stateless alloy `Filter` (addresses + topic0, no range).

use crate::error::SourceError;
use alloy::rpc::types::{Filter, Log};
use shared::{AddressBytes, ChainId, Hash, Height, RawRecord, RecordFilter, RecordIndex};

pub(crate) fn map_log(chain_id: ChainId, log: &Log) -> Result<RawRecord, SourceError> {
    let height = log
        .block_number
        .ok_or_else(|| SourceError::Malformed("log missing block_number".into()))?;
    let block_hash = log
        .block_hash
        .ok_or_else(|| SourceError::Malformed("log missing block_hash".into()))?;
    let index = log
        .log_index
        .ok_or_else(|| SourceError::Malformed("log missing log_index".into()))?;
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| SourceError::Malformed("log missing tx_hash".into()))?;
    Ok(RawRecord {
        chain_id,
        height: Height(height),
        block_hash: Hash(block_hash.as_slice().to_vec()),
        index: RecordIndex(index),
        address: AddressBytes(log.inner.address.as_slice().to_vec()),
        selectors: log
            .inner
            .data
            .topics()
            .iter()
            .map(|t| Hash(t.as_slice().to_vec()))
            .collect(),
        data: log.inner.data.data.to_vec(),
        tx_id: Hash(tx_hash.as_slice().to_vec()),
        tx_index: log.transaction_index.unwrap_or(0),
        inner_index: None,
    })
}

/// Alloy log filter (addresses + topic0 selectors), no block range.
pub(crate) fn base_filter(f: &RecordFilter) -> Filter {
    let mut filter = Filter::new();
    if !f.addresses.is_empty() {
        let addrs: Vec<alloy_primitives::Address> = f
            .addresses
            .iter()
            .map(|a| alloy_primitives::Address::from_slice(a.as_slice()))
            .collect();
        filter = filter.address(addrs);
    }
    if !f.selectors.is_empty() {
        let sels: Vec<alloy_primitives::B256> = f
            .selectors
            .iter()
            .map(|s| alloy_primitives::B256::from_slice(s.as_slice()))
            .collect();
        filter = filter.event_signature(sels);
    }
    filter
}
