//! Block metadata (chain-neutral) — used for timestamps + reorg detection.

use crate::model::chain::{ChainId, Hash, Height};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub chain_id: ChainId,
    pub height: Height,
    pub hash: Hash,
    pub parent_hash: Hash,
    /// Unix seconds.
    pub time: i64,
}

/// A stored transaction's calldata, read back for function decoding.
/// `to_addr` is empty for contract-creation txs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxCalldata {
    pub chain_id: ChainId,
    pub height: Height,
    pub block_hash: Hash,
    pub tx_id: Hash,
    pub to_addr: Vec<u8>,
    pub tx_index: u64,
    pub input: Vec<u8>,
}
