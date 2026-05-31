//! The canonical indexed record + decoded outputs (chain-neutral).

use crate::model::chain::{AddressBytes, ChainId, Hash, Height, RecordIndex};
use serde::{Deserialize, Serialize};

/// A raw indexed record — the durable, re-decodable artifact.
/// EVM: event log. Solana: Anchor event / instruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawRecord {
    pub chain_id: ChainId,
    pub height: Height,
    pub block_hash: Hash,
    pub index: RecordIndex,
    /// Emitting contract / program.
    pub address: AddressBytes,
    /// Routing selectors — EVM topics (topic0 first) / Solana discriminator.
    pub selectors: Vec<Hash>,
    /// Opaque payload (ABI/Borsh-encoded non-indexed fields).
    pub data: Vec<u8>,
    /// Transaction id — EVM tx hash, Solana signature.
    pub tx_id: Hash,
    /// Parent-tx position within the block (EVM transactionIndex); 0 where a chain
    /// doesn't provide it.
    pub tx_index: u64,
    /// Inner position (Solana inner instruction); None on EVM.
    pub inner_index: Option<u64>,
}

/// A live-tip log event: a record plus whether the chain reported it as `removed`
/// (reorged out). Only the WS subscription carries `removed`; backfill getLogs never
/// returns removed logs, so that path uses `RawRecord` directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TipLog {
    pub record: RawRecord,
    pub removed: bool,
}

/// Selects records by address + selector set. Chain-scoped by its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFilter {
    pub addresses: Vec<AddressBytes>,
    /// Selector set (EVM topic0 / Solana discriminators); empty = any.
    pub selectors: Vec<Hash>,
}

/// A decoded field value — neutral across ABI and Borsh/IDL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventValue {
    Address(Vec<u8>),
    /// Unsigned integer as a decimal string (arbitrary precision).
    Uint(String),
    /// Signed integer as a decimal string.
    Int(String),
    Bool(bool),
    Bytes(Vec<u8>),
    String(String),
    /// A hash (e.g. indexed-dynamic topic).
    Hash(Vec<u8>),
    /// Arrays / tuples / structs serialized to JSON text.
    Json(String),
}

/// A decoded record: target table + ordered named field values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecodedEvent {
    pub table: String,
    pub fields: Vec<(String, EventValue)>,
}

/// A fully-resolved row ready to upsert into a typed table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRow {
    pub chain_id: ChainId,
    pub height: Height,
    pub block_hash: Hash,
    pub block_time: Option<i64>,
    pub tx_id: Hash,
    pub index: RecordIndex,
    pub event: DecodedEvent,
}
