//! Pure EVM ABI parse + decode. No I/O.
//!
//! `AbiIndex` maps each event's `topic0` to the parsed event + `EventSchema` and
//! decodes a `RawRecord` into an `EventRow`.

mod decode;

pub use decode::{topic0_hash, DecodeError};

use alloy_json_abi::JsonAbi;
use alloy_primitives::B256;
use schema::EventSchema;
use shared::{AddressBytes, EventRow, Hash, RawRecord, RecordIndex, TxCalldata};
use std::collections::HashMap;

struct Entry {
    event: alloy_json_abi::Event,
    schema: EventSchema,
}

struct CallEntry {
    function: alloy_json_abi::Function,
    schema: EventSchema,
}

/// `topic0 → event` index plus `(address, selector) → function` index, built from
/// config contracts.
#[derive(Default)]
pub struct AbiIndex {
    by_topic0: HashMap<B256, Entry>,
    by_call: HashMap<(Vec<u8>, [u8; 4]), CallEntry>,
}

/// A contract's indexing spec.
pub struct ContractSpec {
    pub abi_json: String,
    pub events: Vec<String>,
    /// Subset of ABI functions whose calldata to decode (empty = none).
    pub functions: Vec<String>,
    /// Contract address — scopes calldata routing to direct calls of this contract.
    pub address: AddressBytes,
    pub table_prefix: String,
    pub table_override: Option<String>,
}

impl AbiIndex {
    pub fn build(specs: &[ContractSpec]) -> Result<Self, DecodeError> {
        let mut by_topic0 = HashMap::new();
        let mut by_call = HashMap::new();
        for spec in specs {
            let abi: JsonAbi = serde_json::from_str(&spec.abi_json)
                .map_err(|e| DecodeError::Abi(format!("parse abi: {e}")))?;
            for event in abi.events() {
                if !spec.events.is_empty() && !spec.events.contains(&event.name) {
                    continue;
                }
                let table = spec.table_override.clone().unwrap_or_else(|| {
                    format!("evt_{}_{}", spec.table_prefix, event.name.to_lowercase())
                });
                let schema = abi_schema::schema_for(event, &table);
                by_topic0.insert(
                    event.selector(),
                    Entry {
                        event: event.clone(),
                        schema,
                    },
                );
            }
            for function in abi.functions() {
                if !spec.functions.contains(&function.name) {
                    continue;
                }
                let table = format!(
                    "call_{}_{}",
                    spec.table_prefix,
                    function.name.to_lowercase()
                );
                let schema = abi_schema::schema_for_function(function, &table);
                by_call.insert(
                    (spec.address.0.clone(), function.selector().0),
                    CallEntry {
                        function: function.clone(),
                        schema,
                    },
                );
            }
        }
        Ok(Self { by_topic0, by_call })
    }

    /// All event schemas (for the migrator).
    pub fn schemas(&self) -> Vec<EventSchema> {
        self.by_topic0.values().map(|e| e.schema.clone()).collect()
    }

    /// All calldata (function) schemas (for the migrator).
    pub fn call_schemas(&self) -> Vec<EventSchema> {
        self.by_call.values().map(|e| e.schema.clone()).collect()
    }

    pub fn call_is_empty(&self) -> bool {
        self.by_call.is_empty()
    }

    /// Decode a transaction's calldata, routed by `(to_addr, selector)`.
    /// `None` if the selector/address pair is not configured.
    pub fn decode_call(&self, tx: &TxCalldata) -> Result<Option<EventRow>, DecodeError> {
        if tx.input.len() < 4 {
            return Ok(None);
        }
        let sel: [u8; 4] = tx.input[0..4].try_into().expect("len checked");
        let Some(entry) = self.by_call.get(&(tx.to_addr.clone(), sel)) else {
            return Ok(None);
        };
        let event = decode::decode_call(&entry.function, &entry.schema, &tx.input)?;
        Ok(Some(EventRow {
            chain_id: tx.chain_id,
            height: tx.height,
            block_hash: tx.block_hash.clone(),
            block_time: None,
            tx_id: tx.tx_id.clone(),
            index: RecordIndex(tx.tx_index),
            event,
        }))
    }

    /// All topic0 selectors as `Hash` (for the record filter).
    pub fn topic0s(&self) -> Vec<Hash> {
        self.by_topic0
            .keys()
            .map(|k| Hash(k.as_slice().to_vec()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.by_topic0.is_empty()
    }

    /// Decode a raw record. `None` if topic0 is unknown.
    pub fn decode(&self, raw: &RawRecord) -> Result<Option<EventRow>, DecodeError> {
        let Some(sel0) = raw.selectors.first() else {
            return Ok(None);
        };
        if sel0.0.len() != 32 {
            return Ok(None);
        }
        let topic0 = B256::from_slice(&sel0.0);
        let Some(entry) = self.by_topic0.get(&topic0) else {
            return Ok(None);
        };
        let event = decode::decode_event(&entry.event, &entry.schema, raw)?;
        Ok(Some(EventRow {
            chain_id: raw.chain_id,
            height: raw.height,
            block_hash: raw.block_hash.clone(),
            block_time: None,
            tx_id: raw.tx_id.clone(),
            index: raw.index,
            event,
        }))
    }
}
