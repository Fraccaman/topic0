//! The EVM `Decoder`: an `AbiIndex` over the configured contracts, plus the helpers
//! that build it from config (ABI file load, address parse, table-prefix derivation).

use crate::enrichment;
use crate::error::SourceError;
use abi::{AbiIndex, ContractSpec};
use config::ChainCfg;
use domain::ports::decoder::Decoder;
use schema::EventSchema;
use shared::{AddressBytes, DomainError, EventRow, RawRecord, RecordFilter, TxCalldata};
use std::path::Path;
use std::sync::Arc;

/// EVM decoder: ABI index + tx/receipt aux schemas + record filter.
pub(crate) struct EvmDecoder {
    abi: Arc<AbiIndex>,
    addresses: Vec<AddressBytes>,
}

impl Decoder for EvmDecoder {
    fn decode(&self, record: &RawRecord) -> Result<Option<EventRow>, DomainError> {
        self.abi.decode(record).map_err(DomainError::from)
    }
    fn decode_call(&self, tx: &TxCalldata) -> Result<Option<EventRow>, DomainError> {
        self.abi.decode_call(tx).map_err(DomainError::from)
    }
    fn has_calls(&self) -> bool {
        !self.abi.call_is_empty()
    }
    fn schemas(&self) -> Vec<EventSchema> {
        let mut s = self.abi.schemas();
        s.extend(self.abi.call_schemas());
        s.extend(enrichment::aux_schemas());
        s
    }
    fn record_filter(&self) -> RecordFilter {
        RecordFilter {
            addresses: self.addresses.clone(),
            selectors: self.abi.topic0s(),
        }
    }
}

/// Build the EVM decoder from a chain's contracts (loads ABI files, no network).
pub(crate) fn build_evm_decoder(
    cfg: &ChainCfg,
    base_dir: &Path,
) -> Result<EvmDecoder, SourceError> {
    let mut specs = Vec::new();
    let mut addresses = Vec::new();
    for c in &cfg.contracts {
        let path = base_dir.join(&c.abi);
        let abi_json = std::fs::read_to_string(&path)
            .map_err(|e| SourceError::Malformed(format!("read abi {}: {e}", path.display())))?;
        let address = parse_addr(&c.address)?;
        specs.push(ContractSpec {
            abi_json,
            events: c.events.clone(),
            functions: c.functions.clone(),
            address: address.clone(),
            table_prefix: table_prefix(&c.abi),
            table_override: c.table.clone(),
        });
        addresses.push(address);
    }
    let abi = AbiIndex::build(&specs).map_err(|e| SourceError::Malformed(e.to_string()))?;
    Ok(EvmDecoder {
        abi: Arc::new(abi),
        addresses,
    })
}

fn table_prefix(abi_path: &str) -> String {
    Path::new(abi_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("evt")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn parse_addr(s: &str) -> Result<AddressBytes, SourceError> {
    match shared::hex_decode(s) {
        Some(b) if b.len() == 20 => Ok(AddressBytes(b)),
        _ => Err(SourceError::Malformed(format!("bad address {s}"))),
    }
}
