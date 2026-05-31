//! Decode a `RawRecord` against a parsed event into typed values.

use alloy_dyn_abi::{DynSolValue, EventExt, JsonAbiExt};
use alloy_json_abi::{Event, Function};
use alloy_primitives::B256;
use schema::EventSchema;
use shared::{DecodedEvent, EventValue, Hash, RawRecord};

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("abi error: {0}")]
    Abi(String),
    #[error("decode failed: {0}")]
    Decode(String),
}

impl From<DecodeError> for shared::DomainError {
    fn from(e: DecodeError) -> Self {
        shared::DomainError::Decode(e.to_string())
    }
}

/// Decode one record into a `DecodedEvent`.
pub fn decode_event(
    event: &Event,
    schema: &EventSchema,
    raw: &RawRecord,
) -> Result<DecodedEvent, DecodeError> {
    let topics: Vec<B256> = raw
        .selectors
        .iter()
        .map(|h| B256::from_slice(&h.0))
        .collect();
    let decoded = event
        .decode_log_parts(topics, &raw.data)
        .map_err(|e| DecodeError::Decode(e.to_string()))?;

    let mut indexed = decoded.indexed.into_iter();
    let mut body = decoded.body.into_iter();

    let mut fields = Vec::with_capacity(event.inputs.len());
    for (i, input) in event.inputs.iter().enumerate() {
        let col = &schema.columns[i];
        let value = if input.indexed {
            let v = indexed
                .next()
                .ok_or_else(|| DecodeError::Decode("missing indexed value".into()))?;
            if col.indexed_hash {
                EventValue::Hash(to_hash(&v))
            } else {
                to_value(&v)
            }
        } else {
            let v = body
                .next()
                .ok_or_else(|| DecodeError::Decode("missing body value".into()))?;
            to_value(&v)
        };
        fields.push((col.name.clone(), value));
    }

    Ok(DecodedEvent {
        table: schema.table.clone(),
        fields,
    })
}

/// Decode a transaction's calldata into a `DecodedEvent`. `input` includes the
/// 4-byte selector; the params follow it.
pub fn decode_call(
    function: &Function,
    schema: &EventSchema,
    input: &[u8],
) -> Result<DecodedEvent, DecodeError> {
    let values = function
        .abi_decode_input(&input[4..])
        .map_err(|e| DecodeError::Decode(e.to_string()))?;

    let mut fields = Vec::with_capacity(values.len());
    for (i, v) in values.iter().enumerate() {
        let col = &schema.columns[i];
        fields.push((col.name.clone(), to_value(v)));
    }

    Ok(DecodedEvent {
        table: schema.table.clone(),
        fields,
    })
}

fn to_hash(v: &DynSolValue) -> Vec<u8> {
    match v {
        DynSolValue::FixedBytes(w, _) => w.as_slice().to_vec(),
        _ => Vec::new(),
    }
}

fn to_value(v: &DynSolValue) -> EventValue {
    match v {
        DynSolValue::Address(a) => EventValue::Address(a.as_slice().to_vec()),
        DynSolValue::Bool(b) => EventValue::Bool(*b),
        DynSolValue::Uint(u, _) => EventValue::Uint(u.to_string()),
        DynSolValue::Int(i, _) => EventValue::Int(i.to_string()),
        DynSolValue::Bytes(b) => EventValue::Bytes(b.clone()),
        DynSolValue::FixedBytes(w, size) => EventValue::Bytes(w[..*size].to_vec()),
        DynSolValue::String(s) => EventValue::String(s.clone()),
        // Arrays / tuples / structs → JSON text for the jsonb column.
        other => EventValue::Json(to_json(other).to_string()),
    }
}

/// Convert a dynamic value to `serde_json::Value`.
fn to_json(v: &DynSolValue) -> serde_json::Value {
    use serde_json::Value;
    match v {
        DynSolValue::Address(a) => Value::String(format!("0x{}", shared::hex_encode(a.as_slice()))),
        DynSolValue::Bool(b) => Value::Bool(*b),
        DynSolValue::Uint(u, _) => Value::String(u.to_string()),
        DynSolValue::Int(i, _) => Value::String(i.to_string()),
        DynSolValue::Bytes(b) => Value::String(format!("0x{}", shared::hex_encode(b))),
        DynSolValue::FixedBytes(w, size) => {
            Value::String(format!("0x{}", shared::hex_encode(&w[..*size])))
        }
        DynSolValue::String(s) => Value::String(s.clone()),
        DynSolValue::Array(xs) | DynSolValue::FixedArray(xs) | DynSolValue::Tuple(xs) => {
            Value::Array(xs.iter().map(to_json).collect())
        }
        _ => Value::Null,
    }
}

/// Hex topic0 string → `Hash`.
pub fn topic0_hash(topic0_hex: &str) -> Option<Hash> {
    shared::hex_decode(topic0_hex).map(Hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbiIndex, ContractSpec};
    use alloy_primitives::address;
    use shared::{AddressBytes, ChainId, Hash, Height, RecordIndex, TxCalldata};

    const ERC20: &str = r#"[
      {"type":"event","name":"Transfer","anonymous":false,"inputs":[
        {"name":"from","type":"address","indexed":true},
        {"name":"to","type":"address","indexed":true},
        {"name":"value","type":"uint256","indexed":false}
      ]}
    ]"#;

    #[test]
    fn decodes_erc20_transfer() {
        let idx = AbiIndex::build(&[ContractSpec {
            abi_json: ERC20.into(),
            events: vec!["Transfer".into()],
            functions: vec![],
            address: AddressBytes(vec![3; 20]),
            table_prefix: "token".into(),
            table_override: None,
        }])
        .unwrap();

        let from = address!("1111111111111111111111111111111111111111");
        let to = address!("2222222222222222222222222222222222222222");
        let transfer_sig =
            topic0_hash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
                .unwrap();
        let mut data = vec![0u8; 32];
        data[31] = 100;

        let raw = RawRecord {
            chain_id: ChainId(1),
            height: Height(1),
            block_hash: Hash(vec![0; 32]),
            index: RecordIndex(0),
            address: AddressBytes(vec![3; 20]),
            selectors: vec![
                transfer_sig,
                Hash(from.into_word().to_vec()),
                Hash(to.into_word().to_vec()),
            ],
            data,
            tx_id: Hash(vec![0; 32]),
            tx_index: 0,
            inner_index: None,
        };

        let row = idx.decode(&raw).unwrap().expect("known event");
        assert_eq!(row.event.table, "evt_token_transfer");
        assert_eq!(row.event.fields.len(), 3);
        assert_eq!(row.event.fields[0].0, "from");
        assert!(matches!(&row.event.fields[2].1, EventValue::Uint(s) if s == "100"));
    }

    fn erc20_index() -> AbiIndex {
        AbiIndex::build(&[ContractSpec {
            abi_json: ERC20.into(),
            events: vec!["Transfer".into()],
            functions: vec![],
            address: AddressBytes(vec![3; 20]),
            table_prefix: "token".into(),
            table_override: None,
        }])
        .unwrap()
    }

    fn raw_with_selector(sel: Hash) -> RawRecord {
        RawRecord {
            chain_id: ChainId(1),
            height: Height(1),
            block_hash: Hash(vec![0; 32]),
            index: RecordIndex(0),
            address: AddressBytes(vec![3; 20]),
            selectors: vec![sel],
            data: vec![],
            tx_id: Hash(vec![0; 32]),
            tx_index: 0,
            inner_index: None,
        }
    }

    #[test]
    fn array_and_tuple_values_become_valid_json() {
        use alloy_primitives::U256;
        let arr = DynSolValue::Array(vec![
            DynSolValue::Uint(U256::from(1u64), 256),
            DynSolValue::Uint(U256::from(2u64), 256),
        ]);
        let EventValue::Json(s) = to_value(&arr) else {
            panic!("expected Json");
        };
        // Must parse as JSON.
        let parsed: serde_json::Value = serde_json::from_str(&s).expect("valid json");
        assert_eq!(parsed, serde_json::json!(["1", "2"]));
    }

    #[test]
    fn unknown_topic0_is_skipped() {
        let idx = erc20_index();
        // selector not in the index
        let raw = raw_with_selector(Hash(vec![0xAB; 32]));
        assert!(idx.decode(&raw).unwrap().is_none());
    }

    #[test]
    fn non_32_byte_selector_is_skipped() {
        let idx = erc20_index();
        let raw = raw_with_selector(Hash(vec![0xAB; 8]));
        assert!(idx.decode(&raw).unwrap().is_none());
    }

    #[test]
    fn indexed_dynamic_becomes_hash_column() {
        // Indexed `string` stores only its hash → `<name>_hash`.
        const ABI: &str = r#"[
          {"type":"event","name":"Named","anonymous":false,"inputs":[
            {"name":"label","type":"string","indexed":true},
            {"name":"value","type":"uint256","indexed":false}
          ]}
        ]"#;
        let idx = AbiIndex::build(&[ContractSpec {
            abi_json: ABI.into(),
            events: vec![],
            functions: vec![],
            address: AddressBytes(vec![3; 20]),
            table_prefix: "c".into(),
            table_override: None,
        }])
        .unwrap();
        let schema = &idx.schemas()[0];
        let label = &schema.columns[0];
        assert_eq!(label.name, "label_hash");
        assert!(label.indexed_hash);
    }

    const ERC20_FN: &str = r#"[
      {"type":"function","name":"transfer","stateMutability":"nonpayable",
       "inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],
       "outputs":[{"name":"","type":"bool"}]}
    ]"#;

    fn transfer_calldata(to: &[u8; 20], amount: u8) -> Vec<u8> {
        // selector transfer(address,uint256) = 0xa9059cbb
        let mut input = vec![0xa9, 0x05, 0x9c, 0xbb];
        let mut to_word = vec![0u8; 32];
        to_word[12..].copy_from_slice(to);
        input.extend_from_slice(&to_word);
        let mut amount_word = vec![0u8; 32];
        amount_word[31] = amount;
        input.extend_from_slice(&amount_word);
        input
    }

    fn calldata_index(addr: &[u8; 20]) -> AbiIndex {
        AbiIndex::build(&[ContractSpec {
            abi_json: ERC20_FN.into(),
            events: vec![],
            functions: vec!["transfer".into()],
            address: AddressBytes(addr.to_vec()),
            table_prefix: "token".into(),
            table_override: None,
        }])
        .unwrap()
    }

    fn call_tx(to: &[u8; 20], input: Vec<u8>) -> TxCalldata {
        TxCalldata {
            chain_id: ChainId(1),
            height: Height(1),
            block_hash: Hash(vec![0; 32]),
            tx_id: Hash(vec![7; 32]),
            to_addr: to.to_vec(),
            tx_index: 3,
            input,
        }
    }

    #[test]
    fn decodes_erc20_transfer_calldata() {
        let addr = [0x33u8; 20];
        let idx = calldata_index(&addr);
        let tx = call_tx(&addr, transfer_calldata(&[0x22u8; 20], 100));

        let row = idx.decode_call(&tx).unwrap().expect("known fn");
        assert_eq!(row.event.table, "call_token_transfer");
        assert_eq!(row.index, RecordIndex(3)); // tx_index
        let fields = &row.event.fields;
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "to");
        assert!(matches!(&fields[0].1, EventValue::Address(a) if a == &vec![0x22u8; 20]));
        assert_eq!(fields[1].0, "amount");
        assert!(matches!(&fields[1].1, EventValue::Uint(s) if s == "100"));
    }

    #[test]
    fn calldata_routing_scoped_by_to_addr() {
        let idx = calldata_index(&[0x33u8; 20]);
        // Right selector, wrong recipient contract → not decoded.
        let tx = call_tx(&[0x44u8; 20], transfer_calldata(&[0x22u8; 20], 1));
        assert!(idx.decode_call(&tx).unwrap().is_none());
    }

    #[test]
    fn calldata_unknown_selector_is_skipped() {
        let addr = [0x33u8; 20];
        let idx = calldata_index(&addr);
        let bad = call_tx(&addr, vec![0x00, 0x00, 0x00, 0x00, 0, 0, 0, 0]);
        assert!(idx.decode_call(&bad).unwrap().is_none());
        // Too short to hold a selector.
        let short = call_tx(&addr, vec![0x01, 0x02]);
        assert!(idx.decode_call(&short).unwrap().is_none());
    }

    #[test]
    fn call_schema_has_tx_pk_and_no_topic0() {
        let idx = calldata_index(&[0x33u8; 20]);
        let cs = idx.call_schemas();
        assert_eq!(cs.len(), 1);
        assert_eq!(
            cs[0].pk_columns,
            vec!["chain_id".to_string(), "tx_id".to_string()]
        );
        assert!(cs[0].topic0.is_none());
        assert!(!idx.call_is_empty());
    }

    #[test]
    fn unnamed_param_gets_positional_name() {
        const ABI: &str = r#"[
          {"type":"event","name":"Anon","anonymous":false,"inputs":[
            {"name":"","type":"uint256","indexed":false}
          ]}
        ]"#;
        let idx = AbiIndex::build(&[ContractSpec {
            abi_json: ABI.into(),
            events: vec![],
            functions: vec![],
            address: AddressBytes(vec![3; 20]),
            table_prefix: "c".into(),
            table_override: None,
        }])
        .unwrap();
        assert_eq!(idx.schemas()[0].columns[0].name, "arg0");
    }
}
