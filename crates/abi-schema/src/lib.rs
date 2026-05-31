//! ABI event → schema IR. The ABI→IR half of the pipeline; the SQL backends consume
//! the resulting `EventSchema`. Depends on alloy only here, never in the core layers.

use alloy_json_abi::{Event, Function};
// Re-export the IR types so consumers of the builder get its output type without a
// separate `schema` dependency.
pub use schema::{ColumnDef, ColumnType, EventSchema};

/// Bit width from a `uintN`/`intN` name (`uint256`→256, bare `uint`→256).
fn parse_bits(ty: &str, prefix: &str) -> u16 {
    ty.strip_prefix(prefix)
        .and_then(|n| n.parse().ok())
        .unwrap_or(256)
}

/// Column name for the `i`-th param: snake_cased, or `argN` when unnamed.
fn col_name(i: usize, raw: &str) -> String {
    if raw.is_empty() {
        format!("arg{i}")
    } else {
        shared::to_snake_case(raw)
    }
}

/// Map a Solidity type to a logical column type.
pub fn column_type_for(ty: &str) -> ColumnType {
    if ty.ends_with(']') || ty.starts_with('(') || ty == "tuple" {
        return ColumnType::Json;
    }
    match ty {
        "address" => ColumnType::Address,
        "bool" => ColumnType::Bool,
        "string" => ColumnType::Utf8,
        "bytes" => ColumnType::Bytes,
        _ if ty.starts_with("bytes") => ColumnType::Bytes,
        _ if ty.starts_with("uint") => ColumnType::UInt(parse_bits(ty, "uint")),
        _ if ty.starts_with("int") => ColumnType::Int(parse_bits(ty, "int")),
        _ => ColumnType::Bytes,
    }
}

/// Build the `EventSchema` for one event.
pub fn schema_for(event: &Event, table: &str) -> EventSchema {
    let mut columns = Vec::with_capacity(event.inputs.len());
    let mut indexed_positions = Vec::new();
    for (i, p) in event.inputs.iter().enumerate() {
        let dynamic = matches!(p.ty.as_str(), "string" | "bytes") || p.ty.ends_with(']');
        let indexed_hash = p.indexed && dynamic;
        let name = col_name(i, &p.name);
        // Indexed dynamic types store only their hash → bytea.
        let (name, ty) = if indexed_hash {
            (format!("{name}_hash"), ColumnType::Bytes)
        } else {
            (name, column_type_for(&p.ty))
        };
        columns.push(ColumnDef {
            name,
            ty,
            indexed_hash,
        });
        if p.indexed {
            indexed_positions.push(i);
        }
    }
    EventSchema {
        table: table.to_string(),
        event: event.name.clone(),
        topic0: Some(format!("{:#x}", event.selector())),
        columns,
        indexed_positions,
        pk_columns: EventSchema::event_pk(),
    }
}

/// Build the `EventSchema` for one function's calldata (one row per tx).
/// All params are non-indexed; PK is `(chain_id, tx_id)` like the aux tables.
pub fn schema_for_function(func: &Function, table: &str) -> EventSchema {
    let mut columns = Vec::with_capacity(func.inputs.len());
    for (i, p) in func.inputs.iter().enumerate() {
        columns.push(ColumnDef {
            name: col_name(i, &p.name),
            ty: column_type_for(&p.ty),
            indexed_hash: false,
        });
    }
    EventSchema {
        table: table.to_string(),
        event: func.name.clone(),
        topic0: None,
        columns,
        indexed_positions: vec![],
        pk_columns: vec!["chain_id".into(), "tx_id".into()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_solidity_types_to_logical() {
        assert_eq!(column_type_for("address"), ColumnType::Address);
        assert_eq!(column_type_for("bool"), ColumnType::Bool);
        assert_eq!(column_type_for("string"), ColumnType::Utf8);
        assert_eq!(column_type_for("bytes"), ColumnType::Bytes);
        assert_eq!(column_type_for("bytes32"), ColumnType::Bytes);
        assert_eq!(column_type_for("uint256"), ColumnType::UInt(256));
        assert_eq!(column_type_for("int8"), ColumnType::Int(8));
        assert_eq!(column_type_for("uint64"), ColumnType::UInt(64));
        assert_eq!(column_type_for("uint"), ColumnType::UInt(256)); // bare → 256
    }

    #[test]
    fn arrays_and_tuples_map_to_json() {
        assert_eq!(column_type_for("uint256[]"), ColumnType::Json);
        assert_eq!(column_type_for("address[3]"), ColumnType::Json);
        assert_eq!(column_type_for("tuple"), ColumnType::Json);
        assert_eq!(column_type_for("(uint256,address)"), ColumnType::Json);
    }

    fn event(abi_json: &str) -> alloy_json_abi::Event {
        serde_json::from_str::<alloy_json_abi::JsonAbi>(abi_json)
            .unwrap()
            .events()
            .next()
            .unwrap()
            .clone()
    }

    #[test]
    fn schema_for_builds_columns_topic0_and_indexed() {
        let s = schema_for(
            &event(
                r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                    {"name":"from","type":"address","indexed":true},
                    {"name":"to","type":"address","indexed":true},
                    {"name":"value","type":"uint256","indexed":false}]}]"#,
            ),
            "evt_x",
        );
        assert_eq!(s.table, "evt_x");
        assert_eq!(s.event, "Transfer");
        assert!(s.topic0.is_some());
        assert_eq!(s.indexed_positions, vec![0, 1]);
        assert_eq!(s.pk_columns, EventSchema::event_pk());
        let cols: Vec<_> = s.columns.iter().map(|c| (c.name.as_str(), c.ty)).collect();
        assert_eq!(
            cols,
            vec![
                ("from", ColumnType::Address),
                ("to", ColumnType::Address),
                ("value", ColumnType::UInt(256)),
            ]
        );
    }

    #[test]
    fn schema_for_hashes_indexed_dynamic_and_names_unnamed() {
        let s = schema_for(
            &event(
                r#"[{"type":"event","name":"E","anonymous":false,"inputs":[
                    {"name":"label","type":"string","indexed":true},
                    {"name":"","type":"uint256","indexed":false}]}]"#,
            ),
            "t",
        );
        // Indexed dynamic → `<name>_hash`, stored as bytes.
        assert_eq!(s.columns[0].name, "label_hash");
        assert!(s.columns[0].indexed_hash);
        assert_eq!(s.columns[0].ty, ColumnType::Bytes);
        // Unnamed param → positional name.
        assert_eq!(s.columns[1].name, "arg1");
    }
}
