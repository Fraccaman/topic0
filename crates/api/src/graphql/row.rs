//! The typed object for one table: meta columns + decoded columns, plus nested
//! `transaction`/`receipt` for event tables. Field resolvers read straight out of the
//! per-row JSON the read repo produces.

use super::scalars::{to_gql, type_ref};
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use schema::{ColumnType, EventSchema};
use serde_json::Value as Json;

/// Typed object for `s`. `is_event` adds the nested aux objects; `has_type` guards them
/// against a missing aux schema.
pub(super) fn row_object(
    s: &EventSchema,
    is_event: bool,
    has_type: &impl Fn(&str) -> bool,
) -> Object {
    let mut obj = Object::new(&s.table)
        .field(json_field("chain_id", "chain_id", ColumnType::Int64, true))
        .field(json_field("height", "height", ColumnType::Int64, true))
        .field(json_field(
            "block_hash",
            "block_hash",
            ColumnType::Bytes,
            true,
        ))
        .field(json_field(
            "block_time",
            "block_time",
            ColumnType::Timestamp,
            false,
        ))
        .field(json_field("tx_id", "tx_id", ColumnType::Bytes, true));
    if shared::table_has_idx(&s.table) {
        obj = obj.field(json_field("idx", "idx", ColumnType::Int64, true));
    }
    for c in &s.columns {
        obj = obj.field(json_field(&c.name, &c.name, c.ty, false));
    }
    if is_event {
        if has_type("transactions") {
            obj = obj.field(nested_field("transaction", "transactions", "transaction"));
        }
        if has_type("receipts") {
            obj = obj.field(nested_field("receipt", "receipts", "receipt"));
        }
    }
    obj
}

/// A scalar field reading `key` from the parent row JSON.
fn json_field(name: &str, key: &str, ty: ColumnType, nn: bool) -> Field {
    let key = key.to_string();
    Field::new(name, type_ref(ty, nn), move |rctx| {
        let key = key.clone();
        FieldFuture::new(async move {
            let parent = rctx
                .parent_value
                .downcast_ref::<Json>()
                .ok_or_else(|| async_graphql::Error::new("internal: bad parent value"))?;
            match parent.get(&key) {
                None | Some(Json::Null) => Ok(None),
                Some(v) => Ok(Some(FieldValue::value(to_gql(ty, v)?))),
            }
        })
    })
}

/// A nested object field (`transaction`/`receipt`) reading an embedded JSON object.
fn nested_field(field_name: &'static str, type_name: &'static str, key: &'static str) -> Field {
    Field::new(field_name, TypeRef::named(type_name), move |rctx| {
        FieldFuture::new(async move {
            let parent = rctx
                .parent_value
                .downcast_ref::<Json>()
                .ok_or_else(|| async_graphql::Error::new("internal: bad parent value"))?;
            match parent.get(key) {
                Some(v) if v.is_object() => Ok(Some(FieldValue::owned_any(v.clone()))),
                _ => Ok(None),
            }
        })
    })
}
