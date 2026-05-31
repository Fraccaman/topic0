//! `ColumnType` → GraphQL scalar mapping + row-value rendering. numeric(78,0) and the
//! bigint meta columns overflow GraphQL `Int`, so they surface as a String-encoded
//! `BigInt`; bytea is `0x`-hex, arrays/tuples a `JSON` scalar.

use super::{BIG_INT, JSON_SCALAR};
use async_graphql::dynamic::TypeRef;
use async_graphql::Value as GqlValue;
use schema::ColumnType;
use serde_json::Value as Json;

/// GraphQL output type for a logical column. `nn` = non-null.
pub(super) fn type_ref(ty: ColumnType, nn: bool) -> TypeRef {
    let name = match ty {
        ColumnType::Bool => TypeRef::BOOLEAN,
        ColumnType::UInt(_) | ColumnType::Int(_) | ColumnType::Int64 => BIG_INT,
        ColumnType::Json => JSON_SCALAR,
        // Address / Bytes (0x-hex), Utf8, Timestamp (ISO string) → String.
        _ => TypeRef::STRING,
    };
    if nn {
        TypeRef::named_nn(name)
    } else {
        TypeRef::named(name)
    }
}

/// Render a row's JSON value to its GraphQL scalar. Numerics arrive as JSON strings
/// (projected `::text`), meta ints as JSON numbers — both surface as BigInt strings.
pub(super) fn to_gql(ty: ColumnType, v: &Json) -> async_graphql::Result<GqlValue> {
    Ok(match ty {
        ColumnType::Bool => GqlValue::Boolean(v.as_bool().unwrap_or(false)),
        ColumnType::Json => GqlValue::from_json(v.clone())
            .map_err(|e| async_graphql::Error::new(format!("json: {e}")))?,
        _ => GqlValue::String(match v {
            Json::String(s) => s.clone(),
            other => other.to_string(),
        }),
    })
}
