//! The `<table>_connection` object: `nodes`, `endCursor`, `hasNext`, and a lazy
//! `totalCount` (resolved only when selected → one extra `count(*)`).

use super::{into_gql, Reader, BIG_INT};
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, ResolverContext, TypeRef};
use async_graphql::Value as GqlValue;
use serde_json::Value as Json;
use shared::QuerySpec;

/// One page of rows plus what `totalCount` needs to re-count lazily.
pub(super) struct ConnData {
    pub(super) rows: Vec<Json>,
    pub(super) end_cursor: Option<String>,
    pub(super) has_next: bool,
    pub(super) spec: QuerySpec,
}

pub(super) fn conn_name(table: &str) -> String {
    format!("{table}_connection")
}

pub(super) fn connection_object(table: &str) -> Object {
    let row_type = table.to_string();
    Object::new(conn_name(table))
        .field(Field::new(
            "nodes",
            TypeRef::named_nn_list_nn(row_type),
            |rctx| {
                FieldFuture::new(async move {
                    let data = conn_data(&rctx)?;
                    Ok(Some(FieldValue::list(
                        data.rows.iter().map(|v| FieldValue::owned_any(v.clone())),
                    )))
                })
            },
        ))
        .field(Field::new(
            "endCursor",
            TypeRef::named(TypeRef::STRING),
            |rctx| {
                FieldFuture::new(async move {
                    Ok(conn_data(&rctx)?.end_cursor.clone().map(FieldValue::value))
                })
            },
        ))
        .field(Field::new(
            "hasNext",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |rctx| {
                FieldFuture::new(
                    async move { Ok(Some(FieldValue::value(conn_data(&rctx)?.has_next))) },
                )
            },
        ))
        .field(Field::new("totalCount", TypeRef::named(BIG_INT), |rctx| {
            FieldFuture::new(async move {
                let reader = rctx.ctx.data::<Reader>()?;
                let n = reader
                    .count(&conn_data(&rctx)?.spec)
                    .await
                    .map_err(into_gql)?;
                Ok(Some(FieldValue::value(GqlValue::String(n.to_string()))))
            })
        }))
}

/// Downcast a connection field's parent to its `ConnData`.
fn conn_data<'a>(rctx: &'a ResolverContext) -> async_graphql::Result<&'a ConnData> {
    rctx.parent_value
        .downcast_ref::<ConnData>()
        .ok_or_else(|| async_graphql::Error::new("internal: bad connection value"))
}
