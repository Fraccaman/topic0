//! The top-level query field for a table: parse args (typed shortcuts + `where`), decide
//! tx/receipt joins from the selection (`look_ahead`), run the read repo, and return a
//! connection.

use super::connection::{conn_name, ConnData};
use super::inputs::parse_op;
use super::{into_gql, Reader, FILTER_INPUT, ORDER_ENUM};
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use serde_json::Value as Json;
use shared::{Cursor, Filter, FilterOp, QuerySpec};

/// `<table>(first, after, chainId, fromHeight, toHeight, orderBy, where): <table>_connection`.
pub(super) fn query_field(table: &str, is_event: bool) -> Field {
    let table_s = table.to_string();
    Field::new(table, TypeRef::named(conn_name(table)), move |rctx| {
        let table = table_s.clone();
        FieldFuture::new(async move {
            let reader = rctx.ctx.data::<Reader>()?.clone();

            let first = rctx
                .args
                .get("first")
                .map(|v| v.i64())
                .transpose()?
                .unwrap_or(100)
                .max(1) as u32;
            let after = rctx
                .args
                .get("after")
                .map(|v| v.string().map(|s| Cursor(s.to_string())))
                .transpose()?;
            let descending = match rctx.args.get("orderBy") {
                Some(v) => v.enum_name()? == "DESC",
                None => false,
            };

            // Typed shortcut args lower to filters; `where` adds arbitrary predicates.
            let mut filters: Vec<Filter> = Vec::new();
            if let Some(c) = rctx.args.get("chainId") {
                filters.push(eq_filter("chain_id", FilterOp::Eq, c.i64()?));
            }
            if let Some(h) = rctx.args.get("fromHeight") {
                filters.push(eq_filter("height", FilterOp::Gte, h.i64()?));
            }
            if let Some(h) = rctx.args.get("toHeight") {
                filters.push(eq_filter("height", FilterOp::Lte, h.i64()?));
            }
            if let Some(w) = rctx.args.get("where") {
                for item in w.list()?.iter() {
                    let obj = item.object()?;
                    let column = obj.try_get("column")?.string()?.to_string();
                    let value = obj.try_get("value")?.string()?.to_string();
                    let op = match obj.get("op") {
                        Some(o) => parse_op(o.enum_name()?),
                        None => FilterOp::Eq,
                    };
                    filters.push(Filter { column, op, value });
                }
            }

            // Conditional joins: the nested objects sit two hops down —
            // connection.nodes.{transaction,receipt}.
            let look = rctx.ctx.look_ahead();
            let include_tx = is_event && look.field("nodes").field("transaction").exists();
            let include_receipt = is_event && look.field("nodes").field("receipt").exists();

            let spec = QuerySpec {
                table,
                filters,
                sort: Vec::new(),
                first,
                after,
                include_tx,
                include_receipt,
                descending,
            };
            let metric_table = spec.table.clone();
            let started = std::time::Instant::now();
            let page = match reader.query(&spec).await {
                Ok(p) => {
                    metrics::counter!("graphql_requests_total", "table" => metric_table.clone(), "status" => "ok").increment(1);
                    p
                }
                Err(e) => {
                    metrics::counter!("graphql_requests_total", "table" => metric_table, "status" => "error").increment(1);
                    return Err(into_gql(e));
                }
            };
            metrics::histogram!("graphql_request_duration_seconds", "table" => metric_table.clone())
                .record(started.elapsed().as_secs_f64());
            metrics::histogram!("query_rows_returned", "table" => metric_table)
                .record(page.items.len() as f64);
            let rows = page
                .items
                .iter()
                .map(|s| serde_json::from_str(s).unwrap_or(Json::Null))
                .collect();
            Ok(Some(FieldValue::owned_any(ConnData {
                rows,
                end_cursor: page.end_cursor.map(|c| c.0),
                has_next: page.has_next,
                spec,
            })))
        })
    })
    .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("chainId", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("fromHeight", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("toHeight", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("orderBy", TypeRef::named(ORDER_ENUM)))
    .argument(InputValue::new(
        "where",
        TypeRef::List(Box::new(TypeRef::NonNull(Box::new(TypeRef::Named(
            FILTER_INPUT.into(),
        ))))),
    ))
}

fn eq_filter(column: &str, op: FilterOp, value: i64) -> Filter {
    Filter {
        column: column.into(),
        op,
        value: value.to_string(),
    }
}
