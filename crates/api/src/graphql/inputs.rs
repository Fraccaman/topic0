//! Input types for the query fields: the `where` predicate object, comparison `Op`,
//! and sort `OrderDir`.

use super::{FILTER_INPUT, OP_ENUM, ORDER_ENUM};
use async_graphql::dynamic::{Enum, InputObject, InputValue, TypeRef};
use shared::FilterOp;

pub(super) fn op_enum() -> Enum {
    Enum::new(OP_ENUM)
        .item("EQ")
        .item("NE")
        .item("GT")
        .item("GTE")
        .item("LT")
        .item("LTE")
}

pub(super) fn order_enum() -> Enum {
    Enum::new(ORDER_ENUM).item("ASC").item("DESC")
}

pub(super) fn filter_input() -> InputObject {
    InputObject::new(FILTER_INPUT)
        .field(InputValue::new(
            "column",
            TypeRef::named_nn(TypeRef::STRING),
        ))
        .field(InputValue::new("op", TypeRef::named(OP_ENUM)))
        .field(InputValue::new("value", TypeRef::named_nn(TypeRef::STRING)))
}

/// Parse a GraphQL `Op` enum name to a `FilterOp` (defaults to `Eq`).
pub(super) fn parse_op(s: &str) -> FilterOp {
    match s {
        "NE" => FilterOp::Ne,
        "GT" => FilterOp::Gt,
        "GTE" => FilterOp::Gte,
        "LT" => FilterOp::Lt,
        "LTE" => FilterOp::Lte,
        _ => FilterOp::Eq,
    }
}
