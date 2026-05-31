//! Dynamic GraphQL schema, assembled at runtime from the configured `EventSchema`s:
//! one typed object + query field per table, nested `transaction`/`receipt` objects,
//! keyset pagination, and a lazy `totalCount`. Tables are config-driven, so the schema
//! can't be a compile-time `derive` — it's built with `async-graphql`'s `dynamic` API.
//!
//! Field resolvers read straight out of the per-row JSON the read repo already produces
//! (`Page<String>` of JSON objects); the repo contract is unchanged.
//!
//! Layout: [`scalars`] (type mapping), [`row`] (per-table object), [`connection`] (page
//! wrapper + `totalCount`), [`query`] (query field + arg parsing), [`inputs`]
//! (`where`/`Op`/`OrderDir`).

mod connection;
mod inputs;
mod query;
mod row;
mod scalars;

use async_graphql::dynamic::{Object, Scalar, Schema, SchemaError};
use domain::ports::repository::EventQueryRepository;
use schema::EventSchema;
use shared::DomainError;
use std::sync::Arc;

pub(crate) type Reader = Arc<dyn EventQueryRepository>;

// GraphQL type names shared across the submodules.
const BIG_INT: &str = "BigInt";
const JSON_SCALAR: &str = "JSON";
const FILTER_INPUT: &str = "FilterInput";
const OP_ENUM: &str = "Op";
const ORDER_ENUM: &str = "OrderDir";

/// Build the read schema from every event + aux `EventSchema`. `max_complexity`/
/// `max_depth` guard against unbounded queries.
pub(crate) fn build_schema(
    schemas: &[EventSchema],
    reader: Reader,
    max_complexity: usize,
    max_depth: usize,
) -> Result<Schema, SchemaError> {
    let has_type = |t: &str| schemas.iter().any(|s| s.table == t);

    let mut query = Object::new("Query");
    let mut builder = Schema::build("Query", None, None)
        .register(Scalar::new(BIG_INT))
        .register(Scalar::new(JSON_SCALAR))
        .register(inputs::op_enum())
        .register(inputs::order_enum())
        .register(inputs::filter_input())
        .data(reader);

    for s in schemas {
        // Only true events (those with a `topic0`) get the nested transaction/receipt
        // joins; call (calldata) and aux tables do not, so a decoded column can't clash
        // with the injected join name.
        let is_event = s.topic0.is_some();
        builder = builder.register(row::row_object(s, is_event, &has_type));
        // Queryable tables (those with a block-position `idx`: events + transactions)
        // get a connection type and a top-level query field. Receipts are read via tx.
        if shared::table_has_idx(&s.table) {
            builder = builder.register(connection::connection_object(&s.table));
            query = query.field(query::query_field(&s.table, is_event));
        }
    }

    builder
        .register(query)
        .limit_complexity(max_complexity)
        .limit_depth(max_depth)
        .finish()
}

/// Surface a storage error as a GraphQL error.
fn into_gql(e: DomainError) -> async_graphql::Error {
    async_graphql::Error::new(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use schema::{ColumnDef, ColumnType};
    use shared::{Page, QuerySpec};

    /// Returns empty pages / zero counts — enough to drive the resolvers without a DB.
    struct MockReader;

    #[async_trait]
    impl EventQueryRepository for MockReader {
        async fn query(&self, _: &QuerySpec) -> Result<Page<String>, DomainError> {
            Ok(Page {
                items: Vec::new(),
                end_cursor: None,
                has_next: false,
            })
        }
        async fn count(&self, _: &QuerySpec) -> Result<u64, DomainError> {
            Ok(0)
        }
    }

    fn col(name: &str, ty: ColumnType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            ty,
            indexed_hash: false,
        }
    }

    fn aux(table: &str, cols: Vec<ColumnDef>) -> EventSchema {
        EventSchema {
            table: table.into(),
            event: table.into(),
            topic0: None,
            columns: cols,
            indexed_positions: Vec::new(),
            pk_columns: vec!["chain_id".into(), "tx_id".into()],
        }
    }

    fn sample_schemas() -> Vec<EventSchema> {
        vec![
            EventSchema {
                table: "evt_token_transfer".into(),
                event: "Transfer".into(),
                topic0: Some("0xddf2".into()),
                columns: vec![
                    col("from", ColumnType::Address),
                    col("to", ColumnType::Address),
                    col("value", ColumnType::UInt(256)),
                ],
                indexed_positions: vec![0, 1],
                pk_columns: EventSchema::event_pk(),
            },
            aux(
                "transactions",
                vec![
                    col("from_addr", ColumnType::Address),
                    col("nonce", ColumnType::UInt(256)),
                ],
            ),
            aux("receipts", vec![col("status", ColumnType::Bool)]),
        ]
    }

    fn build() -> Schema {
        build_schema(&sample_schemas(), Arc::new(MockReader), 2000, 16)
            .expect("dynamic schema builds")
    }

    #[test]
    fn schema_renders_typed_tables() {
        let sdl = build().sdl();
        // Per-table object + connection + the query field; receipts has no query field.
        assert!(sdl.contains("type evt_token_transfer"));
        assert!(sdl.contains("type evt_token_transfer_connection"));
        assert!(sdl.contains("type transactions_connection"));
        assert!(!sdl.contains("type receipts_connection"));
        assert!(sdl.contains("scalar BigInt"));
        // Nested objects on the event type only.
        assert!(sdl.contains("transaction: transactions"));
        assert!(sdl.contains("receipt: receipts"));
    }

    #[tokio::test]
    async fn executes_pagination_and_filters() {
        let schema = build();
        let r = schema
            .execute(
                "{ evt_token_transfer(first: 5, orderBy: DESC, chainId: 1, \
                   where: [{column: \"from\", value: \"0xabcd\"}]) \
                   { hasNext endCursor nodes { tx_id value } } }",
            )
            .await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
    }

    #[tokio::test]
    async fn total_count_resolves() {
        let schema = build();
        let r = schema
            .execute("{ evt_token_transfer { totalCount } }")
            .await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(r.data.to_string().contains("totalCount"));
    }

    #[tokio::test]
    async fn nested_tx_selection_is_valid() {
        // Selecting the nested object exercises the look_ahead → include path.
        let schema = build();
        let r = schema
            .execute(
                "{ evt_token_transfer { nodes { transaction { nonce } receipt { status } } } }",
            )
            .await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
    }

    #[tokio::test]
    async fn unknown_field_is_rejected() {
        let schema = build();
        let r = schema.execute("{ evt_token_transfer { nope } }").await;
        assert!(!r.errors.is_empty());
    }
}
