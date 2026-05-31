//! Render the schema IR → Postgres DDL. The logical→Postgres type mapping lives in
//! `sql_pg` (shared with `db-query`).

use schema::EventSchema;
use sql_pg::pg_type;

/// All DDL for one table, in apply order: create → add missing columns → indexes →
/// foreign keys. Every statement is guarded (`IF NOT EXISTS`) so re-running is a no-op.
pub fn table_statements(schema: &EventSchema) -> Vec<String> {
    let mut out = vec![create_table(schema)];
    out.extend(add_columns(schema));
    out.extend(create_indexes(schema));
    out.extend(foreign_keys(schema));
    out
}

/// Resolved primary key: declared `pk_columns`, else the standard event PK.
fn pk(schema: &EventSchema) -> Vec<String> {
    if schema.pk_columns.is_empty() {
        EventSchema::event_pk()
    } else {
        schema.pk_columns.clone()
    }
}

/// `CREATE TABLE IF NOT EXISTS` — meta columns + decoded columns + declared PK.
/// Tables without a block-position drop the `idx` meta column (see
/// [`shared::table_has_idx`]).
pub fn create_table(schema: &EventSchema) -> String {
    let mut cols = String::new();
    // `idx` is a meta column for every table except the receipts FK extension.
    if shared::table_has_idx(&schema.table) {
        cols.push_str(",\n  idx        bigint NOT NULL");
    }
    for c in &schema.columns {
        cols.push_str(&format!(",\n  \"{}\" {}", c.name, pg_type(c.ty)));
    }
    let pk_list = pk(schema)
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  \
           chain_id   bigint NOT NULL,\n  \
           height     bigint NOT NULL,\n  \
           block_hash bytea  NOT NULL,\n  \
           block_time timestamptz,\n  \
           tx_id      bytea  NOT NULL{cols},\n  \
           PRIMARY KEY ({pk_list})\n)",
        schema.table
    )
}

/// Foreign-key constraints for a table. `receipts` references its transaction so an
/// orphan receipt can't exist and a reorg delete of a transaction cascades. Guarded
/// so re-running `migrate` is idempotent; transactions is created first (aux order),
/// so its PK target exists.
pub fn foreign_keys(schema: &EventSchema) -> Vec<String> {
    if schema.table != shared::RECEIPTS_TABLE {
        return Vec::new();
    }
    vec!["DO $$ BEGIN \
           IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'receipts_tx_fk') THEN \
             ALTER TABLE receipts ADD CONSTRAINT receipts_tx_fk \
               FOREIGN KEY (chain_id, tx_id) REFERENCES transactions (chain_id, tx_id) \
               ON DELETE CASCADE; \
           END IF; \
         END $$;"
        .to_string()]
}

/// Secondary indexes for a typed table: the `(chain_id, tx_id)` join key plus one
/// btree per indexed (frequently-filtered) param. Aux tables whose PK already leads
/// with `(chain_id, tx_id)` skip the redundant join index.
pub fn create_indexes(schema: &EventSchema) -> Vec<String> {
    let t = &schema.table;
    let pk = pk(schema);
    let mut out = Vec::new();

    // Join key for tx/receipt enrichment — skip if it duplicates the PK prefix.
    if pk.first().map(String::as_str) != Some("chain_id")
        || pk.get(1).map(String::as_str) != Some("tx_id")
    {
        out.push(format!(
            "CREATE INDEX IF NOT EXISTS {t}_tx ON {t} (chain_id, tx_id)"
        ));
    }

    // Indexed params are the columns clients filter on (e.g. ERC-20 from/to).
    // `indexed_positions` indexes into `columns` 1:1 (same build order). `INCLUDE (idx)`
    // covers the keyset/order column so a filtered scan stays index-only (no heap fetch
    // for the page boundary). Event tables always carry `idx`.
    for &pos in &schema.indexed_positions {
        if let Some(c) = schema.columns.get(pos) {
            out.push(format!(
                "CREATE INDEX IF NOT EXISTS {t}_{col} ON {t} (chain_id, \"{col}\", height) INCLUDE (idx)",
                col = c.name
            ));
        }
    }
    out
}

/// `ADD COLUMN IF NOT EXISTS` for each decoded column.
pub fn add_columns(schema: &EventSchema) -> Vec<String> {
    schema
        .columns
        .iter()
        .map(|c| {
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \"{}\" {}",
                schema.table,
                c.name,
                pg_type(c.ty)
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{ColumnDef, ColumnType, EventSchema};

    fn transfer_schema() -> EventSchema {
        EventSchema {
            table: "evt_erc20_transfer".into(),
            event: "Transfer".into(),
            topic0: Some("0xddf2".into()),
            columns: vec![
                ColumnDef {
                    name: "from".into(),
                    ty: ColumnType::Address,
                    indexed_hash: false,
                },
                ColumnDef {
                    name: "to".into(),
                    ty: ColumnType::Address,
                    indexed_hash: false,
                },
                ColumnDef {
                    name: "value".into(),
                    ty: ColumnType::UInt(256),
                    indexed_hash: false,
                },
            ],
            indexed_positions: vec![0, 1],
            pk_columns: EventSchema::event_pk(),
        }
    }

    #[test]
    fn create_table_has_meta_and_typed_columns() {
        let sql = create_table(&transfer_schema());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS evt_erc20_transfer"));
        assert!(sql.contains("PRIMARY KEY (\"chain_id\", \"height\", \"idx\")"));
        assert!(sql.contains("\"value\" numeric(78,0)"));
        assert!(sql.contains("\"from\" bytea"));
    }

    #[test]
    fn indexes_cover_join_key_and_indexed_params() {
        let idx = create_indexes(&transfer_schema());
        // tx join key + the two indexed params (from, to); value is not indexed.
        assert_eq!(idx.len(), 3);
        assert!(idx
            .iter()
            .any(|s| s.contains("evt_erc20_transfer_tx ON evt_erc20_transfer (chain_id, tx_id)")));
        assert!(idx
            .iter()
            .any(|s| s.contains("(chain_id, \"from\", height) INCLUDE (idx)")));
        assert!(idx
            .iter()
            .any(|s| s.contains("(chain_id, \"to\", height) INCLUDE (idx)")));
        assert!(idx.iter().all(|s| s.contains("IF NOT EXISTS")));
    }

    #[test]
    fn aux_table_skips_redundant_tx_index() {
        let mut s = transfer_schema();
        s.table = "transactions".into();
        s.pk_columns = vec!["chain_id".into(), "tx_id".into()];
        s.indexed_positions.clear();
        // PK already leads (chain_id, tx_id); no indexed params → no indexes.
        assert!(create_indexes(&s).is_empty());
    }

    #[test]
    fn receipts_table_omits_idx_and_emits_fk() {
        let mut s = transfer_schema();
        s.table = "receipts".into();
        s.pk_columns = vec!["chain_id".into(), "tx_id".into()];
        let sql = create_table(&s);
        assert!(!sql.contains("idx")); // no block-position column
        assert!(sql.contains("PRIMARY KEY (\"chain_id\", \"tx_id\")"));

        let fks = foreign_keys(&s);
        assert_eq!(fks.len(), 1);
        assert!(fks[0].contains("receipts_tx_fk"));
        assert!(fks[0].contains("REFERENCES transactions (chain_id, tx_id)"));
        assert!(fks[0].contains("ON DELETE CASCADE"));
        // Non-receipts tables get no FK and keep idx.
        assert!(foreign_keys(&transfer_schema()).is_empty());
        assert!(create_table(&transfer_schema()).contains("idx        bigint NOT NULL"));
    }

    #[test]
    fn add_columns_are_idempotent() {
        let stmts = add_columns(&transfer_schema());
        assert_eq!(stmts.len(), 3);
        assert!(stmts[2].contains("ADD COLUMN IF NOT EXISTS \"value\" numeric(78,0)"));
    }

    #[test]
    fn aux_table_uses_declared_pk() {
        let mut s = transfer_schema();
        s.table = "transactions".into();
        s.pk_columns = vec!["chain_id".into(), "tx_id".into()];
        let sql = create_table(&s);
        assert!(sql.contains("PRIMARY KEY (\"chain_id\", \"tx_id\")"));
    }

    #[test]
    fn add_columns_stay_idempotent_as_schema_grows() {
        // New column appended → still emits IF NOT EXISTS for all.
        let mut s = transfer_schema();
        s.columns.push(ColumnDef {
            name: "fee".into(),
            ty: ColumnType::UInt(256),
            indexed_hash: false,
        });
        let stmts = add_columns(&s);
        assert_eq!(stmts.len(), 4);
        assert!(stmts
            .iter()
            .all(|st| st.contains("ADD COLUMN IF NOT EXISTS")));
        assert!(stmts[3].contains("\"fee\" numeric(78,0)"));
    }

    #[test]
    fn table_statements_orders_create_then_columns_then_indexes() {
        let stmts = table_statements(&transfer_schema());
        // create_table + 3 add_columns + 3 indexes, no FK (not receipts).
        assert_eq!(stmts.len(), 7);
        assert!(stmts[0].starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(stmts[1].contains("ADD COLUMN IF NOT EXISTS"));
        assert!(stmts.last().unwrap().contains("CREATE INDEX IF NOT EXISTS"));
    }

    #[test]
    fn receipts_table_statements_end_with_fk() {
        let mut s = transfer_schema();
        s.table = shared::RECEIPTS_TABLE.into();
        s.pk_columns = vec!["chain_id".into(), "tx_id".into()];
        let stmts = table_statements(&s);
        assert!(stmts[0].starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(stmts.last().unwrap().contains("receipts_tx_fk"));
    }

    #[test]
    fn no_decoded_columns_still_valid_table() {
        // Aux table with only meta columns.
        let mut s = transfer_schema();
        s.columns.clear();
        let sql = create_table(&s);
        assert!(!sql.contains(",,")); // no double comma
        assert!(sql.contains("idx"));
        assert!(sql.contains("PRIMARY KEY"));
        assert!(add_columns(&s).is_empty());
    }
}
