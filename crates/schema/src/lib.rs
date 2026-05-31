//! Typed-table schema IR — the storage-neutral ABI/IDL→table contract. A builder
//! (`abi-schema`) produces it from an ABI; `sql-pg` maps `ColumnType` to Postgres for
//! the SQL backends (`migrator`, `db-query`). A zero-dependency leaf: no storage,
//! serde, or alloy.

/// Logical column type. Storage-neutral — the Postgres mapping lives in the SQL
/// backends. Trailing comments show the current Postgres rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// 20-byte address → bytea.
    Address,
    /// Solidity `uintN` (N = 8..=256) → numeric(78,0). `N` is retained for fidelity.
    UInt(u16),
    /// Solidity `intN` → numeric(78,0).
    Int(u16),
    /// → boolean.
    Bool,
    /// Dynamic `bytes`, `bytesN`, or an indexed-dynamic hash → bytea.
    Bytes,
    /// Solidity `string` → text.
    Utf8,
    /// Arrays / tuples → jsonb.
    Json,
    /// Storage-native bigint meta column (chain_id / height / idx) → bigint.
    Int64,
    /// Storage-native timestamp meta column (block_time) → timestamptz.
    Timestamp,
}

/// One decoded column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    /// True if it came from an indexed dynamic type (stored as `<name>_hash`).
    pub indexed_hash: bool,
}

/// A typed table: routing selector, columns, and primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSchema {
    pub table: String,
    /// Source event/struct name.
    pub event: String,
    /// Routing selector (hex topic0 / discriminator); None for anonymous/aux.
    pub topic0: Option<String>,
    pub columns: Vec<ColumnDef>,
    /// Positions in `columns` that are indexed (decoded from selectors).
    pub indexed_positions: Vec<usize>,
    /// Primary-key column names.
    pub pk_columns: Vec<String>,
}

impl EventSchema {
    /// Standard event-table PK.
    pub fn event_pk() -> Vec<String> {
        vec!["chain_id".into(), "height".into(), "idx".into()]
    }
}
