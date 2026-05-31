//! Transport-neutral query spec for the read side.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDir {
    Asc,
    Desc,
}

/// An equality/comparison filter against a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    pub column: String,
    pub op: FilterOp,
    /// Raw value; bound as a typed parameter.
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sort {
    pub column: String,
    pub dir: SortDir,
}

/// Opaque keyset cursor encoding `(block_number, log_index)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor(pub String);

/// A normalized read query against one table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySpec {
    pub table: String,
    pub filters: Vec<Filter>,
    pub sort: Vec<Sort>,
    pub first: u32,
    pub after: Option<Cursor>,
    /// LEFT JOIN + embed the nested `transaction` object (only when the query selects it).
    pub include_tx: bool,
    /// LEFT JOIN + embed the nested `receipt` object (only when the query selects it).
    pub include_receipt: bool,
    /// Order by `(height, idx)` descending instead of ascending (newest-first).
    pub descending: bool,
}

/// A page of results plus the continuation cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub end_cursor: Option<Cursor>,
    pub has_next: bool,
}
