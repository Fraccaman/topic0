//! Shared entities: chain-neutral value objects, models, and the shared error.
//! Pure data, no I/O and no chain SDK types — the leaf crate all others depend on.

pub mod error;
pub mod model;
pub mod util;

pub use error::DomainError;
pub use model::block::{BlockMeta, TxCalldata};
pub use model::chain::{AddressBytes, ChainCaps, ChainId, Hash, Height, RecordIndex};
pub use model::query::{Cursor, Filter, FilterOp, Page, QuerySpec, Sort, SortDir};
pub use model::record::{DecodedEvent, EventRow, EventValue, RawRecord, RecordFilter, TipLog};
pub use model::spend::{CostUnits, MicroUsd, PlanProfile, RpcCall, SpendRecord};
pub use model::work::{Epoch, Lease, WorkItem, WorkKind};
pub use util::{
    hex_decode, hex_encode, is_valid_ident, table_has_idx, to_snake_case, RECEIPTS_TABLE,
};
