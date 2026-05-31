use schema::EventSchema;
use shared::{DomainError, EventRow, RawRecord, RecordFilter, TxCalldata};

/// Decodes raw records into typed rows and declares the tables it produces.
/// Bound to one chain.
pub trait Decoder: Send + Sync {
    /// Decode one record. `None` if its selector is unknown (not indexed).
    fn decode(&self, record: &RawRecord) -> Result<Option<EventRow>, DomainError>;

    /// Decode a transaction's calldata into a typed row. `None` if its
    /// `(to_addr, selector)` is not a configured function.
    fn decode_call(&self, _tx: &TxCalldata) -> Result<Option<EventRow>, DomainError> {
        Ok(None)
    }

    /// Whether this decoder has any calldata (function) tables — lets the pipeline
    /// skip the transactions read when no `functions` are configured.
    fn has_calls(&self) -> bool {
        false
    }

    /// All typed-table schemas this decoder produces (event + aux + call tables).
    fn schemas(&self) -> Vec<EventSchema>;

    /// The record filter (addresses + selectors) this decoder wants fetched.
    fn record_filter(&self) -> RecordFilter;
}
