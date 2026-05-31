//! Business logic over ports. No SQL, no HTTP.

pub mod decoding;
pub mod ingestion;
pub mod reorg;
pub mod worker;

pub use decoding::decode_range;
pub use ingestion::{FetchedRange, IngestOutcome, IngestionService};
pub use reorg::ReorgService;
pub use worker::DecodeWorker;
