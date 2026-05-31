//! Ports (traits) over the `shared` entities. Infra-free interfaces adapters
//! implement and services consume.

pub mod ports;

// Re-export shared entities for ergonomic `domain::RawRecord` etc.
pub use shared::*;

pub use ports::api_server::ApiServer;
pub use ports::chain_source::ChainSource;
pub use ports::cost_model::CostModel;
pub use ports::decoder::Decoder;
pub use ports::migrator::{Migrator, SchemaPlan};
pub use ports::queue_repo::QueueRepository;
pub use ports::repository::{
    BlockRepository, CursorRepository, EventQueryRepository, EventRepository, RawRecordRepository,
};
pub use ports::unit_of_work::{IngestBatch, UnitOfWork};
