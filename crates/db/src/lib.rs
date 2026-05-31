//! Write side: Postgres pool, base schema, repositories, and the atomic
//! `UnitOfWork` ingest commit. Runtime-checked queries (no DATABASE_URL at build).

pub mod base_schema;
pub mod encode;
pub mod error;
pub mod pool;
pub mod repositories;
pub mod unit_of_work;

pub use error::DbError;
pub use pool::connect;
pub use unit_of_work::PgUnitOfWork;

pub use repositories::block_repo::PgBlockRepository;
pub use repositories::cursor_repo::PgCursorRepository;
pub use repositories::event_repo::PgEventRepository;
pub use repositories::queue_repo::PgQueueRepository;
pub use repositories::raw_record_repo::PgRawRecordRepository;

pub type PgPool = sqlx::PgPool;
