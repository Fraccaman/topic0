//! ABI→DDL engine: generate `CREATE TABLE` / `ADD COLUMN` from event schemas,
//! diff against the live DB, and apply. Impls `domain::Migrator`. CLI-time only.

mod ddl;
mod error;
mod migrator;

pub use error::MigrateError;
pub use migrator::PgMigrator;
