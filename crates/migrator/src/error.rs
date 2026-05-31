//! Crate-local error. Wraps `sqlx::Error`; folds into `shared::DomainError` at the
//! port boundary so `sqlx` never leaks past `migrator`.

use shared::DomainError;

#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

impl From<MigrateError> for DomainError {
    fn from(e: MigrateError) -> Self {
        DomainError::Schema(e.to_string())
    }
}
