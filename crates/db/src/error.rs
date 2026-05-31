use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("encode error: {0}")]
    Encode(String),
}

impl From<DbError> for shared::DomainError {
    fn from(e: DbError) -> Self {
        shared::DomainError::Storage(e.to_string())
    }
}
