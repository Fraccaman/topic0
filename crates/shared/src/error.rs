//! The shared error every port surfaces. Adapters fold their errors into this at
//! the boundary; no `sqlx`/`reqwest` types leak inward.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("source/RPC error: {0}")]
    Source(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("schema/migration error: {0}")]
    Schema(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid input: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, DomainError>;
