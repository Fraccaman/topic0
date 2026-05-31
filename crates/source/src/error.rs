use thiserror::Error;

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("rpc transport error: {0}")]
    Transport(String),
    #[error("rpc returned malformed data: {0}")]
    Malformed(String),
    #[error("provider kind '{0}' unknown")]
    UnknownKind(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl From<SourceError> for shared::DomainError {
    fn from(e: SourceError) -> Self {
        shared::DomainError::Source(e.to_string())
    }
}
