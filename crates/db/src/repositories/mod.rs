pub mod block_repo;
pub mod cursor_repo;
pub mod event_repo;
pub mod queue_repo;
pub mod raw_record_repo;

/// Reject unsafe table/column identifiers (defense in depth).
pub(crate) fn safe_ident(name: &str) -> Result<&str, crate::error::DbError> {
    if shared::is_valid_ident(name) {
        Ok(name)
    } else {
        Err(crate::error::DbError::Encode(format!(
            "unsafe identifier: {name}"
        )))
    }
}

/// Rows per multi-row INSERT under Postgres' 65535 bind-param cap, given columns/row.
pub(crate) fn chunk_rows(cols: usize) -> usize {
    (65535 / cols.max(1)).max(1)
}
