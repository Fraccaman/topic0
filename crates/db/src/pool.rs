use crate::error::DbError;
use sqlx::postgres::PgPoolOptions;

/// Build a Postgres pool from config.
pub async fn connect(url: &str, max_conns: u32) -> Result<sqlx::PgPool, DbError> {
    let pool = PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(url)
        .await?;
    Ok(pool)
}
