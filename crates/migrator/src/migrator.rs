//! `PgMigrator`: the Postgres `domain::Migrator` impl. Diffs config schemas against
//! the live DB (`information_schema`), renders additive DDL via [`ddl`], and applies.

use async_trait::async_trait;
use domain::ports::migrator::{Migrator, SchemaPlan};
use schema::EventSchema;
use shared::DomainError;
use sqlx::Row;

use crate::ddl;
use crate::error::MigrateError;

pub struct PgMigrator {
    pool: sqlx::PgPool,
}

impl PgMigrator {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DomainError> {
        let row = sqlx::query(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = $1)",
        )
        .bind(table)
        .fetch_one(&self.pool)
        .await
        .map_err(MigrateError::from)?;
        Ok(row.get::<bool, _>(0))
    }
}

#[async_trait]
impl Migrator for PgMigrator {
    async fn plan(&self, desired: &[EventSchema]) -> Result<SchemaPlan, DomainError> {
        let mut plan = SchemaPlan::default();
        for schema in desired {
            if !self.table_exists(&schema.table).await? {
                plan.summary.push(format!("CREATE TABLE {}", schema.table));
            }
            // Additive only, never destructive.
            plan.statements.extend(ddl::table_statements(schema));
        }
        Ok(plan)
    }

    async fn apply(&self, plan: &SchemaPlan, allow_destructive: bool) -> Result<(), DomainError> {
        if plan.destructive && !allow_destructive {
            return Err(DomainError::Schema(
                "plan contains destructive changes; rerun with --allow-destructive".into(),
            ));
        }
        for stmt in &plan.statements {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(MigrateError::from)?;
        }
        Ok(())
    }

    async fn is_in_sync(&self, desired: &[EventSchema]) -> Result<bool, DomainError> {
        for schema in desired {
            if !self.table_exists(&schema.table).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
