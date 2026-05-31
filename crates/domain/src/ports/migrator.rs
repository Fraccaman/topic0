use async_trait::async_trait;
use schema::EventSchema;
use shared::DomainError;

/// A planned schema change (diff result), rendered as ordered DDL statements.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaPlan {
    /// Human-readable summary lines.
    pub summary: Vec<String>,
    /// Ordered DDL statements to apply.
    pub statements: Vec<String>,
    /// True if any statement is destructive (type change / drop).
    pub destructive: bool,
}

impl SchemaPlan {
    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }
}

/// Schema migration port — diff desired (config ABIs) vs live DB, apply DDL.
#[async_trait]
pub trait Migrator: Send + Sync {
    /// Diff config schemas against the live DB → a plan (no side effects).
    async fn plan(&self, desired: &[EventSchema]) -> Result<SchemaPlan, DomainError>;

    /// Apply a plan. `allow_destructive` gates type changes / drops.
    async fn apply(&self, plan: &SchemaPlan, allow_destructive: bool) -> Result<(), DomainError>;

    /// Preflight: does the live schema already match `desired`?
    async fn is_in_sync(&self, desired: &[EventSchema]) -> Result<bool, DomainError>;
}
