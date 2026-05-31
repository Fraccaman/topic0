use crate::ports::repository::EventQueryRepository;
use async_trait::async_trait;
use shared::DomainError;
use std::net::SocketAddr;
use std::sync::Arc;

/// Protocol-agnostic read API seam over `EventQueryRepository`.
#[async_trait]
pub trait ApiServer: Send + Sync {
    async fn serve(
        self,
        reader: Arc<dyn EventQueryRepository>,
        addr: SocketAddr,
    ) -> Result<(), DomainError>;
}
