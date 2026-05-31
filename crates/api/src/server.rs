//! HTTP server: mounts the dynamic GraphQL schema (`graphql.rs`) on axum with a
//! playground and graceful shutdown. The one `ApiServer` implementation built.

use crate::graphql::{build_schema, Reader};
use async_graphql_axum::GraphQL;
use async_trait::async_trait;
use axum::{response::Html, routing::get, Router};
use domain::ports::api_server::ApiServer;
use schema::EventSchema;
use shared::DomainError;
use std::net::SocketAddr;

/// GraphQL impl of `ApiServer`. Holds the table schemas (the dynamic GraphQL schema is
/// built per-table from them) and the query guard limits.
pub struct GraphqlApiServer {
    schemas: Vec<EventSchema>,
    max_complexity: usize,
    max_depth: usize,
}

impl GraphqlApiServer {
    /// `schemas` = every event + aux table the API serves (same source as the reader).
    pub fn new(schemas: Vec<EventSchema>) -> Self {
        Self {
            schemas,
            max_complexity: 2000,
            max_depth: 16,
        }
    }
}

async fn playground() -> Html<String> {
    Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

#[async_trait]
impl ApiServer for GraphqlApiServer {
    async fn serve(self, reader: Reader, addr: SocketAddr) -> Result<(), DomainError> {
        let schema = build_schema(&self.schemas, reader, self.max_complexity, self.max_depth)
            .map_err(|e| DomainError::Invalid(format!("graphql schema: {e}")))?;

        let app = Router::new().route(
            "/graphql",
            get(playground).post_service(GraphQL::new(schema)),
        );

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| DomainError::Invalid(format!("bind {addr}: {e}")))?;
        tracing::info!(%addr, "GraphQL API listening at /graphql");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| DomainError::Invalid(format!("serve: {e}")))?;
        tracing::info!("GraphQL API stopped");
        Ok(())
    }
}

/// Completes on SIGINT or SIGTERM, triggering graceful drain.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
    tracing::info!("shutdown signal received");
}
