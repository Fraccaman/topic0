//! `indexer-query` — read-only GraphQL server over the indexed event tables.

use anyhow::{Context, Result};
use api::GraphqlApiServer;
use clap::Parser;
use domain::ports::api_server::ApiServer;
use domain::ports::repository::EventQueryRepository;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "indexer-query", about = "GraphQL read API for the EVM indexer")]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: String,
    /// Override the listen address from config.
    #[arg(long)]
    listen: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let cfg = config::load(&cli.config)?;
    if let Some(addr) = &cfg.indexer.metrics_listen {
        observability::install(addr.parse()?)?;
    }
    let listen = cli.listen.unwrap_or(cfg.query.listen.clone());
    let addr = listen
        .parse()
        .with_context(|| format!("bad listen addr {listen}"))?;

    // Schemas drive filter validation and the typed GraphQL schema: every event +
    // aux table the API may serve, derived from the same config the migrator uses.
    let base = std::path::Path::new(&cli.config)
        .parent()
        .unwrap_or_else(|| std::path::Path::new(""));
    // Tables are chain-agnostic (chain_id is a column), so the same contract on
    // multiple chains yields identical table schemas; dedup by table name.
    let mut seen = std::collections::HashSet::new();
    let mut schemas = Vec::new();
    for chain in &cfg.chains {
        let decoder = source::build_decoder(chain, base)?;
        for s in decoder.schemas() {
            if seen.insert(s.table.clone()) {
                schemas.push(s);
            }
        }
    }

    let pool = db::connect(&cfg.database.url, cfg.database.max_conns).await?;
    let pg = db_query::PgEventQueryRepository::new(pool, &schemas);
    // Optional in-process TTL cache over the read path (`[query] cache_ttl_ms`, 0 = off).
    let reader: Arc<dyn EventQueryRepository> = if cfg.query.cache_ttl_ms > 0 {
        let ttl = std::time::Duration::from_millis(cfg.query.cache_ttl_ms);
        tracing::info!(ttl_ms = cfg.query.cache_ttl_ms, "read cache enabled");
        Arc::new(db_query::CachingEventQueryRepository::new(
            Arc::new(pg),
            ttl,
        ))
    } else {
        Arc::new(pg)
    };

    // Reader borrowed `&schemas`; the server now owns them to build the typed schema.
    GraphqlApiServer::new(schemas).serve(reader, addr).await?;
    Ok(())
}
