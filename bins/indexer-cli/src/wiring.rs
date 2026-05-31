//! Composition root: build services from config + a pool. The only place that wires
//! concrete adapters (`db`, `source`, `migrator`) into the service layer.

use anyhow::{Context, Result};
use config::{ChainCfg, Config};
use domain::ports::chain_source::ChainSource;
use domain::ports::decoder::Decoder;
use domain::ports::migrator::Migrator;
use schema::EventSchema;
use shared::ChainId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Load config and connect the pool — the common head of every data-path command
/// (backfill/resync/follow/decode/run).
pub(crate) async fn boot(config_path: &str) -> Result<(Config, db::PgPool)> {
    let cfg = config::load(config_path)?;
    let pool = db::connect(&cfg.database.url, cfg.database.max_conns).await?;
    Ok((cfg, pool))
}

/// Directory holding ABI files referenced by config (its parent dir).
pub(crate) fn base_dir(config_path: &str) -> PathBuf {
    Path::new(config_path)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default()
}

pub(crate) fn find_chain(cfg: &Config, id: u64) -> Result<&ChainCfg> {
    cfg.chains
        .iter()
        .find(|c| c.id == id)
        .with_context(|| format!("chain {id} not in config"))
}

pub(crate) fn new_ingest(
    pool: &db::PgPool,
    source: Box<dyn ChainSource>,
) -> services::IngestionService {
    services::IngestionService::new(source, Box::new(db::PgUnitOfWork::new(pool.clone())))
}

pub(crate) fn build_reorg(pool: &db::PgPool, tables: Vec<String>) -> services::ReorgService {
    services::ReorgService::new(
        Box::new(db::PgBlockRepository::new(pool.clone())),
        Box::new(db::PgUnitOfWork::new(pool.clone())),
        tables,
    )
}

/// Boot preflight: the live schema must already match the config.
pub(crate) async fn preflight(pool: &db::PgPool, schemas: &[EventSchema]) -> Result<()> {
    let in_sync = migrator::PgMigrator::new(pool.clone())
        .is_in_sync(schemas)
        .await?;
    if !in_sync {
        anyhow::bail!("schema out of sync with config; run `indexer migrate`");
    }
    Ok(())
}

/// Build a decoder per configured chain (no RPC).
pub(crate) fn build_decoders(
    cfg: &Config,
    base: &Path,
) -> Result<HashMap<ChainId, Arc<dyn Decoder>>> {
    let mut map = HashMap::new();
    for chain in &cfg.chains {
        let decoder: Arc<dyn Decoder> = Arc::from(source::build_decoder(chain, base)?);
        map.insert(ChainId(chain.id), decoder);
    }
    Ok(map)
}

pub(crate) fn new_worker(
    pool: &db::PgPool,
    decoders: HashMap<ChainId, Arc<dyn Decoder>>,
) -> services::DecodeWorker {
    services::DecodeWorker::new(
        Box::new(db::PgQueueRepository::new(pool.clone())),
        Box::new(db::PgEventRepository::new(pool.clone())),
        Box::new(db::PgRawRecordRepository::new(pool.clone())),
        Box::new(db::PgBlockRepository::new(pool.clone())),
        decoders,
    )
}
