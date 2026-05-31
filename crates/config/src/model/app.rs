//! App-level config: the top-level `Config` plus global indexer / database / queue /
//! query sections.

use super::ChainCfg;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub indexer: IndexerCfg,
    pub database: DatabaseCfg,
    #[serde(default)]
    pub queue: QueueCfg,
    #[serde(default)]
    pub query: QueryCfg,
    #[serde(default)]
    pub chains: Vec<ChainCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexerCfg {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    /// Block ranges processed concurrently in the ingest loop.
    #[serde(default = "default_range_concurrency")]
    pub range_concurrency: usize,
    /// Concurrent block/receipt RPCs per range in `fetch_aux`.
    #[serde(default = "default_aux_concurrency")]
    pub aux_concurrency: usize,
    /// Tip poll cadence in seconds for `run`/`follow`. CLI `--interval` overrides.
    #[serde(default = "default_tip_interval_secs")]
    pub tip_interval_secs: u64,
    /// Prometheus `/metrics` listen address; omit to disable the exporter.
    #[serde(default)]
    pub metrics_listen: Option<String>,
}

impl Default for IndexerCfg {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            batch_size: default_batch_size(),
            range_concurrency: default_range_concurrency(),
            aux_concurrency: default_aux_concurrency(),
            tip_interval_secs: default_tip_interval_secs(),
            metrics_listen: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseCfg {
    pub url: String,
    #[serde(default = "default_max_conns")]
    pub max_conns: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueCfg {
    #[serde(default = "default_queue_kind")]
    pub kind: String,
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    #[serde(default = "default_poll_idle_ms")]
    pub poll_idle_ms: u64,
}

impl Default for QueueCfg {
    fn default() -> Self {
        Self {
            kind: default_queue_kind(),
            poll_ms: default_poll_ms(),
            poll_idle_ms: default_poll_idle_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryCfg {
    #[serde(default = "default_api")]
    pub api: String,
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_expose")]
    pub expose: String,
    /// Read-cache TTL in milliseconds; `0` disables caching.
    #[serde(default = "default_cache_ttl_ms")]
    pub cache_ttl_ms: u64,
}

impl Default for QueryCfg {
    fn default() -> Self {
        Self {
            api: default_api(),
            listen: default_listen(),
            expose: default_expose(),
            cache_ttl_ms: default_cache_ttl_ms(),
        }
    }
}

fn default_log_level() -> String {
    "info".into()
}
fn default_batch_size() -> u32 {
    500
}
fn default_max_conns() -> u32 {
    16
}
fn default_queue_kind() -> String {
    "postgres".into()
}
fn default_poll_ms() -> u64 {
    50
}
fn default_poll_idle_ms() -> u64 {
    1000
}
fn default_api() -> String {
    "graphql".into()
}
fn default_listen() -> String {
    "0.0.0.0:8080".into()
}
fn default_expose() -> String {
    "finalized".into()
}
fn default_cache_ttl_ms() -> u64 {
    1000
}
fn default_range_concurrency() -> usize {
    4
}
fn default_aux_concurrency() -> usize {
    8
}
fn default_tip_interval_secs() -> u64 {
    6
}
