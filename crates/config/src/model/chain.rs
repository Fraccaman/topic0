//! Per-chain config: the chain itself, its source backend + provider limits, and the
//! contracts/events to index.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ChainCfg {
    pub id: u64,
    #[serde(default)]
    pub name: String,
    /// Chain family ("evm" default) — selects the adapter.
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
    pub source: SourceCfg,
    #[serde(default)]
    pub contracts: Vec<ContractCfg>,
}

impl ChainCfg {
    /// Earliest `start_block` across this chain's contracts; `None` if none set.
    pub fn start_block(&self) -> Option<u64> {
        self.contracts.iter().filter_map(|c| c.start_block).min()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceCfg {
    pub kind: String,
    pub http: String,
    #[serde(default)]
    pub ws: Option<String>,
    #[serde(default)]
    pub limits: Option<LimitsCfg>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct LimitsCfg {
    pub max_rps: Option<u32>,
    pub max_cu_per_sec: Option<u32>,
    pub max_batch: Option<u32>,
    pub max_getlogs_blocks: Option<u32>,
    pub max_getlogs_results: Option<u32>,
    pub monthly_quota_cu: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContractCfg {
    pub address: String,
    pub abi: String,
    #[serde(default)]
    pub events: Vec<String>,
    /// Subset of ABI functions whose calldata to decode (empty = none).
    #[serde(default)]
    pub functions: Vec<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub start_block: Option<u64>,
}

fn default_confirmations() -> u64 {
    12
}
fn default_kind() -> String {
    "evm".into()
}
