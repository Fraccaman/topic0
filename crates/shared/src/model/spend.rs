//! Spend / pricing entities — billable calls, units, money, and plan limits.

use crate::model::chain::ChainId;
use serde::{Deserialize, Serialize};

/// A billable unit of RPC work; `CostModel` maps it to units/money.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcCall {
    GetLogs { blocks: u64, results: u64 },
    BlockNumber,
    BlockByNumber { count: u64, full: bool },
    TxByHash { count: u64 },
    Receipt { count: u64 },
    LogSubscription,
    Other,
}

/// Provider-native cost units (Alchemy CU, QuickNode credits, …).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CostUnits(pub u64);

/// Money in micro-USD (1e-6 USD); avoids floats.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MicroUsd(pub u64);

/// A single accounted spend event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendRecord {
    pub chain_id: ChainId,
    pub units: CostUnits,
    pub micro_usd: MicroUsd,
}

/// Provider-plan hard limits the client self-tunes against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanProfile {
    pub max_rps: u32,
    pub max_cu_per_sec: Option<u32>,
    pub max_batch: u32,
    pub max_getlogs_blocks: u32,
    pub max_getlogs_results: u32,
    pub monthly_quota: Option<u64>,
}

impl Default for PlanProfile {
    fn default() -> Self {
        Self {
            max_rps: 25,
            max_cu_per_sec: None,
            max_batch: 100,
            max_getlogs_blocks: 2000,
            max_getlogs_results: 10_000,
            monthly_quota: None,
        }
    }
}
