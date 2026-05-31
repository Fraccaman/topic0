//! Alchemy compute-unit pricing. CU per method from the published table.

use domain::CostModel;
use shared::{CostUnits, MicroUsd, RpcCall};

/// Compute units per method (Alchemy published rates).
const CU_GET_LOGS: u64 = 75;
const CU_BLOCK_NUMBER: u64 = 10;
const CU_BLOCK_BY_NUMBER: u64 = 16;
const CU_TX_BY_HASH: u64 = 15;
const CU_RECEIPT: u64 = 15;
const CU_SUBSCRIBE: u64 = 10;

/// PAYG overage ≈ $1.20 / 1M CU = 1.2 micro-USD per CU.
const MICRO_USD_PER_CU_NUM: u64 = 12;
const MICRO_USD_PER_CU_DEN: u64 = 10;

pub struct AlchemyCost;

impl AlchemyCost {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AlchemyCost {
    fn default() -> Self {
        Self::new()
    }
}

impl CostModel for AlchemyCost {
    fn cost(&self, call: &RpcCall) -> (CostUnits, MicroUsd) {
        let u = match *call {
            RpcCall::GetLogs { .. } => CU_GET_LOGS,
            RpcCall::BlockNumber => CU_BLOCK_NUMBER,
            RpcCall::BlockByNumber { count, .. } => CU_BLOCK_BY_NUMBER * count.max(1),
            RpcCall::TxByHash { count } => CU_TX_BY_HASH * count.max(1),
            RpcCall::Receipt { count } => CU_RECEIPT * count.max(1),
            RpcCall::LogSubscription => CU_SUBSCRIBE,
            RpcCall::Other => 0,
        };
        (
            CostUnits(u),
            MicroUsd(u * MICRO_USD_PER_CU_NUM / MICRO_USD_PER_CU_DEN),
        )
    }

    fn rate(&self) -> (u64, u64) {
        (MICRO_USD_PER_CU_NUM, MICRO_USD_PER_CU_DEN)
    }

    fn name(&self) -> &str {
        "alchemy"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cu(call: RpcCall) -> u64 {
        AlchemyCost::new().cost(&call).0 .0
    }

    #[test]
    fn cu_per_method_matches_published_rates() {
        assert_eq!(
            cu(RpcCall::GetLogs {
                blocks: 2000,
                results: 5
            }),
            75
        );
        assert_eq!(
            cu(RpcCall::BlockByNumber {
                count: 1,
                full: true
            }),
            16
        );
        assert_eq!(cu(RpcCall::TxByHash { count: 1 }), 15);
        assert_eq!(cu(RpcCall::Receipt { count: 1 }), 15);
        assert_eq!(cu(RpcCall::BlockNumber), 10);
        assert_eq!(cu(RpcCall::LogSubscription), 10);
        assert_eq!(cu(RpcCall::Other), 0);
    }

    #[test]
    fn count_scales_units_with_floor_of_one() {
        assert_eq!(cu(RpcCall::Receipt { count: 20 }), 15 * 20);
        // count = 0 still bills one method.
        assert_eq!(
            cu(RpcCall::BlockByNumber {
                count: 0,
                full: true
            }),
            16
        );
    }

    #[test]
    fn micro_usd_is_1_2x_cu() {
        // 75 CU × 1.2 = 90 micro-USD.
        let (_, money) = AlchemyCost::new().cost(&RpcCall::GetLogs {
            blocks: 1,
            results: 1,
        });
        assert_eq!(money.0, 90);
    }
}
