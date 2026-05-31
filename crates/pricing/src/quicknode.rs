//! QuickNode credit pricing. ~20 credits per standard method.

use domain::CostModel;
use shared::{CostUnits, MicroUsd, RpcCall};

const CR_STD: u64 = 20;

/// Rough marginal credit price on a paid plan (~$49 / 80M credits ≈ 0.6 micro-USD).
const MICRO_USD_PER_CR_NUM: u64 = 6;
const MICRO_USD_PER_CR_DEN: u64 = 10;

pub struct QuickNodeCost;

impl QuickNodeCost {
    pub fn new() -> Self {
        Self
    }
}

impl Default for QuickNodeCost {
    fn default() -> Self {
        Self::new()
    }
}

impl CostModel for QuickNodeCost {
    fn cost(&self, call: &RpcCall) -> (CostUnits, MicroUsd) {
        let u = match *call {
            RpcCall::GetLogs { .. } => CR_STD,
            RpcCall::BlockNumber => CR_STD,
            RpcCall::BlockByNumber { count, .. } => CR_STD * count.max(1),
            RpcCall::TxByHash { count } => CR_STD * count.max(1),
            RpcCall::Receipt { count } => CR_STD * count.max(1),
            RpcCall::LogSubscription => CR_STD,
            RpcCall::Other => 0,
        };
        (
            CostUnits(u),
            MicroUsd(u * MICRO_USD_PER_CR_NUM / MICRO_USD_PER_CR_DEN),
        )
    }

    fn rate(&self) -> (u64, u64) {
        (MICRO_USD_PER_CR_NUM, MICRO_USD_PER_CR_DEN)
    }

    fn name(&self) -> &str {
        "quicknode"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_credits_per_method() {
        let c = QuickNodeCost::new();
        assert_eq!(
            c.cost(&RpcCall::GetLogs {
                blocks: 5,
                results: 1
            })
            .0
             .0,
            20
        );
        assert_eq!(c.cost(&RpcCall::Receipt { count: 3 }).0 .0, 60);
        assert_eq!(c.cost(&RpcCall::Other).0 .0, 0);
    }

    #[test]
    fn micro_usd_is_0_6x_credits() {
        // 20 credits × 0.6 = 12 micro-USD.
        let (_, money) = QuickNodeCost::new().cost(&RpcCall::GetLogs {
            blocks: 1,
            results: 1,
        });
        assert_eq!(money.0, 12);
    }
}
