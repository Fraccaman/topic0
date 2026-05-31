//! Self-hosted / generic node — effectively free.

use domain::CostModel;
use shared::{CostUnits, MicroUsd, RpcCall};

pub struct FreeNodeCost {
    name: String,
}

impl FreeNodeCost {
    pub fn new(kind: &str) -> Self {
        Self {
            name: format!("{kind}:free"),
        }
    }
}

impl CostModel for FreeNodeCost {
    fn cost(&self, _call: &RpcCall) -> (CostUnits, MicroUsd) {
        (CostUnits(0), MicroUsd(0))
    }

    fn rate(&self) -> (u64, u64) {
        (0, 1)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_call_is_free() {
        let c = FreeNodeCost::new("anvil");
        for call in [
            RpcCall::GetLogs {
                blocks: 9,
                results: 9,
            },
            RpcCall::BlockByNumber {
                count: 99,
                full: true,
            },
            RpcCall::Receipt { count: 99 },
            RpcCall::LogSubscription,
            RpcCall::Other,
        ] {
            let (units, money) = c.cost(&call);
            assert_eq!(units.0, 0);
            assert_eq!(money.0, 0);
        }
        assert_eq!(c.name(), "anvil:free");
    }
}
