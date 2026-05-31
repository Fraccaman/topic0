use shared::{CostUnits, MicroUsd, RpcCall};

/// Maps billable RPC calls to provider-native units and money.
pub trait CostModel: Send + Sync {
    /// Cost of a call as `(provider units, money)` — e.g. Alchemy CU + micro-USD.
    fn cost(&self, call: &RpcCall) -> (CostUnits, MicroUsd);
    /// Money per unit as `(numerator, denominator)` micro-USD — lets the ledger bill
    /// quota-gated overage from cumulative units. `(0, 1)` on free backends.
    fn rate(&self) -> (u64, u64);
    /// Stable identifier (e.g. "alchemy:free").
    fn name(&self) -> &str;
}
