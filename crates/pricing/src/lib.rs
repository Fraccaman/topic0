//! Pure cost math: per-provider `CostModel` impls and the `SpendLedger`. No I/O.

pub mod alchemy;
pub mod free_node;
pub mod ledger;
pub mod quicknode;

pub use alchemy::AlchemyCost;
pub use free_node::FreeNodeCost;
pub use ledger::SpendLedger;
pub use quicknode::QuickNodeCost;

use domain::CostModel;

/// Build the `CostModel` for a source `kind`. Caps come from config `[limits]`.
pub fn for_kind(kind: &str) -> Option<Box<dyn CostModel>> {
    match kind {
        "alchemy" => Some(Box::new(AlchemyCost::new())),
        "quicknode" => Some(Box::new(QuickNodeCost::new())),
        "free_node" | "generic_rpc" => Some(Box::new(FreeNodeCost::new(kind))),
        _ => None,
    }
}
