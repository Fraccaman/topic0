//! Queue work-item entities (pointers, not payloads).

use crate::model::chain::{ChainId, Height};
use serde::{Deserialize, Serialize};

/// Monotonic fence; bumped on reorg to invalidate stale in-flight items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Epoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkKind {
    Backfill,
    Tip,
    Reorg,
}

/// A height range to (re)process for one chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub chain_id: ChainId,
    pub from: Height,
    pub to: Height,
    pub kind: WorkKind,
    pub epoch: Epoch,
}

/// A leased work item with the handle the queue needs to ack/reclaim.
/// `lease_seq` fences a stale ack: reclaim + re-lease advances the seq, so an old
/// lease's ack no longer matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease<T> {
    pub id: i64,
    pub lease_seq: i64,
    pub item: T,
}
