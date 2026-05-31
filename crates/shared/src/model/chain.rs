//! Core value objects — chain-neutral newtypes. No chain SDK types here.

use serde::{Deserialize, Serialize};

macro_rules! u64_newtype {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl $name {
            #[inline]
            pub const fn new(v: u64) -> Self { Self(v) }
            #[inline]
            pub const fn get(self) -> u64 { self.0 }
        }
        impl From<u64> for $name { #[inline] fn from(v: u64) -> Self { Self(v) } }
        impl From<$name> for u64 { #[inline] fn from(v: $name) -> Self { v.0 } }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

u64_newtype!(
    /// Chain identifier (e.g. 1 = mainnet, 8453 = base, a chosen id for Solana).
    ChainId
);
u64_newtype!(
    /// Block height — EVM block number, Solana slot.
    Height
);
u64_newtype!(
    /// Record index within a height — EVM log_index, Solana record/instruction index.
    RecordIndex
);

/// Variable-length hash (block hash / tx id). 32 bytes on EVM, 32/64 on Solana.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash(pub Vec<u8>);

impl Hash {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
    pub fn from_slice(s: &[u8]) -> Self {
        Self(s.to_vec())
    }
    pub fn hex(&self) -> String {
        format!("0x{}", crate::util::hex_encode(&self.0))
    }
}

/// Variable-length address — EVM contract (20 bytes) or Solana program pubkey (32).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AddressBytes(pub Vec<u8>);

impl AddressBytes {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
    pub fn from_slice(s: &[u8]) -> Self {
        Self(s.to_vec())
    }
}

/// Per-chain capabilities the pipeline branches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainCaps {
    /// True if the chain can reorg (rollback on hash divergence).
    pub supports_reorg: bool,
    /// True if the source has a live tip subscription (WS); tip loop uses pushed
    /// logs instead of polling.
    pub supports_subscribe: bool,
}

impl Default for ChainCaps {
    fn default() -> Self {
        Self {
            supports_reorg: true,
            supports_subscribe: false,
        }
    }
}
