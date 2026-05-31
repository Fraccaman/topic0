//! EVM chain adapter: `ChainSource` over alloy with per-call spend metering + rate
//! limiter + `EvmDecoder`, and the `build_chain` registry. Cost math in `pricing`,
//! ABI decode in `abi`.

pub mod batch;
pub mod client;
pub mod decoder;
pub mod enrichment;
pub mod error;
pub mod limiter;
pub mod map;
pub mod metric;
pub mod registry;
pub mod retry;

pub use client::RpcLogSource;
pub use error::SourceError;
pub use registry::{build_chain, build_decoder, BuiltChain};
