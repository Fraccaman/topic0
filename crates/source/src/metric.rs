//! Metric helpers for the RPC client: `RpcCall` → method label, transient-error
//! reason classification, and an RAII guard that tracks per-endpoint call count,
//! in-flight concurrency, and latency for the duration of one real RPC sub-call.

use shared::RpcCall;
use std::time::Instant;

/// JSON-RPC method label for an `RpcCall` (Prometheus `method` dimension).
pub(crate) fn method_name(call: &RpcCall) -> &'static str {
    match call {
        RpcCall::GetLogs { .. } => "eth_getLogs",
        RpcCall::BlockNumber => "eth_blockNumber",
        RpcCall::BlockByNumber { .. } => "eth_getBlockByNumber",
        RpcCall::TxByHash { .. } => "eth_getTransactionByHash",
        RpcCall::Receipt { .. } => "eth_getTransactionReceipt",
        RpcCall::LogSubscription => "eth_subscribe",
        RpcCall::Other => "other",
    }
}

/// Coarse transient-failure class for retry/failure counters.
pub(crate) fn error_reason(e: &str) -> &'static str {
    let e = e.to_ascii_lowercase();
    if e.contains("429") || e.contains("too many requests") || e.contains("limit") {
        "429"
    } else if e.contains("timeout") || e.contains("timed out") {
        "timeout"
    } else if e.contains("503") || e.contains("502") {
        "5xx"
    } else if e.contains("connection") {
        "connection"
    } else {
        "other"
    }
}

/// Tracks one in-flight RPC sub-call: increments `rpc_in_flight` on creation and, on
/// drop, decrements it and records `rpc_call_duration_seconds`.
pub(crate) struct CallGuard {
    chain: String,
    method: &'static str,
    started: Instant,
}

impl CallGuard {
    pub(crate) fn new(chain: String, method: &'static str) -> Self {
        metrics::counter!("rpc_calls_total", "chain_id" => chain.clone(), "method" => method)
            .increment(1);
        metrics::gauge!("rpc_in_flight", "chain_id" => chain.clone(), "method" => method)
            .increment(1.0);
        Self {
            chain,
            method,
            started: Instant::now(),
        }
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        metrics::gauge!("rpc_in_flight", "chain_id" => self.chain.clone(), "method" => self.method)
            .decrement(1.0);
        metrics::histogram!("rpc_call_duration_seconds", "chain_id" => self.chain.clone(), "method" => self.method)
            .record(self.started.elapsed().as_secs_f64());
    }
}
