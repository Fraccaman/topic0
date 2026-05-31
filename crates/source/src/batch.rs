//! JSON-RPC batching for `RpcLogSource`: pack per-item calls into `max_batch` batches,
//! run `aux_concurrency` batches in flight, retry each batch as a unit, and meter
//! CU/rps per sub-call. Used by `fetch_aux`'s block/receipt fetches.

use crate::client::RpcLogSource;
use crate::error::SourceError;
use crate::retry::with_retry;
use alloy::providers::Provider;
use alloy::rpc::client::BatchRequest;
use alloy::rpc::json_rpc::{RpcRecv, RpcSend};
use futures::stream::{StreamExt, TryStreamExt};
use shared::RpcCall;

impl RpcLogSource {
    fn max_batch(&self) -> usize {
        (self.plan.max_batch as usize).max(1)
    }

    /// Provider compute-units for a call (0 on free/unmetered backends) — drives the
    /// CU-per-second limiter so CU-heavy bursts stay under the plan cap.
    pub(crate) fn cu_for(&self, call: &RpcCall) -> u32 {
        u32::try_from(self.cost_model.cost(call).0 .0).unwrap_or(u32::MAX)
    }

    /// Call `method` for every item in JSON-RPC batches of `max_batch`, with
    /// `aux_concurrency` batches in flight. Each result is returned paired with its
    /// input item — batches complete out of order, so position can't be relied on.
    pub(crate) async fn batched<I, P, R, F>(
        &self,
        items: Vec<I>,
        method: &'static str,
        mk_params: F,
    ) -> Result<Vec<(I, R)>, SourceError>
    where
        I: Clone + Send + Sync,
        P: RpcSend,
        R: RpcRecv,
        F: Fn(&I) -> P + Sync,
    {
        let mk = &mk_params;
        let chunks = items.chunks(self.max_batch()).map(<[I]>::to_vec);
        let nested: Vec<Vec<(I, R)>> = futures::stream::iter(chunks)
            .map(|chunk| async move { self.batch_chunk(chunk, method, mk).await })
            .buffer_unordered(self.aux_concurrency)
            .try_collect()
            .await?;
        Ok(nested.into_iter().flatten().collect())
    }

    /// One JSON-RPC batch round-trip, retried as a unit on transient errors. The
    /// limiter is acquired once per sub-call — a batch is one round-trip but bills
    /// per method, so rate/spend accounting is unchanged.
    async fn batch_chunk<I, P, R, F>(
        &self,
        chunk: Vec<I>,
        method: &'static str,
        mk_params: &F,
    ) -> Result<Vec<(I, R)>, SourceError>
    where
        I: Clone,
        P: RpcSend,
        R: RpcRecv,
        F: Fn(&I) -> P,
    {
        let count = chunk.len() as u64;
        metrics::histogram!("rpc_batch_size", "method" => method).record(count as f64);
        with_retry(method, || async {
            // CU cost of the whole batch (billed per method-call), then per-call rps.
            let call = match method {
                "eth_getBlockByNumber" => RpcCall::BlockByNumber { count, full: true },
                "eth_getTransactionReceipt" => RpcCall::Receipt { count },
                _ => RpcCall::Other,
            };
            self.limiter.acquire_cu(self.cu_for(&call)).await;
            for _ in &chunk {
                self.limiter.acquire().await;
            }
            let _g = self.enter(&call);
            let mut batch = BatchRequest::new(self.provider.client());
            let mut waiters = Vec::with_capacity(chunk.len());
            for it in &chunk {
                waiters.push(
                    batch
                        .add_call::<P, R>(method, &mk_params(it))
                        .map_err(|e| e.to_string())?,
                );
            }
            batch.await.map_err(|e| e.to_string())?;
            let mut out = Vec::with_capacity(chunk.len());
            for (it, w) in chunk.iter().zip(waiters) {
                out.push((it.clone(), w.await.map_err(|e| e.to_string())?));
            }
            Ok(out)
        })
        .await
    }
}
