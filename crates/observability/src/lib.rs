//! Metrics exporter wiring. Installs the global `metrics` recorder backed by a
//! Prometheus scrape endpoint (`/metrics`) on its own HTTP listener, sets histogram
//! buckets, and records `build_info`. The one place that knows the exporter; emit
//! sites across crates use the `metrics` facade macros and stay backend-agnostic.

use std::net::SocketAddr;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

/// Latency buckets (seconds) spanning sub-millisecond DB calls to tens-of-seconds
/// RPC round-trips — one set covers both since the suffix matches every `_seconds`.
const SECONDS_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Block-count buckets for range/rollback/backfill widths, tied to `max_getlogs_blocks`.
const BLOCKS_BUCKETS: &[f64] = &[1.0, 8.0, 32.0, 128.0, 512.0, 2000.0, 8000.0];

/// Install the global Prometheus recorder and serve `/metrics` at `addr`.
/// Must run inside a Tokio runtime (spawns the listener task). Call once per process.
pub fn install(addr: SocketAddr) -> anyhow::Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), SECONDS_BUCKETS)?
        .set_buckets_for_metric(Matcher::Suffix("_blocks".into()), BLOCKS_BUCKETS)?
        .install()?;
    tracing::info!(%addr, "metrics exporter listening at /metrics");
    record_build_info();
    Ok(())
}

/// Deploy-correlation series: constant `1`, labelled with version + git sha.
fn record_build_info() {
    metrics::gauge!(
        "build_info",
        "version" => env!("CARGO_PKG_VERSION"),
        "git_sha" => option_env!("GIT_SHA").unwrap_or("unknown"),
    )
    .set(1.0);
}

#[cfg(test)]
mod tests {
    use super::{BLOCKS_BUCKETS, SECONDS_BUCKETS};
    use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

    /// The facade macros render through the Prometheus exporter with the configured
    /// buckets — the same path `/metrics` serves, without a global install.
    #[test]
    fn renders_emitted_series() {
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), SECONDS_BUCKETS)
            .unwrap()
            .set_buckets_for_metric(Matcher::Suffix("_blocks".into()), BLOCKS_BUCKETS)
            .unwrap()
            .build_recorder();

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("rpc_calls_total", "chain_id" => "1", "method" => "eth_getLogs")
                .increment(3);
            metrics::gauge!("queue_depth").set(7.0);
            metrics::histogram!("decode_duration_seconds", "chain_id" => "1").record(0.02);
        });

        let out = recorder.handle().render();
        assert!(out.contains("rpc_calls_total"), "counter: {out}");
        assert!(out.contains("method=\"eth_getLogs\""), "label: {out}");
        assert!(out.contains("queue_depth 7"), "gauge: {out}");
        assert!(
            out.contains("decode_duration_seconds_bucket"),
            "histogram: {out}"
        );
    }
}
