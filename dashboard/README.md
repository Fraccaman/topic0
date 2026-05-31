# Grafana dashboard

`env-indexer.json` — dashboard for the metrics exported at `/metrics` (see
`[indexer] metrics_listen` in `config.toml`).

## Panels

5 rows over the exported series:

- **Health & Lag** — cursor staleness (`time() - last_advance_timestamp_seconds`),
  fetch lag (`chain_head_block - cursor_height_block`), `queue_depth`.
- **RPC** — calls/sec & in-flight & p95 by method, retries/failures, cache hit ratio
  (`rpc_aux_fetched/touched`).
- **Decode & Queue** — decoded rows/sec by table, decode p95 + errors, queue
  enqueue/ack/reclaim.
- **Reorg & Tip/WS** — reorgs/sec + rollback depth, WS reconnects/removed, WS logs/sec.
- **Query API** — GraphQL req/sec by status, GraphQL p95, SQL p95.

Two template variables: `datasource` (Prometheus) and `chain` (multi, from
`label_values(cursor_height_block, chain_id)`).

## Import

Grafana → Dashboards → New → Import → upload `env-indexer.json` → pick the Prometheus
datasource.

Or provision (Grafana ≥ 9): drop this file under a configured dashboards provider path,
e.g. `/etc/grafana/provisioning/dashboards/`.

## Prometheus scrape

```yaml
scrape_configs:
  - job_name: env-indexer
    static_configs:
      - targets: ["indexer:9100"]   # query bin / decode workers on their own ports
```
