# Lean EVM Indexer — Architecture Design

## Context

Build a cost-efficient, config-driven EVM log indexer in Rust. It indexes **only**
the contracts/events declared in config, minimizing paid RPC usage
(Alchemy/QuickNode billed per request/compute-unit), decodes logs via their ABI,
and writes **structured typed rows** into the database.

Decisions (from the user):

- **Scope = config contracts only.** Never scan full blocks. Pull only logs matching declared contract addresses + event topics.
- **Minimize RPC spend.** `eth_getLogs` with address+topic filters over wide block ranges; batch timestamp lookups only for blocks that actually had matching logs; websocket log subscription at the tip instead of polling.
- **Static config/manifest** declares chains, contracts, ABIs, start blocks, events to index.
- **Multi-chain**, one deployment, `chain_id` everywhere.
- **Storage = Postgres**, one typed table per event. Real joins, easy reorg deletes, simple ops.
- **Reorg-safe**: track block hashes, delete + re-index on reorg.
- **2–3 small services + a queue** (not a monolith, not a big fleet).
- **Source backend implementation-agnostic**: log fetching sits behind a trait; the RPC/provider impl is swappable without touching ingest/decode logic.
- **Query webserver implementation-agnostic**: the API layer is decoupled from both storage and protocol behind traits; swap Postgres for another store, or REST for GraphQL/gRPC, without rewriting handlers.

Greenfield: `/Users/fraccaman/env-indexer`, no existing code.

---

## Topology (3 services + queue)

```
   config.toml (chains, contracts, ABIs, events)
        │ loaded by all services
        ▼
┌────────────────┐  work item    ┌────────────────┐   typed rows   ┌──────────────┐
│   INGESTOR     │──(PG queue)──▶│    DECODER     │──────────────▶ │              │
│  per chain     │  raw_* first  │   + WRITER     │  upsert        │  Postgres    │
│  eth_getLogs   │               │  ABI decode    │                │  (typed      │
│  filtered      │◀── cursor ────│  reads raw_*   │                │  event       │
│  ws subscribe  │   (in PG)     └────────────────┘                │  tables)     │
└──────┬─────────┘                                                 │              │
       │ getBlockByNumber(full=true) + receipts (matched only)     └──────┬───────┘
       ▼                                                                  ▲
   Alchemy / QuickNode RPC                                  ┌─────────────┴────┐
                                                            │   QUERY API      │
                                                            │   GraphQL R/O    │
                                                            └──────────────────┘
```

- **Queue**: Postgres `work_queue` table (polled, `FOR UPDATE SKIP LOCKED`) — no
  extra infra, enqueued in the same tx as the raw write. `Queue` trait keeps Redis
  Streams / NATS available later if throughput demands.
- All services share `shared` + `config` + `abi` crates. Scale = run more
  decoder workers; ingestor is one task per chain.

> **Crate/trait layout below is illustrative.** [CODEBASE.md](CODEBASE.md) is the
> authoritative implementation structure (layered crates, ports, DI).

Workspace:
```
crates/
  core/      # types: RawLog, DecodedEvent, BlockRef, ReorgEvent
  config/    # parse + validate config.toml, load ABIs
  abi/       # ABI parse + log decode (alloy-json-abi, alloy-dyn-abi)
  source/    # LogSource trait + RPC impl (JSON-RPC+WS, batching, CU accounting)
  storage/   # Postgres impl (sqlx) + StorageReader trait (read seam only)
  api/       # ApiServer trait + GraphQL impl (async-graphql)
services/
  ingestor/  # uses LogSource trait → queue
  decoder/   # decode + write Postgres directly (can run N replicas)
  query/     # ApiServer over StorageReader, protocol-agnostic
```

### Abstraction boundaries (both required implementation-agnostic)

**Source backend** — ingestor depends only on the trait, never a concrete provider:
```rust
trait LogSource {
    async fn head(&self) -> Result<u64>;
    async fn get_logs(&self, filter: LogFilter, from: u64, to: u64) -> Result<Vec<RawLog>>;
    async fn block_times(&self, numbers: &[u64]) -> Result<Vec<BlockTime>>; // batched
    async fn subscribe_logs(&self, filter: LogFilter) -> Result<BoxStream<RawLog>>; // tip
}
```
Impls: `RpcLogSource` (Alchemy/QuickNode). Swap in another provider, a self-hosted
node, or a firehose later with no change to ingest/decode/reorg code. Provider URL
is config, not a code dependency.

**Per-source pricing.** Each `LogSource` carries a `CostModel` so the indexer knows
what every call *costs* on that backend — providers price differently (Alchemy
compute units, QuickNode credits, self-hosted ≈ free). Pricing is bound to the
source, not hardcoded globally:
```rust
enum RpcCall {                      // billable units of work
    GetLogs { blocks: u64, results: u64 },
    BlockByNumber { count: u64, full: bool },  // timestamp (+txs if full); full=true is default path
    TxByHash { count: u64 },        // fallback only on size-metered backends
    Receipt { count: u64 },         // per distinct tx (batched, deduped)
    LogSubscription,
    Other(&'static str),
}
trait CostModel {
    fn units(&self, call: &RpcCall) -> CostUnits;   // method → CU/credits (provider table)
    fn price(&self, units: CostUnits) -> MicroUsd;  // units → money (plan rate)
    fn name(&self) -> &str;
}
trait LogSource {                   // ...as above, plus:
    fn cost_model(&self) -> &dyn CostModel;
    fn plan(&self) -> &PlanProfile;
}
```
Impls: `AlchemyCost`, `QuickNodeCost`, `FreeNodeCost`. The ingestor wraps the source
in a metering layer that, per call, accumulates `(units, micro_usd)` into a
`SpendLedger` (per chain, per source, per method). Drives: live `$spent` metrics,
budget caps (pause/slow when a daily ceiling is hit), and apples-to-apples provider
comparison. Swapping a provider swaps its `CostModel` with it — spend accounting
stays correct automatically.

**Plan-aware tuning (`PlanProfile`).** Each `[chains.source]` declares its provider
caps explicitly in `[chains.source.limits]`, because limits differ by tier (Alchemy
free ≠ Growth ≠ QuickNode Build). The profile is the single place that knows the
backend's hard constraints, and the client self-tunes against it:
```rust
struct PlanProfile {
    max_rps: u32,               // request rate cap (token-bucket throttle)
    max_cu_per_sec: Option<u32>,// compute-unit-per-second cap (Alchemy)
    max_batch: u32,             // JSON-RPC batch size the plan accepts
    max_getlogs_blocks: u32,    // provider getLogs range ceiling (e.g. 2000)
    max_getlogs_results: u32,   // result-count cap (e.g. 10000)
    monthly_quota: Option<u64>, // free-tier CU/credit allotment → quota guard
}
```
- A token-bucket limiter enforces `max_rps`/`max_cu_per_sec` → never trip provider
  429s on the free tier.
- Adaptive getLogs range is **seeded** at `max_getlogs_blocks` and shrinks only on
  result-cap hits — max payload per call, fewest calls.
- HTTP JSON-RPC **batching** packs up to `max_batch` block/receipt lookups per
  round-trip → fewer requests, better throughput (note: providers bill per-method,
  so this is latency/rate-limit headroom, not direct $).
- `monthly_quota` feeds the `SpendLedger` guard to slow/stop before blowing a free
  allotment.

Config selects the plan per source:
```toml
[[chains]]
id = 1
[chains.source]
kind = "alchemy"; plan = "free"; http = "..."; ws = "..."   # see full config below
```

**Query webserver** — decoupled from storage *and* protocol via two seams:
```rust
trait StorageReader {            // storage-agnostic (read seam only)
    async fn query(&self, q: Query) -> Result<RowStream>;
    async fn schema(&self) -> Result<EntitySchema>;
}
trait ApiServer {                // protocol-agnostic
    async fn serve(self, reader: Arc<dyn StorageReader>, addr: SocketAddr) -> Result<()>;
}
```
The API is **protocol-agnostic** via the `ApiServer` trait; **GraphQL is the one
implementation built** (`GraphqlApiServer`). REST/gRPC remain drop-in alternate
`ApiServer` impls, and a different store is an alternate `StorageReader` —
handlers/business logic unchanged. Default wiring: `PostgresReader` +
`GraphqlApiServer`.

No write seam: the decoder writes Postgres directly. The write path (ABI→DDL
generation, `ON CONFLICT` upserts, reorg `DELETE`s) is irreducibly store-specific,
so a `StorageWriter` trait would leak Postgres semantics — a fake abstraction at
real cost. Only the read seam is abstracted, which is what the agnostic-query
requirement needs. Swap the write store later if ever needed (YAGNI).

---

## Queue (ingestor → decoder)

Carries **pointers, not payloads**. Ingestor writes raw data to `raw_*` first, then
enqueues a small work item; the decoder pulls it, reads raw, decodes, writes event
tables. Small queue + decode replayable from raw.

```rust
struct WorkItem { chain_id: u64, from_block: u64, to_block: u64,
                  kind: Backfill | Tip | Reorg | Redecode, epoch: u64 }
trait Queue {                       // swappable via [queue].kind
    async fn push(&self, item: WorkItem) -> Result<()>;
    async fn pull_any(&self) -> Result<Option<Lease<WorkItem>>>;  // lease, one in-flight per chain
    async fn ack(&self, lease: Lease<WorkItem>) -> Result<()>;
}
```

- **Producer**: ingestor, one task per chain → enqueue a range after raw is committed
  + cursor advanced (the `indexer run` supervisor; or `follow`/`backfill` for the
  inline single-chain path).
- **Consumers**: `DecodeWorker`s as **competing consumers** — N tasks (`indexer
  decode --workers N`, or in-process under `run`) each `pull_any()` → decode with
  that chain's `Decoder` → upsert → `ack`. Decode is a pure fn, so any worker takes
  any item.
- **At-least-once**; redelivery harmless because decode upserts on the PK
  (idempotent) — no consumer-side dedup.
- **Parallel across chains, serial within a chain**: `pull_any` leases the oldest
  item **whose chain has no in-flight item**, so a `Reorg` item (`DELETE ≥ B`) runs
  before re-decoding `B..`, while different chains decode concurrently. `epoch`
  fences stale items from orphaned blocks (v2).
- **Backpressure**: `depth()` metric + lag (`head − last_decoded`); ingestor can
  throttle when the queue grows.

Impl: **Postgres** (the chosen queue — no extra infra, same DB as everything else).
A `work_queue` table; **enqueue shares the same transaction as the raw insert** ⇒
exactly-once enqueue. `pull_any` competing-consumer SQL:
```sql
UPDATE work_queue SET locked_at = now() WHERE id = (
  SELECT id FROM work_queue WHERE locked_at IS NULL
    AND chain_id NOT IN (SELECT chain_id FROM work_queue WHERE locked_at IS NOT NULL)
  ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING …;
```
Workers **poll** on a `tokio::time::interval` (50ms busy → ~1s idle), no
`LISTEN/NOTIFY`. A reclaim ticker frees leases whose `locked_at` exceeds the
visibility timeout (crash recovery). The `Queue` trait abstracts it so Redis
Streams / NATS JetStream can be added later if throughput ever demands.

---

## RPC minimization (the core requirement)

This is where the money is saved.

1. **Filtered `eth_getLogs` only.** Build one filter per chain covering **all**
   config contracts at once: `address: [a1,a2,…]`, `topics: [[sig0_a, sig0_b, …]]`
   (union of indexed event signatures). One call returns every matching log across
   the whole range for all contracts — no per-contract or per-block calls.

2. **Wide adaptive ranges.** Start with a large block span (e.g. 2000). On provider
   limits ("query returned more than 10000 results" / "range too large"),
   **halve** the range and retry; **grow** it back on clean success. Maximizes logs
   per request → fewest requests for the backfill.

3. **Matched data only.** We never scan or fetch *unmatched* blocks. Only logs
   (via filtered getLogs) and, for the blocks/txs they touch, the block + receipt —
   nothing else. Cost scales with matched blocks/txs, not chain size.

4. **Timestamps: batched, matched blocks only.** Logs lack timestamps. After a
   getLogs page, collect the **distinct** `blockNumber`s that had matching logs and
   issue **one batched** JSON-RPC call of `eth_getBlockByNumber(n, full=true)` —
   which also returns the block's transactions (see 4b). Cache results in a `blocks`
   table and dedup across pages — a block is fetched at most once, ever.

4b. **tx + receipt enrichment (always on).** We always store the transaction and
   receipt behind each matched log. Two facts drive the cheap path:
   - the **block is fetched regardless** (timestamp), so the block call is sunk;
   - `eth_getBlockByNumber(n, full=true)` returns the timestamp **and the full tx
     objects** of that block in one call.

   So tx data piggybacks on the block fetch instead of a separate
   `eth_getTransactionByHash` per tx. Per distinct matched block with K matched txs:
   ```
   full=true   : cost(BlockByNumber{full:true})                       # 1 call, timestamp + K txs
   split       : cost(BlockByNumber{full:false}) + K·cost(TxByHash)   # K+1 calls
   use full=true if cost(full) ≤ cost(split)
   ```
   On flat-per-method providers (Alchemy 16 CU, QuickNode ~20 cr — the flag doesn't
   change method price) full=true is **always ≤** split for K≥1, and also saves
   round-trips. It only loses on a **size-metered** backend (full block = fatter
   payload incl. non-matched txs). The choice is therefore made by the source's
   `CostModel`, not hardcoded — size-metered backends fall back to `TxByHash`.

   `eth_getBlockReceipts` was considered and **rejected** for this workload: at
   ~1 matched tx/block (the common case) it has nothing to amortize and is priced
   above a single tx-receipt. Receipts stay per distinct tx via
   `eth_getTransactionReceipt` (batched, deduped). Revisit only if a deployment
   shows high matched-tx-per-block density.

   Everything routes through `CostModel`/`SpendLedger`, deduped per distinct
   block/tx and cached in `blocks` / `transactions` / `receipts`.
   - tx (from block, full=true) → `from, to, value, input, gas, gas_price, nonce`
   - receipt → `status, gas_used, effective_gas_price, contract_address`

5. **Tip via WebSocket subscription, not polling.** `eth_subscribe("logs", filter)`
   pushes matching logs as blocks arrive — near-zero CU vs. interval polling. Poll
   fallback (`eth_blockNumber` + getLogs) only if WS unavailable.

   **WS can miss logs — reconcile against the cursor on every (re)connect.** A WS
   subscription is fire-and-forget: no replay, no acks, no cursor. Logs emitted
   while the socket is down (disconnect, provider restart, dropped idle sub, buffer
   overflow on a slow consumer) are **never re-pushed** — the sub resumes at the
   current tip, silently skipping the gap. The cursor (`last_indexed_block`), not
   the WS stream, is the source of truth. So on each WS open/reopen, **close the gap
   with getLogs before trusting the stream**:
   ```
   on WS (re)connect:
     getLogs(from = cursor.last_block + 1, to = head)   # backfill the missed window
     then resume the subscription from head
   ```
   Without this, a disconnect leaves a silent hole until some later backfill happens
   to re-cover the range. M4 bakes reconcile-from-cursor into the WS `LogSource`
   impl — subscribing is not enough on its own.

6. **Checkpoint + idempotency.** Persist `last_indexed_block` + `last_block_hash`
   per chain. Restart resumes exactly; no re-fetching already-indexed ranges.

7. **Cross-contract cache (per-chain-global).** `blocks` / `transactions` /
   `receipts` are keyed by **chain**, not by contract. 50 config contracts touching
   the same block fetch it **once**, shared. The getLogs filter already unions all
   contract addresses per chain, so a single page yields logs for all of them; the
   block/tx/receipt lookups it triggers are deduped chain-wide. Cost grows with
   distinct blocks/txs on the chain, not with contract count.

8. **Cost-aware client.** The metering layer feeds each call through the source's
   `CostModel` → `SpendLedger`, exposing live `$spent`, enforcing daily budget caps
   (pause/slow at ceiling), and respecting rate limits with backoff + jitter.

9. **Two-level parallel pipeline (saturate the rate budget, don't idle).** Frugal ≠
   slow. The `PlanProfile` token bucket is the hard ceiling; concurrency exists only
   to *fill* it instead of blocking serially between round-trips. Two independent
   knobs, both still rps/CU-gated by the same bucket:
   - **`range_concurrency`** (`[indexer]`, default 4) — the ingest loop chunks
     `[next..=safe]` into provider-sized ranges and drives them through
     `buffer_unordered(range_concurrency)`, so several getLogs ranges are in flight
     at once. Ranges **don't** advance the cursor individually; on full success the
     cursor advances once to the contiguous prefix (`safe`). `try_collect`
     short-circuits on any error → cursor stays put, the next loop re-fetches the
     range idempotently (`ON CONFLICT`), so a mid-batch failure leaves **no gap**.
   - **`aux_concurrency`** (`[indexer]`, default 8) — within one range's `fetch_aux`,
     the per-block `getBlockByNumber(full=true)` and per-tx `getTransactionReceipt`
     calls run concurrently via `buffer_unordered(aux_concurrency)`. At ~1 matched
     tx/block these dominate the call count, so overlapping them is the big
     wall-clock win; the token bucket just gets saturated instead of idling between
     calls.

   Net effect: same call count and same `$spent` as the serial path (concurrency
   changes *ordering*, not *volume*), but the backfill runs as fast as the plan's
   rate cap allows. Both knobs default sensibly and need no tuning on the free tier.

---

## Cost reference (one contract, 20k txs, full backfill)

Density assumption: ~1 matched tx/block ⇒ ~20k distinct blocks ≈ 20k distinct txs.
Cheap path = `getLogs` + `getBlockByNumber(full=true)` (timestamp+tx) + `getTransactionReceipt`.

| Method | Calls | Alchemy CU/call | CU |
|---|---|---|---|
| `eth_getLogs` | ~20 | 75 | ~1.5k |
| `eth_getBlockByNumber(full=true)` | 20k | 16 | 320k |
| `eth_getTransactionReceipt` | 20k | 15 | 300k |
| **Total** | | | **~0.62M CU** |

- **Alchemy**: 0.62M CU ≪ 300M free/mo ⇒ **$0**; PAYG overage ~$0.75.
- **QuickNode**: ~40k calls × ~20 cr ≈ 0.8M credits ≪ free tier ⇒ **$0**; marginal on paid ≈ $0.5.
- vs naive split path (separate `getTransactionByHash`): ~0.96M CU → the full=true
  fold saves ~35%.
- Cost scales with **distinct blocks + txs**, not log count. Rates drift — verify
  live; the `CostModel` impls are the single source of truth in code.

## Decode → structured rows

Fully **dynamic at runtime** — no codegen. The `abi` crate reads whatever ABI the
config supplies; the `storage` crate generates DDL from it; the decoder turns each raw
log into a typed row. Crates: `alloy-json-abi` (parse + selector), `alloy-dyn-abi`
(`DynSolType` / `decode_log_parts`), `alloy-primitives` (Address/U256).

**Step 1 — parse ABI → event schema.** For each configured event, pull its params
in order with `(name, type, indexed)` and compute its selector
`topic0 = keccak256("Transfer(address,address,uint256)")`. Build a runtime index
`topic0 → EventSchema`. Example:
```
Transfer(address indexed from, address indexed to, uint256 value)
  topic0 = 0xddf252ad...
  indexed:     from, to
  non-indexed: value
```

**Step 2 — generate DDL via explicit `migrate` (never at runtime).** Schema changes
are an **operator action**, not something the indexer does on its own:
- `indexer migrate` reads the config ABIs, computes the desired schema, diffs it
  against the live DB (`information_schema`), prints the plan, and applies it.
  `--dry-run` previews; destructive changes (type change, dropped column/event)
  require an explicit confirm flag — never silent data loss.
- The `ingestor`/`decoder` services run a **preflight schema check** on boot: if the
  live schema doesn't match the config, they **refuse to start** with a clear "run
  `indexer migrate`" error. No lazy table creation, no auto-DDL on the hot path.
- Pairs with the raw store: after a `migrate` adds a column/table, re-decode from
  `raw_*` backfills it with **zero** RPC.

Map each Solidity type → Postgres column and emit `CREATE TABLE`:

| Solidity | Postgres |
|---|---|
| address | bytea (20) |
| uint256 / int256 | numeric(78,0) |
| uint8..64 | int8 / numeric |
| bool | bool |
| bytes32 | bytea |
| bytes / string | bytea / text |
| T[] / tuple | jsonb |

**Step 3 — decode each log (the core trick = topic/data split).** A raw log is
`{ topics: [topic0, t1, t2, …], data: bytes }`:
- `topics[0]` = selector → routes to which event/table (O(1) hash lookup).
- `topics[1..]` = the **indexed** params, one 32-byte slot each.
- `data` = the **non-indexed** params, ABI-encoded blob (32-byte words, dynamic
  types via offset pointers).
```rust
let ev = abi_index.get(&log.topics[0])?;               // route by topic0; unknown → skip
let decoded = ev.decode_log_parts(&log.topics, &log.data)?; // alloy-dyn-abi
// decoded.indexed -> from topics[1..]   decoded.body -> from data
```
Caveat: an **indexed dynamic** type (string/bytes/array) stores only its *hash* in
the topic — value unrecoverable from the log; emit that column as `<name>_hash`.
Anonymous events (no topic0) decode positionally; flagged in config.

**Step 4 — bind → upsert.** Convert each `DynSolValue` to its Postgres type, bind
positionally, fill metadata (`chain_id`, `block_*`, `tx_hash`, `log_index`) from the
log envelope + cached block/tx/receipt, then upsert (idempotent on the PK).

Decode is a pure fn `(raw log + ABI) → row` ⇒ re-runnable from the raw store
(powers the free re-decode/resync).

Per-event table shape:
```sql
CREATE TABLE evt_<contract>_<event> (
  chain_id     int       NOT NULL,
  block_number bigint    NOT NULL,
  block_hash   bytea     NOT NULL,
  block_time   timestamptz,
  tx_hash      bytea     NOT NULL,
  log_index    int       NOT NULL,
  <param_1>    <type>,            -- decoded event fields
  <param_n>    <type>,
  PRIMARY KEY (chain_id, block_number, log_index)
);
CREATE INDEX ON evt_<...> (<frequently_filtered_param>);
```

Supporting tables:
```sql
blocks       (chain_id, number, hash, time)                 -- timestamp cache
transactions (chain_id, hash, from, to, value, input, gas, gas_price, nonce, block_number)
receipts     (chain_id, tx_hash, status, gas_used, effective_gas_price, contract_address)
cursors      (chain_id, last_block, last_hash)              -- checkpoint
```
`transactions`/`receipts` always populated; event tables join them via
`(chain_id, tx_hash)`.

`PRIMARY KEY (chain_id, block_number, log_index)` makes writes idempotent
(`ON CONFLICT DO NOTHING/UPDATE`) → safe replays.

---

## Raw store → free re-decode / resync

Pay RPC **once, ever**. The raw artifact is written *before* decoding so decode is a
pure, replayable fn of `(raw_logs, ABI)`.

- The decoder reads from the raw store, not the network. Live ingest writes raw +
  enqueues.
- **Resync without refetch**: new event added to a config ABI, a decode bug fixed,
  or a column added → replay decode over `raw_logs`. **Zero** new RPC, runs at disk
  speed. The single biggest lifetime cost saver.
- Decoded event tables are derived and fully rebuildable from `raw_logs`.

### Storage compactness

**1. Only logs are "raw" — drop redundant blobs.** Blocks/txs/receipts are
**ABI-independent**: they are never re-decoded, so the structured `blocks` /
`transactions` / `receipts` tables already *are* their durable form. No `raw_blocks`
tx-json and no `raw_receipts` json — those duplicated the structured tables (and the
logs) and were the fattest blobs (full-block tx arrays, verbose receipts + bloom).
Re-decode reads `raw_logs` and joins the existing structured tables. Removes ~60–70%
of raw bytes outright.

**2. Binary `bytea`, not hex/JSON.** Store `topics` and `data` as raw bytes, numbers
as native `int`/`numeric` — hex/JSON text is ~2× the bytes. Lossless, still decodable.

```sql
raw_logs (
  chain_id     int,
  block_number bigint,
  log_index    int,
  address      bytea,      -- 20 bytes, not 0x-hex
  topics       bytea,      -- concatenated 32-byte topics (topic0..topicN)
  data         bytea,      -- non-indexed event params, ABI-encoded
  tx_hash      bytea,
  PRIMARY KEY (chain_id, block_number, log_index)
);
```
Block hash lives once in `blocks` (log references `block_number`), not per row.

Stacked: drop blobs (≫50%) → binary (~2×) → well below naive JSON. Cold-range
Parquet offload to S3 remains a later option if PG raw still grows painful
(not built now).

## Reorg safety (simple with Postgres)

- Every row carries `block_hash`; `cursors` stores the last block's hash.
- WS log subscriptions deliver a `removed: true` flag on reorged logs; for the poll
  path, detect a parent-hash mismatch against `blocks`.
- On reorg from block `B`: `DELETE FROM <each event table> WHERE chain_id=$1 AND
  block_number >= B`, roll the cursor back to `B-1`, re-fetch from `B`. Cheap point
  deletes — the main reason Postgres fits better than an append-only OLAP here.
- Only act within an unfinalized window (config `confirmations`, e.g. 12 blocks);
  optionally only expose finalized rows to the query API.

---

## Query API (GraphQL)

- Read-only `query` service = **GraphQL** `ApiServer` over `StorageReader`
  (`async-graphql` + `async-graphql-axum`). The `ApiServer`/`StorageReader` seams
  stay so REST/gRPC or another store remain drop-in later, but GraphQL is the one
  built.
- **Schema generated from config**: each event table → a GraphQL type + a
  filterable/paginated list query (e.g. `transfers(where:{from:…}, first:, after:)`),
  derived from the same ABI/DDL the migrator produces. Joins to `transactions` /
  `receipts` exposed as nested fields via `(chain_id, tx_hash)`.
- Resolvers lower GraphQL args → parameterized SQL through `PostgresReader`; cursor
  pagination on `(block_number, log_index)`. Reads gated at the `[query].expose`
  watermark (finalized by default).
- Postgres indexes + partial indexes per common filter; partition large event
  tables by `chain_id` / time only if volume demands.

---

## Config (`config.toml`)

Single full TOML file is the source of truth for schema (`migrate`) and runtime
(ingestor/decoder/query). Secrets via `${ENV}` interpolation, never inline.

```toml
# ─── global ───────────────────────────────────────────────
[indexer]
log_level         = "info"
batch_size        = 500        # decoder write batch into Postgres
range_concurrency = 4          # getLogs ranges in flight at once (backfill pipeline)
aux_concurrency   = 8          # concurrent block/receipt RPCs per range (rps-gated)

[database]
url          = "${DATABASE_URL}"   # postgres://...
max_conns    = 16

[queue]
kind         = "postgres"      # postgres work_queue table (FOR UPDATE SKIP LOCKED, polled)
poll_ms      = 50              # poll interval when busy; backs off toward poll_idle_ms when empty
poll_idle_ms = 1000

[query]
api          = "graphql"       # async-graphql; schema generated from config events
listen       = "0.0.0.0:8080"
expose       = "finalized"     # finalized | provisional  (read visibility)

# ─── per chain ────────────────────────────────────────────
[[chains]]
id            = 1
name          = "ethereum"
confirmations = 12             # unfinalized window for reorg safety
start_block   = 19_000_000

  # source backend behind the LogSource trait — kind selects the impl
  [chains.source]
  kind        = "alchemy"      # alchemy | quicknode | generic_rpc | free_node
  http        = "${ALCHEMY_HTTP}"
  ws          = "${ALCHEMY_WS}"

    # provider caps (PlanProfile) — all explicit, no preset
    [chains.source.limits]
    max_rps             = 25
    max_cu_per_sec      = 660
    max_batch           = 100
    max_getlogs_blocks  = 2000
    max_getlogs_results = 10000
    monthly_quota_cu    = 300_000_000

  # spend guardrails (SpendLedger) for this chain
  [chains.budget]
  daily_usd_cap = 5.0          # pause/slow when hit; omit = unlimited

  # contracts to index on this chain — getLogs filter unions all addresses
  [[chains.contracts]]
  address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # USDC
  abi     = "abis/erc20.json"
  events  = ["Transfer", "Approval"]    # subset of ABI; omit = all events
  # table = "usdc_transfer"             # optional override; else evt_<contract>_<event>

  [[chains.contracts]]
  address     = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"  # Uniswap V2 Router
  abi         = "abis/univ2_router.json"
  events      = ["Swap"]
  start_block = 19_500_000              # optional per-contract start override

# ─── another chain, different provider/plan ───────────────
[[chains]]
id            = 8453
name          = "base"
confirmations = 20
start_block   = 12_000_000

  [chains.source]
  kind = "quicknode"
  http = "${QN_HTTP}"
  ws   = "${QN_WS}"

  [[chains.contracts]]
  address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # USDC on Base
  abi     = "abis/erc20.json"
  events  = ["Transfer"]
```

Notes:
- `source.kind` picks the `LogSource`/`CostModel` impl; `[chains.source.limits]`
  sets the `PlanProfile` caps (rate/quota/range) explicitly.
- tx + receipt are always fetched/stored (joined via `(chain_id, tx_hash)`) — no flag.
- Same file drives every service: `migrate` reads `[[chains.contracts]]` for DDL;
  ingestor reads `[chains.source]`; query reads `[query]`.

---

## Rust dependencies

| Concern | Crate |
|---|---|
| Async runtime | `tokio` (full, multi-thread) |
| Async traits / streams | `async-trait`, `futures` |
| EVM types + keccak | `alloy-primitives` (`Address`, `U256`, `keccak256`) |
| ABI parse | `alloy-json-abi` |
| ABI decode (dynamic) | `alloy-dyn-abi` (`DynSolType`, `decode_log_parts`) |
| RPC client (batch + WS) | `alloy-provider` + `alloy-transport-http` / `-ws` |
| RPC types | `alloy-rpc-types-eth` (`Log`, `Filter`, `Block`, `Receipt`) |
| Postgres (DB + **queue**) | `sqlx` (postgres, macros, tokio-rustls); queue polled via `tokio::time::interval` |
| Big decimals | `rust_decimal` / `bigdecimal` (`numeric(78,0)` ↔ U256) |
| Config | `serde` + `toml` + `figment` (TOML + `${ENV}` merge) |
| CLI | `clap` derive (`run` / `migrate` / `migrate --dry-run`) |
| Rate limiting | `governor` (token bucket for `PlanProfile`) |
| Retry / backoff | `backon` (429/5xx backoff + jitter) |
| **Query API (GraphQL)** | `async-graphql` + `async-graphql-axum` (+ `axum`, `tower-http`) |
| Errors | `thiserror` (libs), `anyhow` (binaries) |
| Tracing | `tracing`, `tracing-subscriber` |
| Metrics | `metrics`, `metrics-exporter-prometheus` (spend / lag / CU) |
| Dev: local chain | `alloy-node-bindings` (Anvil — fork/reorg tests) |
| Dev: containers | `testcontainers` (Postgres in integration tests) |

`alloy-provider` supplies JSON-RPC batching + WS subscriptions → no `jsonrpsee`.
Queue is Postgres via `sqlx` → no Redis/NATS deps. Single web stack: `async-graphql`.

---

## Build order

1. **M1 — fetch path**: `config`, `abi`, `source`. Ingestor does filtered `eth_getLogs`
   with adaptive ranges over a fixed historical span for one contract; print logs.
   Prove RPC-call count is low.
2. **M2 — decode + write**: `storage` crate + `indexer migrate` (ABI→DDL diff/apply, dry-run,
   destructive-confirm) + boot preflight schema check; decoder upserts typed rows,
   batched timestamp fetch + `blocks` cache. End-to-end backfill, one chain.
3. **M3 — raw store + plan tuning**: write `raw_*` tables before decode; decoder
   reads raw not network. `PlanProfile` (rate-limit token bucket, range seeding,
   batching, quota guard) per source. Resync = replay decode over raw, zero RPC.
4. **M4 — live tip**: WS log subscription, cursor checkpointing, resume-on-restart.
5. **M5 — reorg safety**: hash tracking, `removed`/parent-mismatch detection,
   delete-and-reindex within the confirmation window.
6. **M6 — multi-chain + GraphQL API**: one ingestor task per chain, per-chain-global
   cache, `async-graphql` server with config-generated schema + cursor pagination,
   decoder horizontal scale, CU/spend metrics.

## Verification

- **RPC frugality (key metric)**: run M1 against a known contract+range behind a
  counting proxy; assert total requests ≈ `ceil(range/avg_logs_per_call)` +
  distinct-matched-blocks, and that **zero** full-block/receipt calls for unmatched
  blocks occur.
- **Decode correctness**: unit-test `abi` against known event fixtures (e.g. ERC-20
  `Transfer`); assert decoded columns match expected values.
- **Migration**: `migrate` on an empty DB creates expected tables; `--dry-run`
  applies nothing; a destructive diff is refused without the confirm flag; services
  refuse to boot against a stale schema and start cleanly after `migrate`.
- **Idempotency**: replay the same getLogs page twice → row counts unchanged
  (`ON CONFLICT`).
- **Reorg**: drive anvil to fork/reorg; assert rows ≥ reorg block are deleted and
  re-indexed to the canonical chain.
- **Multi-chain**: index two chains concurrently; assert `chain_id` isolation in
  cursors and tables.
- **Queue**: enqueue+raw-insert in one tx — kill the process mid-batch, assert no
  item without its raw rows (exactly-once enqueue); two decoder workers drain
  without double-processing (`SKIP LOCKED`); a `Reorg` item orders before re-decode
  of the same range.
- **Abstraction**: a mock `LogSource` (fixture logs, no network) drives the full
  decode→write path in tests; a second `ApiServer`/`StorageReader` pair compiles
  against the same handlers — proves both seams are real, not leaky.
- **Pricing**: unit-test each `CostModel` against published provider tables (known
  method → expected units/cost); replay a recorded backfill through the metering
  layer and assert `SpendLedger` totals match a hand-computed figure; assert the
  budget cap actually pauses ingestion at the ceiling.
- **Plan tuning**: assert the token bucket caps requests at `max_rps`/`max_cu_per_sec`
  (no 429s under load), getLogs seeds at `max_getlogs_blocks` and shrinks on
  result-cap, and the quota guard halts before `monthly_quota`.
- **Parallel pipeline**: run a backfill with `range_concurrency`/`aux_concurrency`
  both at 1 and at their defaults; assert the **call count and `SpendLedger` total
  are identical** (concurrency changes ordering, not volume) and the concurrent run
  is faster. Kill the process mid-batch → assert the cursor only ever sits at a
  contiguous prefix (no gap) and the re-run is idempotent.
- **Re-decode**: backfill raw, drop all decoded event tables, replay decode from
  `raw_*` with the **RPC client disabled** → event tables fully rebuilt, zero calls.
- **Cross-contract cache**: index two contracts sharing blocks; assert each shared
  block/tx/receipt fetched exactly once (call count independent of contract count).
- **Integration**: docker-compose (Postgres + anvil + queue); backfill a known range
  and diff decoded counts against `cast logs`.
