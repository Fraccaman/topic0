# Codebase Design — Layered Architecture

Companion to [ARCHITECTURE.md](ARCHITECTURE.md). That doc = *what/why* the system
does. This doc = *how the code is laid out*: a clean Rust cargo workspace with strict
layering and a one-way dependency rule.

## Context

The system is several binaries (ingestor, decoder, query API, CLI) over one Postgres
DB and pluggable RPC sources. Without structure these binaries grow tangled SQL +
business logic + transport code. Goal: enforce **layered architecture** so each
concern lives in one place, is unit-testable in isolation, and the DB is swappable
behind ports.

Layers, outer → inner, dependencies point **inward only**:

```
handlers   (GraphQL resolvers, worker loops, CLI commands)   transport / entrypoints
   │  depends on
   ▼
services   (business logic / orchestration)                  use-cases
   │  depends on
   ▼
ports      (repository + adapter traits — in `domain`)       interfaces
   ▲  implemented by
   │
adapters   (db, db-query, source, migrator, api/graphql)     infrastructure
```

The **dependency rule**: `services` know only *ports* (traits), never concrete
Postgres or RPC. Adapters depend on `domain` to implement the ports. Handlers wire
concrete adapters into services at the composition root (the binaries). DTOs live at
the handler boundary and never leak into services or `db`.

---

## Workspace

> **As-built vs aspirational.** The tree below is the **actual** workspace. Two
> crates this doc once planned — `observability` (tracing/metrics init) and
> `bootstrap` (a dedicated DI root) — are **not built**: tracing is initialized
> inline in the bins and `bins/indexer-cli` *is* the composition root. Those
> sections are flagged 🔭 *aspirational* below. The layering rule itself holds in
> the code that exists.

```
env-indexer/
├── Cargo.toml                     # [workspace] members + shared deps
├── crates/
│   │  ── core (pure, no I/O) ──────────────────────────────────────────
│   ├── shared/                    # ★ shared entities + value objects, errors, utils
│   ├── domain/                    # PORTS (traits) over shared entities
│   ├── abi/                       # pure ABI parse + decode (decode → EventRow), no I/O
│   ├── pricing/                   # cost math: CostModel impls + SpendLedger (PlanProfile lives in shared)
│   ├── config/                    # config.toml load + validate → typed Config
│   │  ── infrastructure adapters ─────────────────────────────────────
│   ├── db/                        # write side: pool, UnitOfWork, repositories, base schema, encode
│   ├── db-query/                  # ★ read side: PgEventQueryRepository (QuerySpec→SQL + pagination), one file
│   ├── migrator/                  # ABI→DDL plan/apply engine (used by CLI only)
│   ├── source/                    # RPC adapter: ChainSource impl + metering(pricing) + limiter + enrichment + registry
│   │  ── application ─────────────────────────────────────────────────
│   ├── services/                  # business logic over ports — one module+trait per use-case
│   └── api/                       # GraphQL ApiServer impl (single generic `events` query)
└── bins/
    ├── indexer-cli/               # `indexer`: migrate · backfill · resync · follow ·
    │                              #   run (per-chain ingest loops + decode worker pool) ·
    │                              #   decode --workers N (standalone worker pool)
    │                              #   — ALSO the composition root: wires adapters directly
    └── query/                     # `indexer-query`: GraphQL server
```

> 🔭 Not present: `crates/observability/`, `crates/bootstrap/`, and separate
> `ingestor`/`decoder` bins. The CLI hosts every command and does the wiring.

### Modularity refinements (why this split)

- **CQRS split of `db`** → `db` (write: repos + UnitOfWork) vs **`db-query`** (read
  model: the QuerySpec→SQL engine). Read and write evolve independently; the query
  builder is large enough to own a crate and test in isolation. **`migrator`** (DDL
  gen/diff) leaves `db` entirely — it's a CLI-time concern, not on the hot path, so
  the runtime services never compile it.
- **`pricing` extracted from `source`** → cost models, `PlanProfile`, `SpendLedger`
  are *pure* (no network), so they belong in a core crate unit-tested against
  provider rate tables. `source` keeps only I/O + the metering wrapper that *uses*
  `pricing`.
- 🔭 **`bootstrap` (DI root)** *would* remove the config→pool→source wiring; today that
  wiring lives in `bins/indexer-cli/main.rs` instead.
- 🔭 **`observability`** *would* centralize tracing/metrics init; today each bin inits
  `tracing-subscriber` inline and metrics aren't exported.
- **One unit per file** (partial): `services/` is one use-case per module; `domain/ports/`
  mostly groups by concern (the five repo traits share `repository.rs`).
- **Pure decode in `abi`**: `(RawRecord, &EventSchema) → EventRow` is a pure function in
  `abi`; the `Decoder` port wraps it per-chain and `DecodingService`/`DecodeWorker` only
  orchestrate (pull → fetch raw → decode → upsert).

Crate dependency graph (arrows = "depends on"), grouped by layer:

```
              ┌──────────────── bins/* ────────────────┐   (composition entrypoints)
              │  indexer-cli is the composition root:   │
              │  imports + wires all adapters + services │
              ▼                                          ▼
       api  services  (+ db, db-query, migrator, source — concrete adapters wired here)
        │      │
        ▼      ▼
       services ──▶ domain ──▶ shared
                      ▲          ▲ ▲ ▲
   adapters implement │          │ │ └── abi      ──▶ shared
   the domain ports:  │          │ └──── pricing  ──▶ shared
   db, db-query,  ────┘          └────── config   ──▶ shared
   source, migrator
   (migrator also ──▶ db for apply)
```

Rules:
- **`shared`** depends on nothing internal (std + `thiserror` + `alloy-primitives`
  value types). Universal leaf.
- **`domain`** = ports over `shared`; **`abi`/`pricing`/`config`** = pure, depend
  only on `shared`.
- **Adapters** (`db`, `db-query`, `migrator`, `source`) implement `domain` ports;
  they may use `pricing`/`abi` but never `services`/`api`.
- **`services`** depend only on `domain` + pure crates — never on a concrete adapter.
- **`bins/indexer-cli`** is the composition root: the only place that imports concrete
  adapters + services and wires them. (A dedicated `bootstrap` crate 🔭 would hold this;
  today it lives in the CLI's `main.rs`.)

---

## Layer detail

### `shared` — shared entities (pure data)

The universal leaf crate. **Entities, value objects, and the shared error — no
traits, no logic, no I/O.** Every other crate depends on it for the common
vocabulary, so it must stay dependency-light and stable.

```
shared/src/
├── lib.rs
├── model/                  # entities + value objects (newtypes)
│   ├── chain.rs            # ChainId, Height, Hash, AddressBytes (newtypes)
│   ├── record.rs          # RawRecord, RecordIndex, RecordFilter, EventValue, EventRow
│   ├── block.rs            # BlockMeta, transaction/receipt enrichment rows
│   ├── work.rs             # WorkItem, WorkKind, Epoch
│   ├── schema.rs           # EventSchema, ColumnDef, AbiType→SqlType mapping types
│   ├── query.rs            # QuerySpec, Filter, FilterOp, Sort, Cursor, Page<T> (transport-neutral)
│   └── spend.rs            # RpcCall, CostUnits, MicroUsd, PlanProfile
├── util.rs                 # small cross-cutting helpers
└── error.rs                # DomainError (thiserror); no sqlx/reqwest types leak here
```

Note: `PlanProfile` lives here (in `spend.rs`), consumed by both `pricing` and
`source`. Only `serde` derive and `alloy-primitives` value types allowed — keep it a leaf.

### `domain` — ports (traits) over shared entities

Pure, infra-free interfaces. The contract adapters implement and services consume.
Holds **no data definitions** — those live in `shared`.

Focused ports, grouped by file:
```
domain/src/
├── lib.rs                  # re-exports shared entities for ergonomic `domain::RawRecord`
└── ports/
    ├── repository.rs       # EventRepository, RawRecordRepository, BlockRepository,
    │                       #   CursorRepository, EventQueryRepository (read side → db-query)
    ├── queue_repo.rs       # QueueRepository
    ├── chain_source.rs     # ChainSource (head, fetch_records, fetch_aux, subscribe; carries plan + cost model)
    ├── cost_model.rs       # CostModel
    ├── decoder.rs          # Decoder — raw record → EventRow rows (per-chain, built from ABI)
    ├── migrator.rs         # Migrator — plan()/apply() schema port (impl in `migrator` crate)
    ├── api_server.rs       # ApiServer — protocol-agnostic serve seam (impl in `api` crate)
    └── unit_of_work.rs     # UnitOfWork — atomic multi-repo transaction boundary (IngestBatch)
```

Notes vs the "one trait per file" ideal: the five repository traits share
`repository.rs` rather than splitting per file. There is **no** `LogSource`,
`TxRepository`, or `ReceiptRepository` port — the source seam is `ChainSource`, and
transaction/receipt rows are written through `BlockRepository`/`UnitOfWork` enrichment,
not dedicated repos. Decode is itself a port (`Decoder`), built per chain from the ABI.

Ports are small and intention-revealing, e.g.:
```rust
#[async_trait]
pub trait RawRecordRepository {
    async fn insert_batch(&self, records: &[RawRecord]) -> Result<(), DomainError>;
    async fn range(&self, chain: ChainId, from: Height, to: Height)
        -> Result<Vec<RawRecord>, DomainError>;
}

#[async_trait]
pub trait EventQueryRepository {                 // read side, used by query API
    async fn query(&self, spec: &QuerySpec) -> Result<Page<EventRow>, DomainError>;
}
```
`UnitOfWork::commit_ingest(IngestBatch)` writes raw records + block metas + enrichment
+ enqueue + cursor advance atomically in one tx — the exactly-once guarantee from ARCHITECTURE.

### `db` — write side (repositories + UnitOfWork)

Owns the **write** SQL + transaction boundary. Implements the write repository ports
for Postgres via `sqlx`.

```
db/src/
├── lib.rs
├── pool.rs                 # PgPool builder + connect(url, max_conns)
├── base_schema.rs          # creates the fixed tables (raw_records, blocks, cursors, work_queue, …)
├── unit_of_work.rs         # PgUnitOfWork: commit_ingest(IngestBatch) in one sqlx::Transaction
├── encode.rs               # shared model ↔ SQL params (bind/encode); block+tx+receipt enrichment rows
├── repositories/
│   ├── event_repo.rs       # PgEventRepository: upsert_batch, delete_from_block, query (read seam shares it)
│   ├── raw_record_repo.rs  # PgRawRecordRepository: insert_batch, range
│   ├── block_repo.rs       # PgBlockRepository: block metas + tx/receipt enrichment upserts
│   ├── cursor_repo.rs      # PgCursorRepository: get/advance/rewind
│   └── queue_repo.rs       # PgQueueRepository: push, pull_any(per-chain SKIP LOCKED), ack, reclaim, depth
└── error.rs                # DbError (thiserror) wrapping sqlx::Error → maps to DomainError
```
Repositories return **shared models**, never `sqlx::Row`. tx/receipt rows ride the
`block_repo`/`encode` enrichment path (no separate repos). `db` depends on `domain` +
`shared` + `config`; never on `services`.

### `db-query` — read side (the read model)

The CQRS read half, separate so query logic evolves without touching write repos.

```
db-query/src/
└── lib.rs                  # PgEventQueryRepository: QuerySpec → parameterized SQL (bound params,
                            #   injection-safe) + keyset pagination on (height, log_index) → Page<EventRow>
```
Implements `EventQueryRepository`. Builder, pagination, and reader are collapsed into
one file today (small enough to not warrant the split). Shares the `PgPool` (passed
in) but owns no writes.

### `migrator` — ABI→DDL engine (CLI-time only)

Dynamic schema generation, kept out of the runtime services so the hot path never
compiles it. Implements the `Migrator` port so `SchemaService` depends on the trait,
not this crate.

```
migrator/src/
├── lib.rs                  # crate root: module decls + re-exports (PgMigrator, MigrateError)
├── migrator.rs             # PgMigrator: impl domain::Migrator — plan() (diff config ABIs vs
│                           #   information_schema), apply(plan, allow_destructive); --dry-run handled by caller
├── ddl.rs                  # AbiType→Postgres column; per-table DDL (CREATE TABLE/ALTER/index/FK) generation
└── error.rs                # MigrateError (thiserror) wrapping sqlx::Error → maps to DomainError
```
(`diff`/`apply` are folded into the `Migrator` impl in `migrator.rs`, not separate files.)

### `pricing` — pure cost math

No I/O. Unit-tested against published provider rate tables.

```
pricing/src/
├── lib.rs
├── alchemy.rs              # AlchemyCost impl
├── quicknode.rs            # QuickNodeCost impl
├── free_node.rs            # FreeNodeCost (~0)
└── ledger.rs               # SpendLedger: accumulate units, monthly free-quota guard (remaining_quota/quota_exhausted)
```
Implements the `CostModel` port. `PlanProfile` lives in `shared::model::spend`, not
here. `source` depends on `pricing`, not vice-versa.

### `source` — RPC adapter (I/O only)

Implements `ChainSource` (struct `RpcLogSource`) for real providers; holds the
token-bucket limiter, batching, and the metering wrapper that *uses* `pricing`. Pure
cost math lives in `pricing`.

```
source/src/
├── lib.rs
├── client.rs               # RpcLogSource: alloy-provider http+ws; getLogs/getBlock(full)/receipts; backon retry
├── enrichment.rs           # block(full)→tx rows + receipt rows assembly (the aux fetch)
├── metering.rs             # MeteredSource: wraps a ChainSource, feeds pricing::CostModel → SpendLedger
├── limiter.rs              # governor token bucket from PlanProfile
├── registry.rs             # build_chain / build_decoder: kind+limits (config) → ChainSource + Decoder + ledger
└── error.rs                # SourceError
```
Note: `client::fetch_records` applies **adaptive result-cap halving** — a range that
overflows `max_getlogs_results` (explicit provider error, or a page that hits the cap)
is split in half and each side retried, down to a single block. The split is transient
(caller range/cursor untouched). On a non-cap getLogs error the call returns `Err` and
ingestion aborts without advancing the cursor.

### 🔭 `observability` — cross-cutting init (not built)

Planned crate to centralize `tracing-subscriber` + Prometheus metrics init. **Not
present today**: each bin sets up `tracing-subscriber` inline in `main`. Spend/lag/CU
metrics are not yet exported.

### `services` — business logic

The use-cases. Depend only on ports + `abi`. No SQL, no HTTP, no GraphQL.

```
services/src/
├── lib.rs
├── ingestion.rs            # IngestionService: fetch via ChainSource, write raw + enqueue (UnitOfWork), advance cursor
├── decoding.rs             # decode_range() fn + DecodingService (inline single-chain decode)
├── worker.rs               # DecodeWorker: queue consumer — pull_any → decode (per-chain Decoder) → upsert → ack
├── reorg.rs                # ReorgService: detect (hash divergence), delete≥B, rewind, requeue
└── schema.rs               # SchemaService: orchestrate migrate via domain::Migrator port + preflight check
```
(SpendService/QueryService not split out yet — spend lives in the `SpendLedger`;
query logic is in `db-query`.)

Each service takes its dependencies as **`Box<dyn Port>`**, injected by the binary.
Example (actual shape):
```rust
pub struct IngestionService { source: Box<dyn ChainSource>, uow: Box<dyn UnitOfWork> }
impl IngestionService {
    pub async fn ingest_range(&self, filter: &RecordFilter, from: Height, to: Height,
        epoch: Epoch, advance_cursor: bool) -> Result<IngestOutcome, DomainError> {
        /* fetch_records → fetch_aux → uow.commit_ingest(IngestBatch { raw, blocks, enrich, enqueue, cursor }) */
    }
}
```
→ trivially unit-tested with mock ports, no DB/network (see `services/tests/`).

### `api` — protocol-agnostic server + GraphQL impl

Transport layer for reads. **`ApiServer` is the protocol-agnostic seam (port);
`GraphqlApiServer` is the one implementation built** (REST/gRPC could be added as
sibling impls). The resolver is **thin**: parse args → `shared::QuerySpec` → call the
`EventQueryRepository` port → return rows as JSON. No business logic, no SQL here.

```
api/src/
├── lib.rs
└── graphql.rs              # GraphqlApiServer (impl domain::ApiServer): async-graphql Schema + axum serve,
                            #   playground, and the single QueryRoot::events resolver
```

The schema is **not** generated per event-type today. There is one generic query:
```graphql
events(table: String!, first: Int = 100, after: String,
       fromHeight: Int, toHeight: Int): EventConnection
# EventConnection { nodes { json }, hasNext, endCursor }   — each row returned as a JSON object
```
🔭 *Aspirational:* per-table GraphQL types, nested tx/receipt resolvers, meta/spend
resolvers, a request/response DTO layer, and a `QueryService` (validation/visibility
watermark) — none built. Reads go resolver → `EventQueryRepository` directly, and the
`expose` watermark is not yet enforced in the resolver.

### 🔭 `bootstrap` — composition root (not built)

Planned DI crate to hold the config→pool→source/repos wiring. **Not present**: that
wiring lives directly in `bins/indexer-cli/src/main.rs` (helpers like `new_ingest`,
`build_decoder_repos`, `build_reorg`, `source::build_chain`). The `query` bin wires
its own `PgEventQueryRepository` + `GraphqlApiServer`.

### `bins/*` — entrypoints (also the wiring)

`bins/indexer-cli/src/main.rs` is `clap`-dispatched (`migrate`/`backfill`/`resync`/
`follow`/`run`/`decode`) and **also performs all adapter wiring** — there
is no `bootstrap` layer between it and the adapters. Each command builds the pool +
source + repos, then drives the relevant service. `bins/query/src/main.rs` boots the
GraphQL server. Both init `tracing-subscriber` inline.

---

## Vertical slices (how a request flows through the layers)

**Read (GraphQL query `events(table:…)`):**
```
api::graphql QueryRoot::events  (resolver: args → shared::QuerySpec with height Gte/Lte filters)
  → domain::EventQueryRepository (port)
    → db-query::PgEventQueryRepository (build param SQL + keyset pagination, run → Page<EventRow>)
  ← Page<EventRow>  → EventConnection { nodes{json}, hasNext, endCursor }
```

**Write (ingest a block range):**
```
indexer-cli process_window (backfill/follow loop)
  → services::IngestionService::ingest_range
    → domain::ChainSource (port)  → source::MeteredSource (getLogs + block(full) + receipts)
    → domain::UnitOfWork (port) → db::PgUnitOfWork::commit_ingest tx {
          RawRecordRepository.insert_batch,
          BlockRepository.upsert (block metas + tx + receipt enrichment),
          QueueRepository.push(WorkItem),
          CursorRepository.advance
      }  ← single commit = exactly-once enqueue
```

**Decode (queue → event tables) — `indexer decode --workers N` / in-process under `run`:**
```
DecodeWorker pool (competing consumers)
  → services::DecodeWorker::run_once
    → QueueRepository.pull_any   (oldest item whose chain has none in-flight; SKIP LOCKED)
    → decoders[item.chain_id]    (per-chain Decoder)
    → RawRecordRepository.range  → Decoder::decode(raw) → EventRow batch (+ block_time)
    → EventRepository.upsert_batch
    → QueueRepository.ack
  (+ reclaim ticker frees leases past the visibility timeout)
```

---

## Conventions (Rust best practice)

- **Layered errors** (no god-enum): each adapter owns a focused error —
  `db::DbError`, `source::SourceError`, `migrator::MigrateError`, `abi::DecodeError`
  (all `thiserror`). They fold into `shared::DomainError` via `#[from]` at the port
  boundary, so services see one error type while each layer keeps a precise one.
  Infra types (`sqlx`, `reqwest`) never cross inward. Binaries use `anyhow` at `main`.
- **Async traits**: `async-trait` on ports; concrete impls can use native async.
- **Dependency injection**: services are generic over ports or hold `Arc<dyn Port>`;
  no global singletons. Constructed at the composition root.
- **No leaky abstractions**: `sqlx`, `alloy`, `async-graphql` types stay inside their
  owning crate. `shared`, `domain` & `services` import none of them (except value
  types like `alloy-primitives::Address`).
- **Newtypes over primitives**: `ChainId(u64)`, `Height(u64)`, `Hash`, `AddressBytes`
  — no bare `u64`/`String` for domain ids.
- **`#![deny(warnings)]` + clippy pedantic** in CI; `rustfmt` enforced.
- **Tests**: unit tests per service with mock ports (`mockall` or hand-rolled);
  `db` integration tests against `testcontainers` Postgres; api tests run resolvers
  against an in-memory mock `EventQueryRepository`.

| Concern                  | Crate(s)            |
|--------------------------|---------------------|
| Mock ports for tests     | `mockall` (dev-dep) |
| Test fixtures / Postgres | `testcontainers`    |

---

## Mapping to ARCHITECTURE build order

Same M1–M6 milestones, placed in the layers that exist:
- **M1 fetch**: `shared`, `domain`, `config`, `pricing`, `source`, `abi`.
- **M2 decode+migrate**: `db` (write repos), `migrator`, `services::decoding`,
  `services::schema`, `bins/indexer-cli migrate`.
- **M3 raw+plan**: `db::raw_record_repo`, `source::limiter`
  + `PlanProfile`, `services::ingestion` writes raw via `UnitOfWork`.
- **M4 tip**: `source` ws subscribe feeding `IngestionService` (`follow`).
- **M5 reorg**: `services::reorg` + `db` delete/rewind.
- **M6 multi-chain + API**: `api` (`graphql.rs`), `db-query`, `bins/query`; per-chain
  task supervision in `bins/indexer-cli run`. (🔭 `bootstrap`, `observability`, and a
  `services::query` layer remain unbuilt.)

## Verification

- **Layer boundaries**: a CI check (e.g. `cargo-deny`/`cargo-modules` or a simple
  dependency lint) asserts `shared`, `domain`, `services`, `pricing`, and `abi` have
  **no** dependency on `sqlx`, `alloy-provider`, or `async-graphql`; that `shared`
  depends on no internal crate; and that only `bins/indexer-cli` (and `bins/query`)
  import concrete adapter crates (`db`, `db-query`, `source`, `migrator`, `api`).
  (Once `bootstrap` 🔭 exists, it becomes the single allowed importer.)
- **Repository contract tests**: run the same port test-suite against the Pg adapter
  (testcontainers) — proves impls honor the trait contract.
- **Service unit tests**: `IngestionService`/`DecodingService` with mock ports assert
  orchestration (e.g. raw-insert + enqueue happen in one `UnitOfWork`, cursor advances
  only after commit) with zero DB/network.
- **Query builder**: property-test `QuerySpec → SQL` emits only bound params (no
  interpolation), and keyset pagination is stable across pages.
- **API**: resolver tests over a mock `EventQueryRepository` assert DTO mapping +
  visibility-watermark filtering, independent of Postgres.
- **End-to-end**: the existing docker-compose (Postgres + anvil) integration from
  ARCHITECTURE, now driven through the binaries.
```
