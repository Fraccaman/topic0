# env-indexer task runner — `just <recipe>`. Run `just` to list.
set dotenv-load := true

config   := "config.toml"
chain    := "1"
# Local dev Postgres (override via env or `just DATABASE_URL=… <recipe>`).
export DATABASE_URL := env_var_or_default("DATABASE_URL", "postgres://postgres:pw@localhost:55432/indexer")

_default:
    @just --list

# ── build / quality ──────────────────────────────────────────────
build:
    cargo build --workspace

release:
    cargo build --workspace --release

test:
    cargo test --workspace

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

# Everything CI runs.
ci: fmt-check clippy test

# ── local dev Postgres (throwaway docker) ────────────────────────
pg-up:
    -docker rm -f idx_pg
    docker run -d --name idx_pg -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=indexer \
        -p 55432:5432 postgres:16-alpine
    @echo "waiting for postgres…"
    @until docker exec idx_pg pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
    @echo "postgres ready on :55432"

pg-down:
    -docker rm -f idx_pg

# Delete the named compose Postgres volume (destroys all persisted data).
pg-volume-rm:
    -docker volume rm env-indexer-pgdata

psql *ARGS:
    docker exec -it idx_pg psql -U postgres -d indexer {{ARGS}}

# ── indexer commands (local, via cargo) ──────────────────────────
migrate *ARGS:
    cargo run --bin indexer -- migrate --config {{config}} {{ARGS}}

migrate-dry:
    cargo run --bin indexer -- migrate --config {{config}} --dry-run

backfill from to chain=chain:
    cargo run --bin indexer -- backfill --config {{config}} --chain {{chain}} --from {{from}} --to {{to}}

resync from to chain=chain:
    cargo run --bin indexer -- resync --config {{config}} --chain {{chain}} --from {{from}} --to {{to}}

follow chain=chain:
    cargo run --bin indexer -- follow --config {{config}} --chain {{chain}}

# Supervisor: ingest every chain + in-process decode pool.
run workers="4":
    cargo run --bin indexer -- run --config {{config}} --workers {{workers}}

# Standalone decode-worker pool (scale-out).
decode workers="4":
    cargo run --bin indexer -- decode --config {{config}} --workers {{workers}}

query:
    cargo run --bin indexer-query -- --config {{config}}

# ── docker compose ───────────────────────────────────────────────
docker-build:
    docker compose build

# `just up indexer query` → starts those profiles.
up *PROFILES:
    docker compose {{ prepend("--profile ", PROFILES) }} up -d

down:
    docker compose down

logs *ARGS:
    docker compose logs -f {{ARGS}}

# Scale decode workers: `just scale-decode 4`.
scale-decode n="4":
    docker compose --profile decode up -d --scale decode={{n}}

# ── housekeeping ─────────────────────────────────────────────────
clean:
    cargo clean
    -docker rm -f idx_pg
