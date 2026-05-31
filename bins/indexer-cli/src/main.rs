//! `indexer` CLI — migrate / backfill / resync / follow / decode / run over pluggable
//! chains. Modules: `cli` (args), `wiring` (composition root), `commands` (handlers),
//! `ingest` (window + tip loop), `tip` (tip state machine), `plan` (range planning),
//! `supervisor` (chain restart), `worker` (decode pool), `shutdown` (signals).

mod cli;
mod commands;
mod ingest;
mod plan;
mod shutdown;
mod supervisor;
mod tip;
mod wiring;
mod worker;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Cmd};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    if let Some(addr) = metrics_listen(&cli.cmd) {
        observability::install(addr.parse()?)?;
    }

    match cli.cmd {
        Cmd::Migrate {
            config,
            dry_run,
            allow_destructive,
        } => commands::migrate(&config, dry_run, allow_destructive).await,
        Cmd::Backfill {
            config,
            chain,
            from,
            to,
        } => commands::backfill(&config, chain, from, to).await,
        Cmd::Resync {
            config,
            chain,
            from,
            to,
        } => commands::resync(&config, chain, from, to).await,
        Cmd::Follow {
            config,
            chain,
            interval,
        } => commands::follow(&config, chain, interval).await,
        Cmd::Decode { config, workers } => commands::decode_workers(&config, workers).await,
        Cmd::Run {
            config,
            workers,
            interval,
        } => commands::run(&config, workers, interval).await,
    }
}

/// Resolve the `[indexer] metrics_listen` address for the active command, if set.
/// Returns `None` (exporter disabled) when unset or the config can't be loaded here —
/// the command itself surfaces any load error.
fn metrics_listen(cmd: &Cmd) -> Option<String> {
    let path = match cmd {
        Cmd::Migrate { config, .. }
        | Cmd::Backfill { config, .. }
        | Cmd::Resync { config, .. }
        | Cmd::Follow { config, .. }
        | Cmd::Decode { config, .. }
        | Cmd::Run { config, .. } => config,
    };
    config::load(path).ok()?.indexer.metrics_listen
}
