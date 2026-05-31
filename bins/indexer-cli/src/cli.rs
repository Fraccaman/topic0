//! CLI surface: `clap` argument parsing.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "indexer", about = "Config-driven, chain-agnostic indexer")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Diff config schemas against the DB and apply (DDL).
    Migrate {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_destructive: bool,
    },
    /// Backfill a height range for one chain: ingest → decode → write.
    Backfill {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long)]
        chain: u64,
        #[arg(long)]
        from: u64,
        #[arg(long)]
        to: u64,
    },
    /// Re-decode a range from the raw store (zero RPC).
    Resync {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long)]
        chain: u64,
        #[arg(long)]
        from: u64,
        #[arg(long)]
        to: u64,
    },
    /// Follow the chain tip: resume, catch up, index new finalized blocks.
    Follow {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long)]
        chain: u64,
        /// Tip poll cadence (seconds); overrides `[indexer].tip_interval_secs`.
        #[arg(long)]
        interval: Option<u64>,
    },
    /// Run decode workers: drain the work queue, decode ranges, upsert.
    Decode {
        #[arg(long, default_value = "config.toml")]
        config: String,
        /// Number of concurrent worker tasks.
        #[arg(long, default_value_t = 4)]
        workers: usize,
    },
    /// Supervisor: per-chain ingest loops + an in-process decode worker pool.
    Run {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// Tip poll cadence (seconds); overrides `[indexer].tip_interval_secs`.
        #[arg(long)]
        interval: Option<u64>,
    },
}
