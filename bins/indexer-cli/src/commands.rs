//! Command handlers — one per CLI subcommand. Each wires services via `wiring` then
//! drives `ingest`/`worker`; no business logic lives here.

use crate::ingest::ingest_loop;
use crate::plan::run_pipeline;
use crate::shutdown::shutdown_token;
use crate::supervisor::supervised_ingest;
use crate::wiring::{
    base_dir, boot, build_decoders, build_reorg, find_chain, new_ingest, preflight,
};
use crate::worker::spawn_worker_pool;
use anyhow::Result;
use domain::ports::decoder::Decoder;
use domain::ports::migrator::Migrator;
use domain::ports::queue_repo::QueueRepository;
use shared::{ChainId, Height};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(crate) async fn migrate(
    config_path: &str,
    dry_run: bool,
    allow_destructive: bool,
) -> Result<()> {
    let cfg = config::load(config_path)?;
    let base = base_dir(config_path);

    let mut schemas = Vec::new();
    for chain in &cfg.chains {
        let decoder = source::build_decoder(chain, &base)?;
        schemas.extend(decoder.schemas());
    }

    let pool = db::connect(&cfg.database.url, cfg.database.max_conns).await?;
    db::base_schema::init(&pool).await?;

    let mig = migrator::PgMigrator::new(pool);
    let plan = mig.plan(&schemas).await?;
    if dry_run {
        println!("-- dry run: {} statement(s) --", plan.statements.len());
        for s in &plan.statements {
            println!("{s};");
        }
    } else {
        mig.apply(&plan, allow_destructive).await?;
        println!("applied {} statement(s)", plan.statements.len());
    }
    Ok(())
}

pub(crate) async fn backfill(config_path: &str, chain_id: u64, from: u64, to: u64) -> Result<()> {
    let (cfg, pool) = boot(config_path).await?;
    let base = base_dir(config_path);
    let chain = find_chain(&cfg, chain_id)?;

    let built = source::build_chain(chain, &base, cfg.indexer.aux_concurrency).await?;
    let filter = built.decoder.record_filter();
    let schemas = built.decoder.schemas();
    let ledger = built.ledger.clone();
    let step = built.source.plan().max_getlogs_blocks.max(1) as u64;
    let range_concurrency = cfg.indexer.range_concurrency.max(1);

    db::base_schema::init(&pool).await?;
    preflight(&pool, &schemas).await?;

    let decoder: Arc<dyn Decoder> = Arc::from(built.decoder);
    let ingest = new_ingest(&pool, built.source);
    let cursors = db::PgCursorRepository::new(pool.clone());
    let token = shutdown_token();

    // Decode runs off the fetch critical path: the pipeline enqueues ranges, a worker
    // pool decodes them concurrently (and after the fetch finishes). Child token so
    // SIGINT stops the workers too, but we also stop them once the queue drains.
    let mut decoders: HashMap<ChainId, Arc<dyn Decoder>> = HashMap::new();
    decoders.insert(ChainId(chain_id), decoder);
    let worker_token = token.child_token();
    let workers = spawn_worker_pool(
        &pool,
        &decoders,
        range_concurrency,
        cfg.queue.poll_ms,
        cfg.queue.poll_idle_ms,
        worker_token.clone(),
    );

    let out = run_pipeline(
        &ingest,
        &cursors,
        ChainId(chain_id),
        &filter,
        from,
        to,
        step,
        range_concurrency,
        &ledger,
        &token,
    )
    .await?;

    drain_queue(&pool, &token).await?;
    worker_token.cancel();
    for h in workers {
        let _ = h.await;
    }

    println!(
        "backfill complete: {} records ingested, decoded via worker pool; spent {} units (~${:.4})",
        out.records,
        ledger.spent_units().0,
        ledger.billable_micro_usd().0 as f64 / 1_000_000.0
    );
    Ok(())
}

/// Block until the work queue is empty (all enqueued ranges decoded + acked) or
/// shutdown is requested. Leased-but-unacked items still count, so depth 0 = fully
/// drained.
async fn drain_queue(pool: &db::PgPool, token: &CancellationToken) -> Result<()> {
    let q = db::PgQueueRepository::new(pool.clone());
    loop {
        if token.is_cancelled() || q.depth().await? == 0 {
            break;
        }
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    Ok(())
}

pub(crate) async fn resync(config_path: &str, chain_id: u64, from: u64, to: u64) -> Result<()> {
    let (cfg, pool) = boot(config_path).await?;
    let base = base_dir(config_path);
    let chain = find_chain(&cfg, chain_id)?;
    let decoder = source::build_decoder(chain, &base)?;

    let events = services::decode_range(
        decoder.as_ref(),
        &db::PgRawRecordRepository::new(pool.clone()),
        &db::PgBlockRepository::new(pool.clone()),
        &db::PgEventRepository::new(pool.clone()),
        ChainId(chain_id),
        Height(from),
        Height(to),
    )
    .await?;
    println!("resync complete: {events} rows re-decoded from raw (0 RPC calls)");
    Ok(())
}

pub(crate) async fn follow(config_path: &str, chain_id: u64, interval: Option<u64>) -> Result<()> {
    let (cfg, pool) = boot(config_path).await?;
    let interval = interval.unwrap_or(cfg.indexer.tip_interval_secs);
    let base = base_dir(config_path);
    let chain = find_chain(&cfg, chain_id)?;
    let confirmations = chain.confirmations;

    let built = source::build_chain(chain, &base, cfg.indexer.aux_concurrency).await?;
    let filter = built.decoder.record_filter();
    let schemas = built.decoder.schemas();
    let tables: Vec<String> = schemas.iter().map(|s| s.table.clone()).collect();
    let ledger = built.ledger.clone();
    let step = built.source.plan().max_getlogs_blocks.max(1) as u64;
    let range_concurrency = cfg.indexer.range_concurrency.max(1);
    let start_block = chain.start_block().expect("validated: chain has a start");

    db::base_schema::init(&pool).await?;
    preflight(&pool, &schemas).await?;

    // Single-chain `run`: the ingest loop enqueues ranges, a worker pool decodes them
    // off the tip critical path.
    let decoder: Arc<dyn Decoder> = Arc::from(built.decoder);
    let mut decoders: HashMap<ChainId, Arc<dyn Decoder>> = HashMap::new();
    decoders.insert(ChainId(chain_id), decoder);

    let ingest = new_ingest(&pool, built.source);
    let reorg = build_reorg(&pool, tables);
    let cursors = db::PgCursorRepository::new(pool.clone());
    let token = shutdown_token();

    let workers = spawn_worker_pool(
        &pool,
        &decoders,
        range_concurrency,
        cfg.queue.poll_ms,
        cfg.queue.poll_idle_ms,
        token.clone(),
    );

    tracing::info!(chain = chain_id, confirmations, "following tip");
    ingest_loop(
        ingest,
        reorg,
        cursors,
        filter,
        ledger,
        confirmations,
        start_block,
        step,
        range_concurrency,
        interval,
        token.clone(),
    )
    .await?;

    for h in workers {
        let _ = h.await;
    }
    Ok(())
}

pub(crate) async fn decode_workers(config_path: &str, workers: usize) -> Result<()> {
    let (cfg, pool) = boot(config_path).await?;
    let base = base_dir(config_path);
    let decoders = build_decoders(&cfg, &base)?;

    let token = shutdown_token();
    let handles = spawn_worker_pool(
        &pool,
        &decoders,
        workers,
        cfg.queue.poll_ms,
        cfg.queue.poll_idle_ms,
        token.clone(),
    );
    tracing::info!(workers, "decode workers running");

    for h in handles {
        let _ = h.await;
    }
    tracing::info!("decode workers stopped");
    Ok(())
}

pub(crate) async fn run(config_path: &str, workers: usize, interval: Option<u64>) -> Result<()> {
    let (cfg, pool) = boot(config_path).await?;
    let interval = interval.unwrap_or(cfg.indexer.tip_interval_secs);
    let base = base_dir(config_path);
    db::base_schema::init(&pool).await?;

    let token = shutdown_token();
    let mut decoders: HashMap<ChainId, Arc<dyn Decoder>> = HashMap::new();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for chain in &cfg.chains {
        // Decoder is network-free and immutable → build once for the worker map; the
        // source (HTTP/WS) is (re)built per attempt inside the supervised loop.
        let decoder = source::build_decoder(chain, &base)?;
        let schemas = decoder.schemas();
        preflight(&pool, &schemas).await?;
        let tables: Vec<String> = schemas.iter().map(|s| s.table.clone()).collect();
        decoders.insert(ChainId(chain.id), Arc::from(decoder));

        handles.push(tokio::spawn(supervised_ingest(
            chain.clone(),
            base.clone(),
            pool.clone(),
            tables,
            cfg.indexer.aux_concurrency,
            cfg.indexer.range_concurrency.max(1),
            interval,
            token.clone(),
        )));
    }

    handles.extend(spawn_worker_pool(
        &pool,
        &decoders,
        workers,
        cfg.queue.poll_ms,
        cfg.queue.poll_idle_ms,
        token.clone(),
    ));
    tracing::info!(chains = cfg.chains.len(), workers, "supervisor running");

    // On SIGINT/SIGTERM tasks finish their in-flight range/item, then exit.
    for h in handles {
        let _ = h.await;
    }
    tracing::info!("supervisor stopped");
    Ok(())
}
