// FDP SUI Benchmark - SDK-based High-Throughput I/O Benchmark
//
// This benchmark bypasses the `sui client` CLI overhead by directly using
// the SUI SDK to submit transactions. It's designed to maximize disk I/O
// for measuring Write Amplification Factor (WAF) on FDP vs non-FDP storage.
//
// Key features:
// - Direct SDK transaction submission (no CLI process spawning)
// - Async connection pooling to the SUI node
// - Batched PTB transactions (multiple operations per TX)
// - Mixed CREATE/UPDATE workload for hot/cold data segregation testing
// - Memory-efficient with configurable concurrency limits
//
// Usage:
//   cargo run --release -- \
//     --rpc-url http://127.0.0.1:9000 \
//     --package-id <PACKAGE_ID> \
//     --duration 300 \
//     --workers 8 \
//     --batch-size 50 \
//     --target-tps 500

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use futures::{StreamExt, stream::FuturesUnordered};
use rand::Rng;
use rand::SeedableRng;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sui_sdk::{SuiClient, SuiClientBuilder};
use sui_sdk::rpc_types::{
    SuiTransactionBlockEffectsAPI,
    SuiTransactionBlockResponseOptions,
};
use sui_sdk::types::{
    base_types::{ObjectID, ObjectRef, SuiAddress},
    crypto::{get_key_pair, SuiKeyPair, AccountKeyPair, KeypairTraits},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Transaction, TransactionData},
    transaction_driver_types::ExecuteTransactionRequestType,
    Identifier,
};
use tokio::sync::{Semaphore, RwLock};
use tokio::time::sleep;
use tracing::{info, warn, error, debug};

/// FDP SUI Benchmark - High-throughput I/O load generator
#[derive(Parser, Debug, Clone)]
#[clap(name = "fdp-sui-bench")]
struct Args {
    /// SUI RPC URL
    #[clap(long, default_value = "http://127.0.0.1:9000")]
    rpc_url: String,

    /// Package ID of the deployed io_churn contract
    #[clap(long, env = "FDP_PACKAGE_ID")]
    package_id: String,

    /// Benchmark duration in seconds
    #[clap(long, default_value = "300")]
    duration: u64,

    /// Number of concurrent workers
    #[clap(long, default_value = "32")]
    workers: usize,

    /// Objects per transaction batch (higher = more I/O per TX)
    #[clap(long, default_value = "100")]
    batch_size: usize,

    /// Target transactions per second (0 = unlimited)
    #[clap(long, default_value = "0")]
    target_tps: u64,

    /// Maximum concurrent in-flight transactions
    #[clap(long, default_value = "500")]
    max_inflight: usize,

    /// Percentage of CREATE operations (vs UPDATE)
    #[clap(long, default_value = "30")]
    create_pct: u8,

    /// Initial seed objects to create per worker
    #[clap(long, default_value = "200")]
    seed_objects: usize,

    /// Gas budget per transaction
    #[clap(long, default_value = "500000000")]
    gas_budget: u64,

    /// Stats reporting interval in seconds
    #[clap(long, default_value = "10")]
    stats_interval: u64,

    /// Use 4KB LargeBlob objects instead of MicroCounters for more I/O per TX
    #[clap(long, default_value = "false")]
    use_blobs: bool,

    /// Output file for results (JSON)
    #[clap(long)]
    output: Option<String>,

    /// Keystore path for signing transactions
    #[clap(long)]
    keystore: Option<String>,
}

/// Tracked object for updates
#[derive(Debug, Clone)]
struct TrackedObject {
    id: ObjectID,
    version: u64,
    digest: sui_sdk::types::base_types::ObjectDigest,
}

/// Worker state
struct WorkerState {
    id: usize,
    address: SuiAddress,
    keypair: SuiKeyPair,
    gas_coin: ObjectRef,
    objects: Vec<TrackedObject>,
}

/// Global benchmark statistics
struct BenchStats {
    tx_submitted: AtomicU64,
    tx_success: AtomicU64,
    tx_failed: AtomicU64,
    objects_created: AtomicU64,
    objects_updated: AtomicU64,
    start_time: Instant,
}

impl BenchStats {
    fn new() -> Self {
        Self {
            tx_submitted: AtomicU64::new(0),
            tx_success: AtomicU64::new(0),
            tx_failed: AtomicU64::new(0),
            objects_created: AtomicU64::new(0),
            objects_updated: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn report(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let submitted = self.tx_submitted.load(Ordering::Relaxed);
        let success = self.tx_success.load(Ordering::Relaxed);
        let failed = self.tx_failed.load(Ordering::Relaxed);
        let created = self.objects_created.load(Ordering::Relaxed);
        let updated = self.objects_updated.load(Ordering::Relaxed);

        let tps = if elapsed > 0.0 { success as f64 / elapsed } else { 0.0 };
        let ops_rate = if elapsed > 0.0 { (created + updated) as f64 / elapsed } else { 0.0 };

        format!(
            "Elapsed: {:.1}s | TX: {} submitted, {} success, {} failed | TPS: {:.1} | Objects: {} created, {} updated | Ops/s: {:.1}",
            elapsed, submitted, success, failed, tps, created, updated, ops_rate
        )
    }
}

/// Main benchmark runner
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    info!("╔═══════════════════════════════════════════════════════════════╗");
    info!("║  FDP SUI Benchmark - SDK-based High-Throughput I/O           ║");
    info!("╚═══════════════════════════════════════════════════════════════╝");
    info!("");
    info!("Configuration:");
    info!("  RPC URL:       {}", args.rpc_url);
    info!("  Package ID:    {}", args.package_id);
    info!("  Duration:      {}s", args.duration);
    info!("  Workers:       {}", args.workers);
    info!("  Batch Size:    {} objects/tx", args.batch_size);
    info!("  Max Inflight:  {}", args.max_inflight);
    info!("  Create %:      {}%", args.create_pct);
    info!("  Seed Objects:  {} per worker", args.seed_objects);
    info!("");

    // Parse package ID
    let package_id = ObjectID::from_hex_literal(&args.package_id)
        .context("Invalid package ID format")?;

    // Connect to SUI node
    info!("Connecting to SUI node...");
    let client = SuiClientBuilder::default()
        .build(&args.rpc_url)
        .await
        .context("Failed to connect to SUI node")?;

    info!("Connected to SUI node");

    // Cache reference gas price (fetch once, not per transaction)
    let cached_rgp = client
        .governance_api()
        .get_reference_gas_price()
        .await
        .unwrap_or(1000);
    info!("Cached reference gas price: {}", cached_rgp);

    // Running flag for workers
    let running = Arc::new(AtomicBool::new(true));

    // Semaphore for concurrency control - per-worker semaphore for better parallelism
    let semaphore = Arc::new(Semaphore::new(args.max_inflight));

    // Initialize workers IN PARALLEL (much faster than sequential)
    info!("Initializing {} workers in parallel...", args.workers);
    let init_start = Instant::now();
    
    // Create all keypairs first (fast, no I/O)
    let keypairs: Vec<_> = (0..args.workers)
        .map(|i| {
            let (address, keypair): (SuiAddress, AccountKeyPair) = get_key_pair();
            (i, address, keypair)
        })
        .collect();
    
    // Request gas from faucet in parallel batches (to avoid overwhelming faucet)
    let batch_size = 8; // Process 8 workers at a time
    let mut workers = Vec::new();
    
    for chunk in keypairs.chunks(batch_size) {
        let mut faucet_futures = Vec::new();
        for (i, address, keypair) in chunk {
            let client = client.clone();
            let addr = *address;
            let id = *i;
            let kp = keypair.copy();
            faucet_futures.push(async move {
                let gas_coin = request_gas_from_faucet(&client, addr).await?;
                Ok::<_, anyhow::Error>((id, addr, kp, gas_coin))
            });
        }
        
        // Execute batch in parallel
        let results = futures::future::join_all(faucet_futures).await;
        for result in results {
            let (id, address, keypair, gas_coin) = result?;
            info!("Worker {}: ready", id);
            workers.push(Arc::new(RwLock::new(WorkerState {
                id,
                address,
                keypair: SuiKeyPair::Ed25519(keypair),
                gas_coin,
                objects: Vec::new(),
            })));
        }
    }
    info!("Workers initialized in {:.1}s", init_start.elapsed().as_secs_f64());

    // Create seed objects for each worker IN PARALLEL
    info!("Creating seed objects ({} per worker) in parallel...", args.seed_objects);
    let seed_start = Instant::now();
    let mut seed_futures = Vec::new();
    for worker in &workers {
        let client = client.clone();
        let w = worker.clone();
        seed_futures.push(async move {
            create_seed_objects(&client, w, package_id, args.seed_objects, args.gas_budget).await
        });
    }
    // Execute all seed creations in parallel
    let seed_results = futures::future::join_all(seed_futures).await;
    for result in seed_results {
        result?;
    }
    info!("Seed objects created in {:.1}s", seed_start.elapsed().as_secs_f64());

    // Initialize stats AFTER setup - this ensures DURATION measures actual benchmark time
    let stats = Arc::new(BenchStats::new());
    
    // Start benchmark
    info!("");
    info!("═══════════════════════════════════════════════════════════════");
    info!("  BENCHMARK STARTED (duration: {}s)", args.duration);
    info!("═══════════════════════════════════════════════════════════════");

    // Start stats reporter
    let stats_clone = stats.clone();
    let running_clone = running.clone();
    let stats_interval = args.stats_interval;
    tokio::spawn(async move {
        while running_clone.load(Ordering::Relaxed) {
            sleep(Duration::from_secs(stats_interval)).await;
            info!("{}", stats_clone.report());
        }
    });

    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let mut handles = FuturesUnordered::new();

    // Spawn worker tasks
    for worker in workers {
        let client = client.clone();
        let args = args.clone();
        let stats = stats.clone();
        let running = running.clone();
        let semaphore = semaphore.clone();

        let handle = tokio::spawn(async move {
            run_worker(
                client,
                worker,
                package_id,
                args,
                stats,
                running,
                semaphore,
                deadline,
                cached_rgp,
            ).await
        });

        handles.push(handle);
    }

    // Wait for all workers
    while let Some(result) = handles.next().await {
        if let Err(e) = result {
            error!("Worker error: {:?}", e);
        }
    }

    // Stop stats reporter
    running.store(false, Ordering::Relaxed);

    // Final report
    info!("");
    info!("═══════════════════════════════════════════════════════════════");
    info!("  BENCHMARK COMPLETE");
    info!("═══════════════════════════════════════════════════════════════");
    info!("{}", stats.report());

    // Write output file if requested
    if let Some(output_path) = &args.output {
        let elapsed = stats.start_time.elapsed().as_secs_f64();
        let result = serde_json::json!({
            "duration_secs": elapsed,
            "tx_submitted": stats.tx_submitted.load(Ordering::Relaxed),
            "tx_success": stats.tx_success.load(Ordering::Relaxed),
            "tx_failed": stats.tx_failed.load(Ordering::Relaxed),
            "objects_created": stats.objects_created.load(Ordering::Relaxed),
            "objects_updated": stats.objects_updated.load(Ordering::Relaxed),
            "tps": stats.tx_success.load(Ordering::Relaxed) as f64 / elapsed,
            "config": {
                "workers": args.workers,
                "batch_size": args.batch_size,
                "create_pct": args.create_pct,
                "max_inflight": args.max_inflight,
            }
        });

        std::fs::write(output_path, serde_json::to_string_pretty(&result)?)?;
        info!("Results written to {}", output_path);
    }

    Ok(())
}

/// Request gas from the local faucet
async fn request_gas_from_faucet(client: &SuiClient, address: SuiAddress) -> Result<ObjectRef> {
    // Try local faucet first
    let faucet_url = "http://127.0.0.1:9123/gas";

    let faucet_client = reqwest::Client::new();
    
    // Retry faucet request up to 3 times
    let mut faucet_success = false;
    for attempt in 1..=3 {
        let response = faucet_client
            .post(faucet_url)
            .json(&serde_json::json!({
                "FixedAmountRequest": {
                    "recipient": address.to_string()
                }
            }))
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                debug!("Faucet request succeeded for {} (attempt {})", address, attempt);
                faucet_success = true;
                break;
            }
            Ok(resp) => {
                warn!("Faucet returned status {} for {} (attempt {})", resp.status(), address, attempt);
            }
            Err(e) => {
                warn!("Faucet request error for {} (attempt {}): {}", address, attempt, e);
            }
        }
        
        if attempt < 3 {
            sleep(Duration::from_millis(500)).await;
        }
    }
    
    if !faucet_success {
        warn!("All faucet attempts failed for {}, checking existing coins...", address);
    }

    // Wait for the faucet transaction to be processed
    sleep(Duration::from_secs(2)).await;

    // Retry getting coins with exponential backoff
    for attempt in 1..=5 {
        let coins = client
            .coin_read_api()
            .get_coins(address, None, None, None)
            .await
            .context("Failed to get coins")?;

        if let Some(coin) = coins.data.into_iter().max_by_key(|c| c.balance) {
            info!("Got gas coin for {}: {} (balance: {})", address, coin.coin_object_id, coin.balance);
            return Ok((coin.coin_object_id, coin.version, coin.digest));
        }
        
        if attempt < 5 {
            let delay = Duration::from_millis(500 * (1 << attempt)); // exponential backoff
            debug!("No coins found for {} (attempt {}), retrying in {:?}...", address, attempt, delay);
            sleep(delay).await;
        }
    }

    Err(anyhow!("No gas coins found for address {} after multiple retries", address))
}

/// Create initial seed objects for a worker
async fn create_seed_objects(
    client: &SuiClient,
    worker: Arc<RwLock<WorkerState>>,
    package_id: ObjectID,
    count: usize,
    gas_budget: u64,
) -> Result<()> {
    let mut remaining = count;
    let batch_size = 100; // Create in batches

    while remaining > 0 {
        let batch = remaining.min(batch_size);
        remaining -= batch;

        let mut state = worker.write().await;

        // Build create_batch transaction
        let mut builder = ProgrammableTransactionBuilder::new();
        // Must call pure() before programmable_move_call to avoid borrow conflict
        let batch_arg = builder.pure(batch as u64).unwrap();
        builder.programmable_move_call(
            package_id,
            Identifier::new("io_churn").unwrap(),
            Identifier::new("create_batch").unwrap(),
            vec![],
            vec![batch_arg],
        );

        let pt = builder.finish();

        // Get reference gas price
        let rgp = client
            .governance_api()
            .get_reference_gas_price()
            .await
            .unwrap_or(1000);

        let tx_data = TransactionData::new_programmable(
            state.address,
            vec![state.gas_coin],
            pt,
            gas_budget,
            rgp,
        );

        // Sign and create transaction using Transaction::from_data_and_signer
        let tx = Transaction::from_data_and_signer(
            tx_data,
            vec![&state.keypair],
        );

        let response = client
            .quorum_driver_api()
            .execute_transaction_block(
                tx,
                SuiTransactionBlockResponseOptions::new()
                    .with_effects()
                    .with_object_changes(),
                Some(ExecuteTransactionRequestType::WaitForEffectsCert),
            )
            .await
            .context("Failed to execute create_batch")?;

        // Update gas coin
        if let Some(effects) = &response.effects {
            let gas_obj = effects.gas_object();
            state.gas_coin = (gas_obj.object_id(), gas_obj.version(), gas_obj.reference.digest);

            // Track created objects
            if let Some(changes) = &response.object_changes {
                for change in changes {
                    if let sui_sdk::rpc_types::ObjectChange::Created { object_id, version, digest, .. } = change {
                        state.objects.push(TrackedObject {
                            id: *object_id,
                            version: version.value(),
                            digest: *digest,
                        });
                    }
                }
            }
        }

        debug!("Worker {}: created {} seed objects, total: {}", state.id, batch, state.objects.len());
    }

    Ok(())
}

/// Run a single worker
async fn run_worker(
    client: SuiClient,
    worker: Arc<RwLock<WorkerState>>,
    package_id: ObjectID,
    args: Args,
    stats: Arc<BenchStats>,
    running: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
    deadline: Instant,
    cached_rgp: u64,
) -> Result<()> {
    // Use StdRng which is Send (unlike thread_rng)
    let mut rng = rand::rngs::StdRng::from_entropy();

    while running.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Acquire permit
        let _permit = semaphore.acquire().await?;

        // Decide operation type
        let do_create = rng.gen_range(0..100) < args.create_pct as u32;

        let result = if args.use_blobs {
            // Use 4KB LargeBlob objects (40x more I/O per object)
            if do_create {
                execute_create_blob_batch(&client, &worker, package_id, args.batch_size, args.gas_budget, cached_rgp).await
            } else {
                execute_update_blob_batch(&client, &worker, package_id, args.batch_size, args.gas_budget, cached_rgp).await
            }
        } else {
            // Use MicroCounter objects (~100 bytes each)
            if do_create {
                execute_create_batch(&client, &worker, package_id, args.batch_size, args.gas_budget, cached_rgp).await
            } else {
                execute_update_batch(&client, &worker, package_id, args.batch_size, args.gas_budget, cached_rgp).await
            }
        };

        stats.tx_submitted.fetch_add(1, Ordering::Relaxed);

        match result {
            Ok((created, updated)) => {
                stats.tx_success.fetch_add(1, Ordering::Relaxed);
                stats.objects_created.fetch_add(created, Ordering::Relaxed);
                stats.objects_updated.fetch_add(updated, Ordering::Relaxed);
            }
            Err(e) => {
                stats.tx_failed.fetch_add(1, Ordering::Relaxed);
                debug!("Transaction failed: {:?}", e);
            }
        }

        // Rate limiting if target TPS is set
        if args.target_tps > 0 {
            let target_interval = Duration::from_secs_f64(1.0 / args.target_tps as f64 * args.workers as f64);
            sleep(target_interval).await;
        }
    }

    Ok(())
}
/// Execute a create_batch transaction
async fn execute_create_batch(
    client: &SuiClient,
    worker: &Arc<RwLock<WorkerState>>,
    package_id: ObjectID,
    count: usize,
    gas_budget: u64,
    rgp: u64,
) -> Result<(u64, u64)> {
    let mut state = worker.write().await;

    let mut builder = ProgrammableTransactionBuilder::new();
    // Must call pure() before programmable_move_call to avoid borrow conflict
    let count_arg = builder.pure(count as u64).unwrap();
    builder.programmable_move_call(
        package_id,
        Identifier::new("io_churn").unwrap(),
        Identifier::new("create_batch").unwrap(),
        vec![],
        vec![count_arg],
    );

    let pt = builder.finish();

    let tx_data = TransactionData::new_programmable(
        state.address,
        vec![state.gas_coin],
        pt,
        gas_budget,
        rgp,
    );

    // Sign and create transaction using Transaction::from_data_and_signer
    let tx = Transaction::from_data_and_signer(
        tx_data,
        vec![&state.keypair],
    );

    let response = client
        .quorum_driver_api()
        .execute_transaction_block(
            tx,
            SuiTransactionBlockResponseOptions::new()
                .with_effects()
                .with_object_changes(),
            Some(ExecuteTransactionRequestType::WaitForEffectsCert),
        )
        .await?;

    let mut created_count = 0u64;

    if let Some(effects) = &response.effects {
        let gas_obj = effects.gas_object();
        state.gas_coin = (gas_obj.object_id(), gas_obj.version(), gas_obj.reference.digest);

        if let Some(changes) = &response.object_changes {
            for change in changes {
                if let sui_sdk::rpc_types::ObjectChange::Created { object_id, version, digest, .. } = change {
                    state.objects.push(TrackedObject {
                        id: *object_id,
                        version: version.value(),
                        digest: *digest,
                    });
                    created_count += 1;
                }
            }
        }
    }

    Ok((created_count, 0))
}

/// Execute an update batch transaction (increment_simple on multiple objects)
async fn execute_update_batch(
    client: &SuiClient,
    worker: &Arc<RwLock<WorkerState>>,
    package_id: ObjectID,
    count: usize,
    gas_budget: u64,
    rgp: u64,
) -> Result<(u64, u64)> {
    let mut state = worker.write().await;

    if state.objects.is_empty() {
        return Err(anyhow!("No objects to update"));
    }

    let update_count = count.min(state.objects.len());
    let mut builder = ProgrammableTransactionBuilder::new();

    // Select objects to update (round-robin with random start)
    let start_idx = rand::rngs::StdRng::from_entropy().gen_range(0..state.objects.len());
    let mut updated_indices = Vec::new();

    for i in 0..update_count {
        let idx = (start_idx + i) % state.objects.len();
        let obj = &state.objects[idx];

        let obj_arg = builder.obj(sui_sdk::types::transaction::ObjectArg::ImmOrOwnedObject(
            (obj.id, obj.version.into(), obj.digest),
        ))?;

        builder.programmable_move_call(
            package_id,
            Identifier::new("io_churn").unwrap(),
            Identifier::new("increment_simple").unwrap(),
            vec![],
            vec![obj_arg],
        );

        updated_indices.push(idx);
    }

    let pt = builder.finish();

    let tx_data = TransactionData::new_programmable(
        state.address,
        vec![state.gas_coin],
        pt,
        gas_budget,
        rgp,
    );

    // Sign and create transaction using Transaction::from_data_and_signer
    let tx = Transaction::from_data_and_signer(
        tx_data,
        vec![&state.keypair],
    );

    let response = client
        .quorum_driver_api()
        .execute_transaction_block(
            tx,
            SuiTransactionBlockResponseOptions::new()
                .with_effects()
                .with_object_changes(),
            Some(ExecuteTransactionRequestType::WaitForEffectsCert),
        )
        .await?;

    let mut updated_count = 0u64;

    if let Some(effects) = &response.effects {
        // Update gas coin
        let gas_obj = effects.gas_object();
        state.gas_coin = (gas_obj.object_id(), gas_obj.version(), gas_obj.reference.digest);

        // Update object versions
        if let Some(changes) = &response.object_changes {
            for change in changes {
                if let sui_sdk::rpc_types::ObjectChange::Mutated { object_id, version, digest, .. } = change {
                    if let Some(obj) = state.objects.iter_mut().find(|o| o.id == *object_id) {
                        obj.version = version.value();
                        obj.digest = *digest;
                        updated_count += 1;
                    }
                }
            }
        }
    }

    Ok((0, updated_count))
}

/// Execute a create_blob_batch transaction (4KB objects instead of ~100B)
async fn execute_create_blob_batch(
    client: &SuiClient,
    worker: &Arc<RwLock<WorkerState>>,
    package_id: ObjectID,
    count: usize,
    gas_budget: u64,
    rgp: u64,
) -> Result<(u64, u64)> {
    let mut state = worker.write().await;

    // Limit blob batch size since each blob is 4KB
    let batch = count.min(20); // 20 blobs = 80KB per TX

    let mut builder = ProgrammableTransactionBuilder::new();
    let count_arg = builder.pure(batch as u64).unwrap();
    builder.programmable_move_call(
        package_id,
        Identifier::new("io_churn").unwrap(),
        Identifier::new("create_blob_batch").unwrap(),
        vec![],
        vec![count_arg],
    );

    let pt = builder.finish();

    let tx_data = TransactionData::new_programmable(
        state.address,
        vec![state.gas_coin],
        pt,
        gas_budget,
        rgp,
    );

    let tx = Transaction::from_data_and_signer(
        tx_data,
        vec![&state.keypair],
    );

    let response = client
        .quorum_driver_api()
        .execute_transaction_block(
            tx,
            SuiTransactionBlockResponseOptions::new()
                .with_effects()
                .with_object_changes(),
            Some(ExecuteTransactionRequestType::WaitForEffectsCert),
        )
        .await?;

    let mut created_count = 0u64;

    if let Some(effects) = &response.effects {
        let gas_obj = effects.gas_object();
        state.gas_coin = (gas_obj.object_id(), gas_obj.version(), gas_obj.reference.digest);

        if let Some(changes) = &response.object_changes {
            for change in changes {
                if let sui_sdk::rpc_types::ObjectChange::Created { object_id, version, digest, .. } = change {
                    state.objects.push(TrackedObject {
                        id: *object_id,
                        version: version.value(),
                        digest: *digest,
                    });
                    created_count += 1;
                }
            }
        }
    }

    Ok((created_count, 0))
}

/// Execute an update_blob batch transaction (4KB update per object)
async fn execute_update_blob_batch(
    client: &SuiClient,
    worker: &Arc<RwLock<WorkerState>>,
    package_id: ObjectID,
    count: usize,
    gas_budget: u64,
    rgp: u64,
) -> Result<(u64, u64)> {
    let mut state = worker.write().await;

    if state.objects.is_empty() {
        return Err(anyhow!("No objects to update"));
    }

    // Limit blob updates since each is 4KB
    let update_count = count.min(20).min(state.objects.len());
    let mut builder = ProgrammableTransactionBuilder::new();

    let start_idx = rand::rngs::StdRng::from_entropy().gen_range(0..state.objects.len());
    let mut updated_indices = Vec::new();

    for i in 0..update_count {
        let idx = (start_idx + i) % state.objects.len();
        let obj = &state.objects[idx];

        let obj_arg = builder.obj(sui_sdk::types::transaction::ObjectArg::ImmOrOwnedObject(
            (obj.id, obj.version.into(), obj.digest),
        ))?;

        // Use update_blob instead of increment_simple
        builder.programmable_move_call(
            package_id,
            Identifier::new("io_churn").unwrap(),
            Identifier::new("update_blob").unwrap(),
            vec![],
            vec![obj_arg],
        );

        updated_indices.push(idx);
    }

    let pt = builder.finish();

    let tx_data = TransactionData::new_programmable(
        state.address,
        vec![state.gas_coin],
        pt,
        gas_budget,
        rgp,
    );

    let tx = Transaction::from_data_and_signer(
        tx_data,
        vec![&state.keypair],
    );

    let response = client
        .quorum_driver_api()
        .execute_transaction_block(
            tx,
            SuiTransactionBlockResponseOptions::new()
                .with_effects()
                .with_object_changes(),
            Some(ExecuteTransactionRequestType::WaitForEffectsCert),
        )
        .await?;

    let mut updated_count = 0u64;

    if let Some(effects) = &response.effects {
        let gas_obj = effects.gas_object();
        state.gas_coin = (gas_obj.object_id(), gas_obj.version(), gas_obj.reference.digest);

        if let Some(changes) = &response.object_changes {
            for change in changes {
                if let sui_sdk::rpc_types::ObjectChange::Mutated { object_id, version, digest, .. } = change {
                    if let Some(obj) = state.objects.iter_mut().find(|o| o.id == *object_id) {
                        obj.version = version.value();
                        obj.digest = *digest;
                        updated_count += 1;
                    }
                }
            }
        }
    }

    Ok((0, updated_count))
}
