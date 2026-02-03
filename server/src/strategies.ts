/**
 * Benchmark execution strategies
 */

import { Transaction } from '@mysten/sui/transactions';
import { SuiContext, executeTransaction } from './sui-utils.js';
import { BenchmarkConfig } from './config.js';

export interface BenchmarkStats {
  totalTx: number;
  successTx: number;
  failedTx: number;
  totalBytesWritten: number;
  startTime: number;
  lastTxTime: number;
}

export function createStats(): BenchmarkStats {
  return {
    totalTx: 0,
    successTx: 0,
    failedTx: 0,
    totalBytesWritten: 0,
    startTime: Date.now(),
    lastTxTime: Date.now(),
  };
}

/**
 * Create blobs strategy - simple blob creation
 */
export async function runBlobsStrategy(
  ctx: SuiContext,
  packageId: string,
  config: BenchmarkConfig,
  stats: BenchmarkStats
): Promise<void> {
  const tx = new Transaction();
  
  // Create batch of blobs
  tx.moveCall({
    target: `${packageId}::bloat::create_blobs_batch`,
    arguments: [
      tx.pure.u64(config.blobSizeKb),
      tx.pure.u64(config.batchSize),
    ],
  });
  
  // Set gas budget (estimate: 1M gas per 100KB)
  const estimatedGas = config.blobSizeKb * config.batchSize * 10000 + 10_000_000;
  tx.setGasBudget(estimatedGas);
  
  stats.totalTx++;
  
  try {
    await executeTransaction(ctx, tx);
    stats.successTx++;
    stats.totalBytesWritten += config.blobSizeKb * config.batchSize * 1024;
    stats.lastTxTime = Date.now();
  } catch (e) {
    stats.failedTx++;
    throw e;
  }
}

/**
 * Varied blobs strategy - different sizes for fragmentation
 */
export async function runVariedStrategy(
  ctx: SuiContext,
  packageId: string,
  config: BenchmarkConfig,
  stats: BenchmarkStats
): Promise<void> {
  const tx = new Transaction();
  
  tx.moveCall({
    target: `${packageId}::bloat::create_varied_blobs`,
    arguments: [
      tx.pure.u64(config.blobSizeKb),
      tx.pure.u64(config.batchSize),
    ],
  });
  
  // Varied sizes average to ~1.25x base size
  const estimatedGas = config.blobSizeKb * config.batchSize * 12500 + 10_000_000;
  tx.setGasBudget(estimatedGas);
  
  stats.totalTx++;
  
  try {
    await executeTransaction(ctx, tx);
    stats.successTx++;
    // Average size is 1.25x for varied
    stats.totalBytesWritten += config.blobSizeKb * config.batchSize * 1024 * 1.25;
    stats.lastTxTime = Date.now();
  } catch (e) {
    stats.failedTx++;
    throw e;
  }
}

/**
 * Churn strategy - create, update, and delete to maximize GC pressure
 */
export async function runChurnStrategy(
  ctx: SuiContext,
  packageId: string,
  config: BenchmarkConfig,
  stats: BenchmarkStats,
  ownedBlobs: string[]
): Promise<void> {
  const tx = new Transaction();
  
  // If we have blobs, update some and delete some
  if (ownedBlobs.length > 0) {
    // Update first blob
    const blobToUpdate = ownedBlobs[0];
    tx.moveCall({
      target: `${packageId}::bloat::update_blob`,
      arguments: [
        tx.object(blobToUpdate),
        tx.pure.u64(config.blobSizeKb * 2),  // Double size on update
      ],
    });
    
    // Delete oldest if we have many
    if (ownedBlobs.length > 20) {
      const blobToDelete = ownedBlobs[ownedBlobs.length - 1];
      tx.moveCall({
        target: `${packageId}::bloat::delete_blob`,
        arguments: [tx.object(blobToDelete)],
      });
      ownedBlobs.pop();
    }
  }
  
  // Always create new blobs too
  tx.moveCall({
    target: `${packageId}::bloat::create_blobs_batch`,
    arguments: [
      tx.pure.u64(config.blobSizeKb),
      tx.pure.u64(Math.max(1, config.batchSize - 2)),
    ],
  });
  
  const estimatedGas = config.blobSizeKb * config.batchSize * 15000 + 20_000_000;
  tx.setGasBudget(estimatedGas);
  
  stats.totalTx++;
  
  try {
    const result = await executeTransaction(ctx, tx, true);
    stats.successTx++;
    stats.totalBytesWritten += config.blobSizeKb * config.batchSize * 1024 * 1.5;
    stats.lastTxTime = Date.now();
    
    // Track new blobs
    const newBlobs = result.objectChanges
      ?.filter(c => c.type === 'created' && 'objectType' in c && c.objectType?.includes('Blob'))
      .map(c => (c as any).objectId) || [];
    ownedBlobs.push(...newBlobs);
  } catch (e) {
    stats.failedTx++;
    throw e;
  }
}

/**
 * Mixed strategy - rotate through all strategies
 */
export async function runMixedStrategy(
  ctx: SuiContext,
  packageId: string,
  config: BenchmarkConfig,
  stats: BenchmarkStats,
  ownedBlobs: string[],
  iteration: number
): Promise<void> {
  const strategies = ['blobs', 'varied', 'churn'];
  const strategy = strategies[iteration % strategies.length];
  
  switch (strategy) {
    case 'blobs':
      await runBlobsStrategy(ctx, packageId, config, stats);
      break;
    case 'varied':
      await runVariedStrategy(ctx, packageId, config, stats);
      break;
    case 'churn':
      await runChurnStrategy(ctx, packageId, config, stats, ownedBlobs);
      break;
  }
}

/**
 * Update-Heavy Strategy - Maximize LSM-tree version churn
 * 
 * This strategy creates maximum storage layer activity by repeatedly updating
 * the same objects. Each update creates a new version in RocksDB, which:
 * 1. Writes new data to L0 (memtable flush)
 * 2. Triggers compaction as versions accumulate
 * 3. Forces merge of old versions (write amplification)
 * 4. Eventually triggers garbage collection of old versions
 * 
 * Phase 1 (Warmup): Create a pool of blobs
 * Phase 2 (Churn): Heavily update existing blobs (80% updates, 20% creates)
 */
export async function runUpdateHeavyStrategy(
  ctx: SuiContext,
  packageId: string,
  config: BenchmarkConfig,
  stats: BenchmarkStats,
  ownedBlobs: string[],
  iteration: number
): Promise<void> {
  const isWarmup = ownedBlobs.length < config.updatePoolSize;
  const shouldUpdate = !isWarmup && Math.random() < config.updateRatio;
  
  const tx = new Transaction();
  
  if (shouldUpdate && ownedBlobs.length > 0) {
    // UPDATE PHASE: Update multiple existing blobs
    // Pick random blobs to update (up to batchSize)
    const updateCount = Math.min(config.batchSize, ownedBlobs.length);
    const indicesToUpdate = new Set<number>();
    
    while (indicesToUpdate.size < updateCount) {
      indicesToUpdate.add(Math.floor(Math.random() * ownedBlobs.length));
    }
    
    for (const idx of indicesToUpdate) {
      const blobId = ownedBlobs[idx];
      // Vary the new size to create different write patterns
      const newSizeKb = config.blobSizeKb + (iteration % 5) * 20;  // 100-180KB
      
      tx.moveCall({
        target: `${packageId}::bloat::update_blob`,
        arguments: [
          tx.object(blobId),
          tx.pure.u64(newSizeKb),
        ],
      });
    }
    
    // Estimate gas: updates are more expensive due to reading old data
    const estimatedGas = config.blobSizeKb * updateCount * 15000 + 20_000_000;
    tx.setGasBudget(estimatedGas);
    
    stats.totalTx++;
    
    try {
      await executeTransaction(ctx, tx);
      stats.successTx++;
      // Updates rewrite the entire blob
      stats.totalBytesWritten += config.blobSizeKb * updateCount * 1024;
      stats.lastTxTime = Date.now();
    } catch (e) {
      stats.failedTx++;
      throw e;
    }
  } else {
    // CREATE PHASE: Create new blobs (warmup or occasional creates)
    tx.moveCall({
      target: `${packageId}::bloat::create_blobs_batch`,
      arguments: [
        tx.pure.u64(config.blobSizeKb),
        tx.pure.u64(config.batchSize),
      ],
    });
    
    const estimatedGas = config.blobSizeKb * config.batchSize * 10000 + 10_000_000;
    tx.setGasBudget(estimatedGas);
    
    stats.totalTx++;
    
    try {
      const result = await executeTransaction(ctx, tx, true);
      stats.successTx++;
      stats.totalBytesWritten += config.blobSizeKb * config.batchSize * 1024;
      stats.lastTxTime = Date.now();
      
      // Track new blobs (add to pool, but cap at 2x pool size to allow rotation)
      const newBlobs = result.objectChanges
        ?.filter(c => c.type === 'created' && 'objectType' in c && c.objectType?.includes('Blob'))
        .map(c => (c as any).objectId) || [];
      
      ownedBlobs.push(...newBlobs);
      
      // Trim pool to 2x size (keep some rotation)
      if (ownedBlobs.length > config.updatePoolSize * 2) {
        ownedBlobs.splice(0, ownedBlobs.length - config.updatePoolSize * 2);
      }
      
      if (isWarmup && ownedBlobs.length >= config.updatePoolSize) {
        console.log(`[update_heavy] Warmup complete: ${ownedBlobs.length} blobs in pool`);
      }
    } catch (e) {
      stats.failedTx++;
      throw e;
    }
  }
}

/**
 * Format bytes to human readable
 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

/**
 * Print stats
 */
export function printStats(stats: BenchmarkStats): void {
  const elapsed = (Date.now() - stats.startTime) / 1000;
  const rate = stats.totalBytesWritten / elapsed;
  const ratePerMin = rate * 60;
  
  console.log(`\n[stats] ────────────────────────────────────`);
  console.log(`  Elapsed:     ${elapsed.toFixed(1)}s`);
  console.log(`  Transactions: ${stats.successTx}/${stats.totalTx} (${stats.failedTx} failed)`);
  console.log(`  Data written: ${formatBytes(stats.totalBytesWritten)}`);
  console.log(`  Write rate:   ${formatBytes(ratePerMin)}/min`);
  console.log(`────────────────────────────────────────────\n`);
}
