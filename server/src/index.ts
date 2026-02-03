/**
 * Sui Storage Bloat Benchmark - Main Entry Point
 * 
 * Usage:
 *   SUI_CONFIG_DIR=/path/to/config npm start
 *   
 * Environment variables:
 *   SUI_CONFIG_DIR  - Path to Sui config directory (required)
 *   BLOB_SIZE_KB    - Size of each blob in KB (default: 100)
 *   BATCH_SIZE      - Blobs per transaction (default: 5)
 *   CONCURRENCY     - Parallel workers (default: 4)
 *   STRATEGY        - blobs|varied|churn|mixed (default: mixed)
 *   DURATION_MINUTES - How long to run, 0=forever (default: 0)
 */

import * as fs from 'fs';
import * as path from 'path';
import { defaultConfig, loadConfigFromEnv, BenchmarkConfig } from './config.js';
import { initSuiContext, detectRpcUrl, SuiContext } from './sui-utils.js';
import { publishPackage } from './publish.js';
import {
  createStats,
  runBlobsStrategy,
  runVariedStrategy,
  runChurnStrategy,
  runMixedStrategy,
  runUpdateHeavyStrategy,
  printStats,
  BenchmarkStats,
} from './strategies.js';

const PACKAGE_ID_FILE = path.resolve(process.cwd(), '.package_id');

async function main() {
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║         SUI STORAGE BLOAT BENCHMARK                       ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
  
  // Load config - merge env config on top of defaults
  const envConfig = loadConfigFromEnv();
  const config: BenchmarkConfig = { 
    ...defaultConfig,
    // Only override if env config has actual values
    ...(Object.fromEntries(
      Object.entries(envConfig).filter(([_, v]) => v !== undefined)
    ))
  };
  
  // Validate config dir
  const configDir = config.configDir || process.env.SUI_CONFIG_DIR;
  if (!configDir) {
    console.error('ERROR: SUI_CONFIG_DIR environment variable is required');
    console.error('');
    console.error('Usage: SUI_CONFIG_DIR=/path/to/sui_node npm start');
    process.exit(1);
  }
  
  if (!fs.existsSync(configDir)) {
    console.error(`ERROR: Config directory not found: ${configDir}`);
    process.exit(1);
  }
  
  config.configDir = configDir;
  config.keystorePath = path.join(configDir, 'sui.keystore');
  
  if (!fs.existsSync(config.keystorePath)) {
    console.error(`ERROR: Keystore not found: ${config.keystorePath}`);
    process.exit(1);
  }
  
  // Auto-detect RPC URL
  config.rpcUrl = detectRpcUrl(configDir);
  
  console.log('[config] Settings:');
  console.log(`  Config Dir:    ${config.configDir}`);
  console.log(`  RPC URL:       ${config.rpcUrl}`);
  console.log(`  Blob Size:     ${config.blobSizeKb} KB`);
  console.log(`  Batch Size:    ${config.batchSize}`);
  console.log(`  Concurrency:   ${config.concurrency}`);
  console.log(`  Strategy:      ${config.strategy}`);
  if (config.strategy === 'update_heavy') {
    console.log(`  Update Pool:   ${config.updatePoolSize} blobs`);
    console.log(`  Update Ratio:  ${(config.updateRatio * 100).toFixed(0)}% updates`);
  }
  console.log(`  Duration:      ${config.durationMinutes || 'infinite'} min`);
  console.log('');
  
  // Initialize Sui context
  console.log('[init] Connecting to Sui...');
  const ctx = await initSuiContext(config.rpcUrl, config.keystorePath);
  
  // Publish or load package
  console.log('[init] Preparing Move package...');
  let packageId: string;
  
  if (fs.existsSync(PACKAGE_ID_FILE)) {
    packageId = fs.readFileSync(PACKAGE_ID_FILE, 'utf-8').trim();
    console.log(`[init] Using existing package: ${packageId}`);
    
    // Verify it exists
    try {
      await ctx.client.getObject({ id: packageId });
    } catch {
      console.log('[init] Package not found on chain, republishing...');
      packageId = await publishPackage(config.rpcUrl, config.keystorePath);
    }
  } else {
    packageId = await publishPackage(config.rpcUrl, config.keystorePath);
  }
  
  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  BENCHMARK STARTING');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');
  
  // Run benchmark
  const stats = createStats();
  const ownedBlobs: string[] = [];
  let iteration = 0;
  
  const endTime = config.durationMinutes > 0 
    ? Date.now() + config.durationMinutes * 60 * 1000 
    : Infinity;
  
  // Stats printer interval
  const statsPrinter = setInterval(() => printStats(stats), 10000);
  
  // Graceful shutdown
  let running = true;
  process.on('SIGINT', () => {
    console.log('\n[benchmark] Shutting down...');
    running = false;
  });
  process.on('SIGTERM', () => {
    running = false;
  });
  
  try {
    while (running && Date.now() < endTime) {
      try {
        // Run selected strategy
        switch (config.strategy) {
          case 'blobs':
            await runBlobsStrategy(ctx, packageId, config, stats);
            break;
          case 'varied':
            await runVariedStrategy(ctx, packageId, config, stats);
            break;
          case 'churn':
            await runChurnStrategy(ctx, packageId, config, stats, ownedBlobs);
            break;
          case 'update_heavy':
            await runUpdateHeavyStrategy(ctx, packageId, config, stats, ownedBlobs, iteration);
            break;
          case 'mixed':
          default:
            await runMixedStrategy(ctx, packageId, config, stats, ownedBlobs, iteration);
            break;
        }
        
        iteration++;
        
        // Brief progress indicator
        if (iteration % 10 === 0) {
          process.stdout.write('.');
        }
        if (iteration % 100 === 0) {
          process.stdout.write(`[${iteration}]\n`);
        }
        
      } catch (e: any) {
        console.error(`\n[error] Transaction failed: ${e.message}`);
        
        // Back off on errors
        await new Promise(r => setTimeout(r, 1000));
        
        // If too many failures, pause longer
        if (stats.failedTx > stats.successTx && stats.totalTx > 10) {
          console.log('[error] Too many failures, pausing for 5s...');
          await new Promise(r => setTimeout(r, 5000));
        }
      }
    }
  } finally {
    clearInterval(statsPrinter);
  }
  
  console.log('\n');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  BENCHMARK COMPLETE');
  console.log('═══════════════════════════════════════════════════════════');
  printStats(stats);
}

// Run
main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
