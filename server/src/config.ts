/**
 * Sui Storage Bloat Benchmark - Configuration
 */

export interface BenchmarkConfig {
  // Sui RPC endpoint (auto-detected from validator config)
  rpcUrl: string;
  
  // Benchmark parameters
  blobSizeKb: number;           // Size of each blob in KB
  batchSize: number;            // Blobs per transaction
  concurrency: number;          // Parallel transactions
  targetMbPerMin: number;       // Target write rate
  durationMinutes: number;      // How long to run (0 = infinite)
  
  // Strategy: 'blobs' | 'varied' | 'churn' | 'mixed' | 'update_heavy' | 'mixed_lifetime'
  // update_heavy: Maximize object version churn by repeatedly updating same objects
  // mixed_lifetime: Create hot (small, updated) + cold (large, write-once) data for FDP evaluation
  strategy: 'blobs' | 'varied' | 'churn' | 'mixed' | 'update_heavy' | 'mixed_lifetime';
  
  // Mixed-lifetime strategy parameters (for FDP hot/cold separation testing)
  hotSizeKb: number;            // Size of hot objects (account state) in KB
  coldSizeKb: number;           // Size of cold objects (ledger data) in KB
  hotBatchSize: number;         // Hot objects per transaction
  coldBatchSize: number;        // Cold objects per transaction
  hotPoolSize: number;          // Number of hot objects to maintain
  hotUpdateRounds: number;      // Times to update each hot object before replacing
  hotRatio: number;             // Ratio of hot vs cold operations (0.0-1.0)
  
  // Update-heavy strategy parameters
  updatePoolSize: number;       // Number of blobs to keep for updating
  updateRatio: number;          // Ratio of updates vs creates (0.0-1.0)
  
  // Paths
  packageId?: string;           // Published package ID (if already deployed)
  keystorePath: string;         // Path to sui.keystore
  configDir: string;            // Sui config directory
}

export const defaultConfig: BenchmarkConfig = {
  rpcUrl: 'http://127.0.0.1:9000',  // Will be overridden by auto-detection
  blobSizeKb: 100,                   // 100KB blobs
  batchSize: 5,                      // 5 blobs per tx = 500KB per tx
  concurrency: 4,                    // 4 parallel transactions
  targetMbPerMin: 5000,              // Target 5GB/min
  durationMinutes: 0,                // Run forever
  strategy: 'mixed',
  updatePoolSize: 100,               // Keep 100 blobs for update rotation
  updateRatio: 0.8,                  // 80% updates, 20% creates (after warmup)
  // Mixed-lifetime defaults (hot/cold for FDP)
  hotSizeKb: 10,                     // 10KB hot objects (account state)
  coldSizeKb: 200,                   // 200KB cold objects (ledger data)
  hotBatchSize: 20,                  // Many small hot objects per TX
  coldBatchSize: 3,                  // Few large cold objects per TX
  hotPoolSize: 50,                   // 50 hot objects per worker to churn
  hotUpdateRounds: 5,                // Update each 5 times before replacing
  hotRatio: 0.8,                     // 80% hot ops, 20% cold ops
  keystorePath: '',
  configDir: '',
};

export function loadConfigFromEnv(): Partial<BenchmarkConfig> {
  return {
    rpcUrl: process.env.SUI_RPC_URL,
    blobSizeKb: process.env.BLOB_SIZE_KB ? parseInt(process.env.BLOB_SIZE_KB) : undefined,
    batchSize: process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : undefined,
    concurrency: process.env.CONCURRENCY ? parseInt(process.env.CONCURRENCY) : undefined,
    targetMbPerMin: process.env.TARGET_MB_PER_MIN ? parseInt(process.env.TARGET_MB_PER_MIN) : undefined,
    durationMinutes: process.env.DURATION_MINUTES ? parseInt(process.env.DURATION_MINUTES) : undefined,
    strategy: process.env.STRATEGY as BenchmarkConfig['strategy'],
    updatePoolSize: process.env.UPDATE_POOL_SIZE ? parseInt(process.env.UPDATE_POOL_SIZE) : undefined,
    updateRatio: process.env.UPDATE_RATIO ? parseFloat(process.env.UPDATE_RATIO) : undefined,
    // Mixed-lifetime env vars
    hotSizeKb: process.env.HOT_SIZE_KB ? parseInt(process.env.HOT_SIZE_KB) : undefined,
    coldSizeKb: process.env.COLD_SIZE_KB ? parseInt(process.env.COLD_SIZE_KB) : undefined,
    hotBatchSize: process.env.HOT_BATCH_SIZE ? parseInt(process.env.HOT_BATCH_SIZE) : undefined,
    coldBatchSize: process.env.COLD_BATCH_SIZE ? parseInt(process.env.COLD_BATCH_SIZE) : undefined,
    hotPoolSize: process.env.HOT_POOL_SIZE ? parseInt(process.env.HOT_POOL_SIZE) : undefined,
    hotUpdateRounds: process.env.HOT_UPDATE_ROUNDS ? parseInt(process.env.HOT_UPDATE_ROUNDS) : undefined,
    hotRatio: process.env.HOT_RATIO ? parseFloat(process.env.HOT_RATIO) : undefined,
    packageId: process.env.PACKAGE_ID,
    keystorePath: process.env.KEYSTORE_PATH,
    configDir: process.env.SUI_CONFIG_DIR,
  };
}
