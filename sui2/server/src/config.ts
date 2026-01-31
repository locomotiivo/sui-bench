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
  
  // Strategy: 'blobs' | 'varied' | 'churn' | 'mixed'
  strategy: 'blobs' | 'varied' | 'churn' | 'mixed';
  
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
    packageId: process.env.PACKAGE_ID,
    keystorePath: process.env.KEYSTORE_PATH,
    configDir: process.env.SUI_CONFIG_DIR,
  };
}
