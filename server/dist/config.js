/**
 * Sui Storage Bloat Benchmark - Configuration
 */
export const defaultConfig = {
    rpcUrl: 'http://127.0.0.1:9000', // Will be overridden by auto-detection
    blobSizeKb: 100, // 100KB blobs
    batchSize: 5, // 5 blobs per tx = 500KB per tx
    concurrency: 4, // 4 parallel transactions
    targetMbPerMin: 5000, // Target 5GB/min
    durationMinutes: 0, // Run forever
    strategy: 'mixed',
    updatePoolSize: 100, // Keep 100 blobs for update rotation
    updateRatio: 0.8, // 80% updates, 20% creates (after warmup)
    // Mixed-lifetime defaults (hot/cold for FDP)
    hotSizeKb: 10, // 10KB hot objects (account state)
    coldSizeKb: 200, // 200KB cold objects (ledger data)
    hotBatchSize: 20, // Many small hot objects per TX
    coldBatchSize: 3, // Few large cold objects per TX
    hotPoolSize: 50, // 50 hot objects per worker to churn
    hotUpdateRounds: 5, // Update each 5 times before replacing
    hotRatio: 0.8, // 80% hot ops, 20% cold ops
    keystorePath: '',
    configDir: '',
};
export function loadConfigFromEnv() {
    return {
        rpcUrl: process.env.SUI_RPC_URL,
        blobSizeKb: process.env.BLOB_SIZE_KB ? parseInt(process.env.BLOB_SIZE_KB) : undefined,
        batchSize: process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : undefined,
        concurrency: process.env.CONCURRENCY ? parseInt(process.env.CONCURRENCY) : undefined,
        targetMbPerMin: process.env.TARGET_MB_PER_MIN ? parseInt(process.env.TARGET_MB_PER_MIN) : undefined,
        durationMinutes: process.env.DURATION_MINUTES ? parseInt(process.env.DURATION_MINUTES) : undefined,
        strategy: process.env.STRATEGY,
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
