import { SuiClient } from "@mysten/sui/client";
import { Transaction } from "@mysten/sui/transactions";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import * as fs from "fs";
import * as path from "path";
//
// Device Write Benchmark - TypeScript SDK Version
//
// This benchmark uses the SUI TypeScript SDK for better concurrent transaction handling.
// It monitors device writes via /proc/diskstats to measure actual NVMe I/O.
//
// Configuration
const CONFIG = {
    rpcUrl: "http://127.0.0.1:9000",
    suiConfigDir: process.env.SUI_CONFIG_DIR || `${process.env.HOME}/f2fs_fdp_mount/p0/sui_node`,
    nvmeDevice: process.env.NVME_DEVICE || "nvme0n1",
    workers: parseInt(process.env.WORKERS || "16"),
    durationSec: parseInt(process.env.DURATION || "60"),
    blobSizeKb: parseInt(process.env.BLOB_SIZE_KB || "200"),
    batchCount: parseInt(process.env.BATCH_COUNT || "50"), // 200KB * 50 = 10MB per tx
};
const stats = {
    txSubmitted: 0,
    txSuccess: 0,
    txFailed: 0,
    bytesWrittenApp: 0,
    startDeviceBytes: 0,
    startTime: 0,
};
// Read device sectors written from /proc/diskstats
function getDeviceSectorsWritten() {
    try {
        const diskstats = fs.readFileSync("/proc/diskstats", "utf-8");
        for (const line of diskstats.split("\n")) {
            const parts = line.trim().split(/\s+/);
            if (parts[2] === CONFIG.nvmeDevice) {
                // Field 10 (0-indexed: 9) is sectors written
                return parseInt(parts[9]) || 0;
            }
        }
    }
    catch (e) {
        console.error("Failed to read /proc/diskstats:", e);
    }
    return 0;
}
function getDeviceBytesWritten() {
    return getDeviceSectorsWritten() * 512;
}
function formatBytes(bytes) {
    if (bytes >= 1024 * 1024 * 1024) {
        return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
    }
    else {
        return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    }
}
function log(message) {
    console.log(`[${new Date().toLocaleTimeString()}] ${message}`);
}
// Load SUI configuration
function loadSuiConfig() {
    const configPath = path.join(CONFIG.suiConfigDir, "client.yaml");
    if (!fs.existsSync(configPath)) {
        throw new Error(`SUI config not found at: ${configPath}`);
    }
    const config = fs.readFileSync(configPath, "utf-8");
    const privateKeyMatch = config.match(/privkey:\s*(.+)/);
    if (!privateKeyMatch) {
        throw new Error("Private key not found in client.yaml");
    }
    return privateKeyMatch[1].trim();
}
// Load package ID
function loadPackageId() {
    const packageIdPath = path.join(CONFIG.suiConfigDir, ".package_id");
    if (!fs.existsSync(packageIdPath)) {
        throw new Error(`Package ID file not found: ${packageIdPath}. Run deploy script first.`);
    }
    return fs.readFileSync(packageIdPath, "utf-8").trim();
}
// Worker function - runs transactions in a loop
async function worker(workerId, client, keypair, packageId) {
    log(`Worker ${workerId} started`);
    while (true) {
        try {
            // Create transaction to call create_blobs_batch
            const tx = new Transaction();
            tx.moveCall({
                target: `${packageId}::bloat::create_blobs_batch`,
                arguments: [
                    tx.pure.u64(CONFIG.blobSizeKb),
                    tx.pure.u64(CONFIG.batchCount),
                ],
            });
            // Sign and execute
            const result = await client.signAndExecuteTransaction({
                transaction: tx,
                signer: keypair,
                options: {
                    showEffects: true,
                },
            });
            if (result.effects?.status?.status === "success") {
                stats.txSuccess++;
                stats.bytesWrittenApp += CONFIG.blobSizeKb * CONFIG.batchCount * 1024;
            }
            else {
                stats.txFailed++;
                log(`Worker ${workerId}: TX failed: ${result.effects?.status?.error}`);
            }
        }
        catch (error) {
            stats.txFailed++;
            log(`Worker ${workerId}: Error: ${error}`);
        }
        stats.txSubmitted++;
        // Small delay to prevent overwhelming the node
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}
// Monitor function - prints stats every 10 seconds
async function monitor() {
    const startTime = Date.now();
    const startDeviceBytes = getDeviceBytesWritten();
    while (true) {
        await new Promise(resolve => setTimeout(resolve, 10000));
        const elapsedSec = (Date.now() - startTime) / 1000;
        const currentDeviceBytes = getDeviceBytesWritten();
        const deviceBytesDelta = currentDeviceBytes - startDeviceBytes;
        const deviceRateMBMin = (deviceBytesDelta / elapsedSec) * 60 / (1024 * 1024);
        const writeAmp = stats.bytesWrittenApp > 0 ? deviceBytesDelta / stats.bytesWrittenApp : 0;
        log(`After ${elapsedSec.toFixed(0)}s: Txs=${stats.txSubmitted} (ok=${stats.txSuccess}, fail=${stats.txFailed})`);
        log(`  Device: ${formatBytes(deviceBytesDelta)}, Rate=${deviceRateMBMin.toFixed(0)} MB/min`);
        log(`  App: ${formatBytes(stats.bytesWrittenApp)}, WA=${writeAmp.toFixed(2)}x`);
    }
}
// Main function
async function main() {
    log("SUI Device Write Benchmark (TypeScript SDK)");
    log(`Configuration:`);
    log(`  RPC: ${CONFIG.rpcUrl}`);
    log(`  Workers: ${CONFIG.workers}`);
    log(`  Duration: ${CONFIG.durationSec}s`);
    log(`  Per TX: ${CONFIG.blobSizeKb}KB × ${CONFIG.batchCount} = ${(CONFIG.blobSizeKb * CONFIG.batchCount / 1024).toFixed(1)}MB`);
    log(`  Device: ${CONFIG.nvmeDevice}`);
    log("");
    try {
        // Initialize SUI client
        const client = new SuiClient({ url: CONFIG.rpcUrl });
        // Load configuration
        const privateKey = loadSuiConfig();
        const keypair = Ed25519Keypair.fromSecretKey(privateKey);
        const packageId = loadPackageId();
        log(`Package ID: ${packageId}`);
        log(`Address: ${keypair.getPublicKey().toSuiAddress()}`);
        log("");
        // Initialize stats
        stats.startDeviceBytes = getDeviceBytesWritten();
        stats.startTime = Date.now();
        // Start monitor
        monitor();
        // Start workers
        const workers = [];
        for (let i = 0; i < CONFIG.workers; i++) {
            workers.push(worker(i, client, keypair, packageId));
        }
        // Wait for duration
        await new Promise(resolve => setTimeout(resolve, CONFIG.durationSec * 1000));
        // Stop workers (they will exit when the process ends)
        process.exit(0);
    }
    catch (error) {
        log(`Error: ${error}`);
        process.exit(1);
    }
}
// Handle graceful shutdown
process.on("SIGINT", () => {
    log("Shutting down...");
    process.exit(0);
});
process.on("SIGTERM", () => {
    log("Shutting down...");
    process.exit(0);
});
// Run the benchmark
main().catch(console.error);
