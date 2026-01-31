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
        return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
    }
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}
function formatRate(bytesPerSec) {
    const mbPerMin = (bytesPerSec * 60) / 1024 / 1024;
    const gbPerMin = mbPerMin / 1024;
    return `${mbPerMin.toFixed(0)} MB/min (${gbPerMin.toFixed(2)} GB/min)`;
}
// Load keypair from SUI config
function loadKeypair() {
    const configPath = path.join(CONFIG.suiConfigDir, "client.yaml");
    const content = fs.readFileSync(configPath, "utf-8");
    // Parse keystore path from client.yaml
    const keystoreMatch = content.match(/keystore:\s*\n\s+File:\s+(.+)/);
    if (!keystoreMatch) {
        throw new Error("Could not find keystore path in client.yaml");
    }
    const keystorePath = keystoreMatch[1].trim();
    const keystoreContent = fs.readFileSync(keystorePath, "utf-8");
    const keys = JSON.parse(keystoreContent);
    if (keys.length === 0) {
        throw new Error("No keys in keystore");
    }
    // First key is typically the active one
    const keyBase64 = keys[0];
    const keyBytes = Buffer.from(keyBase64, "base64");
    // The stored key includes a 1-byte flag prefix
    const secretKey = keyBytes.slice(1, 33);
    return Ed25519Keypair.fromSecretKey(secretKey);
}
// Get package ID from saved file
function getPackageId() {
    const packageIdPath = path.join(CONFIG.suiConfigDir, ".package_id");
    if (!fs.existsSync(packageIdPath)) {
        throw new Error(`Package ID not found at ${packageIdPath}. Run sui-benchmark.sh first.`);
    }
    return fs.readFileSync(packageIdPath, "utf-8").trim();
}
// Worker that continuously sends transactions
async function worker(client, keypair, packageId, workerId, endTime) {
    const address = keypair.toSuiAddress();
    while (Date.now() < endTime) {
        try {
            // Build transaction
            const tx = new Transaction();
            tx.moveCall({
                target: `${packageId}::bloat::create_blobs_batch`,
                arguments: [
                    tx.pure.u64(CONFIG.blobSizeKb),
                    tx.pure.u64(CONFIG.batchCount),
                ],
            });
            tx.setGasBudget(5000000000);
            stats.txSubmitted++;
            // Execute transaction
            const result = await client.signAndExecuteTransaction({
                transaction: tx,
                signer: keypair,
                options: {
                    showEffects: true,
                },
            });
            if (result.effects?.status?.status === "success") {
                stats.txSuccess++;
                stats.bytesWrittenApp += CONFIG.blobSizeKb * 1024 * CONFIG.batchCount;
            }
            else {
                stats.txFailed++;
                // Don't log every failure to avoid spam
            }
        }
        catch (e) {
            stats.txFailed++;
            // Log occasional errors
            if (stats.txFailed % 100 === 1) {
                console.log(`[Worker ${workerId}] Error: ${e.message?.substring(0, 100)}`);
            }
        }
    }
}
// Print current stats
function printStats() {
    const now = Date.now();
    const elapsed = (now - stats.startTime) / 1000;
    const currentDeviceBytes = getDeviceBytesWritten();
    const deviceWritten = currentDeviceBytes - stats.startDeviceBytes;
    const deviceRate = deviceWritten / elapsed;
    const appWritten = stats.bytesWrittenApp;
    const wa = appWritten > 0 ? deviceWritten / appWritten : 0;
    console.log(`\n[${new Date().toTimeString().split(" ")[0]}] --- Stats after ${elapsed.toFixed(0)}s ---`);
    console.log(`  Txs: ${stats.txSubmitted} (Success: ${stats.txSuccess}, Failed: ${stats.txFailed})`);
    console.log(`  App Written:    ${formatBytes(appWritten)} (blob data)`);
    console.log(`  Device Written: ${formatBytes(deviceWritten)} (actual I/O)`);
    console.log(`  Device Rate:    ${formatRate(deviceRate)}`);
    console.log(`  Write Amp:      ${wa.toFixed(2)}x`);
}
async function main() {
    console.log("═══════════════════════════════════════════════════════════════");
    console.log("  DEVICE WRITE BENCHMARK - TypeScript SDK Version");
    console.log("═══════════════════════════════════════════════════════════════");
    console.log("");
    // Load configuration
    const packageId = getPackageId();
    const keypair = loadKeypair();
    const address = keypair.toSuiAddress();
    console.log(`  Package:    ${packageId}`);
    console.log(`  Address:    ${address}`);
    console.log(`  Workers:    ${CONFIG.workers}`);
    console.log(`  Duration:   ${CONFIG.durationSec}s`);
    console.log(`  Per TX:     ${CONFIG.blobSizeKb}KB × ${CONFIG.batchCount} = ${(CONFIG.blobSizeKb * CONFIG.batchCount / 1024).toFixed(0)}MB`);
    console.log(`  Device:     /dev/${CONFIG.nvmeDevice}`);
    console.log("");
    // Initialize client
    const client = new SuiClient({ url: CONFIG.rpcUrl });
    // Test connection
    try {
        await client.getLatestCheckpointSequenceNumber();
        console.log("  ✓ Connected to SUI node");
    }
    catch (e) {
        console.error("  ✗ Failed to connect to SUI node at", CONFIG.rpcUrl);
        process.exit(1);
    }
    // Record baseline
    stats.startDeviceBytes = getDeviceBytesWritten();
    stats.startTime = Date.now();
    const endTime = stats.startTime + CONFIG.durationSec * 1000;
    console.log(`  Initial device writes: ${formatBytes(stats.startDeviceBytes)}`);
    console.log("");
    console.log(`Starting ${CONFIG.workers} workers...`);
    // Start workers
    const workers = [];
    for (let i = 0; i < CONFIG.workers; i++) {
        workers.push(worker(client, keypair, packageId, i, endTime));
    }
    // Print stats every 5 seconds
    const statsInterval = setInterval(printStats, 5000);
    // Wait for all workers to complete
    await Promise.all(workers);
    clearInterval(statsInterval);
    // Final stats
    const totalElapsed = (Date.now() - stats.startTime) / 1000;
    const finalDeviceBytes = getDeviceBytesWritten();
    const totalDeviceWritten = finalDeviceBytes - stats.startDeviceBytes;
    const avgRate = totalDeviceWritten / totalElapsed;
    const avgRateMbMin = (avgRate * 60) / 1024 / 1024;
    const avgRateGbMin = avgRateMbMin / 1024;
    console.log("\n═══════════════════════════════════════════════════════════════");
    console.log("  BENCHMARK COMPLETE");
    console.log("═══════════════════════════════════════════════════════════════");
    console.log("");
    console.log(`  Duration:         ${totalElapsed.toFixed(1)}s`);
    console.log(`  Workers:          ${CONFIG.workers}`);
    console.log(`  Transactions:     ${stats.txSuccess} successful / ${stats.txSubmitted} total`);
    console.log("");
    console.log("  ─── DEVICE WRITES (actual SSD I/O) ───");
    console.log(`  Total Written:    ${formatBytes(totalDeviceWritten)}`);
    console.log(`  Write Rate:       ${formatRate(avgRate)}`);
    console.log("");
    console.log("  Target for FDP GC: 5-10 GB/min");
    if (avgRateMbMin >= 5120) {
        console.log("  Status:           ✓ TARGET ACHIEVED!");
    }
    else if (avgRateMbMin >= 2048) {
        console.log("  Status:           ~ Close to target");
    }
    else {
        console.log("  Status:           ✗ Need more throughput");
    }
    console.log("");
}
main().catch(console.error);
