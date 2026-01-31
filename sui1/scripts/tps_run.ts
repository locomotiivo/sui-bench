/**
 * tps_run-optimal.ts - High-Throughput Owned Object Benchmark
 * 
 * KEY INSIGHT: SUI has two transaction types:
 * 1. Shared Object TXs → Go through consensus → SLOW, serialized
 * 2. Owned Object TXs  → Direct execution → FAST, parallelizable
 * 
 * For MAXIMUM throughput + disk I/O, use OWNED objects:
 * - Coin splits/merges create new object versions
 * - Each account operates on ITS OWN coins (no contention)
 * - True parallelization across 5000+ accounts
 * - Rapid storage growth from object versioning
 */

import { SuiClient } from '@mysten/sui/client';
import { Transaction } from '@mysten/sui/transactions';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { getActiveConfig } from './.config.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const cfg = getActiveConfig();

// ============== CONFIGURATION ==============
const BENCHMARK_DURATION_MS = (cfg.duration || 1200) * 1000;

// Operations per TX - coin splits are cheaper than counter ops
const SPLITS_PER_TX = cfg.commandsPerPtb || 256;  // More ops = more disk I/O

// Gas budget for coin operations (much cheaper than shared object)
const GAS_BUDGET = 500_000_000n;  // 0.5 SUI is enough for 256 splits

// Timing - can be aggressive with owned objects!
const ITER_INTERVAL_MS = cfg.iterInterval || 5;  // 5ms - very fast!
const MAX_RETRIES = 1;  // Owned objects rarely fail

// Concurrency - SCALE UP! No shared object bottleneck
const MAX_ACCOUNTS = cfg.targetCount || 5000;
const CONCURRENT_ACCOUNTS = Math.min(500, MAX_ACCOUNTS);  // 500 truly concurrent

// Safety
const MIN_BALANCE_MIST = 1_000_000_000n;  // 1 SUI minimum
const MEMORY_THRESHOLD_PCT = 85;
const MEMORY_CHECK_INTERVAL_MS = 5000;
const MAX_CONSECUTIVE_FAILURES = 50;

// Split amount (tiny, just to create object churn)
const SPLIT_AMOUNT = 1000n;  // 0.000001 SUI

// ===========================================

let benchmarkEndTime = 0;
let shutdownRequested = false;
let memoryExceeded = false;

process.on('SIGINT', () => {
    console.log('\n⚠️  Shutdown requested...');
    shutdownRequested = true;
});

function shouldContinue(): boolean {
    return !shutdownRequested && !memoryExceeded && Date.now() < benchmarkEndTime;
}

function getMemoryUsage(): { usedPct: number; usedMB: number; totalMB: number } {
    try {
        const meminfo = fs.readFileSync('/proc/meminfo', 'utf8');
        let total = 0, available = 0;
        for (const line of meminfo.split('\n')) {
            if (line.startsWith('MemTotal:')) total = parseInt(line.split(/\s+/)[1]!) / 1024;
            else if (line.startsWith('MemAvailable:')) available = parseInt(line.split(/\s+/)[1]!) / 1024;
        }
        const used = total - available;
        return { usedPct: (used / total) * 100, usedMB: Math.round(used), totalMB: Math.round(total) };
    } catch { return { usedPct: 0, usedMB: 0, totalMB: 0 }; }
}

function startMemoryMonitor(): NodeJS.Timeout {
    return setInterval(() => {
        const mem = getMemoryUsage();
        if (mem.usedPct > MEMORY_THRESHOLD_PCT) {
            console.log(`\n🛑 MEMORY LIMIT: ${mem.usedPct.toFixed(1)}%`);
            memoryExceeded = true;
        }
    }, MEMORY_CHECK_INTERVAL_MS);
}

interface AccountInfo {
    index: number;
    address: string;
    keypair: Ed25519Keypair;
    completedTx: number;
    failedTx: number;
    totalOperations: number;
    status: 'pending' | 'running' | 'completed' | 'exhausted' | 'skipped';
    consecutiveFailures: number;
    lastError: string | null;
}

interface Stats {
    startTime: number;
    endTime: number | null;
    totalTxAttempts: number;
    successfulTx: number;
    failedTx: number;
    totalOperations: number;
    gasErrors: number;
    versionErrors: number;
    otherErrors: number;
}

const stats: Stats = {
    startTime: 0, endTime: null,
    totalTxAttempts: 0, successfulTx: 0, failedTx: 0, totalOperations: 0,
    gasErrors: 0, versionErrors: 0, otherErrors: 0,
};

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function loadAccounts(): { address: string; privateKey: string }[] {
    const p = path.join(__dirname, 'multi_accounts.json');
    if (!fs.existsSync(p)) throw new Error('multi_accounts.json not found!');
    return JSON.parse(fs.readFileSync(p, 'utf8'));
}

async function preflightChecks(
    client: SuiClient,
    accounts: AccountInfo[]
): Promise<AccountInfo[]> {
    console.log(`\n🔍 Pre-flight: ${accounts.length} accounts...`);
    const valid: AccountInfo[] = [];
    
    // Parallel balance checks
    const BATCH = 100;
    for (let i = 0; i < accounts.length; i += BATCH) {
        const batch = accounts.slice(i, i + BATCH);
        const results = await Promise.allSettled(
            batch.map(acc => client.getBalance({ owner: acc.address }))
        );
        results.forEach((r, j) => {
            if (r.status === 'fulfilled' && BigInt(r.value.totalBalance) >= MIN_BALANCE_MIST) {
                valid.push(batch[j]!);
            } else {
                batch[j]!.status = 'skipped';
            }
        });
        if ((i + BATCH) % 1000 === 0) console.log(`   ${Math.min(i + BATCH, accounts.length)}/${accounts.length}...`);
    }
    
    console.log(`   ✅ ${valid.length} valid accounts`);
    return valid;
}

function categorizeError(msg: string): 'gas' | 'version' | 'other' {
    const l = msg.toLowerCase();
    if (l.includes('gas')) return 'gas';
    if (l.includes('version') || l.includes('equivocation')) return 'version';
    return 'other';
}

/**
 * OWNED OBJECT BENCHMARK: Coin Split/Merge Operations
 * 
 * Each TX:
 * 1. Splits gas coin into N tiny coins
 * 2. Merges them back into gas coin
 * 
 * This creates MASSIVE object churn without needing counters!
 * - No shared object consensus
 * - Each account is independent
 * - True parallelization
 */
async function runAccountTask(client: SuiClient, acc: AccountInfo): Promise<void> {
    while (shouldContinue() && acc.status === 'running') {
        if (acc.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
            acc.status = 'exhausted';
            break;
        }
        
        const tx = new Transaction();
        tx.setGasBudget(GAS_BUDGET);
        
        // Split gas coin into many small coins, then merge back
        // This creates N new objects then destroys them → disk writes!
        const splits: any[] = [];
        for (let i = 0; i < SPLITS_PER_TX; i++) {
            splits.push(tx.splitCoins(tx.gas, [SPLIT_AMOUNT]));
        }
        
        // Merge all splits back (cleanup + more disk I/O)
        if (splits.length > 0) {
            tx.mergeCoins(tx.gas, splits);
        }
        
        let success = false;
        for (let attempt = 0; attempt < MAX_RETRIES && !success && shouldContinue(); attempt++) {
            try {
                stats.totalTxAttempts++;
                const result = await client.signAndExecuteTransaction({
                    signer: acc.keypair,
                    transaction: tx,
                    options: { showEffects: true },
                });
                
                if (result.effects?.status.status === 'success') {
                    stats.successfulTx++;
                    stats.totalOperations += SPLITS_PER_TX * 2;  // split + merge
                    acc.totalOperations += SPLITS_PER_TX * 2;
                    acc.completedTx++;
                    acc.consecutiveFailures = 0;
                    success = true;
                } else {
                    const err = result.effects?.status.error || 'Unknown';
                    acc.lastError = err;
                    const t = categorizeError(err);
                    if (t === 'gas') stats.gasErrors++;
                    else if (t === 'version') stats.versionErrors++;
                    else stats.otherErrors++;
                }
            } catch (e: any) {
                acc.lastError = e.message;
                const t = categorizeError(e.message);
                if (t === 'gas') stats.gasErrors++;
                else if (t === 'version') stats.versionErrors++;
                else stats.otherErrors++;
            }
        }
        
        if (!success) {
            stats.failedTx++;
            acc.failedTx++;
            acc.consecutiveFailures++;
        }
        
        await sleep(ITER_INTERVAL_MS);
    }
}

function startProgressReporter(accounts: AccountInfo[]): NodeJS.Timeout {
    return setInterval(() => {
        const elapsed = (Date.now() - stats.startTime) / 1000;
        const remaining = Math.max(0, (benchmarkEndTime - Date.now()) / 1000);
        const tps = elapsed > 0 ? stats.successfulTx / elapsed : 0;
        const opsPerSec = elapsed > 0 ? stats.totalOperations / elapsed : 0;
        const mem = getMemoryUsage();
        const successRate = stats.totalTxAttempts > 0 
            ? ((stats.successfulTx / stats.totalTxAttempts) * 100).toFixed(0) : '0';
        
        const running = accounts.filter(a => a.status === 'running').length;
        const exhausted = accounts.filter(a => a.status === 'exhausted').length;

        console.log(
            `⏱️ ${Math.floor(elapsed)}s (${Math.floor(remaining/60)}m${Math.floor(remaining%60)}s) | ` +
            `🏃${running} 💀${exhausted} | ` +
            `✅${stats.successfulTx} (${successRate}%) ❌${stats.failedTx} | ` +
            `TPS:${tps.toFixed(1)} CPS:${opsPerSec.toFixed(0)} | RAM:${mem.usedPct.toFixed(0)}%`
        );
    }, 3000);
}

function printReport(accounts: AccountInfo[]) {
    const elapsed = stats.endTime ? (stats.endTime - stats.startTime) / 1000 : 0;
    const avgTPS = elapsed > 0 ? stats.successfulTx / elapsed : 0;
    const avgCPS = elapsed > 0 ? stats.totalOperations / elapsed : 0;
    const successRate = stats.totalTxAttempts > 0 
        ? ((stats.successfulTx / stats.totalTxAttempts) * 100).toFixed(1) : '0';
    const mem = getMemoryUsage();
    
    console.log('\n' + '═'.repeat(70));
    console.log('                      📊 BENCHMARK COMPLETE');
    console.log('═'.repeat(70));
    console.log(`Duration:           ${elapsed.toFixed(0)}s`);
    console.log(`Successful TX:      ${stats.successfulTx.toLocaleString()} (${successRate}%)`);
    console.log(`Failed TX:          ${stats.failedTx.toLocaleString()}`);
    console.log(`Total Operations:   ${stats.totalOperations.toLocaleString()}`);
    console.log('─'.repeat(70));
    console.log(`Average TPS:        ${avgTPS.toFixed(2)}`);
    console.log(`Average CPS:        ${avgCPS.toFixed(0)}`);
    console.log('─'.repeat(70));
    console.log(`Error breakdown: Gas=${stats.gasErrors} Version=${stats.versionErrors} Other=${stats.otherErrors}`);
    console.log(`Final memory:       ${mem.usedPct.toFixed(1)}%`);
    console.log('═'.repeat(70));
}

async function main() {
    const rpcUrl = cfg.rpcList?.[cfg.rpcIndex] ?? 'http://127.0.0.1:9000';
    const client = new SuiClient({ url: rpcUrl });
    
    console.log('🚀 SUI High-Throughput Benchmark (Owned Object Operations)');
    console.log('══════════════════════════════════════════════════════════════════════');
    console.log(`Mode:               COIN SPLIT/MERGE (owned objects, no consensus)`);
    console.log(`Duration:           ${(cfg.duration || 1200) / 60} minutes`);
    console.log(`Splits per TX:      ${SPLITS_PER_TX}`);
    console.log(`TX interval:        ${ITER_INTERVAL_MS}ms`);
    console.log(`Max accounts:       ${MAX_ACCOUNTS}`);
    console.log(`Concurrent:         ${CONCURRENT_ACCOUNTS}`);
    console.log('══════════════════════════════════════════════════════════════════════');
    
    // Load accounts
    console.log('\n📂 Loading accounts...');
    const rawAccounts = loadAccounts();
    const accountsToUse = rawAccounts.slice(0, MAX_ACCOUNTS);
    console.log(`   Loaded ${accountsToUse.length} accounts`);
    
    const accounts: AccountInfo[] = accountsToUse.map((acc, idx) => {
        const { secretKey } = decodeSuiPrivateKey(acc.privateKey);
        return {
            index: idx,
            address: acc.address,
            keypair: Ed25519Keypair.fromSecretKey(secretKey),
            completedTx: 0, failedTx: 0, totalOperations: 0,
            status: 'pending' as const,
            consecutiveFailures: 0, lastError: null,
        };
    });
    
    const valid = await preflightChecks(client, accounts);
    if (valid.length === 0) throw new Error('No valid accounts!');

    console.log('\n' + '═'.repeat(70));
    console.log(`Ready: ${valid.length} accounts, ${SPLITS_PER_TX} splits/TX, ${ITER_INTERVAL_MS}ms interval`);
    console.log('═'.repeat(70));
    console.log('\nStarting in 3s...\n');
    await sleep(3000);
    
    const memMonitor = startMemoryMonitor();
    const progressInterval = startProgressReporter(accounts);
    
    stats.startTime = Date.now();
    benchmarkEndTime = stats.startTime + BENCHMARK_DURATION_MS;
    console.log(`🏁 Running until ${new Date(benchmarkEndTime).toLocaleTimeString()}\n`);
    
    // Run waves of concurrent accounts
    while (shouldContinue()) {
        const available = valid.filter(a => 
            a.status !== 'exhausted' && a.consecutiveFailures < MAX_CONSECUTIVE_FAILURES
        );
        
        if (available.length === 0) {
            console.log('\n⚠️ All accounts exhausted');
            break;
        }
        
        // Rotate through accounts in chunks
        const chunk = available.slice(0, Math.min(CONCURRENT_ACCOUNTS, available.length));
        chunk.forEach(acc => acc.status = 'running');
        
        // Run all concurrently
        await Promise.race([
            Promise.all(chunk.map(acc => runAccountTask(client, acc))),
            sleep(60000),  // 1 minute max per wave
            new Promise<void>(resolve => {
                const check = setInterval(() => {
                    if (!shouldContinue()) { clearInterval(check); resolve(); }
                }, 1000);
            })
        ]);
        
        if (shouldContinue()) await sleep(50);
    }
    
    clearInterval(memMonitor);
    clearInterval(progressInterval);
    stats.endTime = Date.now();
    
    printReport(accounts);
}

main().catch(err => {
    console.error('\n❌ Fatal:', err.message);
    process.exit(1);
});