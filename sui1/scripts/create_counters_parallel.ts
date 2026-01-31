/**
 * create_counters_parallel-optimal.ts - FAST Parallel Counter Creation
 * 
 * Problem: Serial creation of 256k counters = 512 batches × 5s = 40+ minutes
 * Solution: Create from MULTIPLE accounts in PARALLEL
 * 
 * With 50 parallel creators × 500 counters/batch × 10 batches each = 250k counters in ~2 minutes
 */

import { SuiClient } from '@mysten/sui/client';
import { Transaction } from '@mysten/sui/transactions';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { getActiveConfig } from './.config.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config();
const cfg = getActiveConfig();

// ============== CONFIGURATION ==============
const TARGET_COUNTERS = parseInt(process.env.NUM_COUNTERS || '250000');
const COUNTERS_PER_BATCH = 500;           // Max per TX
const PARALLEL_CREATORS = 50;             // Accounts creating simultaneously
const BATCHES_PER_CREATOR = Math.ceil(TARGET_COUNTERS / PARALLEL_CREATORS / COUNTERS_PER_BATCH);
const GAS_BUDGET = 2_000_000_000n;         // 2 SUI per batch
// ===========================================

interface CreatorAccount {
    index: number;
    address: string;
    keypair: Ed25519Keypair;
    countersCreated: number;
    batchesCompleted: number;
    failed: boolean;
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function main() {
    const rpcUrl = cfg.rpcList?.[cfg.rpcIndex] || 'http://127.0.0.1:9000';
    const client = new SuiClient({ url: rpcUrl });
    
    console.log('🚀 Parallel Counter Creation');
    console.log('══════════════════════════════════════════════════════════════════════');
    console.log(`Target counters:     ${TARGET_COUNTERS.toLocaleString()}`);
    console.log(`Parallel creators:   ${PARALLEL_CREATORS}`);
    console.log(`Counters per batch:  ${COUNTERS_PER_BATCH}`);
    console.log(`Batches per creator: ${BATCHES_PER_CREATOR}`);
    console.log(`Estimated time:      ~${Math.ceil(BATCHES_PER_CREATOR * 3 / 60)} minutes`);
    console.log('══════════════════════════════════════════════════════════════════════');
    
    // Load funded accounts
    const accountsPath = path.join(__dirname, 'multi_accounts.json');
    if (!fs.existsSync(accountsPath)) {
        throw new Error('multi_accounts.json not found! Run generate_accounts.ts first.');
    }
    
    const rawAccounts = JSON.parse(fs.readFileSync(accountsPath, 'utf8'));
    if (rawAccounts.length < PARALLEL_CREATORS) {
        throw new Error(`Need ${PARALLEL_CREATORS} accounts, only have ${rawAccounts.length}`);
    }
    
    // Setup creator accounts
    const creators: CreatorAccount[] = rawAccounts.slice(0, PARALLEL_CREATORS).map((acc: any, idx: number) => {
        const { secretKey } = decodeSuiPrivateKey(acc.privateKey);
        return {
            index: idx,
            address: acc.address,
            keypair: Ed25519Keypair.fromSecretKey(secretKey),
            countersCreated: 0,
            batchesCompleted: 0,
            failed: false,
        };
    });
    
    console.log(`\n📂 Using ${creators.length} creator accounts`);
    
    // Get package info
    const PACKAGE_ID = cfg.packageId;
    const MODULE_NAME = cfg.module;
    const FUNCTION_NAME = cfg.opCreateCounter;
    const GLOBAL_STATE_ID = cfg.globalStateId;
    
    if (!PACKAGE_ID || !GLOBAL_STATE_ID) {
        throw new Error('Package not deployed! Run deploy_tps_test.ts first.');
    }
    
    console.log(`📦 Package: ${PACKAGE_ID}`);
    console.log(`🌐 GlobalState: ${GLOBAL_STATE_ID}`);
    
    const startTime = Date.now();
    let totalCreated = 0;
    let totalBatches = 0;
    
    // Create counter creation task for one account
    async function createBatches(creator: CreatorAccount): Promise<void> {
        for (let batch = 0; batch < BATCHES_PER_CREATOR && !creator.failed; batch++) {
            try {
                const tx = new Transaction();
                tx.setGasBudget(GAS_BUDGET);
                
                for (let i = 0; i < COUNTERS_PER_BATCH; i++) {
                    tx.moveCall({
                        target: `${PACKAGE_ID}::${MODULE_NAME}::${FUNCTION_NAME}`,
                        arguments: [tx.object(GLOBAL_STATE_ID)],
                    });
                }
                
                const result = await client.signAndExecuteTransaction({
                    signer: creator.keypair,
                    transaction: tx,
                    options: { showEffects: true },
                });
                
                if (result.effects?.status.status === 'success') {
                    creator.countersCreated += COUNTERS_PER_BATCH;
                    creator.batchesCompleted++;
                    totalCreated += COUNTERS_PER_BATCH;
                    totalBatches++;
                } else {
                    console.log(`   Creator ${creator.index} batch ${batch} failed: ${result.effects?.status.error}`);
                }
            } catch (e: any) {
                console.log(`   Creator ${creator.index} error: ${e.message.slice(0, 50)}`);
                // Continue trying
            }
            
            // Small delay between batches from same account
            await sleep(100);
        }
    }
    
    // Progress reporter
    const progressInterval = setInterval(() => {
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = totalCreated / elapsed;
        const remaining = (TARGET_COUNTERS - totalCreated) / rate;
        console.log(
            `⏱️ ${Math.floor(elapsed)}s | ` +
            `✅ ${totalCreated.toLocaleString()}/${TARGET_COUNTERS.toLocaleString()} | ` +
            `📦 ${totalBatches} batches | ` +
            `⚡ ${rate.toFixed(0)}/s | ` +
            `ETA: ${Math.ceil(remaining)}s`
        );
    }, 5000);
    
    console.log('\n🏁 Starting parallel creation...\n');
    
    // Run all creators in parallel!
    await Promise.all(creators.map(c => createBatches(c)));
    
    clearInterval(progressInterval);
    
    const elapsed = (Date.now() - startTime) / 1000;
    
    console.log('\n' + '═'.repeat(70));
    console.log('                    ✅ COUNTER CREATION COMPLETE');
    console.log('═'.repeat(70));
    console.log(`Total created:      ${totalCreated.toLocaleString()}`);
    console.log(`Total batches:      ${totalBatches}`);
    console.log(`Time:               ${elapsed.toFixed(1)}s`);
    console.log(`Rate:               ${(totalCreated / elapsed).toFixed(0)} counters/s`);
    console.log('═'.repeat(70));
    
    // Now fetch counter IDs
    console.log('\n📥 Fetching counter IDs from chain...');
}

main().catch(err => {
    console.error('\n❌ Fatal:', err.message);
    process.exit(1);
});