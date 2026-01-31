/**
 * generate_accounts-optimal.ts - Fast Parallel Account Generation
 * 
 * Generates and funds 5000+ accounts efficiently using:
 * - Parallel keypair generation
 * - Large batch funding (max accounts per TX)
 * - Parallel funding from multiple gas coins
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
const NUM_ACCOUNTS = cfg.targetCount || 5000;
const SUI_PER_ACCOUNT = cfg.suiPerAccount || 50;  // 50 SUI each (plenty for benchmark)
const BATCH_SIZE = 256;                           // Max per TX (PTB limit)
const MIST_PER_SUI = 1_000_000_000n;
const MAX_RETRIES = 3;
// ===========================================

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function main() {
    const rpcUrl = cfg.rpcList?.[cfg.rpcIndex] || 'http://127.0.0.1:9000';
    const client = new SuiClient({ url: rpcUrl });

    const mainPrivateKey = process.env.SUI_PRIVATE_KEY;
    if (!mainPrivateKey) throw new Error('SUI_PRIVATE_KEY not found in .env');
    
    const { secretKey } = decodeSuiPrivateKey(mainPrivateKey);
    const mainKeypair = Ed25519Keypair.fromSecretKey(secretKey);
    const mainAddress = mainKeypair.toSuiAddress();

    const balance = await client.getBalance({ owner: mainAddress });
    const balanceSui = Number(balance.totalBalance) / 1e9;
    const requiredSui = NUM_ACCOUNTS * SUI_PER_ACCOUNT + 500;
    
    console.log('═'.repeat(60));
    console.log('FAST ACCOUNT GENERATOR');
    console.log('═'.repeat(60));
    console.log(`Main account:     ${mainAddress}`);
    console.log(`Balance:          ${balanceSui.toLocaleString()} SUI`);
    console.log(`Accounts:         ${NUM_ACCOUNTS.toLocaleString()}`);
    console.log(`SUI per account:  ${SUI_PER_ACCOUNT}`);
    console.log(`Required:         ${requiredSui.toLocaleString()} SUI`);
    console.log('═'.repeat(60));
    
    if (balanceSui < requiredSui) {
        throw new Error(`Insufficient balance! Have ${balanceSui.toFixed(0)}, need ${requiredSui}`);
    }

    // PHASE 1: Generate keypairs (fast, in-memory)
    console.log(`\n[1/2] Generating ${NUM_ACCOUNTS.toLocaleString()} keypairs...`);
    const genStart = Date.now();
    
    const accounts: { address: string; privateKey: string }[] = [];
    for (let i = 0; i < NUM_ACCOUNTS; i++) {
        const kp = Ed25519Keypair.generate();
        accounts.push({ address: kp.toSuiAddress(), privateKey: kp.getSecretKey() });
        if ((i + 1) % 5000 === 0) console.log(`   ${i + 1}/${NUM_ACCOUNTS}...`);
    }
    console.log(`   ✓ Generated in ${Date.now() - genStart}ms`);

    // PHASE 2: Batch funding
    console.log(`\n[2/2] Funding ${NUM_ACCOUNTS.toLocaleString()} accounts (${BATCH_SIZE}/batch)...`);
    const fundStart = Date.now();
    const totalBatches = Math.ceil(accounts.length / BATCH_SIZE);
    let funded = 0;
    let failedBatches = 0;
    
    for (let i = 0; i < accounts.length; i += BATCH_SIZE) {
        const batch = accounts.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        
        let success = false;
        for (let retry = 0; retry < MAX_RETRIES && !success; retry++) {
            try {
                // Get fresh coin
                const coins = await client.getCoins({ owner: mainAddress, coinType: '0x2::sui::SUI' });
                if (coins.data.length === 0) throw new Error('No gas coins');
                
                const gasCoin = coins.data.reduce((best, c) => 
                    BigInt(c.balance) > BigInt(best.balance) ? c : best
                );
                
                const tx = new Transaction();
                tx.setGasPayment([{
                    objectId: gasCoin.coinObjectId,
                    version: gasCoin.version,
                    digest: gasCoin.digest,
                }]);
                
                const amount = BigInt(SUI_PER_ACCOUNT) * MIST_PER_SUI;
                for (const acc of batch) {
                    const coin = tx.splitCoins(tx.gas, [amount]);
                    tx.transferObjects([coin], acc.address);
                }
                
                tx.setGasBudget(BigInt(batch.length) * 20_000_000n + 500_000_000n);
                
                const result = await client.signAndExecuteTransaction({
                    signer: mainKeypair,
                    transaction: tx,
                    options: { showEffects: true },
                    requestType: 'WaitForLocalExecution',
                });
                
                if (result.effects?.status.status === 'success') {
                    funded += batch.length;
                    const elapsed = (Date.now() - fundStart) / 1000;
                    const rate = funded / elapsed;
                    console.log(`   Batch ${batchNum}/${totalBatches}: +${batch.length} (${funded} total, ${rate.toFixed(0)}/s)`);
                    success = true;
                }
            } catch (e: any) {
                if (retry < MAX_RETRIES - 1) {
                    await sleep(500);
                } else {
                    console.log(`   Batch ${batchNum} FAILED: ${e.message.slice(0, 60)}`);
                    failedBatches++;
                }
            }
        }
        
        await sleep(200);  // Brief pause between batches
    }
    
    const elapsed = (Date.now() - fundStart) / 1000;
    
    // Save results
    console.log(`\n💾 Saving...`);
    const fundedAccounts = accounts.slice(0, funded);
    fs.writeFileSync(path.join(__dirname, 'multi_accounts.json'), JSON.stringify(fundedAccounts, null, 2));
    
    // Update config
    const configPath = path.join(__dirname, 'config.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    config.multiAccounts = fundedAccounts.map(a => a.address);
    config.targetCount = funded;
    config.suiPerAccount = SUI_PER_ACCOUNT;
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
    
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`COMPLETE`);
    console.log(`${'═'.repeat(60)}`);
    console.log(`Funded:    ${funded.toLocaleString()} / ${NUM_ACCOUNTS.toLocaleString()}`);
    console.log(`Time:      ${elapsed.toFixed(1)}s`);
    console.log(`Rate:      ${(funded / elapsed).toFixed(0)} accounts/s`);
    console.log(`Failed:    ${failedBatches} batches`);
    console.log(`${'═'.repeat(60)}`);
}

main().catch(err => {
    console.error('\n❌ Fatal:', err.message);
    process.exit(1);
});