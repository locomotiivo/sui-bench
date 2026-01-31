import { getFullnodeUrl, SuiClient } from '@mysten/sui/client';
import { Transaction } from '@mysten/sui/transactions';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import * as dotenv from 'dotenv';
import { getActiveConfig } from './.config';

// Load environment variables
dotenv.config();
const cfg = getActiveConfig();

// ================= Configuration Area =================
const MIN_SUI_THRESHOLD = Number(cfg.fee.minSuiThreshold || 0.04); 
const TARGET_COUNT = Number(cfg.targetCount || 1000);
const MIST_PER_SUI = 1_000_000_000;
const SPLIT_AMOUNT_SUI = Number(cfg.fee.splitAmountSui || 0.2);

type SuiNetwork = 'mainnet' | 'testnet' | 'devnet' | 'localnet';
const NETWORK = (cfg.network || 'testnet') as SuiNetwork
// ===========================================

/**
 * Main entry: Get list of Gas object IDs that meet requirements
 */
export async function getGasCoinIds(): Promise<string[]> {
    // 1. Initialize
    const client = new SuiClient({ url: getFullnodeUrl(NETWORK) });
    const privateKey = process.env.SUI_PRIVATE_KEY;
    if (!privateKey) throw new Error('SUI_PRIVATE_KEY not found');
    
    const { secretKey } = decodeSuiPrivateKey(privateKey);
    const keypair = Ed25519Keypair.fromSecretKey(secretKey);
    const address = keypair.toSuiAddress();

    // 2. Check current status
    // Get coins that meet the amount threshold
    const validCoins = await getValidCoins(client, address);

    // If sufficient quantity, return specified amount
    if (validCoins.length >= TARGET_COUNT) {
        console.log(`✅ Sufficient Gas objects available: ${validCoins.length} (will return the ${TARGET_COUNT} with highest balance)`);

        return validCoins
            // Sort by balance descending, prioritize higher balance
            .sort((a, b) => Number(b.balance) - Number(a.balance))
            // Take only the first TARGET_COUNT
            .slice(0, TARGET_COUNT)
            // Extract IDs
            .map(coin => coin.coinObjectId);
    }

    console.log(`⚠️ Insufficient Gas objects (current valid: ${validCoins.length}, target: ${TARGET_COUNT}), starting [merge -> split] process...`);

    // 3. Execute merge and split logic
    await mergeAndSplit(client, keypair, address);

    // 4. Recursive call (wait a few seconds to recheck status)
    console.log(`⏳ Waiting 3 seconds to ensure on-chain state synchronization...`);
    await new Promise(resolve => setTimeout(resolve, 3000));
    return getGasCoinIds();
}

/**
 * Core logic: First merge all coins into the largest one, then split into specified number of smaller pieces
 */
async function mergeAndSplit(client: SuiClient, keypair: Ed25519Keypair, address: string) {
    // 1. Get all coins in the account (no amount restriction)
    const allCoins = await getAllCoins(client, address);

    if (allCoins.length === 0) {
        throw new Error("❌ No SUI objects found in account, please fund or request faucet first!");
    }

    // 2. Sort: balance from high to low
    const sortedCoins = allCoins.sort((a, b) => Number(b.balance) - Number(a.balance));

    // Primary coin (highest balance), used as Gas payment object and merge target
    const primaryCoin = sortedCoins[0];
    const totalBalance = sortedCoins.reduce((sum, coin) => sum + Number(coin.balance), 0);

    // Calculate total required amount (target count * split amount per piece)
    const requiredAmountMist = BigInt(TARGET_COUNT) * BigInt(Math.floor(SPLIT_AMOUNT_SUI * MIST_PER_SUI));

    // Check if total balance is sufficient (reserve 0.05 SUI as Gas buffer)
    const gasBuffer = BigInt(0.05 * MIST_PER_SUI);
    if (BigInt(totalBalance) < (requiredAmountMist + gasBuffer)) {
        const currentSui = (totalBalance / MIST_PER_SUI).toFixed(4);
        const requiredSui = ((Number(requiredAmountMist) + Number(gasBuffer)) / MIST_PER_SUI).toFixed(4);
        throw new Error(`❌ Insufficient account balance! Current: ${currentSui} SUI, need at least: ${requiredSui} SUI (including Gas)`);
    }

    console.log(`🔨 Reorganizing Gas objects...`);
    console.log(`   - Primary object: ${primaryCoin.coinObjectId} (balance: ${(Number(primaryCoin.balance)/MIST_PER_SUI).toFixed(2)} SUI)`);
    console.log(`   - Small objects to merge: ${sortedCoins.length - 1}`);
    console.log(`   - Target split count: ${TARGET_COUNT} (${SPLIT_AMOUNT_SUI} SUI each)`);

    // 3. Build transaction (merge + split completed in the same PTB)
    const tx = new Transaction();

    // Set Gas payment object
    tx.setGasPayment([{
        objectId: primaryCoin.coinObjectId,
        version: primaryCoin.version,
        digest: primaryCoin.digest
    }]);

    // Step A: Merge
    // Merge all objects except the primary one
    // Note: PTB has input object limit (typically recommended not to exceed 500), slice here to prevent overflow
    const coinsToMerge = sortedCoins.slice(1, 500).map(c => c.coinObjectId);
    
    if (coinsToMerge.length > 0) {
        tx.mergeCoins(tx.gas, coinsToMerge);
    }

    const splitAmountMist = BigInt(Math.floor(SPLIT_AMOUNT_SUI * MIST_PER_SUI));

    console.log(`   - Starting split and distribution (${TARGET_COUNT} times)...`);

    for (let i = 0; i < TARGET_COUNT; i++) {
        // 1. Split a new coin with specified amount from Gas object
        // Note: splitCoins returns a Result (representing vector<Coin>)
        const coin = tx.splitCoins(tx.gas, [splitAmountMist]);

        // 2. Transfer this new coin to current address
        // SDK will correctly handle the Result passing here
        tx.transferObjects([coin], address);
    }

    // 4. Execute transaction
    try {
        const result = await client.signAndExecuteTransaction({
            signer: keypair,
            transaction: tx,
            options: { showEffects: true }
        });
        console.log(`🚀 Reorganization transaction successful! Digest: ${result.digest}`);
    } catch (e) {
        console.error("❌ Reorganization transaction failed:", e);
        throw e;
    }
}

/**
 * Helper: Get all SUI objects in account (no amount threshold)
 */
async function getAllCoins(client: SuiClient, address: string) {
    let hasNext = true;
    let cursor = null;
    const allCoins = [];

    while (hasNext) {
        const res: any = await client.getCoins({
            owner: address,
            coinType: '0x2::sui::SUI',
            cursor: cursor
        });
        allCoins.push(...res.data);
        hasNext = res.hasNextPage;
        cursor = res.nextCursor;
    }
    return allCoins;
}

/**
 * Helper: Get objects that meet the balance threshold
 */
async function getValidCoins(client: SuiClient, address: string) {
    const allCoins = await getAllCoins(client, address);
    const thresholdMist = MIN_SUI_THRESHOLD * MIST_PER_SUI;
    return allCoins.filter(coin => Number(coin.balance) >= thresholdMist);
}