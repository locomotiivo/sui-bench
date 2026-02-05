/**
 * Sui Client Utilities
 */
import { SuiClient } from '@mysten/sui/client';
import { Transaction } from '@mysten/sui/transactions';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import * as fs from 'fs';
import * as path from 'path';
/**
 * Load keypair from sui.keystore file
 */
export function loadKeypair(keystorePath) {
    const keystore = JSON.parse(fs.readFileSync(keystorePath, 'utf-8'));
    if (!Array.isArray(keystore) || keystore.length === 0) {
        throw new Error('Keystore is empty or invalid');
    }
    // First key in keystore
    const privateKeyBase64 = keystore[0];
    try {
        // Try new format first (suiprivkey...)
        const { secretKey } = decodeSuiPrivateKey(privateKeyBase64);
        return Ed25519Keypair.fromSecretKey(secretKey);
    }
    catch {
        // Fall back to raw base64 format (older keystore format)
        // The key is base64 encoded, first byte is the scheme flag (0 = Ed25519)
        const keyBytes = Buffer.from(privateKeyBase64, 'base64');
        // Skip the first byte (scheme flag) and use remaining 32 bytes as secret key
        if (keyBytes.length === 33) {
            const secretKey = keyBytes.slice(1);
            return Ed25519Keypair.fromSecretKey(secretKey);
        }
        else if (keyBytes.length === 32) {
            return Ed25519Keypair.fromSecretKey(keyBytes);
        }
        else {
            throw new Error(`Unexpected key length: ${keyBytes.length}`);
        }
    }
}
/**
 * Auto-detect RPC URL from validator config
 */
export function detectRpcUrl(configDir) {
    // Look for validator config file
    const files = fs.readdirSync(configDir);
    const validatorConfig = files.find(f => f.match(/^127\.0\.0\.1-\d+\.yaml$/));
    if (validatorConfig) {
        const content = fs.readFileSync(path.join(configDir, validatorConfig), 'utf-8');
        // Extract json-rpc-address
        const match = content.match(/json-rpc-address:\s*"([^"]+)"/);
        if (match) {
            return `http://${match[1]}`;
        }
    }
    // Fallback to default
    return 'http://127.0.0.1:9000';
}
/**
 * Initialize Sui context
 */
export async function initSuiContext(rpcUrl, keystorePath) {
    const client = new SuiClient({ url: rpcUrl });
    const keypair = loadKeypair(keystorePath);
    const address = keypair.getPublicKey().toSuiAddress();
    console.log(`[sui] Connected to: ${rpcUrl}`);
    console.log(`[sui] Address: ${address}`);
    // Get gas coins
    const coins = await client.getCoins({ owner: address, coinType: '0x2::sui::SUI' });
    const gasCoins = coins.data.map(c => c.coinObjectId);
    console.log(`[sui] Available gas coins: ${gasCoins.length}`);
    if (gasCoins.length === 0) {
        throw new Error('No SUI coins available for gas. Run faucet first.');
    }
    return { client, keypair, address, gasCoins };
}
/**
 * Execute a transaction
 */
export async function executeTransaction(ctx, tx, showEffects = false) {
    const result = await ctx.client.signAndExecuteTransaction({
        transaction: tx,
        signer: ctx.keypair,
        options: {
            showEffects: true,
            showObjectChanges: showEffects,
        },
    });
    if (result.effects?.status?.status !== 'success') {
        throw new Error(`Transaction failed: ${JSON.stringify(result.effects?.status)}`);
    }
    return result;
}
/**
 * Get reference gas price
 */
export async function getGasPrice(client) {
    const gasPrice = await client.getReferenceGasPrice();
    return BigInt(gasPrice);
}
/**
 * Split coins for parallel transactions
 */
export async function splitCoinsForParallel(ctx, count, amountPerCoin) {
    if (ctx.gasCoins.length >= count) {
        return ctx.gasCoins.slice(0, count);
    }
    console.log(`[sui] Splitting coins for ${count} parallel workers...`);
    const tx = new Transaction();
    const amounts = Array(count - 1).fill(amountPerCoin);
    const coins = tx.splitCoins(tx.gas, amounts.map(a => tx.pure.u64(a)));
    // Transfer split coins to self
    for (let i = 0; i < count - 1; i++) {
        tx.transferObjects([coins[i]], tx.pure.address(ctx.address));
    }
    const result = await executeTransaction(ctx, tx, true);
    // Get created coin IDs
    const createdCoins = result.objectChanges
        ?.filter(c => c.type === 'created' && 'objectId' in c)
        .map(c => c.objectId) || [];
    // Refresh gas coins
    const allCoins = await ctx.client.getCoins({ owner: ctx.address, coinType: '0x2::sui::SUI' });
    ctx.gasCoins = allCoins.data.map(c => c.coinObjectId);
    return ctx.gasCoins.slice(0, count);
}
