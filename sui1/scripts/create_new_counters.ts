import { SuiClient } from '@mysten/sui/client';
import { Transaction } from '@mysten/sui/transactions';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

import { getActiveConfig } from './.config';

// [FIX KEY POINT] Manually define __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables
dotenv.config();
const cfg = getActiveConfig();

// ================= Configuration Section =================
// 1. Package ID
const NETWORK = cfg.network;
const PACKAGE_ID = cfg.packageId;

// 2. Module name and function name
const MODULE_NAME = cfg.module;
const FUNCTION_NAME = cfg.opCreateCounter;

// 3. Parameter object required by create_counter (based on your CLI command)
const ARGS_OBJECT_ID = cfg.globalStateId;

// 4. Number of counters to create in a single batch (recommended 50-100, too many may cause Gas limit exceeded or package too large)
const BATCH_SIZE = 500;


// ===========================================

async function main() {
    // 1. Initialize Client - use custom RPC for localnet
    let rpcUrl: string;
    if (NETWORK === 'localnet' || NETWORK === 'devnet') {
        rpcUrl = cfg.rpcList[cfg.rpcIndex] || 'http://127.0.0.1:9000';
    } else {
        // For mainnet/testnet, use the RPC from config or default
        rpcUrl = cfg.rpcList[cfg.rpcIndex] || `https://fullnode.${NETWORK}.sui.io:443`;
    }
    
    console.log(`🌐 RPC URL: ${rpcUrl}`);
    
    const client = new SuiClient({ url: rpcUrl });

    // 2. Load private key
    const privateKey = process.env.SUI_PRIVATE_KEY;
    if (!privateKey) {
        throw new Error('Please configure SUI_PRIVATE_KEY in .env file');
    }
    const { secretKey } = decodeSuiPrivateKey(privateKey);
    const keypair = Ed25519Keypair.fromSecretKey(secretKey);
    const address = keypair.toSuiAddress();

    console.log(`👤 Executing account: ${address}`);
    console.log(`📦 Preparing to batch create ${BATCH_SIZE} counters...`);

    // 3. Build transaction block (PTB)
    const tx = new Transaction();

    // Loop to add moveCall commands
    // This allows completing N creations in 1 transaction, greatly saving time and Gas
    for (let i = 0; i < BATCH_SIZE; i++) {
        tx.moveCall({
            target: `${PACKAGE_ID}::${MODULE_NAME}::${FUNCTION_NAME}`,
            arguments: [
                tx.object(ARGS_OBJECT_ID) // Pass that fixed parameter object
            ]
        });
    }

    // Set Gas budget (batch operations consume more Gas, set it sufficiently, here approximately 0.05 SUI)
    tx.setGasBudget(2_000_000_000);

    // 4. Execute transaction and get result
    try {
        const startTime = Date.now();

        const result = await client.signAndExecuteTransaction({
            signer: keypair,
            transaction: tx,
            options: {
                showEffects: true,
                showObjectChanges: true, // [CRITICAL] Must enable this to see newly created object IDs
            }
        });

        const endTime = Date.now();

        if (result.effects?.status.status === 'success') {
            console.log(`✅ Transaction executed successfully! Digest: ${result.digest}`);
            console.log(`⏱️ Time elapsed: ${endTime - startTime} ms`);

            // 5. Parse and extract newly created object IDs
            const createdObjectIds: string[] = [];

            if (result.objectChanges) {
                // Iterate through change list
                for (const change of result.objectChanges) {
                    // Filter for 'created' type changes
                if (
                    change.type === 'created' &&
                    'objectType' in change &&
                    change.objectType.includes('Counter')
                ) {
                        createdObjectIds.push(change.objectId);
                    }
                }
            }

            console.log(`\n🎉 Successfully created ${createdObjectIds.length} counter objects:`);
            // console.log(JSON.stringify(createdObjectIds, null, 2));
        } else {
            console.error(`❌ Transaction failed: ${result.effects?.status.error}`);
        }

    } catch (e) {
        console.error("Error occurred during execution:", e);
    }
}

main();