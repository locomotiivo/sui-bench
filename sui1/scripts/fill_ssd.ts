import { SuiClient } from '@mysten/sui/client';
import { Transaction } from '@mysten/sui/transactions';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import { getActiveConfig } from './.config.ts';

dotenv.config();
const cfg = getActiveConfig();

const PACKAGE_ID = cfg.packageId;
const MODULE_NAME = cfg.module;
const CREATE_FUNCTION = cfg.opCreateCounter;
const OPERATE_FUNCTION = cfg.opOperate;
const GLOBAL_STATE_ID = cfg.globalStateId;
const BATCH_SIZE = 100;
const MUTATIONS_PER_COUNTER = 10;
const FILL_TARGET_GB = cfg.fillTargetGB || 45;
const DISK_PATH = '/home/femu/f2fs_fdp_mount';

function getFreeDiskSpace(): number {
    const stats = fs.statfsSync(DISK_PATH);
    return (stats.bfree * stats.bsize) / (1024 * 1024 * 1024);
}

async function verifyObjectsExist(client: SuiClient, objectIds: string[]): Promise<boolean> {
    for (const id of objectIds) {
        try {
            await client.getObject({ id, options: { showType: true } });
        } catch (e) {
            console.error(`Object ${id} not found:`, e);
            return false;
        }
    }
    return true;
}

async function main() {
    const rpcUrl = cfg.rpcList[cfg.rpcIndex] || 'http://127.0.0.1:9000';
    const client = new SuiClient({ url: rpcUrl });

    const privateKey = process.env.SUI_PRIVATE_KEY;
    if (!privateKey) throw new Error('SUI_PRIVATE_KEY not found');
    const { secretKey } = decodeSuiPrivateKey(privateKey);
    const keypair = Ed25519Keypair.fromSecretKey(secretKey);

    console.log(`Starting SSD fill (target: ${FILL_TARGET_GB}GB used). Ctrl+C to stop.`);
    let createdCounters: string[] = [];
    let totalCreated = 0;
    let totalMutated = 0;

    while (true) {
        const freeGB = getFreeDiskSpace();
        console.log(`Current free space: ${freeGB.toFixed(2)}GB`);
        if (64 - freeGB >= FILL_TARGET_GB) {
            console.log(`Target reached. Stopping.`);
            break;
        }

        // Phase 1: Create batch
        try {
            const createTx = new Transaction();
            for (let i = 0; i < BATCH_SIZE; i++) {
                createTx.moveCall({
                    target: `${PACKAGE_ID}::${MODULE_NAME}::${CREATE_FUNCTION}`,
                    arguments: [createTx.object(GLOBAL_STATE_ID)],
                });
            }
            createTx.setGasBudget(500_000_000);

            const createResult = await client.signAndExecuteTransaction({
                signer: keypair,
                transaction: createTx,
                options: { showObjectChanges: true, showEffects: true }
            });

            if (createResult.effects?.status.status === 'success') {
                const newCounters = createResult.objectChanges
                    ?.filter(change => change.type === 'created' && 'objectType' in change && change.objectType.includes('Counter'))
                    .map(change => (change as any).objectId) || [];
                createdCounters.push(...newCounters);
                totalCreated += newCounters.length;
                console.log(`Created ${newCounters.length} counters (total: ${totalCreated})`);

                // NEW: Delay for sync
                await new Promise(r => setTimeout(r, 2000));  // 2s wait post-creation
            } else {
                console.error('Create failed:', createResult.effects?.status.error);
                await new Promise(r => setTimeout(r, 2000));
                continue;
            }
        } catch (e: any) {
            console.error('Create exception:', e.message);
            await new Promise(r => setTimeout(r, 2000));
            continue;
        }

        // Phase 2: Mutate in batches
        for (let i = 0; i < createdCounters.length; i += BATCH_SIZE) {
            const batch = createdCounters.slice(i, i + BATCH_SIZE);

            // NEW: Verify batch exists before mutating
            const exists = await verifyObjectsExist(client, batch);
            if (!exists) {
                console.warn(`Skipping mutation batch (objects not synced yet). Retrying in 2s...`);
                await new Promise(r => setTimeout(r, 2000));
                continue;  // Retry loop implicitly
            }

            const mutateTx = new Transaction();
            for (const counterId of batch) {
                for (let j = 0; j < MUTATIONS_PER_COUNTER; j++) {
                    mutateTx.moveCall({
                        target: `${PACKAGE_ID}::${MODULE_NAME}::${OPERATE_FUNCTION}`,
                        arguments: [mutateTx.object(counterId)],
                    });
                }
            }
            mutateTx.setGasBudget(500_000_000);

            try {
                const mutateResult = await client.signAndExecuteTransaction({ signer: keypair, transaction: mutateTx });
                if (mutateResult.effects?.status.status === 'success') {
                    totalMutated += batch.length * MUTATIONS_PER_COUNTER;
                    console.log(`Mutated ${batch.length} counters (${MUTATIONS_PER_COUNTER} times each, total ops: ${totalMutated})`);
                } else {
                    console.error('Mutate failed:', mutateResult.effects?.status.error);
                }
            } catch (e: any) {
                console.error('Mutate exception:', e.message);
            }

            // NEW: Inter-batch delay
            await new Promise(r => setTimeout(r, 1000));  // 1s between mutation batches
        }
    }

    console.log(`Done! Total created: ${totalCreated}, mutated: ${totalMutated}`);
}

main().catch(console.error);