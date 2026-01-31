

import { getActiveConfig } from './.config.ts';
import { getFullnodeUrl, SuiClient } from '@mysten/sui/client';

import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const cfg = getActiveConfig();

// ================= Configuration Section =================

const GLOBAL_STATE_ID = cfg.globalStateId;
const NETWORK = cfg.network;

// [FIX KEY POINT] Manually define __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// =========================================

/**
 * Save fetched counter IDs to config.json
 */
function saveCountersToConfig(counterIds: string[]): void {
    const configPath = path.join(__dirname, 'config.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    
    // Ensure counters section exists
    if (!config.counters) {
        config.counters = {};
    }
    
    // Replace counters for this network (fresh fetch = authoritative source)
    config.counters[NETWORK] = counterIds;
    
    // Write back to file
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
    
    console.log(`\n💾 Saved ${counterIds.length} counters to config.json`);
    console.log(`   Network: ${NETWORK}`);
    console.log(`   Path: ${configPath}`);
}

async function main() {
    // 1. Initialize client - handle localnet specially
    let rpcUrl: string;
    if (NETWORK === 'localnet' || NETWORK === 'devnet') {
        rpcUrl = cfg.rpcList[cfg.rpcIndex] || 'http://127.0.0.1:9000';
    } else {
        rpcUrl = cfg.rpcList[cfg.rpcIndex] || `https://fullnode.${NETWORK}.sui.io:443`;
    }
    
    const client = new SuiClient({ url: rpcUrl });
    console.log(`🌐 Connecting to ${NETWORK} at ${rpcUrl}...`);

    try {
        // 2. Get GlobalState object to obtain registry (Table) ID and total_created
        console.log(`📖 Reading GlobalState: ${GLOBAL_STATE_ID}`);
        const globalStateObj = await client.getObject({
            id: GLOBAL_STATE_ID,
            options: { showContent: true }
        });

        if (!globalStateObj.data || !globalStateObj.data.content) {
            throw new Error("Cannot find GlobalState object or content is empty");
        }

        const fields = (globalStateObj.data.content as any).fields;

        // The field names here correspond to the struct definition in Move contract
        const totalCreated = Number(fields.total_created);
        const registryTableId = fields.registry.fields.id.id;

        console.log(`   Total Created: ${totalCreated}`);
        console.log(`   Registry Table ID: ${registryTableId}`);

        if (totalCreated === 0) {
            console.log("⚠️ No Counter objects have been created yet.");
            console.log("   Run create_new_counters.ts first to create some counters.");
            return;
        }

        // 3. Iterate through Table to get all Counter IDs
        console.log(`\n🔍 Fetching ${totalCreated} Counter IDs from Table...`);

        const counterIds: string[] = [];

        // Use concurrency control (10 per batch) to prevent rate limiting
        const batchSize = 10;
        for (let i = 0; i < totalCreated; i += batchSize) {
            const promises = [];
            for (let j = i; j < i + batchSize && j < totalCreated; j++) {
                promises.push(
                    client.getDynamicFieldObject({
                        parentId: registryTableId,
                        name: {
                            type: 'u64',
                            value: j.toString()
                        }
                    })
                );
            }

            const results = await Promise.all(promises);

            for (const res of results) {
                if (res.data && res.data.content) {
                    const content = res.data.content as any;
                    // The counter ID could be in different fields depending on the contract structure
                    const counterId = content.fields.value || content.fields.bytes || content.fields.id;
                    
                    if (typeof counterId === 'string') {
                        counterIds.push(counterId);
                    } else if (counterId && typeof counterId === 'object') {
                        // Handle nested ID structure
                        if (typeof counterId.id === 'string') {
                        counterIds.push(counterId.id);
                        } else if (typeof counterId.bytes === 'string') {
                            counterIds.push(counterId.bytes);
                        }
                    }
                }
            }
            
            // Progress indicator
            const progress = Math.min(i + batchSize, totalCreated);
            process.stdout.write(`\r   Progress: ${progress} / ${totalCreated}`);
        }

        console.log(''); // New line after progress

        console.log(`\n✅ Successfully fetched ${counterIds.length} Counter IDs`);

        // 4. Save to config.json
        if (counterIds.length > 0) {
            saveCountersToConfig(counterIds);
            
            // Print first few and last few for verification
            console.log(`\n📋 Counter IDs (showing first 3 and last 3):`);
            console.log(`   First 3:`);
            counterIds.slice(0, 3).forEach((id, i) => console.log(`     [${i}] ${id}`));
            if (counterIds.length > 6) {
                console.log(`     ...`);
            }
            console.log(`   Last 3:`);
            counterIds.slice(-3).forEach((id, i) => console.log(`     [${counterIds.length - 3 + i}] ${id}`));
        } else {
            console.log("⚠️ No counter IDs were found in the registry.");
        }

        // 5. Optional: Verify counters are valid by spot-checking a few
        if (counterIds.length > 0) {
            console.log(`\n🔬 Spot-checking first counter object...`);
            const spotCheck = await client.getObject({
                id: counterIds[0],
                options: { showContent: true, showType: true }
            });
            
            if (spotCheck.data) {
                console.log(`   ✅ Counter object exists and is accessible`);
                console.log(`   Type: ${spotCheck.data.type}`);
                if (spotCheck.data.content) {
                    const fields = (spotCheck.data.content as any).fields;
                    console.log(`   Value: ${fields?.value || 'N/A'}`);
                }
            } else {
                console.log(`   ⚠️ Could not verify counter object`);
                }
            }

        console.log(`\n🎉 Done! Config updated with ${counterIds.length} counters for ${NETWORK}`);

    } catch (e) {
        console.error("❌ Execution error:", e);
        process.exit(1);
    }
}

main();