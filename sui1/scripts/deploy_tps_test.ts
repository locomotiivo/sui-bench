import { getFullnodeUrl, SuiClient } from '@mysten/sui/client';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import * as dotenv from 'dotenv';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { getActiveConfig } from './.config.ts';

dotenv.config();
const cfg = getActiveConfig();

// ================= Configuration Section =================

type SuiNetwork = 'mainnet' | 'testnet' | 'devnet' | 'localnet';
const NETWORK = (cfg.network || 'localnet') as SuiNetwork

// [FIX KEY POINT] Manually define __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ===========================================

/**
 * Extract JSON object from mixed output (handles build logs before JSON)
 */
function extractJson(output: string): any {
    const jsonStart = output.indexOf('{');
    if (jsonStart === -1) {
        throw new Error('No JSON object found in output');
    }
    const jsonString = output.slice(jsonStart);
    return JSON.parse(jsonString);
}

async function main() {
    console.log(">>> Initializing deployment script...");

    // 1. Calculate absolute path to Move project
    // Now __dirname can be used normally
    const ABSOLUTE_PROJECT_PATH = path.resolve(__dirname, '../tps_test/');
    const SUI_CONFIG_DIR = process.env.SUI_CONFIG_DIR || '/home/femu/sui_config';

    console.log(`📂 Project absolute path: ${ABSOLUTE_PROJECT_PATH}`);

    // Check if Move.toml actually exists to avoid confusing errors later
    const tomlPath = path.join(ABSOLUTE_PROJECT_PATH, 'Move.toml');
    if (!fs.existsSync(tomlPath)) {
        throw new Error(`Cannot find Move.toml file! Please check if path is correct: ${tomlPath}`);
    }

    const client = new SuiClient({ url: getFullnodeUrl(NETWORK) });

    const privateKey = process.env.SUI_PRIVATE_KEY;
    if (!privateKey) throw new Error('Please configure SUI_PRIVATE_KEY in .env file');

    const { secretKey } = decodeSuiPrivateKey(privateKey);
    const keypair = Ed25519Keypair.fromSecretKey(secretKey);
    const address = keypair.toSuiAddress();

    console.log(`👤 Deploying account: ${address}`);

    const balance = await client.getBalance({ owner: address });
    const balanceSui = Number(balance.totalBalance) / 1_000_000_000;
    console.log(`💰 Current balance: ${balanceSui.toFixed(4)} SUI`);

    // Clean up stale ephemeral publication files
    console.log('🧹 Cleaning up stale Pub.*.toml files...');
    const staleFiles = fs.readdirSync(ABSOLUTE_PROJECT_PATH)
        .filter(f => f.startsWith('Pub.') && f.endsWith('.toml'));
    
    staleFiles.forEach(file => {
        const filePath = path.join(ABSOLUTE_PROJECT_PATH, file);
        fs.unlinkSync(filePath);
        console.log(`   Deleted: ${file}`);
    });
    
    if (staleFiles.length === 0) {
        console.log('   No stale files found.');
    }

    console.log('>>> Initializing deployment script (test-publish mode)...');
    console.log(`📂 Project path: ${ABSOLUTE_PROJECT_PATH}`);
    
    try {
        // [CHANGE POINT 1] Remove --path parameter
        // Because we switch to that directory directly through cwd option below, just like manually cd ..
        const command= `SUI_CONFIG_DIR="${SUI_CONFIG_DIR}" sui client test-publish \
            --build-env localnet \
            --json \
            --gas-budget 50000000000`;

        console.log('🚀 Running test-publish...');
        console.log(`   Command: ${command}`);

        const result = execSync(command, {
            encoding: 'utf-8',
            cwd: ABSOLUTE_PROJECT_PATH
        });

        console.log('📄 Parsing transaction result...');
        const txResult = extractJson(result);

        // Check transaction status
        if (txResult.effects?.status?.status !== 'success') {
            throw new Error(`Transaction failed: ${JSON.stringify(txResult.effects?.status)}`);
        }
        const publishedPackage = txResult.objectChanges?.find(
            (change: any) => change.type === 'published'
        );

        if (publishedPackage) {
            console.log('✅ Deployment successful!');
            console.log(`📦 Package ID: ${publishedPackage.packageId}`);
            
            // Find created objects (GlobalState, UpgradeCap, etc.)
            const createdObjects = txResult.objectChanges?.filter(
                (change: any) => change.type === 'created'
            );
            
            console.log('\n📋 Created Objects:');
            createdObjects.forEach((obj: any) => {
                console.log(`   - ${obj.objectType}: ${obj.objectId}`);
            });
            
            const globalState = createdObjects.find(
                (obj: any) => obj.objectType && obj.objectType.includes('::GlobalState')
            );
            const upgradeCap = createdObjects.find(
                (obj: any) => obj.objectType && obj.objectType.includes('::UpgradeCap')
            );
            
            const result = {
                packageId: publishedPackage.packageId,
                globalStateId: globalState?.objectId || null,
                upgradeCapId: upgradeCap?.objectId || null,
                digest: txResult.digest,
            };
            
            // Log extracted values explicitly
            console.log('\n📍 Extracted IDs:');
            console.log(`   Package ID:    ${result.packageId}`);
            console.log(`   GlobalState:   ${result.globalStateId || '❌ NOT FOUND'}`);
            console.log(`   UpgradeCap:    ${result.upgradeCapId || '❌ NOT FOUND'}`);
            
            if (!result.globalStateId) {
                console.warn('\n⚠️  WARNING: GlobalState ID not found! Check objectChanges:');
                console.log(JSON.stringify(createdObjects, null, 2));
            }
            
            return result;
        } else {
            throw new Error('Package ID not found in transaction result');
        }

    } catch (e: any) {
        console.error('❌ Deployment failed!');
        if (e.stdout) console.log('STDOUT:', e.stdout);
        if (e.stderr) console.error('STDERR:', e.stderr);
        throw e;
    }
}

main()
    .then((result) => {
        console.log('\n🎉 Deployment complete!');
        console.log(JSON.stringify(result, null, 2));
        
        const configPath = path.join(__dirname, 'config.json');
        if (!fs.existsSync(configPath)) {
            console.error('❌ config.json not found at:', configPath);
            process.exit(1);
        }

        const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));

        if (!config.object) {
            config.object = {};
        }

        // Set localnet configuration
        config.object.localnet = {
            module: 'tps_test',
            opCreateCounter: 'create_counter',
            opOperate: 'operate',
            package: result.packageId,
            globalState: result.globalStateId,
            upgradeCap: result.upgradeCapId,
        };
        
        // Set network to localnet
        config.network = 'localnet';

        // Initialize counters array for localnet if it doesn't exist
        if (!config.counters) {
            config.counters = {};
        }
        if (!config.counters.localnet) {
            config.counters.localnet = [];
        }

        // Initialize fee config for localnet if it doesn't exist
        if (!config.fee) {
            config.fee = {};
        }
        if (!config.fee.localnet) {
            config.fee.localnet = {
                minSuiThreshold: 0.03,
                splitAmountSui: 0.04
            };
        }

        // Initialize rpcs for localnet if needed
        if (!config.rpcs) {
            config.rpcs = {};
        }
        if (!config.rpcs.localnet) {
            config.rpcs.localnet = ['http://127.0.0.1:9000'];
        }

        // Write updated config
        fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
        
        console.log('\n📝 Updated config.json:');
        console.log(`   network: "${config.network}"`);
        console.log(`   object.localnet.package: "${config.object.localnet.package}"`);
        console.log(`   object.localnet.globalState: "${config.object.localnet.globalState}"`);
        console.log(`   object.localnet.upgradeCap: "${config.object.localnet.upgradeCap}"`);
        
        // Verify the values were saved correctly
        if (!config.object.localnet.globalState) {
            console.error('\n❌ ERROR: globalState was not saved to config.json!');
            process.exit(1);
        }
        
        console.log('\n✅ Ready to run counter creation and benchmarks!');
    })
    .catch((err) => {
        console.error('\n❌ Fatal error:', err.message);
        process.exit(1);
    });