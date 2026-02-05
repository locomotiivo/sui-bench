/**
 * Publish the bloat_storage Move package
 */

import { SuiClient } from '@mysten/sui/client';
import { Transaction } from '@mysten/sui/transactions';
import { initSuiContext, executeTransaction } from './sui-utils.js';
import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';

const MOVE_DIR = path.resolve(process.cwd(), '../move/bloat_storage');
const PACKAGE_ID_FILE = path.resolve(process.cwd(), '.package_id');

export async function publishPackage(
  rpcUrl: string,
  keystorePath: string
): Promise<string> {
  console.log('[publish] Initializing Sui context...');
  const ctx = await initSuiContext(rpcUrl, keystorePath);
  
  // Check if already published
  if (fs.existsSync(PACKAGE_ID_FILE)) {
    const existingId = fs.readFileSync(PACKAGE_ID_FILE, 'utf-8').trim();
    console.log(`[publish] Package already published: ${existingId}`);
    
    // Verify it exists on chain
    try {
      await ctx.client.getObject({ id: existingId });
      return existingId;
    } catch {
      console.log('[publish] Existing package not found on chain, republishing...');
    }
  }
  
  console.log('[publish] Building Move package...');
  
  // Build the package
  try {
    execSync('sui move build', { 
      cwd: MOVE_DIR, 
      stdio: 'inherit',
      env: { ...process.env, SUI_CONFIG_DIR: path.dirname(keystorePath) }
    });
  } catch (e) {
    console.error('[publish] Build failed:', e);
    throw e;
  }
  
  // Read compiled modules
  const buildDir = path.join(MOVE_DIR, 'build/bloat_storage/bytecode_modules');
  const moduleFiles = fs.readdirSync(buildDir).filter(f => f.endsWith('.mv') && !f.includes('dependencies'));
  
  if (moduleFiles.length === 0) {
    throw new Error('No compiled modules found');
  }
  
  console.log(`[publish] Found modules: ${moduleFiles.join(', ')}`);
  
  // Read module bytecode
  const modules = moduleFiles.map(f => {
    const content = fs.readFileSync(path.join(buildDir, f));
    return Array.from(content);
  });
  
  // Read dependencies
  const dependencyIds: string[] = [];  // For local network, usually empty
  
  console.log('[publish] Publishing package...');
  
  const tx = new Transaction();
  
  // Publish - convert Uint8Array to number[][] as required by SDK
  const [upgradeCap] = tx.publish({
    modules: modules.map(m => Array.from(Uint8Array.from(m))),
    dependencies: dependencyIds,
  });
  
  // Transfer upgrade cap to sender
  tx.transferObjects([upgradeCap], tx.pure.address(ctx.address));
  
  // Set gas budget high for publish
  tx.setGasBudget(500_000_000);
  
  const result = await executeTransaction(ctx, tx, true);
  
  // Find published package ID
  const publishedPackage = result.objectChanges?.find(
    c => c.type === 'published'
  );
  
  if (!publishedPackage || !('packageId' in publishedPackage)) {
    console.error('Object changes:', JSON.stringify(result.objectChanges, null, 2));
    throw new Error('Could not find published package ID');
  }
  
  const packageId = publishedPackage.packageId;
  console.log(`[publish] ✓ Package published: ${packageId}`);
  
  // Save package ID
  fs.writeFileSync(PACKAGE_ID_FILE, packageId);
  
  return packageId;
}

// Run if called directly
if (process.argv[1].includes('publish')) {
  const configDir = process.env.SUI_CONFIG_DIR || process.argv[2];
  if (!configDir) {
    console.error('Usage: SUI_CONFIG_DIR=/path/to/config tsx src/publish.ts');
    console.error('   or: tsx src/publish.ts /path/to/config');
    process.exit(1);
  }
  
  const keystorePath = path.join(configDir, 'sui.keystore');
  
  // Auto-detect RPC
  const files = fs.readdirSync(configDir);
  const validatorConfig = files.find(f => f.match(/^127\.0\.0\.1-\d+\.yaml$/));
  let rpcUrl = 'http://127.0.0.1:9000';
  
  if (validatorConfig) {
    const content = fs.readFileSync(path.join(configDir, validatorConfig), 'utf-8');
    const match = content.match(/json-rpc-address:\s*"([^"]+)"/);
    if (match) {
      rpcUrl = `http://${match[1]}`;
    }
  }
  
  publishPackage(rpcUrl, keystorePath)
    .then(id => {
      console.log(`\nPackage ID: ${id}`);
      process.exit(0);
    })
    .catch(e => {
      console.error('Publish failed:', e);
      process.exit(1);
    });
}
