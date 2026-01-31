// config.ts
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

// [FIX KEY POINT] Manually define __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ==========================================
// 1. Define type interfaces (based on config.json structure)
// ==========================================

export type NetworkType = 'mainnet' | 'testnet' | 'localnet' | 'devnet';

// On-chain object configuration details
export interface ChainObjectConfig {
  package: string;
  globalState: string;
  upgradeCap: string;
  // These may be at this level for localnet
  module?: string;
  opCreateCounter?: string;
  opOperate?: string;
}

// Fee configuration details
export interface FeeConfig {
  minSuiThreshold: number;
  splitAmountSui: number;
}

// Complete configuration file structure
export interface AppConfig {
  network: NetworkType;
  targetCount: number;
  startCounterIndex: number;
  rpcIndex: number;
  iters: number;
  iterInterval: number;
  duration: number;
  fillTargetGB: number;
  suiPerAccount: number;
  commandsPerPtb: number;
  concurrent: number;
  startTime: string;
  object: {
    // Common fields (may be at this level)
    module?: string;
    opCreateCounter?: string;
    opOperate?: string;
    // Network-specific configs
    mainnet?: ChainObjectConfig;
    testnet?: ChainObjectConfig;
    localnet?: ChainObjectConfig;
    devnet?: ChainObjectConfig;
  };
  fee: {
    mainnet?: FeeConfig;
    testnet?: FeeConfig;
    localnet?: FeeConfig;
    devnet?: FeeConfig;
  };
  counters: {
    mainnet?: string[];
    testnet?: string[];
    localnet?: string[];
    devnet?: string[];
  };
  rpcs: {
    mainnet?: string[];
    testnet?: string[];
    localnet?: string[];
    devnet?: string[];
  };
}

// ==========================================
// 2. Load and export configuration
// ==========================================

function loadConfig(): AppConfig {
  const configPath = path.join(__dirname, 'config.json');
  const rawConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  return rawConfig as AppConfig;
}

// Load config (will be re-read each time if you want hot-reload)
const config: AppConfig = loadConfig();

export default config;

// ==========================================
// 3. Helper function: Get current active network configuration
// ==========================================

/**
 * Flattened current network configuration structure
 * (Business code uses this directly without worrying about mainnet vs testnet)
 */
export interface ActiveConfig {
  network: NetworkType;
  targetCount: number;
  startCounterIndex: number;
  rpcIndex: number;
  iters: number;
  iterInterval: number;
  duration: number;
  fillTargetGB: number;
  suiPerAccount: number;
  commandsPerPtb: number;
  concurrent: number;
  startTime: string;
  module: string;
  opCreateCounter: string;
  opOperate: string;
  packageId: string;
  globalStateId: string;
  upgradeCapId: string;
  fee: FeeConfig;
  counterList: string[];
  rpcList: string[];
}

/**
 * Automatically assemble current environment configuration based on "network" field in config.json
 */
export function getActiveConfig(): ActiveConfig {
  // Reload config to get latest values
  const freshConfig = loadConfig();
  
  const currentNetwork = freshConfig.network;

  // Extract object configuration for corresponding network
  const objConfig = freshConfig.object[currentNetwork];
  
  if (!objConfig) {
    throw new Error(
      `No object configuration found for network "${currentNetwork}". ` +
      `Available networks: ${Object.keys(freshConfig.object).filter(k => 
        typeof freshConfig.object[k as keyof typeof freshConfig.object] === 'object'
      ).join(', ')}`
    );
  }

  // Get module/function names - check both at object level (common) and network level
  // This handles both the old structure (common at object level) and new structure (per-network)
  const module = freshConfig.object.module || objConfig.module;
  const opCreateCounter = freshConfig.object.opCreateCounter || objConfig.opCreateCounter;
  const opOperate = freshConfig.object.opOperate || objConfig.opOperate;

  if (!module) {
    throw new Error(`"module" not found in config. Check object.module or object.${currentNetwork}.module`);
  }
  if (!opCreateCounter) {
    throw new Error(`"opCreateCounter" not found in config.`);
  }
  if (!opOperate) {
    throw new Error(`"opOperate" not found in config.`);
  }

  // Get fee config with defaults
  const feeConfig = freshConfig.fee?.[currentNetwork] || { 
    minSuiThreshold: 0.03, 
    splitAmountSui: 0.04 
  };

  // Get counter list with default empty array
  const counterList = freshConfig.counters?.[currentNetwork] || [];

  // Get RPC list with default
  const rpcList = freshConfig.rpcs?.[currentNetwork] || ['http://127.0.0.1:9000'];

  return {
    network: currentNetwork,
    targetCount: freshConfig.targetCount,
    startCounterIndex: freshConfig.startCounterIndex,
    rpcIndex: freshConfig.rpcIndex,
    iters: freshConfig.iters,
    iterInterval: freshConfig.iterInterval,
    startTime: freshConfig.startTime,

    // Module and function names (from either level)
    module,
    opCreateCounter,
    opOperate,

    // Network-specific IDs
    packageId: objConfig.package,
    globalStateId: objConfig.globalState,
    upgradeCapId: objConfig.upgradeCap,

    // Network-specific fee configuration
    fee: feeConfig,

    // Network-specific Counter list
    counterList,

    // Network-specific RPC list
    rpcList,

    duration: freshConfig.duration || 1800, // Default 0 = use iters (backward compat)
    fillTargetGB: freshConfig.fillTargetGB || 45, // Default 0 = no fill target
    suiPerAccount: freshConfig.suiPerAccount || 600,
    commandsPerPtb: freshConfig.commandsPerPtb || 256,
    concurrent: freshConfig.concurrent || 500,
  };
}

// ==========================================
// 4. Debug helper
// ==========================================

export function debugConfig(): void {
  try {
    const cfg = getActiveConfig();
    console.log('🔍 Active Configuration:');
    console.log(`   Network:         ${cfg.network}`);
    console.log(`   Module:          ${cfg.module}`);
    console.log(`   Package ID:      ${cfg.packageId}`);
    console.log(`   GlobalState ID:  ${cfg.globalStateId}`);
    console.log(`   UpgradeCap ID:   ${cfg.upgradeCapId}`);
    console.log(`   opCreateCounter: ${cfg.opCreateCounter}`);
    console.log(`   opOperate:       ${cfg.opOperate}`);
    console.log(`   RPC List:        ${cfg.rpcList.join(', ')}`);
    console.log(`   Counters:        ${cfg.counterList.length} loaded`);
  } catch (e: any) {
    console.error('❌ Config Error:', e.message);
  }
}