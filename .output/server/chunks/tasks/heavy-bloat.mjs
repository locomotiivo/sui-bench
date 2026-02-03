import { Transaction } from '@mysten/sui/transactions';
import { m as metrics, k as keypair, s as suiClient } from '../_/executor.mjs';
import { l as logger, d as defineLoggedTask } from '../_/logger.mjs';
import '@mysten/sui/client';
import '@mysten/sui/keypairs/ed25519';
import '@mysten/sui/keypairs/secp256k1';
import '@mysten/sui/keypairs/secp256r1';
import '@mysten/sui/cryptography';
import '@opentelemetry/sdk-metrics';
import '@opentelemetry/exporter-prometheus';
import 'pino';
import 'path';
import 'fs';
import '../runtime.mjs';
import 'node:http';
import 'node:https';
import 'node:fs';
import 'node:url';
import 'std-env';

const BLOAT_PACKAGE_ID = process.env.BLOAT_PACKAGE_ID || "";
const BLOB_SIZE_KB = parseInt(process.env.BLOB_SIZE_KB || "100");
const BLOBS_PER_TX = parseInt(process.env.BLOBS_PER_TX || "20");
const BLOAT_STRATEGY = process.env.BLOAT_STRATEGY || "blobs";
const TX_BATCH_SIZE = parseInt(process.env.TX_BATCH_SIZE || "50");
async function executeBloatTransaction(strategy) {
  const tx = new Transaction();
  tx.setSender(keypair.toSuiAddress());
  tx.setGasBudget(1e8);
  switch (strategy) {
    case "blobs":
      if (!BLOAT_PACKAGE_ID) {
        throw new Error("BLOAT_PACKAGE_ID not set");
      }
      tx.moveCall({
        target: `${BLOAT_PACKAGE_ID}::bloat::create_blobs_batch`,
        arguments: [tx.pure.u64(BLOB_SIZE_KB), tx.pure.u64(BLOBS_PER_TX)]
      });
      break;
    case "coins":
      const amounts = Array(255).fill(1e6);
      const coins = tx.splitCoins(tx.gas, amounts);
      tx.transferObjects(coins, keypair.toSuiAddress());
      break;
    case "churn":
      if (!BLOAT_PACKAGE_ID) {
        throw new Error("BLOAT_PACKAGE_ID not set for churn strategy");
      }
      for (let i = 0; i < 10; i++) {
        tx.moveCall({
          target: `${BLOAT_PACKAGE_ID}::bloat::create_blob`,
          arguments: [tx.pure.u64(BLOB_SIZE_KB)]
        });
      }
      break;
    case "varied":
      if (!BLOAT_PACKAGE_ID) {
        throw new Error("BLOAT_PACKAGE_ID not set for varied strategy");
      }
      tx.moveCall({
        target: `${BLOAT_PACKAGE_ID}::bloat::create_varied_blobs`,
        arguments: [tx.pure.u64(BLOB_SIZE_KB), tx.pure.u64(BLOBS_PER_TX)]
      });
      break;
    default:
      throw new Error(`Unknown strategy: ${strategy}`);
  }
  const result = await suiClient.signAndExecuteTransaction({
    transaction: tx,
    signer: keypair
  });
  return result;
}
async function runHeavyLoadBatch() {
  const promises = [];
  for (let i = 0; i < TX_BATCH_SIZE; i++) {
    promises.push(
      executeBloatTransaction(BLOAT_STRATEGY).catch((err) => {
        logger.error({ err }, "Transaction failed");
        return null;
      })
    );
  }
  const results = await Promise.all(promises);
  const successful = results.filter((r) => r !== null).length;
  metrics.getCounter("heavy-load:batch").add(1);
  metrics.getCounter("heavy-load:tx-success").add(successful);
  metrics.getCounter("heavy-load:tx-failed").add(TX_BATCH_SIZE - successful);
  return { successful, failed: TX_BATCH_SIZE - successful };
}

const heavyBloat = defineLoggedTask({
  meta: {
    name: "execute:heavy-bloat",
    description: "Executes heavy storage bloat transactions"
  },
  async run() {
    const result = await runHeavyLoadBatch();
    return {
      result: {
        successful: result.successful,
        failed: result.failed,
        rate: `${result.successful} tx/batch`
      }
    };
  },
  logResult: (result) => ({
    successful: result.successful,
    failed: result.failed
  })
});

export { heavyBloat as default };
//# sourceMappingURL=heavy-bloat.mjs.map
