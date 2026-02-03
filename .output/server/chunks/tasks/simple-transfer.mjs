import { e as executeTransaction } from '../_/executor.mjs';
import { d as defineLoggedTask } from '../_/logger.mjs';
import '@mysten/sui/client';
import '@mysten/sui/transactions';
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

const simpleTransfer = defineLoggedTask({
  meta: {
    name: "execute:simple-transfer",
    description: "Executes a simple transfer transaction"
  },
  async run() {
    const { digest, effects } = await executeTransaction("simple-transfer", (tx, sender) => {
      const [coin] = tx.splitCoins(tx.gas, [1]);
      tx.transferObjects([coin], sender.toSuiAddress());
    });
    return {
      result: {
        digest,
        effects
      }
    };
  },
  logResult: (result) => ({
    digest: result.digest
  })
});

export { simpleTransfer as default };
//# sourceMappingURL=simple-transfer.mjs.map
