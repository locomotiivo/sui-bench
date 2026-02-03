import { O as OWNED_COUNTER_ID, C as COUNTER_PACKAGE_ID } from '../_/constants.mjs';
import { e as executeTransaction, s as suiClient } from '../_/executor.mjs';
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

const ownedCounter = defineLoggedTask({
  meta: {
    name: "execute:owned-counter",
    description: "Executes a transaction to increment an owned counter"
  },
  async run() {
    const { digest, effects } = await executeTransaction("shared-counter", (tx) => {
      tx.moveCall({
        package: COUNTER_PACKAGE_ID,
        module: "counter",
        function: "increment",
        arguments: [tx.object(OWNED_COUNTER_ID)]
      });
    });
    await suiClient.waitForTransaction({ digest });
    const counter = await suiClient.getObject({
      id: OWNED_COUNTER_ID,
      options: { showContent: true }
    });
    return {
      result: {
        digest,
        effects,
        counter: counter.data.content
      }
    };
  },
  logResult: (result) => ({
    digest: result.digest
  })
});

export { ownedCounter as default };
//# sourceMappingURL=owned-counter.mjs.map
