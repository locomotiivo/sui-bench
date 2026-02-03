import { s as suiBcs } from '../_/index.mjs';
import { C as COUNTER_PACKAGE_ID } from '../_/constants.mjs';
import { e as executeTransaction } from '../_/executor.mjs';
import { d as defineLoggedTask } from '../_/logger.mjs';
import '../_/sui-types.mjs';
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

const createSharedCounter = defineLoggedTask({
  meta: {
    name: "execute:create-shared-counter",
    description: "Creates a shared counter object"
  },
  async run() {
    const { digest, effects } = await executeTransaction("create-shared-counter", (tx, sender) => {
      tx.moveCall({
        package: COUNTER_PACKAGE_ID,
        module: "counter",
        function: "create"
      });
    });
    const parsedEffects = suiBcs.TransactionEffects.fromBase64(effects);
    const created = parsedEffects.V2.changedObjects.filter(([id, change]) => change.idOperation.Created).map(([id, change]) => {
      return {
        objectId: id,
        digest: change.outputState.ObjectWrite[0],
        version: parsedEffects.V2.lamportVersion
      };
    });
    return {
      result: {
        digest,
        effects,
        created
      }
    };
  },
  logResult: (result) => ({
    digest: result.digest
  })
});

export { createSharedCounter as default };
//# sourceMappingURL=create-shared-counter.mjs.map
