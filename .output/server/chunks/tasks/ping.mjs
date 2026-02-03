import { m as metrics, s as suiClient } from '../_/executor.mjs';
import { d as defineTask } from '../runtime.mjs';
import '@mysten/sui/client';
import '@mysten/sui/transactions';
import '@mysten/sui/keypairs/ed25519';
import '@mysten/sui/keypairs/secp256k1';
import '@mysten/sui/keypairs/secp256r1';
import '@mysten/sui/cryptography';
import '@opentelemetry/sdk-metrics';
import '@opentelemetry/exporter-prometheus';
import 'node:http';
import 'node:https';
import 'fs';
import 'path';
import 'node:fs';
import 'node:url';
import 'std-env';

const ping = defineTask({
  meta: {
    name: "report:ping",
    description: "Attempts to estimate network latency by calling sui_getReferenceGasPrice (a very cheap method)"
  },
  run: async () => {
    metrics.measureExecution("report:ping", async () => {
      await suiClient.getReferenceGasPrice();
    });
    return { result: {} };
  }
});

export { ping as default };
//# sourceMappingURL=ping.mjs.map
