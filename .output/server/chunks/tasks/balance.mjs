import { b as MIST_PER_SUI } from '../_/constants2.mjs';
import { k as keypair, s as suiClient, m as metrics } from '../_/executor.mjs';
import { d as defineTask } from '../runtime.mjs';
import '../_/sui-types.mjs';
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

const balance = defineTask({
  meta: {
    name: "report:balance",
    description: "Reports the SUI balance of the current account"
  },
  run: async () => {
    const address = keypair.toSuiAddress();
    const balance = await suiClient.getBalance({
      owner: address
    });
    metrics.setGauge("balance", Number(balance.totalBalance) / Number(MIST_PER_SUI));
    return {
      result: {
        address,
        balance: Number(balance.totalBalance) / Number(MIST_PER_SUI)
      }
    };
  }
});

export { balance as default };
//# sourceMappingURL=balance.mjs.map
