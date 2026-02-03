import { SuiClient, getFullnodeUrl } from '@mysten/sui/client';
import { SerialTransactionExecutor, Transaction } from '@mysten/sui/transactions';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { Secp256k1Keypair } from '@mysten/sui/keypairs/secp256k1';
import { Secp256r1Keypair } from '@mysten/sui/keypairs/secp256r1';
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import { ConsoleMetricExporter, PeriodicExportingMetricReader, MeterProvider } from '@opentelemetry/sdk-metrics';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';

var __defProp = Object.defineProperty;
var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
var __publicField = (obj, key, value) => {
  __defNormalProp(obj, key + "" , value);
  return value;
};
var __accessCheck$1 = (obj, member, msg) => {
  if (!member.has(obj))
    throw TypeError("Cannot " + msg);
};
var __privateGet$1 = (obj, member, getter) => {
  __accessCheck$1(obj, member, "read from private field");
  return getter ? getter.call(obj) : member.get(obj);
};
var __privateAdd$1 = (obj, member, value) => {
  if (member.has(obj))
    throw TypeError("Cannot add the same private member more than once");
  member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
};
var _histograms, _counters, _gauges;
const TXN_HISTOGRAM_BUCKETS = [
  0,
  25,
  50,
  75,
  100,
  125,
  150,
  175,
  200,
  225,
  250,
  275,
  300,
  325,
  350,
  375,
  400,
  425,
  450,
  475,
  500,
  550,
  600,
  650,
  700,
  750,
  800,
  850,
  900,
  950,
  1e3,
  1250,
  1500,
  1750,
  2e3,
  3e3,
  5e3,
  1e4
];
const PING_HISTOGRAM_BUCKETS = [
  0,
  5,
  10,
  15,
  20,
  25,
  30,
  35,
  40,
  45,
  50,
  55,
  60,
  65,
  70,
  75,
  80,
  85,
  90,
  95,
  100,
  110,
  120,
  130,
  140,
  150,
  175,
  200,
  250,
  300,
  400,
  500
];
class Instrumentation {
  constructor(port = null) {
    __publicField(this, "meter");
    __privateAdd$1(this, _histograms, /* @__PURE__ */ new Map());
    __privateAdd$1(this, _counters, /* @__PURE__ */ new Map());
    __privateAdd$1(this, _gauges, /* @__PURE__ */ new Map());
    const exporter = port ? new PrometheusExporter({ port }, () => {
      console.log(`Prometheus scrape endpoint running on port ${port}`);
    }) : new ConsoleMetricExporter();
    const reader = exporter instanceof ConsoleMetricExporter ? new PeriodicExportingMetricReader({
      exporter,
      exportIntervalMillis: 1e4
    }) : exporter;
    const meterProvider = new MeterProvider({
      readers: [reader]
    });
    this.meter = meterProvider.getMeter("benchmark-meter");
  }
  getCounter(name) {
    if (!__privateGet$1(this, _counters).has(name)) {
      __privateGet$1(this, _counters).set(name, this.meter.createCounter(name));
    }
    return __privateGet$1(this, _counters).get(name);
  }
  setGauge(name, value) {
    if (!__privateGet$1(this, _gauges).has(name)) {
      const gauge = this.meter.createObservableGauge(name);
      gauge.addCallback((result) => {
        result.observe(__privateGet$1(this, _gauges).get(name));
      });
    }
    __privateGet$1(this, _gauges).set(name, value);
  }
  getHistogram(name, buckets) {
    if (!__privateGet$1(this, _histograms).has(name)) {
      __privateGet$1(this, _histograms).set(
        name,
        this.meter.createHistogram(name, {
          advice: { explicitBucketBoundaries: buckets }
        })
      );
    }
    return __privateGet$1(this, _histograms).get(name);
  }
  async measureExecution(name, callback) {
    const counter = this.getCounter(`${name}:calls`);
    const success_counter = this.getCounter(`${name}:success`);
    const error_counter = this.getCounter(`${name}:errors`);
    const buckets = name === "report:ping" ? PING_HISTOGRAM_BUCKETS : TXN_HISTOGRAM_BUCKETS;
    const histogram = this.getHistogram(`${name}:duration`, buckets);
    counter.add(0);
    success_counter.add(0);
    error_counter.add(0);
    const start = process.hrtime.bigint();
    counter.add(1);
    try {
      const result = await callback();
      success_counter.add(1);
      const end = process.hrtime.bigint();
      const duration = Number(end - start) / 1e6;
      histogram.record(duration);
      this.setGauge(`${name}:duration`, duration);
      return result;
    } catch (e) {
      error_counter.add(1);
      throw e;
    }
  }
}
_histograms = new WeakMap();
_counters = new WeakMap();
_gauges = new WeakMap();
const metrics = new Instrumentation(
  process.env.PROMETHEUS_PORT ? Number(process.env.PROMETHEUS_PORT) : null
);

var __accessCheck = (obj, member, msg) => {
  if (!member.has(obj))
    throw TypeError("Cannot " + msg);
};
var __privateGet = (obj, member, getter) => {
  __accessCheck(obj, member, "read from private field");
  return getter ? getter.call(obj) : member.get(obj);
};
var __privateAdd = (obj, member, value) => {
  if (member.has(obj))
    throw TypeError("Cannot add the same private member more than once");
  member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
};
var _queue;
class SerialQueue {
  constructor() {
    __privateAdd(this, _queue, []);
  }
  async runTask(task) {
    return new Promise((resolve, reject) => {
      __privateGet(this, _queue).push(() => {
        task().finally(() => {
          __privateGet(this, _queue).shift();
          if (__privateGet(this, _queue).length > 0) {
            __privateGet(this, _queue)[0]();
          }
        }).then(resolve, reject);
      });
      if (__privateGet(this, _queue).length === 1) {
        __privateGet(this, _queue)[0]();
      }
    });
  }
}
_queue = new WeakMap();

const suiClient = new SuiClient({
  url: process.env.SUI_JSON_RPC_URL ?? getFullnodeUrl("testnet")
});
const keypair = fromExportedKeypair(process.env.SUI_PRIVATE_KEY);
const serialExecutor = new SerialTransactionExecutor({
  client: suiClient,
  signer: keypair
});
let gasPrice = null;
async function getGasPrice() {
  if (!gasPrice) {
    gasPrice = await suiClient.getReferenceGasPrice();
    console.log("fetched gasPrice", gasPrice);
  }
  return gasPrice;
}
const queue = new SerialQueue();
function executeTransaction(name, defineTransaction) {
  return queue.runTask(async () => {
    const transaction = await metrics.measureExecution(`build:${name}`, async () => {
      const tx = new Transaction();
      tx.setSenderIfNotSet(keypair.toSuiAddress());
      tx.setGasPrice(await getGasPrice());
      tx.setGasBudget(50000000n);
      await defineTransaction(tx, keypair);
      return tx;
    });
    return metrics.measureExecution(`execute:${name}`, async () => {
      try {
        return await serialExecutor.executeTransaction(transaction);
      } catch (error) {
        console.log("error, clearing gasPrice");
        gasPrice = null;
        throw error;
      }
    });
  });
}
function fromExportedKeypair(secret) {
  const decoded = decodeSuiPrivateKey(secret);
  const schema = decoded.schema;
  const secretKey = decoded.secretKey;
  switch (schema) {
    case "ED25519":
      return Ed25519Keypair.fromSecretKey(secretKey);
    case "Secp256k1":
      return Secp256k1Keypair.fromSecretKey(secretKey);
    case "Secp256r1":
      return Secp256r1Keypair.fromSecretKey(secretKey);
    default:
      throw new Error(`Invalid keypair schema ${schema}`);
  }
}

export { serialExecutor as a, executeTransaction as e, keypair as k, metrics as m, queue as q, suiClient as s };
//# sourceMappingURL=executor.mjs.map
