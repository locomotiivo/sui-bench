import { Transaction } from '@mysten/sui/transactions';
import { suiClient, keypair } from './executor';
import { metrics } from './metrics';
import { logger } from './logger';

const BLOAT_PACKAGE_ID = process.env.BLOAT_PACKAGE_ID || '';
const BLOB_SIZE_KB = parseInt(process.env.BLOB_SIZE_KB || '100');
const BLOBS_PER_TX = parseInt(process.env.BLOBS_PER_TX || '20');
const BLOAT_STRATEGY = process.env.BLOAT_STRATEGY || 'blobs';
const TX_BATCH_SIZE = parseInt(process.env.TX_BATCH_SIZE || '50');

export async function executeBloatTransaction(strategy: string) {
	const tx = new Transaction();
	tx.setSender(keypair.toSuiAddress());
	tx.setGasBudget(100_000_000);

	switch (strategy) {
		case 'blobs':
			// Strategy 1: Large Move objects
			if (!BLOAT_PACKAGE_ID) {
				throw new Error('BLOAT_PACKAGE_ID not set');
			}
			tx.moveCall({
				target: `${BLOAT_PACKAGE_ID}::bloat::create_blobs_batch`,
				arguments: [tx.pure.u64(BLOB_SIZE_KB), tx.pure.u64(BLOBS_PER_TX)],
			});
			break;

		case 'coins':
			// Strategy 2: Massive coin splits
			const amounts = Array(255).fill(1_000_000); // 255 splits of 0.001 SUI
			const coins = tx.splitCoins(tx.gas, amounts);
			// Transfer all to self (creates owned objects)
			tx.transferObjects(coins, keypair.toSuiAddress());
			break;

		case 'churn':
			// Strategy 3: Create cycle (modify/delete requires tracking objects)
			if (!BLOAT_PACKAGE_ID) {
				throw new Error('BLOAT_PACKAGE_ID not set for churn strategy');
			}
			for (let i = 0; i < 10; i++) {
				tx.moveCall({
					target: `${BLOAT_PACKAGE_ID}::bloat::create_blob`,
					arguments: [tx.pure.u64(BLOB_SIZE_KB)],
				});
			}
			break;

		case 'varied':
			// Strategy 4: Varied sizes for fragmentation
			if (!BLOAT_PACKAGE_ID) {
				throw new Error('BLOAT_PACKAGE_ID not set for varied strategy');
			}
			tx.moveCall({
				target: `${BLOAT_PACKAGE_ID}::bloat::create_varied_blobs`,
				arguments: [tx.pure.u64(BLOB_SIZE_KB), tx.pure.u64(BLOBS_PER_TX)],
			});
			break;

		default:
			throw new Error(`Unknown strategy: ${strategy}`);
	}

	const result = await suiClient.signAndExecuteTransaction({
		transaction: tx,
		signer: keypair,
	});

	return result;
}

export async function runHeavyLoadBatch() {
	const promises = [];

	for (let i = 0; i < TX_BATCH_SIZE; i++) {
		promises.push(
			executeBloatTransaction(BLOAT_STRATEGY).catch((err) => {
				logger.error({ err }, 'Transaction failed');
				return null;
			}),
		);
	}

	const results = await Promise.all(promises);
	const successful = results.filter((r) => r !== null).length;

	metrics.getCounter('heavy-load:batch').add(1);
	metrics.getCounter('heavy-load:tx-success').add(successful);
	metrics.getCounter('heavy-load:tx-failed').add(TX_BATCH_SIZE - successful);

	return { successful, failed: TX_BATCH_SIZE - successful };
}
