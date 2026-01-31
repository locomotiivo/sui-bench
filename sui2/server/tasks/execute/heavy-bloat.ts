import { runHeavyLoadBatch } from '~~/utils/heavy-executor';
import { defineLoggedTask } from '~~/utils/logger';

export default defineLoggedTask({
	meta: {
		name: 'execute:heavy-bloat',
		description: 'Executes heavy storage bloat transactions',
	},
	async run() {
		const result = await runHeavyLoadBatch();

		return {
			result: {
				successful: result.successful,
				failed: result.failed,
				rate: `${result.successful} tx/batch`,
			},
		};
	},
	logResult: (result) => ({
		successful: result.successful,
		failed: result.failed,
	}),
});
