//https://nitro.unjs.io/config
export default defineNitroConfig({
	srcDir: 'server',
	esbuild: {
		options: {
			target: 'es2020',
		},
	},
	experimental: {
		tasks: true,
	},
	scheduledTasks: {
		// HEAVY LOAD MODE - Execute bloat transaction every second
		'* * * * * *':
			process.env.HEAVY_LOAD === 'true'
				? ['execute:heavy-bloat']
				: ['execute:simple-transfer', 'execute:shared-counter'],
		// Run balance check every minute (less frequent in heavy mode)
		'* * * * *': ['report:balance'],
		// Run `report:ping` task every 10 seconds
		'*/10 * * * * *': ['report:ping'],
		// Smash coins every hour (disabled in heavy mode)
		'0 * * * *': process.env.HEAVY_LOAD === 'true' ? [] : ['execute:smash-coins'],
	},
});
