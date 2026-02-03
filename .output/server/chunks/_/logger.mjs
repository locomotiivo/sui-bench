import pino from 'pino';
import { resolve, join } from 'path';
import { mkdirSync } from 'fs';
import { d as defineTask } from '../runtime.mjs';

const logDir = resolve(process.cwd(), "logs");
mkdirSync(logDir, { recursive: true });
const transport = pino.transport({
  targets: [
    {
      level: "trace",
      target: "pino/file",
      options: {
        destination: join(logDir, "server.log")
      }
    },
    {
      level: "trace",
      target: "pino/file",
      options: {
        destination: 1
      }
    }
  ]
});
const logger = pino(transport);
function defineLoggedTask(task) {
  const child = logger.child({ task: task.meta.name });
  return defineTask({
    ...task,
    async run(...args) {
      const start = Date.now();
      child.info(`Running task ${task.meta.name}`);
      try {
        const result = await task.run(...args);
        child.info(
          {
            duration: Date.now() - start,
            ...task.logResult?.(result.result)
          },
          `Task ${task.meta.name} completed in ${Date.now() - start}ms`
        );
        return result;
      } catch (error) {
        child.error(
          {
            error,
            duration: Date.now() - start
          },
          `Error running task ${task.meta.name}`
        );
        throw error;
      }
    }
  });
}

export { defineLoggedTask as d, logger as l };
//# sourceMappingURL=logger.mjs.map
