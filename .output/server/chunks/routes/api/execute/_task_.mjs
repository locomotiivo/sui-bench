import { e as eventHandler, g as getQuery, a as getRouterParam, r as runTask } from '../../../runtime.mjs';
import 'node:http';
import 'node:https';
import 'fs';
import 'path';
import 'node:fs';
import 'node:url';
import 'std-env';

const _task_ = eventHandler(async (event) => {
  const payload = { ...getQuery(event) };
  const task = getRouterParam(event, "task");
  let taskResult;
  switch (task) {
    case "simple-transfer":
      taskResult = await runTask("execute:simple-transfer", { payload });
      break;
    case "owned-counter":
      taskResult = await runTask("execute:owned-counter", { payload });
      break;
    case "shared-counter":
      taskResult = await runTask("execute:shared-counter", { payload });
      break;
    case "smash-coins":
      taskResult = await runTask("execute:smash-coins", { payload });
      break;
    default:
      throw new Error(`Task ${task} not found`);
  }
  return taskResult.result;
});

export { _task_ as default };
//# sourceMappingURL=_task_.mjs.map
