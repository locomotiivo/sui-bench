import { e as eventHandler, r as runTask, g as getQuery } from '../../runtime.mjs';
import 'node:http';
import 'node:https';
import 'fs';
import 'path';
import 'node:fs';
import 'node:url';
import 'std-env';

const balance = eventHandler(async (event) => {
  const { result } = await runTask("report:balance", { payload: { ...getQuery(event) } });
  return result;
});

export { balance as default };
//# sourceMappingURL=balance.mjs.map
