import { a as normalizeSuiObjectId } from './sui-types.mjs';

const MIST_PER_SUI = BigInt(1e9);
const MOVE_STDLIB_ADDRESS = "0x1";
const SUI_FRAMEWORK_ADDRESS = "0x2";
normalizeSuiObjectId("0x6");
const SUI_TYPE_ARG = `${SUI_FRAMEWORK_ADDRESS}::sui::SUI`;
normalizeSuiObjectId("0x5");
normalizeSuiObjectId("0x8");

export { MOVE_STDLIB_ADDRESS as M, SUI_FRAMEWORK_ADDRESS as S, SUI_TYPE_ARG as a, MIST_PER_SUI as b };
//# sourceMappingURL=constants2.mjs.map
