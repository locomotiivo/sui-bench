import { getActiveConfig } from './.config';

const cfg = getActiveConfig();
// 1. Define string pool
const counterList = cfg.counterList;

/**
 * Randomly select N unique Counters from the list using address and time as seed
 * Returned list is also shuffled
 * @param address Account address (used as part of random seed)
 * @param n Number of items to retrieve
 * @returns {string[]} String array containing N Counters
 */
export function getRandomNCounters(address: string, n: number): string[] {
  // 1. Parameter validation
  if (n > counterList.length) {
    throw new Error(`Requested quantity N (${n}) exceeds list total length (${counterList.length})`);
  }
  if (n < 0) {
    throw new Error("Quantity N cannot be negative");
  }

  // 2. Clone original array (avoid modifying original data)
  const temp = [...counterList];

  // 3. Generate random seed (algorithm: address character code accumulation + current timestamp)
  let seed = Date.now();
  for (let i = 0; i < address.length; i++) {
    seed += address.charCodeAt(i);
  }

  // 4. Define seeded pseudo-random number generator (LCG algorithm)
  const seededRandom = () => {
    seed = (seed * 9301 + 49297) % 233280;
    return seed / 233280.0;
  };

  // 5. Execute Fisher-Yates shuffle algorithm
  // Shuffle entire list to ensure randomness
  for (let i = temp.length - 1; i > 0; i--) {
    const j = Math.floor(seededRandom() * (i + 1));

    // Swap elements (using ! non-null assertion to resolve TS strict checking issue)
    [temp[i], temp[j]] = [temp[j]!, temp[i]!];
  }

  // 6. Return first N elements
  return temp.slice(0, n);
}


/**
 * Get N Counters sequentially from the list, starting from startIndex.
 * If (startIndex + n) exceeds list length, automatically wraps back to the beginning.
 *
 * @param address Account address (reserved parameter for unified interface specification, not used in current sequential retrieval logic)
 * @param n Number of items to retrieve
 * @param startIndex Starting index position
 * @returns {string[]} String array containing N Counters
 */
export function getNCounters(address: string, n: number, startIndex: number): string[] {
  // 1. Validation: throw exception if list total quantity is less than n
  if (counterList.length < n) {
    throw new Error(`Requested quantity N (${n}) exceeds list total length (${counterList.length}), cannot fulfill request.`);
  }

  if (n < 0) {
    throw new Error("Quantity N cannot be negative");
  }

  const result: string[] = [];
  const len = counterList.length;

  // 2. Loop retrieval
  for (let i = 0; i < n; i++) {
    // Calculate current index:
    // (startIndex + i) % len implements circular queue logic
    // When index reaches len, modulo result returns to 0, achieving "wrap around to 0 after overflow"
    const currentIndex = (startIndex + i) % len;

    // Since counterList is a constant pool and length is validated, this definitely exists, can use ! assertion or direct access
    result.push(counterList[currentIndex]!);
  }

  return result;
}
