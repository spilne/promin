// ---------------------------------------------------------------------------
// Hash tags shared by the stores that keep all their keys in one Redis
// Cluster slot (step queue, scheduler).
// ---------------------------------------------------------------------------

/**
 * The part of `key` Redis Cluster hashes: the text between the first `{`
 * and the next `}` when that is non-empty, else the whole key.
 */
export function hashTagOf(key: string): string {
  const open = key.indexOf("{");
  if (open !== -1) {
    const close = key.indexOf("}", open + 1);
    if (close > open + 1) return key.slice(open + 1, close);
  }
  return key;
}

/**
 * The base of every key of a store that keeps all its keys in one slot:
 * `{<prefix>}`. Two stores with different prefixes land in different slots.
 */
export function storeKeyBase(prefix: string): string {
  if (prefix === "") throw new Error("key prefix must not be empty");
  return `{${prefix}}`;
}
