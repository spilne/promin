// ---------------------------------------------------------------------------
// Hash tags and the one-off move of untagged keys, shared by the stores whose
// keys moved under a hash tag (step queue, scheduler, state machines).
// ---------------------------------------------------------------------------

import type { RedisStoreClient } from "./redis-client.ts";
import { escapeGlob } from "./redis-workflow-keys.ts";

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

const RENAME_LUA = `return redis.call('RENAME', KEYS[1], KEYS[2])`;
const BATCH = 100;

/**
 * Rename the untagged keys of a store. Every key under `<prefix>:` is
 * offered to `target` with the prefix and colon stripped; the key moves to
 * the name it returns, or stays when it returns null. Keys already in the
 * tagged layout must map to null. RENAME across slots needs a standalone
 * Redis; run it with every worker stopped. Returns the keys renamed.
 */
export async function renameLegacyKeys(params: {
  redis: RedisStoreClient;
  prefix: string;
  scanCount: number;
  target: (rest: string) => string | null;
}): Promise<{ renamed: Array<[from: string, to: string]> }> {
  const { redis, prefix } = params;
  const renamed: Array<[string, string]> = [];
  let cursor = "0";
  do {
    const [next, found] = await redis.scan(
      cursor,
      "MATCH",
      `${escapeGlob(prefix)}:*`,
      "COUNT",
      params.scanCount,
    );
    for (const key of found) {
      const to = params.target(key.slice(prefix.length + 1));
      if (to !== null && to !== key) renamed.push([key, to]);
    }
    cursor = next;
  } while (cursor !== "0");

  // A key SCAN reported twice is renamed once.
  const unique = [...new Map(renamed).entries()];
  for (let i = 0; i < unique.length; i += BATCH) {
    await Promise.all(
      unique.slice(i, i + BATCH).map(([from, to]) => redis.eval(RENAME_LUA, 2, from, to)),
    );
  }
  return { renamed: unique };
}
