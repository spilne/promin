// ---------------------------------------------------------------------------
// One-off move of RedisWorkflowStorage keys from the untagged layout to the
// hash-tagged one (see `redis-workflow-keys.ts`).
// ---------------------------------------------------------------------------
//
// The untagged layout was:
//
//   <prefix>:<id>                     workflow hash
//   <prefix>:<id>:<suffix>            steps, tasks, signals, journal, streams ...
//   <prefix>:lock:<id>                lock
//   <prefix>:lock-fence-counter       global fence counter
//   <prefix>:idx:status|name|children|completed|distinct:...
//   <prefix>:sleeps
//
// Per-workflow keys are renamed under `{wf:<id>}`; the indexes are rebuilt
// from the moved workflow hashes (status, name, parent, namespace sets and
// the ordering sorted sets); the sleep schedule and the distinct-value sets
// are copied; the old index keys, the global fence counter and the old
// sleep schedule are deleted. RENAME across slots needs a standalone Redis.
// ---------------------------------------------------------------------------

import type { RedisStoreClient } from "./redis-client.ts";
import { INDEX_HASH_TAG, escapeGlob, type RedisWorkflowKeys } from "./redis-workflow-keys.ts";

const LEGACY_STATUSES = [
  "pending",
  "running",
  "suspended",
  "compensating",
  "completed",
  "failed",
  "tripwire",
] as const;

/** Suffixes that follow `<prefix>:<id>` in a legacy per-workflow key. */
const LEGACY_SUFFIX =
  /^:(steps:|tasks:|signals$|signal_tokens$|signal_token_idempotency$|runs$|attempts$|stream-ids$|streams:|journal:)/;

const RENAME_LUA = `return redis.call('RENAME', KEYS[1], KEYS[2])`;
const MARK_TRACKED_LUA = `
redis.call('HSETNX', KEYS[1], 'iv', '1')
redis.call('HSET', KEYS[1], 'streamsTracked', '1')
return 1
`;
const BATCH = 100;

export async function migrateLegacyWorkflowKeys(params: {
  redis: RedisStoreClient;
  keys: RedisWorkflowKeys;
  scanCount: number;
  /** Rebuild the index entries of one (moved) workflow from its hash. */
  reindex: (workflowId: string) => Promise<void>;
}): Promise<{ workflows: number; keys: number }> {
  const { redis, keys } = params;
  const p = keys.prefix;
  const legacy = (suffix: string) => `${p}:${suffix}`;

  // Legacy workflow ids are the members of the legacy status sets.
  const idSets = await Promise.all(
    LEGACY_STATUSES.map((s) => redis.smembers(legacy(`idx:status:${s}`))),
  );
  const ids = new Set(idSets.flat());

  // One pass over the prefix's keys, skipping the new layout.
  const scanned: string[] = [];
  let cursor = "0";
  do {
    const [next, found] = await redis.scan(
      cursor,
      "MATCH",
      `${escapeGlob(p)}:*`,
      "COUNT",
      params.scanCount,
    );
    for (const key of found) {
      if (key.startsWith(`${p}:{`)) continue;
      scanned.push(key);
    }
    cursor = next;
  } while (cursor !== "0");

  // Classify per-workflow keys. A key belongs to the longest id it starts
  // with whose remainder is a known per-workflow suffix.
  const renames: Array<[string, string]> = [];
  const streams = new Map<string, string[]>();
  for (const key of scanned) {
    const rest = key.slice(p.length + 1);
    if (ids.has(rest)) {
      renames.push([key, keys.wf(rest)]);
      continue;
    }
    if (rest.startsWith("lock:") && ids.has(rest.slice(5))) {
      renames.push([key, keys.lock(rest.slice(5))]);
      continue;
    }
    for (let at = rest.lastIndexOf(":"); at > 0; at = rest.lastIndexOf(":", at - 1)) {
      const id = rest.slice(0, at);
      const suffix = rest.slice(at);
      if (!ids.has(id) || !LEGACY_SUFFIX.test(suffix)) continue;
      renames.push([key, `${keys.wf(id)}${suffix}`]);
      if (suffix.startsWith(":streams:")) {
        const list = streams.get(id) ?? [];
        list.push(suffix.slice(":streams:".length));
        streams.set(id, list);
      }
      break;
    }
  }

  for (let i = 0; i < renames.length; i += BATCH) {
    await Promise.all(
      renames.slice(i, i + BATCH).map(([from, to]) => redis.eval(RENAME_LUA, 2, from, to)),
    );
  }

  // Register every stream (also the ones appended before stream ids were
  // tracked), mark the rows, and index them.
  const migrated = [...ids];
  for (let i = 0; i < migrated.length; i += BATCH) {
    await Promise.all(
      migrated.slice(i, i + BATCH).map(async (id) => {
        if (!(await redis.exists(keys.wf(id)))) return;
        const streamIds = streams.get(id);
        if (streamIds?.length) await redis.sadd(keys.streamIds(id), ...streamIds);
        await redis.eval(MARK_TRACKED_LUA, 1, keys.wf(id));
        await params.reindex(id);
      }),
    );
  }

  // The sleep schedule keeps its member format.
  const sleeps = await redis.zrangebyscore(
    legacy("sleeps"),
    "-inf",
    "+inf",
    "WITHSCORES",
    "LIMIT",
    0,
    -1,
  );
  for (let i = 0; i < sleeps.length; i += 2 * BATCH) {
    const args: string[] = [];
    for (let j = i; j < Math.min(sleeps.length, i + 2 * BATCH); j += 2) {
      args.push(sleeps[j + 1]!, sleeps[j]!);
    }
    await redis.zadd(keys.sleeps, ...args);
  }

  // Distinct-value sets carry over (they outlive purged rows); the other
  // legacy indexes were rebuilt above.
  const distinctPrefix = legacy("idx:distinct:");
  const legacyIndexKeys: string[] = [legacy("sleeps"), legacy("lock-fence-counter")];
  for (const key of scanned) {
    if (key.startsWith(distinctPrefix)) {
      const members = await redis.smembers(key);
      const target = `${p}:${INDEX_HASH_TAG}:distinct:${key.slice(distinctPrefix.length)}`;
      if (members.length > 0) await redis.sadd(target, ...members);
      legacyIndexKeys.push(key);
    } else if (key.startsWith(legacy("idx:"))) {
      legacyIndexKeys.push(key);
    }
  }
  await Promise.all(legacyIndexKeys.map((key) => redis.del(key)));

  return { workflows: ids.size, keys: renames.length };
}
