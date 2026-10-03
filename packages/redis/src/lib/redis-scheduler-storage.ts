// ---------------------------------------------------------------------------
// RedisSchedulerStorage — Redis adapter for SchedulerStorage.
//
// Heavy lifting (cron/rrule/catch-up/jitter/leader loop) lives in the generic
// DurableScheduler shell in @promin/workflow. This file is just the storage
// adapter — schedule CRUD via hashes, due-row lookup via per-namespace ZSETs,
// and SET NX PX-based leader election.
//
// Key layout (with namespace `ns` — global namespace = "_"):
//   {prefix}:ns:{ns}:all          — SET of schedule IDs in that namespace
//   {prefix}:ns:{ns}:due          — ZSET, score=nextRunMs, member=id (per ns!)
//   {prefix}:ns:{ns}:leader       — STRING with TTL, holds instanceId of leader
//   {prefix}:schedule:{id}        — HASH with config + state (namespace-tagged)
// ---------------------------------------------------------------------------

import {
  scheduleMetadataContains,
  SystemWallClock,
  type DurableScheduleConfig,
  type ScheduleCommit,
  type SchedulerStorage,
  type WallClock,
} from "@promin/workflow";
import type { RedisStoreClient } from "./redis-client.ts";

export interface RedisSchedulerStorageConfig {
  redis: RedisStoreClient;
  /** Key prefix for all scheduler keys. Default: "sched". */
  prefix?: string;
  /** Time source for the initial next-run of new schedules. Default: `SystemWallClock`. */
  clock?: WallClock;
}

const GLOBAL_NS = "_";

/**
 * Insert or replace a schedule hash atomically.
 *
 * - Fire state (`lastFiredAt`, `tickCount`) survives a replace.
 * - A namespace change moves the id between the per-namespace `all` sets and
 *   carries any pending next-run over to the new due set.
 * - Due-tracking follows the enabled flag: a disabled schedule is removed
 *   from the due set; an enabled one keeps its pending next-run, or is
 *   seeded at ARGV[4] (now, or `startAt` when that is later) if it has none.
 *
 * KEYS: [schedule_key, all_key, due_key]
 * ARGV: [id, namespace_key_base, namespace, seed_ms, enabled('1'|'0'),
 *        global_ns, field1, value1, ...]
 */
const UPSERT_LUA = `
local schedule_key = KEYS[1]
local all_key = KEYS[2]
local due_key = KEYS[3]
local id = ARGV[1]
local ns_base = ARGV[2]
local ns = ARGV[3]
local seed_ms = ARGV[4]
local enabled = ARGV[5]
local global_ns = ARGV[6]

local existed = redis.call('HEXISTS', schedule_key, 'id') == 1
local last_fired = false
local tick_count = false
local old_ns = false
if existed then
  last_fired = redis.call('HGET', schedule_key, 'lastFiredAt')
  tick_count = redis.call('HGET', schedule_key, 'tickCount')
  old_ns = redis.call('HGET', schedule_key, 'namespace') or global_ns
end

local fields = {}
for i = 7, #ARGV do fields[#fields + 1] = ARGV[i] end
redis.call('DEL', schedule_key)
redis.call('HSET', schedule_key, unpack(fields))
if last_fired then redis.call('HSET', schedule_key, 'lastFiredAt', last_fired) end
if tick_count then redis.call('HSET', schedule_key, 'tickCount', tick_count) end
redis.call('SADD', all_key, id)

if existed and old_ns ~= ns then
  local old_due = ns_base .. old_ns .. ':due'
  redis.call('SREM', ns_base .. old_ns .. ':all', id)
  local score = redis.call('ZSCORE', old_due, id)
  if score then
    redis.call('ZREM', old_due, id)
    redis.call('ZADD', due_key, score, id)
  end
end

if enabled == '1' then
  redis.call('ZADD', due_key, 'NX', seed_ms, id)
else
  redis.call('ZREM', due_key, id)
end
return existed and 0 or 1
`;

/**
 * Set or clear one schedule's next run, honoring the enabled flag: a
 * missing or disabled schedule is removed from its due set instead.
 *
 * KEYS: [schedule_key]
 * ARGV: [id, namespace_key_base, global_ns, next_run_ms ('' = clear)]
 */
const SET_NEXT_RUN_LUA = `
local schedule_key = KEYS[1]
local id = ARGV[1]
local fields = redis.call('HMGET', schedule_key, 'id', 'namespace', 'enabled')
local ns = fields[2] or ARGV[3]
local due_key = ARGV[2] .. ns .. ':due'
if not fields[1] or fields[3] == '0' or ARGV[4] == '' then
  redis.call('ZREM', due_key, id)
else
  redis.call('ZADD', due_key, ARGV[4], id)
end
return 1
`;

/**
 * Toggle a schedule's enabled flag. Disabling removes it from its due set;
 * enabling seeds it at ARGV[5] (now, or a later `startAt`) unless it already
 * has a pending next run. Unknown ids are left alone.
 *
 * KEYS: [schedule_key]
 * ARGV: [id, namespace_key_base, global_ns, enabled ('1'|'0'), now_ms]
 */
const SET_ENABLED_LUA = `
local schedule_key = KEYS[1]
local id = ARGV[1]
local fields = redis.call('HMGET', schedule_key, 'id', 'namespace', 'startAt')
if not fields[1] then return 0 end
local ns = fields[2] or ARGV[3]
local due_key = ARGV[2] .. ns .. ':due'
redis.call('HSET', schedule_key, 'enabled', ARGV[4])
if ARGV[4] == '1' then
  local seed = tonumber(ARGV[5])
  if fields[3] and tonumber(fields[3]) > seed then seed = tonumber(fields[3]) end
  redis.call('ZADD', due_key, 'NX', seed, id)
else
  redis.call('ZREM', due_key, id)
end
return 1
`;

/**
 * Due ids from one namespace's due set, skipping (and pruning) entries
 * whose schedule is missing or disabled so they can't fill the limit.
 *
 * KEYS: [due_key]
 * ARGV: [schedule_key_prefix, now_ms, limit]
 */
const FIND_DUE_LUA = `
local due_key = KEYS[1]
local prefix = ARGV[1]
local now_ms = ARGV[2]
local limit = tonumber(ARGV[3])
local out = {}
local stale = {}
local offset = 0
while #out < limit do
  local ids = redis.call('ZRANGEBYSCORE', due_key, '-inf', now_ms, 'LIMIT', offset, limit)
  if #ids == 0 then break end
  for _, id in ipairs(ids) do
    local fields = redis.call('HMGET', prefix .. id, 'id', 'enabled')
    if fields[1] and fields[2] ~= '0' then
      if #out < limit then out[#out + 1] = id end
    else
      stale[#stale + 1] = id
    end
  end
  offset = offset + #ids
end
for _, id in ipairs(stale) do redis.call('ZREM', due_key, id) end
return out
`;

export class RedisSchedulerStorage implements SchedulerStorage {
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;
  private readonly clock: WallClock;

  constructor(config: RedisSchedulerStorageConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "sched";
    this.clock = config.clock ?? SystemWallClock;
  }

  // -------------------------------------------------------------------------
  // Key helpers — every per-namespace key is prefixed with `:ns:{namespace}:`
  // so multi-tenant deployments don't share due-sets or leader locks.
  // -------------------------------------------------------------------------

  private nsKey(ns: string | undefined): string {
    return ns ?? GLOBAL_NS;
  }

  private allKey(ns: string | undefined): string {
    return `${this.prefix}:ns:${this.nsKey(ns)}:all`;
  }

  private dueKey(ns: string | undefined): string {
    return `${this.prefix}:ns:${this.nsKey(ns)}:due`;
  }

  private leaderKey(ns: string | undefined): string {
    return `${this.prefix}:ns:${this.nsKey(ns)}:leader`;
  }

  private scheduleKey(id: string): string {
    return `${this.prefix}:schedule:${id}`;
  }

  // -------------------------------------------------------------------------
  // Hot path
  // -------------------------------------------------------------------------

  async findDue(params: { now: Date; limit: number; namespace?: string }): Promise<string[]> {
    return await this.findDueIn({ dueKey: this.dueKey(params.namespace), ...params });
  }

  /** Enabled due ids from one due set; prunes disabled or deleted leftovers. */
  private async findDueIn(params: { dueKey: string; now: Date; limit: number }): Promise<string[]> {
    if (params.limit <= 0) return [];
    const ids = await this.redis.eval(
      FIND_DUE_LUA,
      1,
      params.dueKey,
      `${this.prefix}:schedule:`,
      params.now.getTime(),
      params.limit,
    );
    return (ids as string[] | null) ?? [];
  }

  async loadSchedule(id: string): Promise<DurableScheduleConfig | null> {
    const raw = await this.redis.hgetall(this.scheduleKey(id));
    if (!raw || !raw.id) return null;
    return rawToConfig(raw);
  }

  async loadSchedules(ids: string[]): Promise<Map<string, DurableScheduleConfig>> {
    const out = new Map<string, DurableScheduleConfig>();
    if (ids.length === 0) return out;
    // No MGET equivalent for hashes — fan out HGETALL calls. The redis client
    // pipelines these under the hood when called concurrently on the same
    // connection, so this is one round-trip in practice.
    const raws = await Promise.all(ids.map((id) => this.redis.hgetall(this.scheduleKey(id))));
    for (let i = 0; i < ids.length; i++) {
      const raw = raws[i];
      if (raw && raw.id) out.set(ids[i]!, rawToConfig(raw));
    }
    return out;
  }

  async loadScheduleState(
    id: string,
  ): Promise<{ lastFired: Date | null; tickCount: number } | null> {
    const raw = await this.redis.hgetall(this.scheduleKey(id));
    if (!raw || !raw.id) return null;
    return {
      lastFired: raw.lastFiredAt ? new Date(Number(raw.lastFiredAt)) : null,
      tickCount: raw.tickCount ? Number(raw.tickCount) : 0,
    };
  }

  async loadScheduleStates(
    ids: string[],
  ): Promise<Map<string, { lastFired: Date | null; tickCount: number }>> {
    const out = new Map<string, { lastFired: Date | null; tickCount: number }>();
    if (ids.length === 0) return out;
    const raws = await Promise.all(ids.map((id) => this.redis.hgetall(this.scheduleKey(id))));
    for (let i = 0; i < ids.length; i++) {
      const raw = raws[i];
      if (raw && raw.id) {
        out.set(ids[i]!, {
          lastFired: raw.lastFiredAt ? new Date(Number(raw.lastFiredAt)) : null,
          tickCount: raw.tickCount ? Number(raw.tickCount) : 0,
        });
      }
    }
    return out;
  }

  async recordFire(id: string, firedAt: Date, count: number = 1): Promise<void> {
    const current = await this.redis.hget(this.scheduleKey(id), "tickCount");
    const next = (current ? Number(current) : 0) + count;
    await this.redis.hset(this.scheduleKey(id), {
      lastFiredAt: String(firedAt.getTime()),
      tickCount: String(next),
    });
  }

  async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    // The script looks up the schedule's namespace (which due-ZSET) and
    // keeps disabled or deleted schedules out of due-tracking.
    await this.redis.eval(
      SET_NEXT_RUN_LUA,
      1,
      this.scheduleKey(id),
      id,
      `${this.prefix}:ns:`,
      GLOBAL_NS,
      nextRun === null ? "" : nextRun.getTime(),
    );
  }

  async commitPoll(updates: ScheduleCommit[]): Promise<void> {
    if (updates.length === 0) return;

    // Need current tickCounts for the increments — Redis lacks an HSET-with-add
    // primitive, so fetch + recompute. One pipeline round-trip total.
    const tickCounts = await Promise.all(
      updates.map((u) =>
        u.tickIncrement
          ? this.redis.hget(this.scheduleKey(u.id), "tickCount")
          : Promise.resolve(null),
      ),
    );

    // HSET fire-state for any update that fired, then set the next run
    // (the script skips schedules paused since the poll). The client
    // pipelines these concurrent calls on one connection.
    await Promise.all(
      updates.map(async (u, i) => {
        if (u.firedAt !== undefined && u.tickIncrement && u.tickIncrement > 0) {
          const next = (tickCounts[i] ? Number(tickCounts[i]) : 0) + u.tickIncrement;
          await this.redis.hset(this.scheduleKey(u.id), {
            lastFiredAt: String(u.firedAt.getTime()),
            tickCount: String(next),
          });
        }
        await this.setNextRun(u.id, u.nextRun);
      }),
    );
  }

  // -------------------------------------------------------------------------
  // Admin / CRUD
  // -------------------------------------------------------------------------

  async upsertSchedule(config: DurableScheduleConfig): Promise<void> {
    const ns = config.namespace;
    const fields: Record<string, string> = {
      id: config.id,
      timezone: config.timezone ?? "UTC",
      enabled: config.enabled === false ? "0" : "1",
      maxCatchUp: String(config.maxCatchUp ?? 0),
      jitterMs: String(config.jitterMs ?? 0),
    };
    if (ns !== undefined) fields.namespace = ns;
    if (config.name) fields.name = config.name;
    if (config.cron) fields.cron = config.cron;
    if (config.rrule) fields.rrule = config.rrule;
    if (config.intervalMs !== undefined) fields.intervalMs = String(config.intervalMs);
    if (config.startAt) fields.startAt = String(config.startAt.getTime());
    if (config.endAt) fields.endAt = String(config.endAt.getTime());
    if (config.metadata) fields.metadata = JSON.stringify(config.metadata);

    const seedMs = Math.max(this.clock.currentTimeMs(), config.startAt?.getTime() ?? 0);
    await this.redis.eval(
      UPSERT_LUA,
      3,
      this.scheduleKey(config.id),
      this.allKey(ns),
      this.dueKey(ns),
      config.id,
      `${this.prefix}:ns:`,
      this.nsKey(ns),
      seedMs,
      fields.enabled!,
      GLOBAL_NS,
      ...Object.entries(fields).flat(),
    );
  }

  async deleteSchedule(id: string): Promise<void> {
    const ns = (await this.redis.hget(this.scheduleKey(id), "namespace")) ?? undefined;
    await this.redis.del(this.scheduleKey(id));
    await this.redis.srem(this.allKey(ns), id);
    await this.redis.zrem(this.dueKey(ns), id);
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    await this.redis.eval(
      SET_ENABLED_LUA,
      1,
      this.scheduleKey(id),
      id,
      `${this.prefix}:ns:`,
      GLOBAL_NS,
      enabled ? "1" : "0",
      this.clock.currentTimeMs(),
    );
  }

  async listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? 100;
    const filtered = await this.filterSchedules(params);
    return filtered.slice(offset, offset + limit);
  }

  async countSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number> {
    if (
      params?.namespace !== undefined &&
      params.enabled === undefined &&
      params.metadata === undefined
    ) {
      return await this.redis.scard(this.allKey(params.namespace));
    }
    return (await this.filterSchedules(params)).length;
  }

  /**
   * Shared filter pass for list / count. An omitted namespace covers every
   * namespace. Results are sorted by id for stable pagination; metadata
   * containment is applied after loading (Redis has no JSON index here).
   */
  private async filterSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): Promise<DurableScheduleConfig[]> {
    const ids = await this.scheduleIds(params?.namespace);
    ids.sort();
    const configs = await this.loadSchedules(ids);
    const out: DurableScheduleConfig[] = [];
    for (const id of ids) {
      const cfg = configs.get(id);
      if (!cfg) continue;
      if (params?.enabled !== undefined && (cfg.enabled ?? true) !== params.enabled) continue;
      if (params?.metadata && !scheduleMetadataContains(cfg.metadata, params.metadata)) continue;
      out.push(cfg);
    }
    return out;
  }

  private async scheduleIds(namespace: string | undefined): Promise<string[]> {
    if (namespace !== undefined) return await this.redis.smembers(this.allKey(namespace));
    const allKeys = await this.redis.keys(`${this.prefix}:ns:*:all`);
    const sets = await Promise.all(allKeys.map((key) => this.redis.smembers(key)));
    return [...new Set(sets.flat())];
  }

  // -------------------------------------------------------------------------
  // Leader election — SET NX PX with TTL refresh.
  // -------------------------------------------------------------------------

  async tryAcquireLeader(params: {
    instanceId: string;
    namespace?: string;
    ttlMs: number;
  }): Promise<boolean> {
    const key = this.leaderKey(params.namespace);
    const result = await this.redis.set(key, params.instanceId, "PX", params.ttlMs, "NX");
    if (result === "OK") return true;

    // Already held — refresh TTL if we're the holder.
    const holder = await this.redis.get(key);
    if (holder === params.instanceId) {
      await this.redis.pexpire(key, params.ttlMs);
      return true;
    }
    return false;
  }

  async findDueAcross(params: {
    now: Date;
    limit: number;
    namespaces?: readonly (string | undefined)[];
  }): Promise<readonly { id: string; namespace?: string }[]> {
    // Redis layout has one due-ZSET per namespace, so we either union an
    // explicit list (when supplied) or SCAN to discover. SCAN is one
    // round trip on key cardinality = tenant count, which is cheap in
    // practice — and only happens here, not on the per-tenant fast path.
    let dueKeys: Array<{ key: string; namespace?: string }>;
    if (params.namespaces) {
      dueKeys = params.namespaces.map((ns) => ({
        key: this.dueKey(ns),
        namespace: ns,
      }));
    } else {
      // KEYS pattern over the per-namespace `:due` keys. Cardinality =
      // tenant count (low thousands at most), well within KEYS' budget.
      // Switch to the client's SCAN if discovery ever needs to scale
      // beyond that.
      const pattern = `${this.prefix}:ns:*:due`;
      const keys = await this.redis.keys(pattern);
      dueKeys = keys.map((key) => {
        const stripped = key.slice(`${this.prefix}:ns:`.length, -":due".length);
        return { key, namespace: stripped === GLOBAL_NS ? undefined : stripped };
      });
    }
    // Score-bounded zrange across each due-set. Run in parallel — the
    // client pipelines them on a single connection.
    const lists = await Promise.all(
      dueKeys.map(async ({ key, namespace }) => {
        const ids = await this.findDueIn({ dueKey: key, now: params.now, limit: params.limit });
        return ids.map((id) => ({ id, namespace }));
      }),
    );
    // Flatten then trim to the global limit. We do not score-merge across
    // namespaces — order within a namespace stays score-ascending, but
    // cross-namespace ordering is undefined. Acceptable: the caller
    // groups by namespace anyway and tickOnce processes one namespace at
    // a time.
    return lists.flat().slice(0, params.limit);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function rawToConfig(raw: Record<string, string>): DurableScheduleConfig {
  return {
    id: raw.id!,
    namespace: raw.namespace,
    name: raw.name,
    cron: raw.cron,
    rrule: raw.rrule,
    intervalMs: raw.intervalMs !== undefined ? Number(raw.intervalMs) : undefined,
    timezone: raw.timezone ?? "UTC",
    enabled: raw.enabled !== "0",
    startAt: raw.startAt ? new Date(Number(raw.startAt)) : undefined,
    endAt: raw.endAt ? new Date(Number(raw.endAt)) : undefined,
    metadata: raw.metadata ? JSON.parse(raw.metadata) : undefined,
    maxCatchUp: raw.maxCatchUp !== undefined ? Number(raw.maxCatchUp) : 0,
    jitterMs: raw.jitterMs !== undefined ? Number(raw.jitterMs) : 0,
  };
}
