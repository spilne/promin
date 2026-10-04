// ---------------------------------------------------------------------------
// RedisSchedulerStorage — Redis adapter for SchedulerStorage.
//
// Heavy lifting (cron/rrule/catch-up/jitter/leader loop) lives in the generic
// DurableScheduler shell in @promin/workflow. This file is just the storage
// adapter — schedule CRUD via hashes, due-row lookup via per-namespace ZSETs,
// and fenced leader leases (`RedisLeaderLeaseStore`).
//
// Every key starts with `{<prefix>}`, so on Redis Cluster the whole
// scheduler sits in one slot: a fenced poll commit checks the lease epoch
// and writes many schedules and due sets in one script. Schedulers with
// different prefixes land in different slots.
//
// Key layout (base = `{<prefix>}`, namespace `ns` — global namespace = "_"):
//   <base>:namespaces            — SET of every namespace a schedule was put in
//   <base>:ns:<ns>:all           — SET of schedule IDs in that namespace
//   <base>:ns:<ns>:due           — ZSET, score=nextRunMs, member=id (per ns!)
//   <base>:schedule:<id>         — HASH with config + state (namespace-tagged)
//   <base>:lease:{<key>}:holder  — STRING with PX TTL, the lease holder
//   <base>:lease:{<key>}:epoch   — STRING counter, the lease's fencing epoch
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  StaleLeaseError,
  type CommitPollResult,
  type DurableScheduleConfig,
  type LeaderLease,
  type ScheduleCommit,
  type SchedulerStorage,
} from "@promin/workflow/scheduler";
import { scheduleMetadataContains } from "@promin/workflow/storage-kit";
import type { RedisStoreClient } from "./redis-client.ts";
import { RedisLeaderLeaseStore } from "./redis-leader-lease-store.ts";
import { storeKeyBase } from "./redis-key-tags.ts";

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
 * KEYS: [schedule_key, all_key, due_key, namespaces_key]
 * ARGV: [id, namespace_key_base, namespace, seed_ms, enabled('1'|'0'),
 *        global_ns, field1, value1, ...]
 */
const UPSERT_LUA = `
local schedule_key = KEYS[1]
local all_key = KEYS[2]
local due_key = KEYS[3]
redis.call('SADD', KEYS[4], ARGV[3])
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

/**
 * Commit a poll atomically. With a lease (ARGV[1] = '1'), nothing is written
 * unless the lease's epoch key still holds ARGV[2]. Per entry: skip and
 * report it when its expected tick count (if any) no longer matches or the
 * schedule is gone; otherwise advance the fire state and, if requested, set
 * or clear its next run — a disabled schedule always leaves due-tracking.
 *
 * KEYS: [lease_epoch_key] when fenced, else [namespaces_key] (only routes
 *       the script to the scheduler's slot)
 * ARGV: [fenced('1'|'0'), epoch, schedule_key_prefix, namespace_key_base,
 *        global_ns, then per entry: id, fired_at_ms|'', tick_inc,
 *        set_next('1'|'0'), next_run_ms|'', expected_tick_count|'']
 * Returns {'stale', current_epoch} or {'ok', conflict_id, ...}.
 */
const COMMIT_POLL_LUA = `
if ARGV[1] == '1' then
  local current = redis.call('GET', KEYS[1])
  if current ~= ARGV[2] then return {'stale', current or ''} end
end
local prefix = ARGV[3]
local ns_base = ARGV[4]
local global_ns = ARGV[5]
local out = {'ok'}
for i = 6, #ARGV, 6 do
  local id = ARGV[i]
  local key = prefix .. id
  local fields = redis.call('HMGET', key, 'id', 'namespace', 'enabled', 'tickCount')
  local expected = ARGV[i + 5]
  local count = tonumber(fields[4] or '0')
  if not fields[1] then
    if expected ~= '' then out[#out + 1] = id end
    redis.call('ZREM', ns_base .. global_ns .. ':due', id)
  elseif expected ~= '' and tonumber(expected) ~= count then
    out[#out + 1] = id
  else
    local inc = tonumber(ARGV[i + 2])
    if ARGV[i + 1] ~= '' and inc > 0 then
      redis.call('HSET', key, 'lastFiredAt', ARGV[i + 1])
      redis.call('HINCRBY', key, 'tickCount', inc)
    end
    local due_key = ns_base .. (fields[2] or global_ns) .. ':due'
    if fields[3] == '0' then
      redis.call('ZREM', due_key, id)
    elseif ARGV[i + 3] == '1' then
      if ARGV[i + 4] == '' then
        redis.call('ZREM', due_key, id)
      else
        redis.call('ZADD', due_key, ARGV[i + 4], id)
      end
    end
  end
end
return out
`;

/**
 * Delete a schedule and take it out of its namespace's sets.
 *
 * KEYS: [schedule_key]
 * ARGV: [id, namespace_key_base, global_ns]
 */
const DELETE_LUA = `
local ns = redis.call('HGET', KEYS[1], 'namespace') or ARGV[3]
redis.call('DEL', KEYS[1])
redis.call('SREM', ARGV[2] .. ns .. ':all', ARGV[1])
redis.call('ZREM', ARGV[2] .. ns .. ':due', ARGV[1])
return 1
`;

/** KEYS: [schedule_key]  ARGV: [fired_at_ms, count] */
const RECORD_FIRE_LUA = `
redis.call('HSET', KEYS[1], 'lastFiredAt', ARGV[1])
redis.call('HINCRBY', KEYS[1], 'tickCount', ARGV[2])
return 1
`;

export class RedisSchedulerStorage implements SchedulerStorage {
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;
  /** `{<prefix>}`: the start of every key, and the scheduler's slot. */
  private readonly base: string;
  private readonly clock: WallClock;
  private readonly leases: RedisLeaderLeaseStore;

  constructor(config: RedisSchedulerStorageConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "sched";
    this.clock = config.clock ?? SystemWallClock;
    this.base = storeKeyBase(this.prefix);
    // Lease keys under the base share its slot, so a fenced commit checks
    // the epoch in the same script as its writes.
    this.leases = new RedisLeaderLeaseStore({ redis: config.redis, prefix: this.base });
  }

  // -------------------------------------------------------------------------
  // Key helpers — every per-namespace key is prefixed with `:ns:{namespace}:`
  // so multi-tenant deployments don't share due-sets or leader locks.
  // -------------------------------------------------------------------------

  private nsKey(ns: string | undefined): string {
    return ns ?? GLOBAL_NS;
  }

  private allKey(ns: string | undefined): string {
    return `${this.nsBase}${this.nsKey(ns)}:all`;
  }

  private dueKey(ns: string | undefined): string {
    return `${this.nsBase}${this.nsKey(ns)}:due`;
  }

  private scheduleKey(id: string): string {
    return `${this.scheduleBase}${id}`;
  }

  /** Base the scripts append a namespace and `:all` / `:due` to. */
  private get nsBase(): string {
    return `${this.base}:ns:`;
  }

  /** Base the scripts append a schedule id to. */
  private get scheduleBase(): string {
    return `${this.base}:schedule:`;
  }

  /** Set of every namespace (`_` = global) a schedule was put in. */
  private get namespacesKey(): string {
    return `${this.base}:namespaces`;
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
      this.scheduleBase,
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
    await this.redis.eval(
      RECORD_FIRE_LUA,
      1,
      this.scheduleKey(id),
      String(firedAt.getTime()),
      count,
    );
  }

  async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    // The script looks up the schedule's namespace (which due-ZSET) and
    // keeps disabled or deleted schedules out of due-tracking.
    await this.redis.eval(
      SET_NEXT_RUN_LUA,
      1,
      this.scheduleKey(id),
      id,
      this.nsBase,
      GLOBAL_NS,
      nextRun === null ? "" : nextRun.getTime(),
    );
  }

  async commitPoll(params: {
    updates: readonly ScheduleCommit[];
    lease?: LeaderLease;
  }): Promise<CommitPollResult> {
    const { updates, lease } = params;
    if (updates.length === 0 && !lease) return { conflicts: [] };
    // One script: fence check, compare-and-set and every write together.
    const entries = updates.flatMap((u) => [
      u.id,
      u.firedAt ? String(u.firedAt.getTime()) : "",
      String(u.tickIncrement ?? 0),
      u.nextRun === undefined ? "0" : "1",
      u.nextRun ? String(u.nextRun.getTime()) : "",
      u.expectedTickCount === undefined ? "" : String(u.expectedTickCount),
    ]);
    const keys = lease ? [this.leases.keysFor(lease.key).epoch] : [this.namespacesKey];
    const reply = (await this.redis.eval(
      COMMIT_POLL_LUA,
      keys.length,
      ...keys,
      lease ? "1" : "0",
      lease ? String(lease.epoch) : "",
      this.scheduleBase,
      this.nsBase,
      GLOBAL_NS,
      ...entries,
    )) as string[];
    if (reply[0] === "stale") {
      const current = reply[1] ? Number(reply[1]) : null;
      throw new StaleLeaseError({ lease: lease!, currentEpoch: current });
    }
    return { conflicts: reply.slice(1) };
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
      4,
      this.scheduleKey(config.id),
      this.allKey(ns),
      this.dueKey(ns),
      this.namespacesKey,
      config.id,
      this.nsBase,
      this.nsKey(ns),
      seedMs,
      fields.enabled!,
      GLOBAL_NS,
      ...Object.entries(fields).flat(),
    );
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.redis.eval(DELETE_LUA, 1, this.scheduleKey(id), id, this.nsBase, GLOBAL_NS);
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    await this.redis.eval(
      SET_ENABLED_LUA,
      1,
      this.scheduleKey(id),
      id,
      this.nsBase,
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
    const namespaces = await this.redis.smembers(this.namespacesKey);
    const sets = await Promise.all(namespaces.map((ns) => this.redis.smembers(this.allKey(ns))));
    return [...new Set(sets.flat())];
  }

  // -------------------------------------------------------------------------
  // Leader election — fenced leases, expiry on the Redis server clock.
  // -------------------------------------------------------------------------

  async tryAcquireLeader(params: {
    key: string;
    instanceId: string;
    ttlMs: number;
  }): Promise<LeaderLease | null> {
    return await this.leases.tryAcquireLeader(params);
  }

  async releaseLeader(params: { lease: LeaderLease }): Promise<void> {
    await this.leases.releaseLeader(params);
  }

  async findDueAcross(params: {
    now: Date;
    limit: number;
    namespaces?: readonly (string | undefined)[];
  }): Promise<readonly { id: string; namespace?: string }[]> {
    // Redis layout has one due-ZSET per namespace, so we either union an
    // explicit list (when supplied) or every registered namespace's.
    let dueKeys: Array<{ key: string; namespace?: string }>;
    if (params.namespaces) {
      dueKeys = params.namespaces.map((ns) => ({
        key: this.dueKey(ns),
        namespace: ns,
      }));
    } else {
      const namespaces = await this.redis.smembers(this.namespacesKey);
      dueKeys = namespaces.map((ns) => ({
        key: this.dueKey(ns),
        namespace: ns === GLOBAL_NS ? undefined : ns,
      }));
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
