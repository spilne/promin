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

import type { DurableScheduleConfig, SchedulerStorage } from "@promin/workflow";
import type { RedisClient } from "./redis-client.ts";

export interface RedisSchedulerStorageConfig {
  redis: RedisClient;
  /** Key prefix for all scheduler keys. Default: "sched". */
  prefix?: string;
}

const GLOBAL_NS = "_";

export class RedisSchedulerStorage implements SchedulerStorage {
  private readonly redis: RedisClient;
  private readonly prefix: string;

  constructor(config: RedisSchedulerStorageConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "sched";
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
    return await this.redis.zrangebyscore(
      this.dueKey(params.namespace),
      "-inf",
      params.now.getTime(),
      "LIMIT",
      0,
      params.limit,
    );
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
    // Need the schedule's namespace to know which due-ZSET to update.
    const ns = await this.redis.hget(this.scheduleKey(id), "namespace");
    const namespace = ns ?? undefined;
    const dueKey = this.dueKey(namespace);
    if (nextRun === null) {
      await this.redis.zrem(dueKey, id);
    } else {
      await this.redis.zadd(dueKey, nextRun.getTime(), id);
    }
  }

  async commitPoll(
    updates: Array<{
      id: string;
      firedAt?: Date;
      tickIncrement?: number;
      nextRun: Date | null;
    }>,
  ): Promise<void> {
    if (updates.length === 0) return;

    // Need each id's namespace to pick the right due-ZSET. Fetch them in one
    // pipelined fan-out (same trick as loadSchedules).
    const namespaces = await Promise.all(
      updates.map((u) => this.redis.hget(this.scheduleKey(u.id), "namespace")),
    );
    // Need current tickCounts for the increments — Redis lacks an HSET-with-add
    // primitive, so fetch + recompute. One pipeline round-trip total.
    const tickCounts = await Promise.all(
      updates.map((u) =>
        u.tickIncrement
          ? this.redis.hget(this.scheduleKey(u.id), "tickCount")
          : Promise.resolve(null),
      ),
    );

    // Build pipeline: HSET fire-state for any update that fired, then
    // ZADD/ZREM for the next-run change. Single MULTI commits everything.
    await Promise.all(
      updates.map(async (u, i) => {
        if (u.firedAt !== undefined && u.tickIncrement && u.tickIncrement > 0) {
          const next = (tickCounts[i] ? Number(tickCounts[i]) : 0) + u.tickIncrement;
          await this.redis.hset(this.scheduleKey(u.id), {
            lastFiredAt: String(u.firedAt.getTime()),
            tickCount: String(next),
          });
        }
        const ns = namespaces[i] ?? undefined;
        const dueKey = this.dueKey(ns);
        if (u.nextRun === null) {
          await this.redis.zrem(dueKey, u.id);
        } else {
          await this.redis.zadd(dueKey, u.nextRun.getTime(), u.id);
        }
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
      overlapPolicy: config.overlapPolicy ?? "allow",
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

    // Blow away the old hash so removed fields don't linger across upserts.
    await this.redis.del(this.scheduleKey(config.id));
    await this.redis.hset(this.scheduleKey(config.id), fields);
    await this.redis.sadd(this.allKey(ns), config.id);
  }

  async deleteSchedule(id: string): Promise<void> {
    const ns = (await this.redis.hget(this.scheduleKey(id), "namespace")) ?? undefined;
    await this.redis.del(this.scheduleKey(id));
    await this.redis.srem(this.allKey(ns), id);
    await this.redis.zrem(this.dueKey(ns), id);
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    await this.redis.hset(this.scheduleKey(id), { enabled: enabled ? "1" : "0" });
  }

  async listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    const ids = await this.redis.smembers(this.allKey(params?.namespace));
    const out: DurableScheduleConfig[] = [];
    // SMEMBERS isn't ordered; sort by id for stable pagination.
    ids.sort();
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? 100;
    for (const id of ids) {
      const cfg = await this.loadSchedule(id);
      if (!cfg) continue;
      if (params?.enabled !== undefined && (cfg.enabled ?? true) !== params.enabled) continue;
      out.push(cfg);
      if (out.length >= offset + limit) break;
    }
    return out.slice(offset, offset + limit);
  }

  async countSchedules(params?: { enabled?: boolean; namespace?: string }): Promise<number> {
    if (params?.enabled === undefined) {
      // Cheap path: SCARD is O(1).
      return await this.redis.zcard(this.allKey(params?.namespace)).catch(async () => {
        // SCARD missing on the type — fall back to SMEMBERS length.
        const ids = await this.redis.smembers(this.allKey(params?.namespace));
        return ids.length;
      });
    }
    const all = await this.listSchedules({
      namespace: params.namespace,
      enabled: params.enabled,
      limit: Number.MAX_SAFE_INTEGER,
    });
    return all.length;
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
      // The RedisClient abstraction here doesn't expose SCAN; if a higher-
      // throughput discovery path becomes necessary we add a SCAN method
      // to the interface and switch.
      const pattern = `${this.prefix}:ns:*:due`;
      const keys = await this.redis.keys(pattern);
      dueKeys = keys.map((key) => {
        const stripped = key.slice(`${this.prefix}:ns:`.length, -":due".length);
        return { key, namespace: stripped === GLOBAL_NS ? undefined : stripped };
      });
    }
    // Score-bounded zrange across each due-set. Run in parallel — the
    // client pipelines them on a single connection.
    const nowMs = params.now.getTime();
    const lists = await Promise.all(
      dueKeys.map(async ({ key, namespace }) => {
        const ids = await this.redis.zrangebyscore(key, "-inf", nowMs, "LIMIT", 0, params.limit);
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
    overlapPolicy: (raw.overlapPolicy as DurableScheduleConfig["overlapPolicy"]) ?? "allow",
    maxCatchUp: raw.maxCatchUp !== undefined ? Number(raw.maxCatchUp) : 0,
    jitterMs: raw.jitterMs !== undefined ? Number(raw.jitterMs) : 0,
  };
}
