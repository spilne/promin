// ---------------------------------------------------------------------------
// RedisStateMachineStorage — Redis adapter for StateMachineStorage
//
// Key layout:
//   {prefix}:machine:{id}   — HASH with the machine snapshot and revision
//   {prefix}:events:{id}    — LIST of JSON transition events (append-only)
//   {prefix}:lock:{id}      — STRING lock token with PX expiry
// ---------------------------------------------------------------------------

import {
  SystemWallClock,
  type MachineState,
  type StateMachineLockToken,
  type StateMachineStorage,
  type TransitionEvent,
  type WallClock,
} from "@promin/workflow";
import type { RedisStoreClient } from "./redis-client.ts";

export interface RedisStateMachineStorageConfig {
  redis: RedisStoreClient;
  /** Key prefix for all machine keys. Default: "sm". */
  prefix?: string;
  /** TTL for machine keys after reaching a terminal state. Default: no expiry. */
  terminalTtlMs?: number;
  /** TTL for machine keys in non-terminal states — catches stuck/abandoned machines. Default: no expiry. */
  activeTtlMs?: number;
  /** Time source for `createdAt` / `updatedAt`. Default: `SystemWallClock`. */
  clock?: WallClock;
}

/**
 * Compare-and-set transition: only applies when the machine is still in
 * `from` at the expected revision, updating the snapshot, bumping the
 * revision, appending the event and refreshing TTLs in one step. Returns
 * `{current, revision}` on mismatch, nil when missing, and `1` on success.
 * A machine written before revisions existed has no `revision` field; its
 * history length stands in for it.
 *
 * KEYS: [machine_key, events_key]
 * ARGV: [from, to, context_json, updated_at, event_json, ttl_ms ('' = none), expected_revision]
 */
const TRANSITION_LUA = `
local current = redis.call('HGET', KEYS[1], 'current')
if not current then return nil end
local revision = tonumber(redis.call('HGET', KEYS[1], 'revision') or redis.call('LLEN', KEYS[2]))
if current ~= ARGV[1] or revision ~= tonumber(ARGV[7]) then return {current, revision} end
redis.call('HSET', KEYS[1], 'current', ARGV[2], 'context', ARGV[3], 'updatedAt', ARGV[4], 'revision', revision + 1)
redis.call('RPUSH', KEYS[2], ARGV[5])
if ARGV[6] ~= '' then
  redis.call('PEXPIRE', KEYS[1], ARGV[6])
  redis.call('PEXPIRE', KEYS[2], ARGV[6])
end
return 1
`;

/** Delete the lock only while it still holds our token. */
const RELEASE_LOCK_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

/** Move the lock's expiry only while it still holds our token. */
const EXTEND_LOCK_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`;

export class RedisStateMachineStorage implements StateMachineStorage {
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;
  private readonly terminalTtlMs?: number;
  private readonly activeTtlMs?: number;
  private readonly clock: WallClock;
  private readonly terminalStates = new Set<string>();

  constructor(config: RedisStateMachineStorageConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "sm";
    this.terminalTtlMs = config.terminalTtlMs;
    this.activeTtlMs = config.activeTtlMs;
    this.clock = config.clock ?? SystemWallClock;
  }

  /** Register terminal states so storage knows when to set TTL. */
  registerTerminalStates(states: string[]): void {
    for (const s of states) this.terminalStates.add(s);
  }

  private machineKey(id: string): string {
    return `${this.prefix}:machine:${id}`;
  }

  private eventsKey(id: string): string {
    return `${this.prefix}:events:${id}`;
  }

  private lockKey(id: string): string {
    return `${this.prefix}:lock:${id}`;
  }

  async create(params: {
    id: string;
    name: string;
    type?: string;
    namespace?: string;
    initial: string;
    context: unknown;
    version?: string;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    const now = this.clock.now().toISOString();
    const state: Record<string, string> = {
      id: params.id,
      name: params.name,
      current: params.initial,
      context: JSON.stringify(params.context),
      revision: "0",
      createdAt: now,
      updatedAt: now,
    };
    if (params.type) state.type = params.type;
    if (params.namespace) state.namespace = params.namespace;
    if (params.version) state.version = params.version;
    if (params.metadata) state.metadata = JSON.stringify(params.metadata);

    // A re-created machine starts with a fresh snapshot and history.
    await this.redis.del(this.machineKey(params.id), this.eventsKey(params.id));
    await this.redis.hset(this.machineKey(params.id), state);

    if (this.activeTtlMs) {
      await this.redis.pexpire(this.machineKey(params.id), this.activeTtlMs);
    }
  }

  async load(id: string): Promise<MachineState | null> {
    const raw = await this.redis.hgetall(this.machineKey(id));
    if (!raw || !raw.id) return null;
    const revision =
      raw.revision !== undefined ? Number(raw.revision) : await this.redis.llen(this.eventsKey(id));
    return {
      id: raw.id,
      name: raw.name ?? "",
      type: raw.type ?? undefined,
      namespace: raw.namespace ?? undefined,
      current: raw.current ?? "",
      context: raw.context !== undefined ? JSON.parse(raw.context) : undefined,
      version: raw.version ?? undefined,
      metadata: raw.metadata ? JSON.parse(raw.metadata) : undefined,
      revision,
      createdAt: new Date(raw.createdAt ?? 0),
      updatedAt: new Date(raw.updatedAt ?? 0),
    };
  }

  async transition(params: {
    id: string;
    from: string;
    to: string;
    expectedRevision: number;
    event: string;
    context: unknown;
    eventData?: unknown;
    metadata?: unknown;
  }): Promise<void> {
    const now = this.clock.now();
    const event: TransitionEvent = {
      id: crypto.randomUUID(),
      event: params.event,
      from: params.from,
      to: params.to,
      context: params.context,
      eventData: params.eventData,
      metadata: params.metadata,
      createdAt: now,
    };
    const ttlMs = this.terminalStates.has(params.to) ? this.terminalTtlMs : this.activeTtlMs;

    const result = await this.redis.eval(
      TRANSITION_LUA,
      2,
      this.machineKey(params.id),
      this.eventsKey(params.id),
      params.from,
      params.to,
      JSON.stringify(params.context),
      now.toISOString(),
      JSON.stringify(event),
      ttlMs ? String(ttlMs) : "",
      String(params.expectedRevision),
    );
    if (result === null || result === undefined) {
      throw new Error(`Machine ${params.id} not found`);
    }
    if (Array.isArray(result)) {
      throw new Error(
        `Machine ${params.id} is in state "${result[0]}" at revision ${result[1]}, not "${params.from}" at revision ${params.expectedRevision}`,
      );
    }
  }

  async loadEvents(
    id: string,
    params?: { limit?: number; offset?: number },
  ): Promise<TransitionEvent[]> {
    const start = params?.offset ?? 0;
    if (params?.limit !== undefined && params.limit <= 0) return [];
    const end = params?.limit !== undefined ? start + params.limit - 1 : -1;

    const items = await this.redis.lrange(this.eventsKey(id), start, end);
    return items.map((raw) => {
      const e = JSON.parse(raw) as TransitionEvent & { createdAt: string };
      return { ...e, createdAt: new Date(e.createdAt) };
    });
  }

  async tryLock(params: { id: string; durationMs: number }): Promise<StateMachineLockToken | null> {
    const token = crypto.randomUUID();
    const result = await this.redis.set(
      this.lockKey(params.id),
      token,
      "PX",
      Math.max(1, Math.trunc(params.durationMs)),
      "NX",
    );
    return result === "OK" ? token : null;
  }

  async releaseLock(params: { id: string; token: StateMachineLockToken }): Promise<void> {
    await this.redis.eval(RELEASE_LOCK_LUA, 1, this.lockKey(params.id), params.token);
  }

  async extendLock(params: {
    id: string;
    token: StateMachineLockToken;
    durationMs: number;
  }): Promise<boolean> {
    const result = await this.redis.eval(
      EXTEND_LOCK_LUA,
      1,
      this.lockKey(params.id),
      params.token,
      String(Math.max(1, Math.trunc(params.durationMs))),
    );
    return Number(result) === 1;
  }
}
