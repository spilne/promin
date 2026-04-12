import type { StateMachineStorage } from "@promin/workflow";
import type { MachineState, TransitionEvent } from "@promin/workflow";
import type { RedisClient } from "./redis-client.ts";

export interface RedisStateMachineConfig {
  prefix?: string;
  /** TTL for machine keys after reaching a terminal state. Default: no expiry. */
  terminalTtlMs?: number;
  /** TTL for machine keys in non-terminal states — catches stuck/abandoned machines. Default: no expiry. */
  activeTtlMs?: number;
}

export class RedisStateMachineStorage implements StateMachineStorage {
  private readonly prefix: string;
  private readonly terminalTtlMs?: number;
  private readonly activeTtlMs?: number;
  private terminalStates = new Set<string>();

  constructor(
    private readonly redis: RedisClient,
    config?: RedisStateMachineConfig,
  ) {
    this.prefix = config?.prefix ?? "sm";
    this.terminalTtlMs = config?.terminalTtlMs;
    this.activeTtlMs = config?.activeTtlMs;
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
    const now = new Date().toISOString();
    const state: Record<string, string> = {
      id: params.id,
      name: params.name,
      current: params.initial,
      context: JSON.stringify(params.context),
      createdAt: now,
      updatedAt: now,
    };
    if (params.type) state.type = params.type;
    if (params.namespace) state.namespace = params.namespace;
    if (params.version) state.version = params.version;
    if (params.metadata) state.metadata = JSON.stringify(params.metadata);

    await this.redis.hset(this.machineKey(params.id), state);

    // Set active TTL for stuck machine detection
    if (this.activeTtlMs) {
      await this.redis.pexpire(this.machineKey(params.id), this.activeTtlMs);
      await this.redis.pexpire(this.eventsKey(params.id), this.activeTtlMs);
    }
  }

  async load(id: string): Promise<MachineState | null> {
    const raw = await this.redis.hgetall(this.machineKey(id));
    if (!raw || !raw.id) return null;
    return {
      id: raw.id,
      name: raw.name,
      type: raw.type ?? undefined,
      namespace: raw.namespace ?? undefined,
      current: raw.current,
      context: JSON.parse(raw.context),
      version: raw.version ?? undefined,
      metadata: raw.metadata ? JSON.parse(raw.metadata) : undefined,
      createdAt: new Date(raw.createdAt),
      updatedAt: new Date(raw.updatedAt),
    };
  }

  async transition(params: {
    id: string;
    from: string;
    to: string;
    event: string;
    context: unknown;
    metadata?: unknown;
  }): Promise<void> {
    const currentState = await this.redis.hget(this.machineKey(params.id), "current");
    if (currentState !== params.from) {
      throw new Error(`Machine ${params.id} is in state "${currentState}", not "${params.from}"`);
    }

    const now = new Date().toISOString();

    await this.redis.hset(this.machineKey(params.id), {
      current: params.to,
      context: JSON.stringify(params.context),
      updatedAt: now,
    });

    const event: TransitionEvent = {
      id: crypto.randomUUID(),
      event: params.event,
      from: params.from,
      to: params.to,
      context: params.context,
      metadata: params.metadata as Record<string, unknown> | undefined,
      createdAt: new Date(now),
    };
    await this.redis.rpush(this.eventsKey(params.id), JSON.stringify(event));

    // Set TTL based on target state
    if (this.terminalStates.has(params.to) && this.terminalTtlMs) {
      await this.redis.pexpire(this.machineKey(params.id), this.terminalTtlMs);
      await this.redis.pexpire(this.eventsKey(params.id), this.terminalTtlMs);
    } else if (this.activeTtlMs) {
      // Refresh active TTL — machine is still alive
      await this.redis.pexpire(this.machineKey(params.id), this.activeTtlMs);
      await this.redis.pexpire(this.eventsKey(params.id), this.activeTtlMs);
    }
  }

  async loadEvents(
    id: string,
    params?: { limit?: number; offset?: number },
  ): Promise<TransitionEvent[]> {
    const start = params?.offset ?? 0;
    const end = params?.limit ? start + params.limit - 1 : -1;

    const result = await this.redis.eval(
      `return redis.call('LRANGE', KEYS[1], ARGV[1], ARGV[2])`,
      1,
      this.eventsKey(id),
      start,
      end,
    );

    const items = (result as string[]) ?? [];
    return items.map((raw) => {
      const e = JSON.parse(raw);
      return { ...e, createdAt: new Date(e.createdAt) };
    });
  }

  async tryLock(id: string, durationMs: number): Promise<boolean> {
    const lockKey = `${this.prefix}:lock:${id}`;
    const result = await this.redis.set(lockKey, "1", "NX", "PX", durationMs);
    return !!result;
  }

  async releaseLock(id: string): Promise<void> {
    const lockKey = `${this.prefix}:lock:${id}`;
    await this.redis.del(lockKey);
  }
}
