// ---------------------------------------------------------------------------
// StateMachineStorage — pluggable persistence interface
// ---------------------------------------------------------------------------

import type { MachineState, TransitionEvent } from "./state-machine-types.ts";
import { type WallClock, SystemWallClock } from "../shared/wall-clock.ts";

/**
 * Opaque token for one acquisition of a machine lock. `releaseLock` and
 * `extendLock` act only while the lock still carries this token, so a
 * holder whose lock expired and was taken over can neither free nor
 * prolong the new holder's lock.
 */
export type StateMachineLockToken = string;

export interface StateMachineStorage {
  /** Create a machine at revision 0 with an empty event history. */
  create(params: {
    id: string;
    name: string;
    type?: string;
    namespace?: string;
    initial: string;
    context: unknown;
    version?: string;
    metadata?: Record<string, unknown>;
  }): Promise<void>;

  load(id: string): Promise<MachineState | null>;

  /**
   * Compare-and-set transition. Applies only while the machine is in
   * `from` at `expectedRevision`; then sets `to` and `context`, bumps the
   * revision by one and appends one event, all together. Rejects (and
   * changes nothing) when the machine is missing or either value moved.
   * Comparing the revision, not just the state, keeps two concurrent
   * self-loop transitions (`a → a`) from overwriting each other.
   */
  transition(params: {
    id: string;
    from: string;
    to: string;
    expectedRevision: number;
    event: string;
    context: unknown;
    eventData?: unknown;
    metadata?: unknown;
  }): Promise<void>;

  loadEvents(id: string, params?: { limit?: number; offset?: number }): Promise<TransitionEvent[]>;

  /**
   * Take the machine's lock for `durationMs`. Returns a token on success
   * and `null` while another unexpired holder has it. Not re-entrant.
   */
  tryLock(params: { id: string; durationMs: number }): Promise<StateMachineLockToken | null>;

  /** Release the lock if it still carries `token`; otherwise a no-op. */
  releaseLock(params: { id: string; token: StateMachineLockToken }): Promise<void>;

  /**
   * Heartbeat: move the lock's expiry to `durationMs` from now if it still
   * carries `token` and has not expired. Returns `false` when the lock was
   * lost.
   */
  extendLock(params: {
    id: string;
    token: StateMachineLockToken;
    durationMs: number;
  }): Promise<boolean>;
}

// ---------------------------------------------------------------------------
// InMemoryStateMachineStorage — for testing and single-process use
// ---------------------------------------------------------------------------

export class InMemoryStateMachineStorage implements StateMachineStorage {
  private machines = new Map<string, MachineState>();
  private events = new Map<string, TransitionEvent[]>();
  private locks = new Map<string, { token: StateMachineLockToken; expiresAt: number }>();
  private readonly clock: WallClock;

  constructor(config?: { clock?: WallClock }) {
    this.clock = config?.clock ?? SystemWallClock;
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
    const now = this.clock.now();
    this.machines.set(params.id, {
      id: params.id,
      name: params.name,
      type: params.type,
      namespace: params.namespace,
      current: params.initial,
      context: params.context,
      version: params.version,
      metadata: params.metadata,
      revision: 0,
      createdAt: now,
      updatedAt: now,
    });
    this.events.set(params.id, []);
  }

  async load(id: string): Promise<MachineState | null> {
    return this.machines.get(id) ?? null;
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
    const machine = this.machines.get(params.id);
    if (!machine) throw new Error(`Machine ${params.id} not found`);
    if (machine.current !== params.from || machine.revision !== params.expectedRevision) {
      throw new Error(
        `Machine ${params.id} is in state "${machine.current}" at revision ${machine.revision}, not "${params.from}" at revision ${params.expectedRevision}`,
      );
    }

    const now = this.clock.now();
    this.machines.set(params.id, {
      ...machine,
      current: params.to,
      context: params.context,
      revision: machine.revision + 1,
      updatedAt: now,
    });

    const events = this.events.get(params.id) ?? [];
    events.push({
      id: crypto.randomUUID(),
      event: params.event,
      from: params.from,
      to: params.to,
      context: params.context,
      eventData: params.eventData,
      metadata: params.metadata,
      createdAt: now,
    });
    this.events.set(params.id, events);
  }

  async loadEvents(
    id: string,
    params?: { limit?: number; offset?: number },
  ): Promise<TransitionEvent[]> {
    const all = this.events.get(id) ?? [];
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? all.length;
    return all.slice(offset, offset + limit);
  }

  async tryLock(params: { id: string; durationMs: number }): Promise<StateMachineLockToken | null> {
    const lock = this.locks.get(params.id);
    const now = this.clock.currentTimeMs();
    if (lock && lock.expiresAt > now) return null;
    const token = crypto.randomUUID();
    this.locks.set(params.id, { token, expiresAt: now + params.durationMs });
    return token;
  }

  async releaseLock(params: { id: string; token: StateMachineLockToken }): Promise<void> {
    if (this.locks.get(params.id)?.token === params.token) this.locks.delete(params.id);
  }

  async extendLock(params: {
    id: string;
    token: StateMachineLockToken;
    durationMs: number;
  }): Promise<boolean> {
    const lock = this.locks.get(params.id);
    const now = this.clock.currentTimeMs();
    if (!lock || lock.token !== params.token || lock.expiresAt <= now) return false;
    lock.expiresAt = now + params.durationMs;
    return true;
  }
}
