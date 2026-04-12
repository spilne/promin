// ---------------------------------------------------------------------------
// StateMachineStorage — pluggable persistence interface
// ---------------------------------------------------------------------------

import type { MachineState, TransitionEvent } from "./state-machine-types.ts";
import { type Clock, SystemClock } from "@promin/core";

export interface StateMachineStorage {
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

  transition(params: {
    id: string;
    from: string;
    to: string;
    event: string;
    context: unknown;
    metadata?: unknown;
  }): Promise<void>;

  loadEvents(id: string, params?: { limit?: number; offset?: number }): Promise<TransitionEvent[]>;

  tryLock(id: string, durationMs: number): Promise<boolean>;
  releaseLock(id: string): Promise<void>;
}

// ---------------------------------------------------------------------------
// InMemoryStateMachineStorage — for testing and single-process use
// ---------------------------------------------------------------------------

export class InMemoryStateMachineStorage implements StateMachineStorage {
  private machines = new Map<string, MachineState>();
  private events = new Map<string, TransitionEvent[]>();
  private locks = new Map<string, { expiresAt: number }>();
  private readonly clock: Clock;

  constructor(config?: { clock?: Clock }) {
    this.clock = config?.clock ?? SystemClock;
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
    event: string;
    context: unknown;
    metadata?: unknown;
  }): Promise<void> {
    const machine = this.machines.get(params.id);
    if (!machine) throw new Error(`Machine ${params.id} not found`);
    if (machine.current !== params.from) {
      throw new Error(
        `Machine ${params.id} is in state "${machine.current}", not "${params.from}"`,
      );
    }

    const now = this.clock.now();
    this.machines.set(params.id, {
      ...machine,
      current: params.to,
      context: params.context,
      updatedAt: now,
    });

    const events = this.events.get(params.id) ?? [];
    events.push({
      id: crypto.randomUUID(),
      event: params.event,
      from: params.from,
      to: params.to,
      context: params.context,
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

  async tryLock(id: string, durationMs: number): Promise<boolean> {
    const lock = this.locks.get(id);
    const now = this.clock.currentTimeMs();
    if (lock && lock.expiresAt > now) return false;
    this.locks.set(id, { expiresAt: now + durationMs });
    return true;
  }

  async releaseLock(id: string): Promise<void> {
    this.locks.delete(id);
  }
}
