import { eq, and, sql } from "drizzle-orm";
import type { StateMachineStorage, MachineState, TransitionEvent } from "@promin/core";
import { machines, machineEvents } from "./schema.ts";
import type { DrizzleDb } from "./drizzle-db.ts";
import { execRaw } from "./drizzle-db.ts";

function hashToInt32(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash |= 0;
  }
  return hash;
}

export class PgStateMachineStorage implements StateMachineStorage {
  constructor(private readonly db: DrizzleDb) {}

  async create(params: {
    id: string;
    name: string;
    initial: string;
    context: unknown;
    version?: string;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    await this.db.insert(machines).values({
      id: params.id,
      name: params.name,
      current: params.initial,
      context: params.context,
      version: params.version,
      metadata: params.metadata,
    });
  }

  async load(id: string): Promise<MachineState | null> {
    const [row] = await this.db.select().from(machines).where(eq(machines.id, id));
    if (!row) return null;
    return {
      id: row.id,
      name: row.name,
      current: row.current,
      context: row.context,
      version: row.version ?? undefined,
      metadata: (row.metadata as Record<string, unknown>) ?? undefined,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
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
    const now = new Date();

    // Update current state — validate we're in expected state
    const [updated] = await this.db
      .update(machines)
      .set({ current: params.to, context: params.context, updatedAt: now })
      .where(and(eq(machines.id, params.id), eq(machines.current, params.from)))
      .returning({ id: machines.id });

    if (!updated) {
      throw new Error(`Machine ${params.id} is not in state "${params.from}"`);
    }

    // Append event
    await this.db.insert(machineEvents).values({
      machineId: params.id,
      event: params.event,
      fromState: params.from,
      toState: params.to,
      context: params.context,
      metadata: params.metadata,
    });
  }

  async loadEvents(
    id: string,
    params?: { limit?: number; offset?: number },
  ): Promise<TransitionEvent[]> {
    const query = this.db
      .select()
      .from(machineEvents)
      .where(eq(machineEvents.machineId, id))
      .orderBy(machineEvents.id)
      .$dynamic();

    if (params?.limit) query.limit(params.limit);
    if (params?.offset) query.offset(params.offset);

    const rows = await query;
    return rows.map((r) => ({
      id: String(r.id),
      event: r.event,
      from: r.fromState,
      to: r.toState,
      context: r.context,
      metadata: (r.metadata as Record<string, unknown>) ?? undefined,
      createdAt: r.createdAt,
    }));
  }

  async tryLock(id: string, _durationMs: number): Promise<boolean> {
    const [result] = await execRaw(
      this.db,
      sql`SELECT pg_try_advisory_lock(${hashToInt32("sm:" + id)}) as acquired`,
    );
    return result?.acquired === true;
  }

  async releaseLock(id: string): Promise<void> {
    await execRaw(this.db, sql`SELECT pg_advisory_unlock(${hashToInt32("sm:" + id)})`);
  }
}
