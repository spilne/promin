import { eq, and, sql } from "drizzle-orm";
import type { StateMachineStorage, MachineState, TransitionEvent } from "@promin/workflow";
import { machines, machineEvents, machineLocks } from "./schema.ts";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "./exec-raw.ts";

export class PgStateMachineStorage implements StateMachineStorage {
  /** Lock owner id — `releaseLock` only frees leases this instance holds. */
  private readonly instanceId = crypto.randomUUID();

  constructor(private readonly db: DrizzleDb) {}

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
    await this.db.insert(machines).values({
      id: params.id,
      name: params.name,
      machineType: params.type,
      namespace: params.namespace,
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
      type: row.machineType ?? undefined,
      namespace: row.namespace ?? undefined,
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

  /**
   * Lease row in `sm_machine_locks`, expiry on the server clock. Unlike a
   * session advisory lock it excludes callers on every pool connection and
   * process, honours `durationMs`, and is released by row delete rather
   * than by whichever connection the pool happens to hand out.
   */
  async tryLock(id: string, durationMs: number): Promise<boolean> {
    const expiresAt = sql`NOW() + (${Math.max(0, Math.trunc(durationMs))}::double precision * INTERVAL '1 millisecond')`;
    const rows = await execRaw(
      this.db,
      sql`
      INSERT INTO sm_machine_locks (machine_id, locked_by, expires_at)
      VALUES (${id}, ${this.instanceId}, ${expiresAt})
      ON CONFLICT (machine_id) DO UPDATE
        SET locked_by = EXCLUDED.locked_by, expires_at = EXCLUDED.expires_at
        WHERE sm_machine_locks.expires_at < NOW()
      RETURNING machine_id
    `,
    );
    return rows.length > 0;
  }

  async releaseLock(id: string): Promise<void> {
    await this.db
      .delete(machineLocks)
      .where(and(eq(machineLocks.machineId, id), eq(machineLocks.lockedBy, this.instanceId)));
  }
}
