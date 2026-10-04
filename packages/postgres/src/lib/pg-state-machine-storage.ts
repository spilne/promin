import { eq, and, sql } from "drizzle-orm";
import {
  SystemWallClock,
  type StateMachineLockToken,
  type StateMachineStorage,
  type MachineState,
  type TransitionEvent,
  type WallClock,
} from "@promin/workflow";
import { machines, machineEvents, machineLocks } from "./schema.ts";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "./exec-raw.ts";

export interface PgStateMachineStorageOptions {
  /**
   * Time source for `createdAt` / `updatedAt` and event timestamps. The
   * state machine compares `updatedAt` against its own clock for due
   * timeouts, so both sides read the same clock. Default: `SystemWallClock`.
   * Lock expiry stays on the database clock.
   */
  clock?: WallClock;
}

export class PgStateMachineStorage implements StateMachineStorage {
  private readonly clock: WallClock;

  constructor(
    private readonly db: DrizzleDb,
    options?: PgStateMachineStorageOptions,
  ) {
    this.clock = options?.clock ?? SystemWallClock;
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
    await this.db.insert(machines).values({
      id: params.id,
      name: params.name,
      machineType: params.type,
      namespace: params.namespace,
      current: params.initial,
      context: params.context,
      version: params.version,
      metadata: params.metadata,
      createdAt: now,
      updatedAt: now,
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
      revision: row.revision,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
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

    // State update and audit row commit together: a crash between them
    // must not leave a transition without its event.
    await this.db.transaction(async (tx) => {
      // Compare-and-set on (state, revision): a self-loop leaves the state
      // unchanged, so the revision is what tells concurrent writers apart.
      const [updated] = await tx
        .update(machines)
        .set({
          current: params.to,
          context: params.context,
          revision: sql`${machines.revision} + 1`,
          updatedAt: now,
        })
        .where(
          and(
            eq(machines.id, params.id),
            eq(machines.current, params.from),
            eq(machines.revision, params.expectedRevision),
          ),
        )
        .returning({ id: machines.id });

      if (!updated) {
        throw new Error(
          `Machine ${params.id} is not in state "${params.from}" at revision ${params.expectedRevision}`,
        );
      }

      await tx.insert(machineEvents).values({
        machineId: params.id,
        event: params.event,
        fromState: params.from,
        toState: params.to,
        context: params.context,
        eventData: params.eventData,
        metadata: params.metadata,
        createdAt: now,
      });
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
      eventData: r.eventData ?? undefined,
      metadata: (r.metadata as Record<string, unknown>) ?? undefined,
      createdAt: r.createdAt,
    }));
  }

  /**
   * Lease row in `sm_machine_locks`, expiry on the server clock. It
   * excludes callers on every pool connection and process, honours
   * `durationMs`, and is released by row delete. Each
   * acquisition writes a fresh token to `locked_by`; release and extend
   * match on it.
   */
  async tryLock(params: { id: string; durationMs: number }): Promise<StateMachineLockToken | null> {
    const token = crypto.randomUUID();
    const rows = await execRaw(
      this.db,
      sql`
      INSERT INTO sm_machine_locks (machine_id, locked_by, expires_at)
      VALUES (${params.id}, ${token}, ${leaseExpiry(params.durationMs)})
      ON CONFLICT (machine_id) DO UPDATE
        SET locked_by = EXCLUDED.locked_by, expires_at = EXCLUDED.expires_at
        WHERE sm_machine_locks.expires_at <= NOW()
      RETURNING machine_id
    `,
    );
    return rows.length > 0 ? token : null;
  }

  async releaseLock(params: { id: string; token: StateMachineLockToken }): Promise<void> {
    await this.db
      .delete(machineLocks)
      .where(and(eq(machineLocks.machineId, params.id), eq(machineLocks.lockedBy, params.token)));
  }

  async extendLock(params: {
    id: string;
    token: StateMachineLockToken;
    durationMs: number;
  }): Promise<boolean> {
    const rows = await execRaw(
      this.db,
      sql`
      UPDATE sm_machine_locks
      SET expires_at = ${leaseExpiry(params.durationMs)}
      WHERE machine_id = ${params.id} AND locked_by = ${params.token} AND expires_at > NOW()
      RETURNING machine_id
    `,
    );
    return rows.length > 0;
  }
}

/** `durationMs` from now on the database clock. */
function leaseExpiry(durationMs: number) {
  return sql`NOW() + (${Math.max(0, Math.trunc(durationMs))}::double precision * INTERVAL '1 millisecond')`;
}
