// ---------------------------------------------------------------------------
// PostgresWorkerRegistry — Postgres-backed WorkerRegistry
//
// Provides the durable view of the worker fleet that horizontally-scaled
// Zorya (or any other observer) needs. All instances share the same table,
// so `/api/workers` returns the same set regardless of which Zorya replica
// the request hits. Matches InMemoryWorkerRegistry's semantics exactly —
// same idempotency on register, same void-on-missing for heartbeat / drain
// / deregister, same atomic transition-to-dead in detectDead.
// ---------------------------------------------------------------------------

import { eq, sql } from "drizzle-orm";
import type { WorkerRegistry, WorkerInfo, WorkerStatus } from "@promin/workflow";
import { workerRegistry } from "./schema.ts";
import type { PostgresStorageConfig } from "./config.ts";
import { resolveConfig } from "./config.ts";

export class PostgresWorkerRegistry implements WorkerRegistry {
  /** Drizzle schema for migration pipelines that include the workflow tables. */
  static readonly schema = { workerRegistry };

  private readonly config: Required<PostgresStorageConfig>;

  private constructor(config: Required<PostgresStorageConfig>) {
    this.config = config;
  }

  /**
   * Construct a PostgresWorkerRegistry. No lookup seeding required — the
   * table has no enum dependencies, just a CHECK constraint on status.
   */
  static async create(config: PostgresStorageConfig): Promise<PostgresWorkerRegistry> {
    return new PostgresWorkerRegistry(resolveConfig(config));
  }

  private get db() {
    return this.config.db;
  }

  async register(params: {
    workerId: string;
    capabilities: readonly string[];
    concurrency: number;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    // Matches InMemory semantics: re-registering the same workerId
    // replaces the prior row. That's the right thing for a worker that
    // restarts with the same id — it re-declares caps / concurrency
    // and bumps startedAt + heartbeat to now.
    await this.db
      .insert(workerRegistry)
      .values({
        workerId: params.workerId,
        status: "active",
        capabilities: [...params.capabilities],
        concurrency: params.concurrency,
        metadata: params.metadata ?? null,
      })
      .onConflictDoUpdate({
        target: workerRegistry.workerId,
        set: {
          status: "active",
          capabilities: [...params.capabilities],
          concurrency: params.concurrency,
          metadata: params.metadata ?? null,
          // Use server-side NOW() to avoid app/DB clock skew on the
          // startedAt reset. A re-register is conceptually a new
          // lifecycle, so startedAt is refreshed alongside heartbeat.
          startedAt: sql`NOW()`,
          lastHeartbeatAt: sql`NOW()`,
        },
      });
  }

  async heartbeat(workerId: string): Promise<void> {
    // Single-row UPDATE on PK — the hot path. Silently no-ops on
    // missing worker (matches InMemory: a heartbeat from an unknown
    // worker is dropped, not an error). Server-side NOW() so heartbeat
    // freshness isn't at the mercy of the worker's wall clock.
    await this.db
      .update(workerRegistry)
      .set({ lastHeartbeatAt: sql`NOW()` })
      .where(eq(workerRegistry.workerId, workerId));
  }

  async drain(workerId: string): Promise<void> {
    await this.db
      .update(workerRegistry)
      .set({ status: "draining" })
      .where(eq(workerRegistry.workerId, workerId));
  }

  async deregister(workerId: string): Promise<void> {
    // Retire, don't delete — the row stays for forensics until gc() reaps
    // it. Server-side NOW() so retiredAt isn't at the mercy of the
    // worker's wall clock. No-ops on a missing worker.
    await this.db
      .update(workerRegistry)
      .set({ status: "retired", retiredAt: sql`NOW()` })
      .where(eq(workerRegistry.workerId, workerId));
  }

  async list(params?: { status?: WorkerStatus }): Promise<WorkerInfo[]> {
    const rows = params?.status
      ? await this.db.select().from(workerRegistry).where(eq(workerRegistry.status, params.status))
      : await this.db.select().from(workerRegistry);
    return rows.map(rowToWorkerInfo);
  }

  async detectDead(timeoutMs: number): Promise<WorkerInfo[]> {
    // Atomic transition: any active / draining row whose heartbeat is
    // older than `timeoutMs` flips to 'dead' in a single UPDATE ...
    // RETURNING. Skips 'dead' and 'retired' — a retired worker stopped
    // on purpose and must not be relabelled a crash.
    const rows = await this.db
      .update(workerRegistry)
      .set({ status: "dead" })
      .where(
        sql`${workerRegistry.status} NOT IN ('dead', 'retired') AND ${workerRegistry.lastHeartbeatAt} < NOW() - ${timeoutMs} * INTERVAL '1 millisecond'`,
      )
      .returning();
    return rows.map(rowToWorkerInfo);
  }

  async gc(params: { retainMs: number }): Promise<number> {
    // Reap on the most-recent activity: retired_at when the worker
    // retired, otherwise its last heartbeat. One DELETE ... RETURNING.
    const rows = await this.db
      .delete(workerRegistry)
      .where(
        sql`COALESCE(${workerRegistry.retiredAt}, ${workerRegistry.lastHeartbeatAt}) < NOW() - ${params.retainMs} * INTERVAL '1 millisecond'`,
      )
      .returning({ workerId: workerRegistry.workerId });
    return rows.length;
  }
}

function rowToWorkerInfo(row: typeof workerRegistry.$inferSelect): WorkerInfo {
  return {
    workerId: row.workerId,
    status: row.status as WorkerStatus,
    capabilities: row.capabilities ?? [],
    concurrency: row.concurrency ?? 1,
    lastHeartbeat: row.lastHeartbeatAt,
    startedAt: row.startedAt,
    ...(row.retiredAt ? { retiredAt: row.retiredAt } : {}),
    metadata: (row.metadata as Record<string, unknown> | null) ?? undefined,
  };
}
