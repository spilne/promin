// ---------------------------------------------------------------------------
// `PgWorkflowStartQueue` — `WorkflowStartQueue` over Postgres for
// multi-replica deployments.
//
// In split mode (dashboard server doesn't run workflows itself), trigger
// requests land here and workflow-mode workers pick them up. Pg-backed
// so two Zorya replicas can both enqueue; workers connected to either
// see the union.
//
// Claim semantics:
//   - `SELECT … FOR UPDATE SKIP LOCKED` over the pending partition so
//     concurrent claimers don't deadlock or double-claim
//   - the worker's `WorkerWorkflowSpec` versions filter is applied in
//     JS (same shape as the SQLite impl) — it's a small set match
//     that doesn't compose cleanly into a portable SQL predicate
//   - stale claims (worker died mid-execution) drop back to pending
//     after `reclaimAfterMs` so a crashed worker doesn't strand a
//     start; sweep runs on every `claim()` and `list()`
// ---------------------------------------------------------------------------

import { and, asc, eq, sql, type SQL } from "drizzle-orm";
import type { WorkerWorkflowSpec, WorkflowStartQueue, WorkflowStartRecord } from "@promin/workflow";
import { SystemClock, type Clock } from "@promin/core";
import type { DrizzleDb } from "./drizzle-db.ts";
import { workflowStarts } from "./schema.ts";
import { ensureTable as ensureTableFromSchema } from "./schema-utils.ts";

export interface PgWorkflowStartQueueConfig {
  db: DrizzleDb;
  /** Worker stuck mid-execution — re-claimable after this many ms. Default 60s. */
  reclaimAfterMs?: number;
  /**
   * Time source. Default: `SystemClock`. Tests pass a `FakeClock` for
   * deterministic stale-recovery scenarios.
   */
  clock?: Clock;
}

export class PgWorkflowStartQueue implements WorkflowStartQueue {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = workflowStarts;

  private readonly db: DrizzleDb;
  private readonly reclaimAfterMs: number;
  private readonly clock: Clock;

  constructor(config: PgWorkflowStartQueueConfig) {
    this.db = config.db;
    this.reclaimAfterMs = config.reclaimAfterMs ?? 60_000;
    this.clock = config.clock ?? SystemClock;
  }

  /**
   * Idempotent CREATE TABLE for quick bootstrap (demos, tests). Production
   * deployments should run schema via Drizzle migrations instead.
   */
  async ensureTable(): Promise<void> {
    await ensureTableFromSchema(this.db, workflowStarts);
  }

  async enqueue(params: {
    workflowId: string;
    workflowName: string;
    namespace?: string;
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }> {
    const id = generateStartId(this.clock.currentTimeMs());
    const inserted = await this.db
      .insert(workflowStarts)
      .values({
        id,
        workflowId: params.workflowId,
        workflowName: params.workflowName,
        namespace: params.namespace ?? null,
        version: params.version ?? null,
        input: params.input as unknown,
        metadata: params.metadata !== undefined ? (params.metadata as unknown) : null,
        enqueuedAt: this.clock.now(),
        status: "pending",
      })
      .onConflictDoNothing({ target: workflowStarts.workflowId })
      .returning({ id: workflowStarts.id });
    if (inserted[0]) return { id: inserted[0].id };

    const [existing] = await this.db
      .select({ id: workflowStarts.id })
      .from(workflowStarts)
      .where(eq(workflowStarts.workflowId, params.workflowId))
      .limit(1);
    return { id: existing?.id ?? id };
  }

  async claim(params: {
    workflowSpecs: readonly WorkerWorkflowSpec[];
    workerId?: string;
    limit: number;
  }): Promise<WorkflowStartRecord[]> {
    if (params.workflowSpecs.length === 0 || params.limit <= 0) return [];

    const claimed: WorkflowStartRecord[] = [];
    const now = this.clock.now();
    const cutoff = new Date(this.clock.currentTimeMs() - this.reclaimAfterMs).toISOString();

    // postgres-js refuses to bind Date directly against an untyped
    // parameter; same workaround as `PgStepQueue.requeueStuck` —
    // ISO-string + ::timestamptz cast.
    await this.db.transaction(async (tx) => {
      // 1. Sweep stale claims back to pending. The partial index on
      //    (claimed_at) WHERE status='claimed' makes this cheap.
      await tx
        .update(workflowStarts)
        .set({ status: "pending", claimedAt: null, claimedBy: null })
        .where(
          and(
            eq(workflowStarts.status, "claimed"),
            sql`${workflowStarts.claimedAt} < ${cutoff}::timestamptz`,
          ),
        );

      // 2. Find candidate pending rows whose workflow_name matches one
      //    the worker advertises. Version-set match is filtered in JS
      //    after the lock is acquired (same as SQLite impl).
      const names = params.workflowSpecs.map((s) => s.name);
      const namesLiteral = textArrayParam(names);
      // We need a window larger than `limit` ONLY when the caller has
      // version-pinned specs (one of `spec.versions` is non-empty) —
      // those filter rows out in JS so we have to fetch extras. When no
      // version filtering is in play we ask for exactly `limit`, which
      // matters for concurrent saturation: every row we lock here is
      // unavailable to other parallel claimers until this txn commits,
      // so over-locking turns into starvation. The conformance test
      // "100 workers race 100 starts → exactly 100 claims" exercises
      // this directly.
      const hasVersionFilter = params.workflowSpecs.some((s) => s.versions.length > 0);
      const window = hasVersionFilter ? Math.min(params.limit * 4, 200) : params.limit;
      // SELECT ... FOR UPDATE SKIP LOCKED — concurrent claimers see
      // disjoint candidate sets via PG row-level locking.
      const candidates = (await tx
        .select()
        .from(workflowStarts)
        .where(
          and(
            eq(workflowStarts.status, "pending"),
            sql`${workflowStarts.workflowName} = ANY(${namesLiteral})`,
          ),
        )
        .orderBy(asc(workflowStarts.enqueuedAt))
        .limit(window)
        .for("update", { skipLocked: true })) as Array<{
        id: string;
        workflowId: string;
        workflowName: string;
        namespace: string | null;
        version: string | null;
        input: unknown;
        metadata: unknown;
        enqueuedAt: Date;
      }>;

      const specByName = new Map<string, WorkerWorkflowSpec>();
      for (const spec of params.workflowSpecs) specByName.set(spec.name, spec);

      const idsToClaim: string[] = [];
      for (const row of candidates) {
        if (claimed.length >= params.limit) break;
        const spec = specByName.get(row.workflowName);
        if (!spec) continue;
        // Version match: versionless rows always match; pinned rows
        // only match when the spec advertises that exact version OR
        // an empty versions list ("any").
        if (
          row.version !== null &&
          spec.versions.length > 0 &&
          !spec.versions.includes(row.version)
        ) {
          continue;
        }
        idsToClaim.push(row.id);
        const stamped: WorkflowStartRecord = {
          id: row.id,
          workflowId: row.workflowId,
          workflowName: row.workflowName,
          ...(row.namespace !== null && { namespace: row.namespace }),
          input: row.input,
          enqueuedAt: row.enqueuedAt.getTime(),
          claimedAt: now.getTime(),
          ...(row.version !== null && { version: row.version }),
          ...(row.metadata !== null &&
            row.metadata !== undefined && { metadata: row.metadata as Record<string, unknown> }),
          ...(params.workerId !== undefined && { claimedBy: params.workerId }),
        };
        claimed.push(stamped);
      }

      if (idsToClaim.length > 0) {
        const idsLiteral = textArrayParam(idsToClaim);
        await tx
          .update(workflowStarts)
          .set({
            status: "claimed",
            claimedAt: now,
            claimedBy: params.workerId ?? null,
          })
          .where(sql`${workflowStarts.id} = ANY(${idsLiteral})`);
      }
    });

    return claimed;
  }

  async complete(id: string): Promise<void> {
    // Hard delete — matches the in-memory + Sqlite contract. Once a
    // start is completed the queue no longer needs the row, and
    // keeping a 'completed' status would just bloat the table.
    await this.db.delete(workflowStarts).where(eq(workflowStarts.id, id));
  }

  async list(): Promise<WorkflowStartRecord[]> {
    // Sweep stale claims first so the snapshot reflects the current
    // claimable set, matching the in-memory + Sqlite impls.
    const cutoff = new Date(this.clock.currentTimeMs() - this.reclaimAfterMs).toISOString();
    await this.db
      .update(workflowStarts)
      .set({ status: "pending", claimedAt: null, claimedBy: null })
      .where(
        and(
          eq(workflowStarts.status, "claimed"),
          sql`${workflowStarts.claimedAt} < ${cutoff}::timestamptz`,
        ),
      );

    const rows = await this.db
      .select()
      .from(workflowStarts)
      .orderBy(asc(workflowStarts.enqueuedAt));
    return rows.map((r) => {
      const out: WorkflowStartRecord = {
        id: r.id,
        workflowId: r.workflowId,
        workflowName: r.workflowName,
        ...(r.namespace !== null && { namespace: r.namespace }),
        input: r.input,
        enqueuedAt: r.enqueuedAt.getTime(),
      };
      if (r.version !== null) (out as { version?: string }).version = r.version;
      if (r.metadata !== null && r.metadata !== undefined) {
        (out as { metadata?: Record<string, unknown> }).metadata = r.metadata as Record<
          string,
          unknown
        >;
      }
      if (r.claimedAt !== null) (out as { claimedAt?: number }).claimedAt = r.claimedAt.getTime();
      if (r.claimedBy !== null) (out as { claimedBy?: string }).claimedBy = r.claimedBy;
      return out;
    });
  }
}

/** Globally-unique id per enqueue. Same shape as the SQLite impl. */
function generateStartId(nowMs: number): string {
  return `start-${nowMs.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function textArrayParam(arr: readonly string[]): SQL {
  if (arr.length === 0) return sql`ARRAY[]::text[]`;
  return sql`ARRAY[${sql.join(
    arr.map((value) => sql`${value}`),
    sql`, `,
  )}]::text[]`;
}
