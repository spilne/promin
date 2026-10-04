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
//   - every claim stamps a fresh `claim_token` and `heartbeat_at`;
//     `heartbeat` and `complete` only act on the current token, so a
//     worker whose claim went stale can't touch the re-claimed start
//   - claims whose last heartbeat is older than `reclaimAfterMs` drop
//     back to pending so a crashed worker doesn't strand a start; the
//     sweep runs on every `claim()` and `list()`
// ---------------------------------------------------------------------------

import { and, asc, eq, sql } from "drizzle-orm";
import type {
  WorkerWorkflowSpec,
  WorkflowStartClaimRef,
  WorkflowStartQueue,
  WorkflowStartRecord,
} from "@promin/workflow";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { type DrizzleDb, ensureTable as ensureTableFromSchema } from "@spilne/perfect-postgres";
import { workflowStarts } from "./schema.ts";

export interface PgWorkflowStartQueueConfig {
  db: DrizzleDb;
  /** A claim with no heartbeat for this many ms is re-claimable. Default 60s. */
  reclaimAfterMs?: number;
  /**
   * Time source. Default: `SystemWallClock`. Tests pass a `FakeWallClock` for
   * deterministic stale-recovery scenarios.
   */
  clock?: WallClock;
}

export class PgWorkflowStartQueue implements WorkflowStartQueue {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = workflowStarts;

  private readonly db: DrizzleDb;
  private readonly reclaimAfterMs: number;
  private readonly clock: WallClock;

  constructor(config: PgWorkflowStartQueueConfig) {
    this.db = config.db;
    this.reclaimAfterMs = config.reclaimAfterMs ?? 60_000;
    this.clock = config.clock ?? SystemWallClock;
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
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }> {
    const id = generateStartId(this.clock.currentTimeMs());
    await this.db.insert(workflowStarts).values({
      id,
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      version: params.version ?? null,
      input: params.input as unknown,
      metadata: params.metadata !== undefined ? (params.metadata as unknown) : null,
      enqueuedAt: this.clock.now(),
      status: "pending",
    });
    return { id };
  }

  async claim(params: {
    workflowSpecs: readonly WorkerWorkflowSpec[];
    workerId?: string;
    limit: number;
  }): Promise<WorkflowStartRecord[]> {
    if (params.workflowSpecs.length === 0 || params.limit <= 0) return [];

    const claimed: WorkflowStartRecord[] = [];
    const now = this.clock.now();

    await this.db.transaction(async (tx) => {
      // 1. Sweep stale claims back to pending. The partial index on
      //    (heartbeat_at) WHERE status='claimed' makes this cheap.
      await this.sweepStale(tx);

      // 2. Find candidate pending rows whose workflow_name matches one
      //    the worker advertises. Version-set match is filtered in JS
      //    after the lock is acquired (same as SQLite impl).
      const names = params.workflowSpecs.map((s) => s.name);
      const namesLiteral = sql.raw(textArrayLiteral(names));
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
        version: string | null;
        input: unknown;
        metadata: unknown;
        enqueuedAt: Date;
      }>;

      const specByName = new Map<string, WorkerWorkflowSpec>();
      for (const spec of params.workflowSpecs) specByName.set(spec.name, spec);

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
        const stamped: WorkflowStartRecord = {
          id: row.id,
          workflowId: row.workflowId,
          workflowName: row.workflowName,
          input: row.input,
          enqueuedAt: row.enqueuedAt.getTime(),
          claimedAt: now.getTime(),
          heartbeatAt: now.getTime(),
          claimToken: crypto.randomUUID(),
          ...(row.version !== null && { version: row.version }),
          ...(row.metadata !== null &&
            row.metadata !== undefined && { metadata: row.metadata as Record<string, unknown> }),
          ...(params.workerId !== undefined && { claimedBy: params.workerId }),
        };
        claimed.push(stamped);
      }

      if (claimed.length > 0) {
        // One UPDATE stamps every row with its own token: zip the id and
        // token arrays with unnest and join on id.
        const idsLiteral = sql.raw(textArrayLiteral(claimed.map((r) => r.id)));
        const tokensLiteral = sql.raw(textArrayLiteral(claimed.map((r) => r.claimToken!)));
        const nowIso = now.toISOString();
        await tx.execute(sql`
          UPDATE wf_workflow_starts AS s
             SET status = 'claimed',
                 claimed_at = ${nowIso}::timestamptz,
                 claimed_by = ${params.workerId ?? null},
                 claim_token = v.token,
                 heartbeat_at = ${nowIso}::timestamptz
            FROM unnest(${idsLiteral}, ${tokensLiteral}) AS v(id, token)
           WHERE s.id = v.id
        `);
      }
    });

    return claimed;
  }

  async heartbeat(params: WorkflowStartClaimRef): Promise<boolean> {
    // A claim already past the reclaim window is lost even if no sweep
    // has moved it back to pending yet.
    const nowIso = this.clock.now().toISOString();
    const cutoff = this.cutoffIso();
    const rows = await this.db
      .update(workflowStarts)
      .set({ heartbeatAt: sql`${nowIso}::timestamptz` })
      .where(
        and(
          eq(workflowStarts.id, params.id),
          eq(workflowStarts.status, "claimed"),
          eq(workflowStarts.claimToken, params.claimToken),
          sql`${workflowStarts.heartbeatAt} >= ${cutoff}::timestamptz`,
        ),
      )
      .returning({ id: workflowStarts.id });
    return rows.length > 0;
  }

  async complete(params: WorkflowStartClaimRef): Promise<boolean> {
    // Hard delete, fenced by the claim token: a stale claimant can't
    // remove the row a newer claimant is running.
    const rows = await this.db
      .delete(workflowStarts)
      .where(
        and(
          eq(workflowStarts.id, params.id),
          eq(workflowStarts.status, "claimed"),
          eq(workflowStarts.claimToken, params.claimToken),
        ),
      )
      .returning({ id: workflowStarts.id });
    return rows.length > 0;
  }

  async list(): Promise<WorkflowStartRecord[]> {
    // Sweep stale claims first so the snapshot reflects the current
    // claimable set, matching the in-memory + Sqlite impls.
    await this.sweepStale(this.db);

    const rows = await this.db
      .select()
      .from(workflowStarts)
      .orderBy(asc(workflowStarts.enqueuedAt));
    return rows.map((r) => {
      const out: WorkflowStartRecord = {
        id: r.id,
        workflowId: r.workflowId,
        workflowName: r.workflowName,
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
      if (r.claimToken !== null) (out as { claimToken?: string }).claimToken = r.claimToken;
      if (r.heartbeatAt !== null) {
        (out as { heartbeatAt?: number }).heartbeatAt = r.heartbeatAt.getTime();
      }
      return out;
    });
  }

  /**
   * Reclaim cutoff as an ISO string. postgres-js refuses to bind a Date
   * against an untyped parameter, so callers cast it with `::timestamptz`
   * (same workaround as `PgStepQueue.requeueStuck`).
   */
  private cutoffIso(): string {
    return new Date(this.clock.currentTimeMs() - this.reclaimAfterMs).toISOString();
  }

  /** Move claims whose last heartbeat is past the reclaim window back to pending. */
  private async sweepStale(db: Pick<DrizzleDb, "update">): Promise<void> {
    await db
      .update(workflowStarts)
      .set({
        status: "pending",
        claimedAt: null,
        claimedBy: null,
        claimToken: null,
        heartbeatAt: null,
      })
      .where(
        and(
          eq(workflowStarts.status, "claimed"),
          sql`${workflowStarts.heartbeatAt} < ${this.cutoffIso()}::timestamptz`,
        ),
      );
  }
}

/** Globally-unique id per enqueue. Same shape as the SQLite impl. */
function generateStartId(nowMs: number): string {
  return `start-${nowMs.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Render a JS string[] as a Postgres `text[]` literal:
 *   ["foo", "bar"] → `'{"foo","bar"}'::text[]`
 *
 * postgres-js's parameter binding doesn't round-trip string[] cleanly
 * when the cast target is `text[]` — it sends the array as a single
 * delimited string and the receiver parses it as one element. Embedding
 * the literal as raw SQL avoids the binder entirely. Same workaround
 * used by `PgStepQueue` for its `needs` column.
 *
 * Values come from code (workflow names + ids), not user input, so the
 * literal is safe as long as we escape the two special chars (" and \).
 */
function textArrayLiteral(arr: readonly string[]): string {
  if (arr.length === 0) return `'{}'::text[]`;
  const escaped = arr.map((s) => `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`);
  return `'{${escaped.join(",")}}'::text[]`;
}
