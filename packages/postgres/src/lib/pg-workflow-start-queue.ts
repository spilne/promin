// ---------------------------------------------------------------------------
// `PgWorkflowStartQueue` — `WorkflowStartQueue` over Postgres for
// multi-replica deployments.
//
// Trigger requests, wake-ups and recovery sweeps land here and workers
// that host the workflow pick them up. Pg-backed so several Zorya
// replicas can enqueue and workers attached to any of them see the union.
//
// Semantics:
//   - one record per run: `workflow_id` is unique, so enqueueing a run that
//     is already queued adds nothing; on a claimed record it sets
//     `requeued`, and the claim's `complete` puts the start back to pending
//   - `SELECT … FOR UPDATE SKIP LOCKED` over the pending partition so
//     concurrent claimers don't deadlock or double-claim; the worker's
//     (name, versions) specs are a SQL predicate, so starts it can't serve
//     never fill its candidate window
//   - every claim stamps a fresh `claim_token` and `heartbeat_at`;
//     `heartbeat`, `complete` and `release` only act on the current token,
//     so a worker whose claim went stale can't touch the re-claimed start
//   - every time — claim, heartbeat, the reclaim cutoff, a release's
//     `available_at` — is the database's `NOW()`, so replicas and workers
//     on different hosts agree on which claims expired
// ---------------------------------------------------------------------------

import { and, asc, eq, or, sql, type SQL } from "drizzle-orm";
import {
  DEFAULT_START_RECLAIM_AFTER_MS,
  claimTokenFor,
  type WorkerWorkflowSpec,
  type WorkflowStartClaimParams,
  type WorkflowStartClaimRef,
  type WorkflowStartEnqueueParams,
  type WorkflowStartEnqueueResult,
  type WorkflowStartQueue,
  type WorkflowStartRecord,
  type WorkflowStartReleaseParams,
} from "@promin/workflow/distributed";
import { type DrizzleDb, ensureTable as ensureTableFromSchema } from "@spilne/perfect-postgres";
import { workflowStarts } from "./schema.ts";

export interface PgWorkflowStartQueueConfig {
  db: DrizzleDb;
  /**
   * A claim with no heartbeat for this many ms (by the database clock) is
   * re-claimable. Default: `DEFAULT_START_RECLAIM_AFTER_MS` (the default
   * run-lock lease).
   */
  reclaimAfterMs?: number;
}

type StartRow = typeof workflowStarts.$inferSelect;

export class PgWorkflowStartQueue implements WorkflowStartQueue {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = workflowStarts;

  private readonly db: DrizzleDb;
  private readonly reclaimAfterMs: number;

  constructor(config: PgWorkflowStartQueueConfig) {
    this.db = config.db;
    this.reclaimAfterMs = config.reclaimAfterMs ?? DEFAULT_START_RECLAIM_AFTER_MS;
  }

  /**
   * Idempotent CREATE TABLE for quick bootstrap (demos, tests). Production
   * deployments should run schema via Drizzle migrations instead.
   */
  async ensureTable(): Promise<void> {
    await ensureTableFromSchema(this.db, workflowStarts);
  }

  async enqueue(params: WorkflowStartEnqueueParams): Promise<WorkflowStartEnqueueResult> {
    const id = `start-${crypto.randomUUID()}`;
    // One statement: the insert, or — on an existing record, under its row
    // lock — the requeue mark (claimed) or the immediate availability
    // (pending). A concurrent `complete` serializes on the same row lock,
    // and a row it deletes meanwhile makes this insert a fresh record, so
    // the enqueue is never lost.
    const availableAt = params.keepAvailableAt
      ? sql`wf_workflow_starts.available_at`
      : sql`CASE WHEN wf_workflow_starts.status = 'pending' THEN NULL
                 ELSE wf_workflow_starts.available_at END`;
    const [row] = (await this.db.execute(sql`
      INSERT INTO wf_workflow_starts
        (id, workflow_id, workflow_name, version, input, metadata, enqueued_at, status)
      VALUES (${id}, ${params.workflowId}, ${params.workflowName}, ${params.version ?? null},
        ${JSON.stringify(params.input ?? null)}::jsonb,
        ${params.metadata !== undefined ? JSON.stringify(params.metadata) : null}::jsonb,
        NOW(), 'pending')
      ON CONFLICT (workflow_id) DO UPDATE SET
        requeued = wf_workflow_starts.status = 'claimed',
        available_at = ${availableAt}
      RETURNING id
    `)) as unknown as Array<{ id: string }>;
    const stored = row?.id ?? id;
    return { id: stored, created: stored === id };
  }

  async claim(params: WorkflowStartClaimParams): Promise<WorkflowStartRecord[]> {
    if (params.workflowSpecs.length === 0 || params.limit <= 0) return [];
    const claimId = params.claimId;
    if (claimId !== undefined) claimTokenFor({ claimId, index: 0 }); // validates the id

    return this.db.transaction(async (tx) => {
      // 1. Sweep stale claims back to pending.
      await this.sweepStale(tx as unknown as DrizzleDb);

      // 2. A retried call: the starts an earlier call with this id claimed.
      if (claimId !== undefined) {
        const already = (await tx
          .select()
          .from(workflowStarts)
          .where(
            and(
              eq(workflowStarts.status, "claimed"),
              sql`${workflowStarts.claimToken} LIKE ${`${claimId}:%`}`,
            ),
          )
          .orderBy(asc(workflowStarts.enqueuedAt), asc(workflowStarts.id))) as StartRow[];
        if (already.length > 0) return already.map(rowToRecord);
      }

      // 3. Pending, available rows the worker can run, oldest first. The
      //    (name, versions) match is part of the WHERE, so every row locked
      //    here is one this worker takes.
      const candidates = (await tx
        .select({ id: workflowStarts.id })
        .from(workflowStarts)
        .where(
          and(
            eq(workflowStarts.status, "pending"),
            sql`(${workflowStarts.availableAt} IS NULL OR ${workflowStarts.availableAt} <= NOW())`,
            specsPredicate(params.workflowSpecs),
          ),
        )
        .orderBy(asc(workflowStarts.enqueuedAt), asc(workflowStarts.id))
        .limit(params.limit)
        .for("update", { skipLocked: true })) as Array<{ id: string }>;
      if (candidates.length === 0) return [];

      // 4. One UPDATE stamps every row with its own token and the database
      //    time: zip the id and token arrays with unnest and join on id.
      const ids = candidates.map((c) => c.id);
      const tokens = ids.map((_, index) =>
        claimTokenFor({ ...(claimId !== undefined && { claimId }), index }),
      );
      const rows = (await tx.execute(sql`
        UPDATE wf_workflow_starts AS s
           SET status = 'claimed',
               available_at = NULL,
               requeued = false,
               claimed_at = NOW(),
               claimed_by = ${params.workerId ?? null},
               claim_token = v.token,
               heartbeat_at = NOW()
          FROM unnest(${sql.raw(textArrayLiteral(ids))}, ${sql.raw(textArrayLiteral(tokens))})
               AS v(id, token)
         WHERE s.id = v.id
        RETURNING s.*
      `)) as unknown as RawStartRow[];
      return [...rows].map(rawToRecord).sort((a, b) => a.enqueuedAt - b.enqueuedAt);
    });
  }

  async heartbeat(params: WorkflowStartClaimRef): Promise<boolean> {
    // A claim already past the reclaim window is lost even if no sweep
    // has moved it back to pending yet.
    const rows = await this.db
      .update(workflowStarts)
      .set({ heartbeatAt: sql`NOW()` })
      .where(
        and(
          eq(workflowStarts.id, params.id),
          eq(workflowStarts.status, "claimed"),
          eq(workflowStarts.claimToken, params.claimToken),
          sql`${workflowStarts.heartbeatAt} >= ${this.cutoff()}`,
        ),
      )
      .returning({ id: workflowStarts.id });
    return rows.length > 0;
  }

  async complete(params: WorkflowStartClaimRef): Promise<boolean> {
    // Fenced by the claim token: a stale claimant can't touch the row a
    // newer claimant is running. One statement under the row lock: a
    // requeued start goes back to pending, any other is deleted — an
    // enqueue racing it either lands its mark first (and this flips the
    // row) or finds the row gone (and inserts a fresh one).
    const [row] = (await this.db.execute(sql`
      WITH cur AS (
        SELECT id, requeued FROM wf_workflow_starts
        WHERE id = ${params.id} AND status = 'claimed' AND claim_token = ${params.claimToken}
        FOR UPDATE
      ),
      flipped AS (
        UPDATE wf_workflow_starts AS s
           SET status = 'pending', requeued = false, available_at = NULL, claimed_at = NULL,
               claimed_by = NULL, claim_token = NULL, heartbeat_at = NULL
          FROM cur WHERE s.id = cur.id AND cur.requeued
        RETURNING s.id
      ),
      deleted AS (
        DELETE FROM wf_workflow_starts AS s
         USING cur WHERE s.id = cur.id AND NOT cur.requeued
        RETURNING s.id
      )
      SELECT (SELECT count(*) FROM flipped) + (SELECT count(*) FROM deleted) AS n
    `)) as unknown as Array<{ n: number | string }>;
    return Number(row?.n ?? 0) > 0;
  }

  async release(params: WorkflowStartReleaseParams): Promise<boolean> {
    const delayMs = Math.max(0, Math.round(params.delayMs ?? 0));
    const rows = await this.db
      .update(workflowStarts)
      .set(pendingAgain(delayMs > 0 ? sql`NOW() + ${intervalMs(delayMs)}` : null))
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
      .orderBy(asc(workflowStarts.enqueuedAt), asc(workflowStarts.id));
    return rows.map(rowToRecord);
  }

  /** `NOW()` minus the reclaim window: claims last heartbeated before it are stale. */
  private cutoff(): SQL {
    return sql`NOW() - ${intervalMs(this.reclaimAfterMs)}`;
  }

  /** Move claims whose last heartbeat is past the reclaim window back to pending. */
  private async sweepStale(db: Pick<DrizzleDb, "update">): Promise<void> {
    await db
      .update(workflowStarts)
      .set(pendingAgain(null))
      .where(
        and(
          eq(workflowStarts.status, "claimed"),
          sql`${workflowStarts.heartbeatAt} < ${this.cutoff()}`,
        ),
      );
  }
}

/** The columns that put a row back to pending, claimable from `availableAt`. */
function pendingAgain(availableAt: SQL | null) {
  return {
    status: "pending",
    requeued: false,
    availableAt,
    claimedAt: null,
    claimedBy: null,
    claimToken: null,
    heartbeatAt: null,
  } as const;
}

/** `ms` milliseconds as a Postgres interval expression. */
function intervalMs(ms: number): SQL {
  return sql`(${Math.max(0, Math.round(ms))}::double precision * INTERVAL '1 millisecond')`;
}

/**
 * `(name = n1 AND (version IS NULL OR version = ANY(v1))) OR …` — a start
 * matches a spec when the names match and the start is versionless, the
 * spec lists no versions, or the start's version is listed.
 */
function specsPredicate(specs: readonly WorkerWorkflowSpec[]): SQL {
  const clauses = specs.map((spec) => {
    const byName = eq(workflowStarts.workflowName, spec.name);
    if (spec.versions.length === 0) return byName;
    const versions = sql.raw(textArrayLiteral(spec.versions));
    return and(
      byName,
      sql`(${workflowStarts.version} IS NULL OR ${workflowStarts.version} = ANY(${versions}))`,
    )!;
  });
  return clauses.length === 1 ? clauses[0]! : or(...clauses)!;
}

function rowToRecord(r: StartRow): WorkflowStartRecord {
  return {
    id: r.id,
    workflowId: r.workflowId,
    workflowName: r.workflowName,
    input: r.input,
    enqueuedAt: r.enqueuedAt.getTime(),
    ...(r.version !== null && { version: r.version }),
    ...(r.metadata !== null &&
      r.metadata !== undefined && { metadata: r.metadata as Record<string, unknown> }),
    ...(r.availableAt !== null && { availableAt: r.availableAt.getTime() }),
    ...(r.claimedAt !== null && { claimedAt: r.claimedAt.getTime() }),
    ...(r.claimedBy !== null && { claimedBy: r.claimedBy }),
    ...(r.claimToken !== null && { claimToken: r.claimToken }),
    ...(r.heartbeatAt !== null && { heartbeatAt: r.heartbeatAt.getTime() }),
    ...(r.requeued && { requeued: true }),
  };
}

/** A row as `RETURNING s.*` gives it (snake_case columns). */
interface RawStartRow {
  id: string;
  workflow_id: string;
  workflow_name: string;
  version: string | null;
  input: unknown;
  metadata: unknown;
  enqueued_at: Date | string;
  available_at: Date | string | null;
  claimed_at: Date | string | null;
  claimed_by: string | null;
  claim_token: string | null;
  heartbeat_at: Date | string | null;
  requeued: boolean;
}

function rawToRecord(r: RawStartRow): WorkflowStartRecord {
  const time = (v: Date | string | null) => (v === null ? null : new Date(v));
  return rowToRecord({
    id: r.id,
    workflowId: r.workflow_id,
    workflowName: r.workflow_name,
    version: r.version,
    input: typeof r.input === "string" ? JSON.parse(r.input) : r.input,
    metadata: typeof r.metadata === "string" ? JSON.parse(r.metadata) : r.metadata,
    enqueuedAt: new Date(r.enqueued_at),
    availableAt: time(r.available_at),
    claimedAt: time(r.claimed_at),
    claimedBy: r.claimed_by,
    claimToken: r.claim_token,
    heartbeatAt: time(r.heartbeat_at),
    requeued: r.requeued,
    status: "claimed",
  } as StartRow);
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
 * Values are escaped for the array literal (`"` and `\`) and for the
 * enclosing SQL string literal (`'`), so worker-supplied names and
 * versions can't break out of it.
 */
function textArrayLiteral(arr: readonly string[]): string {
  if (arr.length === 0) return `'{}'::text[]`;
  const escaped = arr.map((s) => `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`);
  return `'{${escaped.join(",").replace(/'/g, "''")}}'::text[]`;
}
