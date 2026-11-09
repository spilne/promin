// ---------------------------------------------------------------------------
// PgStepQueue — Postgres SKIP LOCKED implementation of StepQueue
//
// Distributed step dispatch backed by a Postgres table. Workers claim tasks
// with SELECT FOR UPDATE SKIP LOCKED, so each pending task goes to exactly
// one claimer; concurrency-key admission is serialised per key with
// transaction-scoped advisory locks.
// ---------------------------------------------------------------------------

import { eq, and, sql, type SQL } from "drizzle-orm";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  DEFAULT_MAX_DELIVERIES,
  type StepQueue,
  type StepQueueClaimParams,
  type StepQueueCompleteParams,
  type StepQueueFailParams,
  type StepQueueHeartbeatParams,
  type StepQueueEnqueueParams,
  type StepQueueRequeueParams,
  type StepQueueRequeueResult,
  type StepGroupFields,
  type StepTask,
  type StepTaskRecord,
  type ThrottledTask,
} from "@promin/workflow/distributed";
import { deadLetterError, isStepGroupTask, StepGroupLedger } from "@promin/workflow/storage-kit";
import { type DrizzleDb, ensureTable as ensureTableFromSchema } from "@spilne/perfect-postgres";
import { execRaw } from "./exec-raw.ts";
import { assertPgLeaseCurrent } from "./pg-leader-lease-store.ts";
import { stepQueue } from "./schema.ts";

/**
 * Render a JS string[] as a Postgres `text[]` literal:
 *   ["foo", "bar"] → `'{"foo","bar"}'::text[]`
 *
 * Drizzle's parameter binding doesn't round-trip string[] cleanly for text[]
 * columns (it serializes the array into a single delimited string at bind
 * time). Embedding the literal as raw SQL avoids the binder entirely.
 * Values are escaped for the array-literal syntax (double quotes and
 * backslashes) and for the surrounding SQL string literal (single quotes).
 */
function textArrayLiteral(arr: readonly string[]): string {
  if (arr.length === 0) return `'{}'::text[]`;
  const escaped = arr.map(
    (s) => `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/'/g, "''")}"`,
  );
  return `'{${escaped.join(",")}}'::text[]`;
}

/**
 * Advisory-lock key for one `(scope, key)` pair, as SQL. The two-int form
 * keeps these locks out of the single-bigint key space other advisory-lock
 * users (leader election) draw from. The scope is length-prefixed so
 * `a:b` + `c` can't collide with `a` + `b:c`; a 32-bit hash collision only
 * serialises two unrelated keys.
 */
const CONCURRENCY_LOCK_CLASS = sql.raw(`hashtext('promin.wf_step_queue.concurrency')`);

const TASK_COLUMNS = sql.raw(
  `id, workflow_id, step_name, needs, priority, input, deps, depends_on, timeout_ms, ` +
    `attempt, deliveries, status, created_at, version, metadata, concurrency_key, ` +
    `concurrency_scope, concurrency_limit, group_name, group_max_concurrent, group_max_starts, ` +
    `group_window_ms, fairness_key, claim_token, claimed_by, claimed_at, heartbeat_at, ` +
    `heartbeat_details, completed_at, result, error, error_tag, step_metadata, duration_ms, run, consumed_at`,
);

export interface PgStepQueueConfig {
  db: DrizzleDb;
  /** Default namespace for task isolation. Null means unscoped. Default: null. */
  namespace?: string | null;
  /**
   * Deliveries after which `requeueStuck` dead-letters a task instead of
   * requeueing it. Default: `DEFAULT_MAX_DELIVERIES` (10).
   */
  maxDeliveries?: number;
  /**
   * Time source for client-side timestamps — claimedAt on claim, completedAt
   * on complete/fail, the stale cutoff on requeue. Default: `SystemWallClock`.
   * `metrics()` without `until` applies no upper bound, so app-vs-DB clock
   * skew can't drop just-stamped rows from the window.
   */
  clock?: WallClock;
}

type Row = Record<string, any>;

export class PgStepQueue implements StepQueue {
  private readonly db: DrizzleDb;
  private readonly namespace: string | null;
  private readonly maxDeliveries: number;
  private readonly clock: WallClock;

  constructor(config: PgStepQueueConfig) {
    this.db = config.db;
    this.namespace = config.namespace ?? null;
    this.maxDeliveries = config.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
    this.clock = config.clock ?? SystemWallClock;
  }

  /**
   * Get the Drizzle schema for the step queue table.
   * Use this to include in your migration pipeline.
   *
   * @example
   * ```ts
   * // In your drizzle schema file:
   * export { stepQueue } from "@promin/postgres";
   * // Or:
   * export const stepQueue = PgStepQueue.schema;
   * ```
   */
  static readonly schema = stepQueue;

  async enqueue(params: StepQueueEnqueueParams): Promise<string> {
    const ns = params.namespace ?? this.namespace;
    const priority = params.priority ?? 5;
    const needs = params.needs ?? [];
    const inputJson = params.input === undefined ? null : JSON.stringify(params.input);
    const depsJson = JSON.stringify(params.deps ?? {});
    const dependsOnLiteral = sql.raw(textArrayLiteral(params.dependsOn ?? []));
    const metadataJson = params.metadata === undefined ? null : JSON.stringify(params.metadata);
    const needsLiteral = sql.raw(textArrayLiteral(needs));
    // Idempotent on (workflow_id, step_name) via the partial unique index
    // `wf_step_queue_active_uniq`. DO UPDATE is a no-op self-assignment
    // that lets RETURNING surface the existing task id on conflict, so
    // callers always get an id back.
    const rows = await execRaw(
      this.db,
      sql`
        INSERT INTO wf_step_queue (
          workflow_id, step_name, namespace, needs, priority, input, deps, depends_on,
          timeout_ms, version, metadata, attempt, run, concurrency_key, concurrency_scope,
          concurrency_limit, group_name, group_max_concurrent, group_max_starts, group_window_ms,
          fairness_key
        )
        VALUES (
          ${params.workflowId},
          ${params.stepName},
          ${ns},
          ${needsLiteral},
          ${priority},
          ${inputJson}::jsonb,
          ${depsJson}::jsonb,
          ${dependsOnLiteral},
          ${params.timeoutMs ?? null},
          ${params.version ?? null},
          ${metadataJson}::jsonb,
          ${params.attempt ?? 1},
          ${params.run ?? 1},
          ${params.concurrencyKey ?? null},
          ${params.concurrencyScope ?? null},
          ${params.concurrencyLimit ?? null},
          ${params.groupName ?? null},
          ${params.groupMaxConcurrent ?? null},
          ${params.groupMaxStarts ?? null},
          ${params.groupWindowMs ?? null},
          ${params.fairnessKey ?? null}
        )
        ON CONFLICT (workflow_id, step_name) WHERE consumed_at IS NULL
        DO UPDATE SET workflow_id = wf_step_queue.workflow_id
        RETURNING id
      `,
    );
    const id = rows[0]?.id;
    if (id === undefined) {
      throw new Error("PgStepQueue.enqueue: no row returned from INSERT/UPSERT");
    }
    return String(id);
  }

  /**
   * Claim in one transaction:
   *
   * 1. Lock up to `limit` matching pending rows in claim order with
   *    `FOR UPDATE SKIP LOCKED`. Step names, versions, capabilities and the
   *    namespace are all in the WHERE clause, so rows this worker can't run
   *    are never taken (no head-of-line blocking). Keyed rows whose key is
   *    already full in this statement's snapshot are skipped up front.
   * 2. For the keyed candidates, take a transaction-scoped advisory lock per
   *    `(scope, key)`, in a fixed order. Every claimer admitting a keyed task
   *    holds that key's lock until commit, so admissions per key are
   *    serialised.
   * 3. Recount running tasks per key in a fresh statement — taken after the
   *    locks, so its snapshot includes every admission committed before us —
   *    and admit candidates in order while each key stays under its limit.
   * 4. Mark the admitted rows running. Rejected rows are just unlocked at
   *    commit.
   *
   * The snapshot-only check of the old single-statement claim let two
   * claimers with disjoint capabilities both see `running = 0` and both
   * admit under `limit = 1`.
   */
  async claim(params: StepQueueClaimParams): Promise<StepTask[]> {
    const limit = Math.max(1, Math.floor(params.limit));
    if (params.stepNames !== undefined && params.stepNames.length === 0) return [];

    // Capability filter: `needs <@ caps` = "every element of needs is in
    // caps." Empty caps still matches tasks with empty needs (∅ ⊆ ∅).
    const capsLiteral = sql.raw(textArrayLiteral(params.capabilities ?? []));
    const filters: SQL[] = [sql`q.status = 'pending'`, sql`q.needs <@ ${capsLiteral}`];
    if (this.namespace) filters.push(sql`q.namespace = ${this.namespace}`);
    if (params.stepNames !== undefined) {
      filters.push(sql`q.step_name = ANY(${sql.raw(textArrayLiteral(params.stepNames))})`);
    }
    if (params.versions !== undefined) {
      filters.push(
        sql`(q.version IS NULL OR q.version = ANY(${sql.raw(textArrayLiteral(params.versions))}))`,
      );
    }
    // Snapshot pre-check: skip keys that are already full. Only an
    // optimisation — the locked recount below is what enforces the cap.
    filters.push(sql`(
      q.concurrency_limit IS NULL OR q.concurrency_key IS NULL OR q.concurrency_scope IS NULL
      OR (
        SELECT COUNT(*) FROM wf_step_queue r
        WHERE r.status = 'running'
          AND r.concurrency_key IS NOT NULL
          AND r.concurrency_scope = q.concurrency_scope
          AND r.concurrency_key = q.concurrency_key
      ) < q.concurrency_limit
    )`);

    const now = this.clock.now();
    const claimedAt = now.toISOString();
    // Group snapshot pre-check, like the concurrency one above: a task whose
    // group is already out of slots or starts never takes a candidate slot,
    // so it cannot crowd out tasks behind it. The locked recount below is
    // what enforces the limits.
    filters.push(sql`(
      q.group_name IS NULL OR (
        (q.group_max_concurrent IS NULL OR (
          SELECT COUNT(*) FROM wf_step_queue r
          WHERE r.status = 'running' AND r.group_name = q.group_name
        ) < q.group_max_concurrent)
        AND (q.group_max_starts IS NULL OR q.group_window_ms IS NULL OR (
          SELECT COUNT(*) FROM wf_step_queue r
          WHERE r.group_name = q.group_name AND r.status <> 'pending'
            AND r.claimed_at > ${claimedAt}::timestamptz - q.group_window_ms * interval '1 millisecond'
        ) < q.group_max_starts)
      )
    )`);
    const where = sql.join(filters, sql` AND `);

    let admittedOrder: number[] = [];
    const rows = await this.db.transaction(async (txn) => {
      const tx = txn as unknown as DrizzleDb;

      // Fair order is only worth its scan of every pending row when some
      // pending task is in a group; otherwise the plain index-ordered
      // query below stops after `limit` rows.
      const fair =
        (
          await execRaw(
            tx,
            sql`SELECT 1 FROM wf_step_queue WHERE status = 'pending' AND group_name IS NOT NULL LIMIT 1`,
          )
        ).length > 0;

      // The status recheck on `q` is what makes a row another claimer took
      // after our snapshot drop out (READ COMMITTED EvalPlanQual).
      //
      // Fair order: within a priority, a keyed task of a group ranks at its
      // position among that `(group, fairness_key)`'s pending peers plus the
      // key's running tasks, so keys interleave round-robin and a key
      // already using the group yields to one that is not.
      const candidates = fair
        ? await execRaw(
            tx,
            sql`
              WITH running AS (
                SELECT group_name, fairness_key, COUNT(*) AS n
                FROM wf_step_queue
                WHERE status = 'running' AND group_name IS NOT NULL AND fairness_key IS NOT NULL
                GROUP BY group_name, fairness_key
              ),
              base AS (
                SELECT q.id, q.priority, q.created_at, q.group_name,
                  CASE WHEN q.group_name IS NOT NULL AND q.fairness_key IS NOT NULL THEN
                    ROW_NUMBER() OVER (
                      PARTITION BY q.group_name, q.fairness_key ORDER BY q.created_at, q.id
                    ) - 1 + COALESCE(run.n, 0)
                  ELSE 0 END AS fair_rank,
                  -- Slots the task's group has left, by this snapshot.
                  CASE WHEN q.group_name IS NULL THEN NULL ELSE LEAST(
                    COALESCE(q.group_max_concurrent - (
                      SELECT COUNT(*) FROM wf_step_queue r
                      WHERE r.status = 'running' AND r.group_name = q.group_name
                    ), 2147483647),
                    COALESCE(q.group_max_starts - (
                      SELECT COUNT(*) FROM wf_step_queue r
                      WHERE r.group_name = q.group_name AND r.status <> 'pending'
                        AND r.claimed_at > ${claimedAt}::timestamptz
                          - q.group_window_ms * interval '1 millisecond'
                    ), 2147483647)
                  ) END AS avail
                FROM wf_step_queue q
                LEFT JOIN running run
                  ON run.group_name = q.group_name AND run.fairness_key = q.fairness_key
                WHERE ${where}
              ),
              ranked AS (
                SELECT base.*,
                  CASE WHEN group_name IS NULL THEN 0 ELSE ROW_NUMBER() OVER (
                    PARTITION BY group_name ORDER BY priority DESC, fair_rank, created_at, id
                  ) END AS group_pos
                FROM base
              )
              SELECT q.id, q.concurrency_scope, q.concurrency_key, q.concurrency_limit,
                     q.group_name, q.group_max_concurrent, q.group_max_starts,
                     q.group_window_ms, q.fairness_key
              FROM wf_step_queue q
              JOIN ranked ON ranked.id = q.id
              -- Only as many of a group as it has slots for, so a group's
              -- backlog cannot fill the candidate slots and starve the rest.
              WHERE q.status = 'pending' AND ranked.group_pos <= COALESCE(ranked.avail, 2147483647)
              ORDER BY q.priority DESC, ranked.fair_rank ASC, q.created_at ASC, q.id ASC
              LIMIT ${limit}
              FOR UPDATE OF q SKIP LOCKED
            `,
          )
        : await execRaw(
            tx,
            sql`
              SELECT q.id, q.concurrency_scope, q.concurrency_key, q.concurrency_limit,
                     q.group_name, q.group_max_concurrent, q.group_max_starts,
                     q.group_window_ms, q.fairness_key
              FROM wf_step_queue q
              WHERE ${where}
              ORDER BY q.priority DESC, q.created_at ASC, q.id ASC
              LIMIT ${limit}
              FOR UPDATE OF q SKIP LOCKED
            `,
          );
      if (candidates.length === 0) return [];

      const keyOf = (c: Row): string | undefined =>
        c.concurrency_key != null && c.concurrency_scope != null && c.concurrency_limit != null
          ? // Code points, to match Postgres char_length() below.
            `${[...String(c.concurrency_scope)].length}:${c.concurrency_scope}:${c.concurrency_key}`
          : undefined;

      const admitted: number[] = [];
      const keys = [...new Set(candidates.map(keyOf).filter((k): k is string => k !== undefined))];
      const groupNames = [
        ...new Set(candidates.filter((c) => c.group_name != null).map((c) => String(c.group_name))),
      ];
      // Group admissions are serialised per group by the same advisory locks
      // (under a `group:` prefix no concurrency key can start with).
      const lockKeys = [...keys, ...groupNames.map((n) => `group:${n}`)];
      if (lockKeys.length === 0) {
        for (const c of candidates) admitted.push(Number(c.id));
      } else {
        // Lock in hash order so concurrent claimers can't deadlock.
        await execRaw(
          tx,
          sql`
            SELECT pg_advisory_xact_lock(${CONCURRENCY_LOCK_CLASS}, h)
            FROM (
              SELECT DISTINCT hashtext(k) AS h
              FROM unnest(${sql.raw(textArrayLiteral(lockKeys))}) AS k ORDER BY h
            ) s
          `,
        );
        const running = new Map<string, number>();
        if (keys.length > 0) {
          const keysLiteral = sql.raw(textArrayLiteral(keys));
          const counts = await execRaw(
            tx,
            sql`
              SELECT char_length(concurrency_scope) || ':' || concurrency_scope || ':' || concurrency_key AS k,
                     COUNT(*) AS n
              FROM wf_step_queue
              WHERE status = 'running'
                AND concurrency_key IS NOT NULL
                AND char_length(concurrency_scope) || ':' || concurrency_scope || ':' || concurrency_key
                    = ANY(${keysLiteral})
              GROUP BY 1
            `,
          );
          for (const r of counts) running.set(String(r.k), Number(r.n));
        }
        // Recounted after the locks, so the snapshot includes every group
        // admission committed before ours.
        const ledger = await this.groupLedger({ tx, now, candidates });
        for (const c of candidates) {
          const key = keyOf(c);
          const group = groupFields(c);
          if (key !== undefined && (running.get(key) ?? 0) >= Number(c.concurrency_limit)) continue;
          if (isStepGroupTask(group) && !ledger.check(group).ok) continue;
          if (key !== undefined) running.set(key, (running.get(key) ?? 0) + 1);
          if (isStepGroupTask(group)) ledger.admit(group);
          admitted.push(Number(c.id));
        }
      }
      if (admitted.length === 0) return [];
      admittedOrder = admitted;

      return execRaw(
        tx,
        sql`
          UPDATE wf_step_queue
          SET status = 'running',
              claimed_by = ${params.workerId},
              claimed_at = ${claimedAt}::timestamptz,
              claim_token = gen_random_uuid()::text,
              heartbeat_at = NULL,
              deliveries = deliveries + 1
          WHERE id = ANY(${sql.raw(`'{${admitted.join(",")}}'::bigint[]`)})
          RETURNING ${TASK_COLUMNS}
        `,
      );
    });

    // Claim order: priority, fairness rank, age (the order they were admitted in).
    const order = new Map(admittedOrder.map((id, i) => [id, i]));
    return rows
      .map((r) => toTask(r))
      .sort((a, b) => order.get(Number(a.id))! - order.get(Number(b.id))!);
  }

  /**
   * Running tasks and window starts of the groups in `candidates` (or of
   * `groups`), read inside `tx`.
   */
  private async groupLedger(params: {
    tx: DrizzleDb;
    now: Date;
    candidates: readonly Row[];
  }): Promise<StepGroupLedger> {
    const { tx, now, candidates } = params;
    const ledger = new StepGroupLedger(now.getTime());
    const grouped = candidates.filter((c) => c.group_name != null);
    if (grouped.length === 0) return ledger;
    const names = [...new Set(grouped.map((c) => String(c.group_name)))];
    const maxWindowMs = Math.max(0, ...grouped.map((c) => Number(c.group_window_ms ?? 0)));
    const cutoff = new Date(now.getTime() - maxWindowMs).toISOString();
    const rows = await execRaw(
      tx,
      sql`
        SELECT group_name, status, claimed_at
        FROM wf_step_queue
        WHERE group_name = ANY(${sql.raw(textArrayLiteral(names))})
          AND (status = 'running'
               OR (status <> 'pending' AND claimed_at > ${cutoff}::timestamptz))
      `,
    );
    for (const r of rows) {
      ledger.record({
        groupName: String(r.group_name),
        running: r.status === "running",
        ...(r.claimed_at != null && { startedAtMs: toDate(r.claimed_at)!.getTime() }),
      });
    }
    return ledger;
  }

  async throttled(params: { workflowId: string }): Promise<ThrottledTask[]> {
    const pending = await execRaw(
      this.db,
      sql`
        SELECT id, workflow_id, run, step_name, attempt, created_at, group_name,
               group_max_concurrent, group_max_starts, group_window_ms, fairness_key
        FROM wf_step_queue
        WHERE workflow_id = ${params.workflowId} AND status = 'pending' AND group_name IS NOT NULL
        ORDER BY id
      `,
    );
    if (pending.length === 0) return [];
    const ledger = await this.groupLedger({
      tx: this.db,
      now: this.clock.now(),
      candidates: pending,
    });
    const out: ThrottledTask[] = [];
    for (const r of pending) {
      const group = groupFields(r);
      if (!isStepGroupTask(group)) continue;
      const verdict = ledger.check(group);
      if (verdict.ok) continue;
      out.push({
        taskId: String(r.id),
        workflowId: r.workflow_id,
        run: Number(r.run ?? 1),
        stepName: r.step_name,
        attempt: Number(r.attempt),
        groupName: group.groupName,
        reason: verdict.reason,
        since: toDate(r.created_at)!,
        ...(verdict.retryAtMs !== undefined && { retryAt: new Date(verdict.retryAtMs) }),
      });
    }
    return out;
  }

  async release(params: { taskId: string; claimToken: string }): Promise<boolean> {
    const id = Number(params.taskId);
    if (!Number.isSafeInteger(id)) return false;
    const rows = await execRaw(
      this.db,
      sql`
        UPDATE wf_step_queue
        SET status = 'pending', claimed_by = NULL, claimed_at = NULL, claim_token = NULL,
            heartbeat_at = NULL, deliveries = GREATEST(deliveries - 1, 0)
        WHERE id = ${id} AND status = 'running' AND claim_token = ${params.claimToken}
        RETURNING id
      `,
    );
    return rows.length > 0;
  }

  async get(taskId: string): Promise<StepTaskRecord | undefined> {
    const id = Number(taskId);
    if (!Number.isSafeInteger(id)) return undefined;
    const rows = await execRaw(
      this.db,
      sql`SELECT ${TASK_COLUMNS} FROM wf_step_queue WHERE id = ${id}`,
    );
    return rows[0] ? toRecord(rows[0]) : undefined;
  }

  async complete(params: StepQueueCompleteParams): Promise<boolean> {
    const now = this.clock.now();
    const rows = await this.db
      .update(stepQueue)
      .set({
        status: "completed",
        result: params.result,
        stepMetadata: params.stepMetadata ?? null,
        durationMs: params.durationMs,
        completedAt: now,
      })
      .where(
        and(
          eq(stepQueue.id, Number(params.taskId)),
          eq(stepQueue.status, "running"),
          params.claimToken ? eq(stepQueue.claimToken, params.claimToken) : sql`true`,
        ),
      )
      .returning({ id: stepQueue.id });
    return rows.length > 0;
  }

  async fail(params: StepQueueFailParams): Promise<boolean> {
    const now = this.clock.now();
    const rows = await this.db
      .update(stepQueue)
      .set({
        status: "failed",
        error: params.error,
        errorTag: params.errorTag ?? null,
        stepMetadata: params.stepMetadata ?? null,
        durationMs: params.durationMs,
        completedAt: now,
      })
      .where(
        and(
          eq(stepQueue.id, Number(params.taskId)),
          eq(stepQueue.status, "running"),
          params.claimToken ? eq(stepQueue.claimToken, params.claimToken) : sql`true`,
        ),
      )
      .returning({ id: stepQueue.id });
    return rows.length > 0;
  }

  async heartbeat(params: StepQueueHeartbeatParams): Promise<boolean> {
    const rows = await this.db
      .update(stepQueue)
      .set({
        heartbeatAt: this.clock.now(),
        // No details keeps the stored ones (an omitted column is left alone).
        ...(params.details !== undefined && { heartbeatDetails: params.details }),
      })
      .where(
        and(
          eq(stepQueue.id, Number(params.taskId)),
          eq(stepQueue.status, "running"),
          params.claimToken ? eq(stepQueue.claimToken, params.claimToken) : sql`true`,
        ),
      )
      .returning({ id: stepQueue.id });
    return rows.length > 0;
  }

  /**
   * With `lease`, fences against the `wf_leader_leases` table in this same
   * database, so the lease must come from a `PgLeaderLeaseStore` (or
   * `PgSchedulerStorage`) on it; no extra config is needed.
   */
  async requeueStuck(params: StepQueueRequeueParams): Promise<StepQueueRequeueResult> {
    // postgres-js refuses to bind Date directly against an untyped
    // parameter; pass ISO strings and let Postgres cast them.
    const match =
      params.mode === "worker"
        ? sql`claimed_by = ${params.workerId}`
        : sql`COALESCE(heartbeat_at, claimed_at) < ${new Date(
            this.clock.currentTimeMs() - params.olderThanMs,
          ).toISOString()}::timestamptz`;
    const nsFilter = this.namespace ? sql` AND namespace = ${this.namespace}` : sql``;
    const now = this.clock.now().toISOString();
    const max = this.maxDeliveries;

    // SET expressions all read the pre-update row, so `deliveries >= max`
    // means the same thing in every column.
    const update = sql`
        UPDATE wf_step_queue
        SET status = CASE WHEN deliveries >= ${max} THEN 'failed' ELSE 'pending' END,
            error = CASE WHEN deliveries >= ${max} THEN ${deadLetterError(max)} ELSE error END,
            completed_at = CASE WHEN deliveries >= ${max} THEN ${now}::timestamptz ELSE NULL END,
            claimed_by = CASE WHEN deliveries >= ${max} THEN claimed_by ELSE NULL END,
            claimed_at = CASE WHEN deliveries >= ${max} THEN claimed_at ELSE NULL END,
            claim_token = NULL,
            heartbeat_at = NULL
        WHERE status = 'running' AND ${match}${nsFilter}
        RETURNING status
      `;
    const lease = params.lease;
    // Fenced: the lease check share-locks the lease row inside the same
    // transaction as the update, so a takeover waits for the sweep to
    // commit, or the sweep sees the new epoch and writes nothing.
    const rows = lease
      ? await this.db.transaction(async (tx) => {
          const db = tx as unknown as DrizzleDb;
          await assertPgLeaseCurrent({ db, lease });
          return execRaw(db, update);
        })
      : await execRaw(this.db, update);
    let deadLettered = 0;
    for (const r of rows) if (r.status === "failed") deadLettered++;
    return { requeued: rows.length - deadLettered, deadLettered };
  }

  async consume(params: { taskId: string }): Promise<boolean> {
    const id = Number(params.taskId);
    if (!Number.isSafeInteger(id)) return false;
    const rows = await execRaw(
      this.db,
      sql`
        UPDATE wf_step_queue SET consumed_at = ${this.clock.now().toISOString()}::timestamptz
        WHERE id = ${id}
          AND status IN ('completed', 'failed') AND consumed_at IS NULL
        RETURNING id
      `,
    );
    return rows.length > 0;
  }

  async consumeSettled(params: {
    workflowId: string;
    stepNames: readonly string[];
  }): Promise<number> {
    if (params.stepNames.length === 0) return 0;
    const rows = await execRaw(
      this.db,
      sql`
        UPDATE wf_step_queue SET consumed_at = ${this.clock.now().toISOString()}::timestamptz
        WHERE workflow_id = ${params.workflowId}
          AND step_name = ANY(${sql.raw(textArrayLiteral(params.stepNames))})
          AND status IN ('completed', 'failed') AND consumed_at IS NULL
        RETURNING id
      `,
    );
    return rows.length;
  }

  async purge(params: { completedBefore: Date }): Promise<number> {
    const nsFilter = this.namespace ? sql` AND namespace = ${this.namespace}` : sql``;
    const rows = await execRaw(
      this.db,
      sql`
        WITH purged AS (
          DELETE FROM wf_step_queue
          WHERE status IN ('completed', 'failed')
            AND completed_at < ${params.completedBefore.toISOString()}::timestamptz${nsFilter}
          RETURNING 1
        )
        SELECT COUNT(*) AS n FROM purged
      `,
    );
    return Number(rows[0]?.n ?? 0);
  }

  async metrics(params: { since: Date; until?: Date }): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
    avgWaitMs: number;
    avgExecMs: number;
    p95ExecMs: number;
  }> {
    // postgres-js refuses to bind Date directly against an untyped
    // parameter; pass ISO strings and let Postgres cast via ::timestamptz.
    const since = params.since.toISOString();
    // Default `until` ("now") is left unbounded rather than pinned to
    // either clock. Row stamps come from two sources: `created_at` is the
    // DB's `NOW()` (column default), while `claimed_at` / `completed_at` are
    // written from the app-side `clock`. Bounding by the app clock drops
    // just-inserted rows when the DB clock runs ahead; bounding by the DB's
    // `NOW()` drops just-completed rows when the app clock runs ahead (a
    // Docker VM clock lags the host by a few ms under load). Nothing can be
    // stamped after the query runs, so "no upper bound" is exactly "up to
    // now" on both clocks.
    const until = params.until?.toISOString();
    const inWindow = (column: "created_at" | "claimed_at" | "completed_at") => {
      const col = sql.raw(column);
      return until === undefined
        ? sql`${col} >= ${since}::timestamptz`
        : sql`${col} BETWEEN ${since}::timestamptz AND ${until}::timestamptz`;
    };
    // Status uses the column that defines membership-in-window: createdAt
    // for pending, claimedAt for running, completedAt for terminal.
    const nsFilter = this.namespace ? sql` AND namespace = ${this.namespace}` : sql``;

    const [counts, latency] = await Promise.all([
      execRaw(
        this.db,
        sql`
          SELECT status, COUNT(*) as count
          FROM wf_step_queue
          WHERE (
            (status = 'pending'   AND ${inWindow("created_at")}) OR
            (status = 'running'   AND ${inWindow("claimed_at")}) OR
            (status IN ('completed', 'failed') AND ${inWindow("completed_at")})
          )${nsFilter}
          GROUP BY status
        `,
      ),
      // Latency stats pool completed + failed in the window. EXTRACT EPOCH
      // returns seconds — multiply by 1000 for ms. percentile_cont is the
      // SQL standard linear-interp percentile, matching the in-memory
      // implementation.
      execRaw(
        this.db,
        sql`
          SELECT
            AVG(EXTRACT(EPOCH FROM (claimed_at - created_at)) * 1000) AS avg_wait_ms,
            AVG(duration_ms)                                           AS avg_exec_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)  AS p95_exec_ms
          FROM wf_step_queue
          WHERE status IN ('completed', 'failed')
            AND ${inWindow("completed_at")}
            ${nsFilter}
        `,
      ),
    ]);

    const result = {
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      avgWaitMs: 0,
      avgExecMs: 0,
      p95ExecMs: 0,
    };
    for (const row of counts) {
      const s = row.status as "pending" | "running" | "completed" | "failed";
      if (s === "pending" || s === "running" || s === "completed" || s === "failed") {
        result[s] = Number(row.count);
      }
    }
    const lat = latency[0];
    if (lat) {
      result.avgWaitMs = lat.avg_wait_ms != null ? Number(lat.avg_wait_ms) : 0;
      result.avgExecMs = lat.avg_exec_ms != null ? Number(lat.avg_exec_ms) : 0;
      result.p95ExecMs = lat.p95_exec_ms != null ? Number(lat.p95_exec_ms) : 0;
    }
    return result;
  }

  /**
   * Ensure the step queue table exists with all columns.
   * Derived from the Drizzle schema — single source of truth.
   * For production, prefer using migrations instead.
   */
  async ensureTable(): Promise<void> {
    // A table from before step groups lacks their columns, and the index
    // `ensureTableFromSchema` creates on them would fail: add them first.
    // Idempotent, and a no-op when the table does not exist yet.
    for (const [column, type] of [
      ["group_name", "text"],
      ["group_max_concurrent", "integer"],
      ["group_max_starts", "integer"],
      ["group_window_ms", "integer"],
      ["fairness_key", "text"],
      ["heartbeat_details", "jsonb"],
    ] as const) {
      await execRaw(
        this.db,
        sql.raw(`ALTER TABLE IF EXISTS wf_step_queue ADD COLUMN IF NOT EXISTS ${column} ${type}`),
      );
    }
    await ensureTableFromSchema(this.db, stepQueue);
  }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

function toDate(v: unknown): Date | undefined {
  if (v === null || v === undefined) return undefined;
  return v instanceof Date ? v : new Date(v as string);
}

function groupFields(r: Row): StepGroupFields {
  return {
    ...(r.group_name != null && { groupName: String(r.group_name) }),
    ...(r.group_max_concurrent != null && { groupMaxConcurrent: Number(r.group_max_concurrent) }),
    ...(r.group_max_starts != null && { groupMaxStarts: Number(r.group_max_starts) }),
    ...(r.group_window_ms != null && { groupWindowMs: Number(r.group_window_ms) }),
    ...(r.fairness_key != null && { fairnessKey: String(r.fairness_key) }),
  };
}

function toTask(r: Row): StepTask {
  return {
    id: String(r.id),
    workflowId: r.workflow_id,
    stepName: r.step_name,
    needs: (r.needs as string[]) ?? [],
    priority: r.priority ?? 5,
    input: r.input,
    deps: (r.deps as Record<string, unknown> | null) ?? {},
    dependsOn: (r.depends_on as string[] | null) ?? [],
    ...(r.timeout_ms != null && { timeoutMs: Number(r.timeout_ms) }),
    attempt: Number(r.attempt),
    run: Number(r.run ?? 1),
    deliveries: Number(r.deliveries),
    status: r.status,
    createdAt: toDate(r.created_at)!,
    claimToken: r.claim_token ?? undefined,
    version: r.version ?? undefined,
    metadata: (r.metadata as Record<string, unknown> | null) ?? undefined,
    concurrencyKey: r.concurrency_key ?? undefined,
    concurrencyScope: r.concurrency_scope ?? undefined,
    concurrencyLimit: r.concurrency_limit ?? undefined,
    ...(r.heartbeat_details != null && { lastHeartbeatDetails: r.heartbeat_details }),
    ...groupFields(r),
  };
}

function toRecord(r: Row): StepTaskRecord {
  return {
    ...toTask(r),
    claimedBy: r.claimed_by ?? undefined,
    claimedAt: toDate(r.claimed_at),
    heartbeatAt: toDate(r.heartbeat_at),
    completedAt: toDate(r.completed_at),
    result: r.result ?? undefined,
    error: r.error ?? undefined,
    errorTag: r.error_tag ?? undefined,
    stepMetadata: (r.step_metadata as Record<string, unknown> | null) ?? undefined,
    durationMs: r.duration_ms != null ? Number(r.duration_ms) : undefined,
    consumedAt: toDate(r.consumed_at),
  };
}
