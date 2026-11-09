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
import { notifyQuietly, type Wakeups } from "@promin/workflow/distributed";
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
import { STEP_QUEUE_LANE_SQL, stepQueue } from "./schema.ts";

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
  /**
   * How `claim` finds candidates. `"lanes"` (default) probes each concurrency
   * or group lane's head, so a deep throttled backlog costs nothing; `"scan"`
   * is the original index-ordered scan with per-row admission pre-checks
   * (kept as the reference the lane path is tested against).
   */
  claimStrategy?: "lanes" | "scan";
  /**
   * Told when a task becomes claimable (`"steps"`: enqueue, release, requeue,
   * and a settle that frees a concurrency or group slot) and when one settles
   * (`"results"`, keyed by task id), so workers and waiting coordinators can
   * skip the rest of their poll interval. A latency hint sent after the
   * statement commits; polling stays the correctness path.
   */
  wakeups?: Wakeups;
}

type Row = Record<string, any>;

export class PgStepQueue implements StepQueue {
  private readonly db: DrizzleDb;
  private readonly namespace: string | null;
  private readonly maxDeliveries: number;
  private readonly clock: WallClock;
  private readonly claimStrategy: "lanes" | "scan";
  private readonly wakeups: Wakeups | undefined;

  constructor(config: PgStepQueueConfig) {
    this.db = config.db;
    this.namespace = config.namespace ?? null;
    this.maxDeliveries = config.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
    this.clock = config.clock ?? SystemWallClock;
    this.claimStrategy = config.claimStrategy ?? "lanes";
    this.wakeups = config.wakeups;
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
    notifyQuietly(this.wakeups, { topic: "steps" });
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
    const now = this.clock.now();
    const claimedAt = now.toISOString();
    // Row filters: what this worker can run. Concern one row only.
    const rowFilters = (a: SQL): SQL[] => {
      const f: SQL[] = [sql`${a}.status = 'pending'`, sql`${a}.needs <@ ${capsLiteral}`];
      if (this.namespace) f.push(sql`${a}.namespace = ${this.namespace}`);
      if (params.stepNames !== undefined) {
        f.push(sql`${a}.step_name = ANY(${sql.raw(textArrayLiteral(params.stepNames))})`);
      }
      if (params.versions !== undefined) {
        f.push(
          sql`(${a}.version IS NULL OR ${a}.version = ANY(${sql.raw(textArrayLiteral(params.versions))}))`,
        );
      }
      return f;
    };
    // Admission pre-checks (snapshot only: an optimisation — the locked
    // recounts below enforce the caps). They read only a row's lane columns
    // (namespace, concurrency key and limit, group fields), so the lane path
    // evaluates them once per lane instead of once per backlog row.
    //
    // A keyed task whose key is already full is skipped up front.
    const concurrencyPrecheck = (a: SQL): SQL => sql`(
      ${a}.concurrency_limit IS NULL OR ${a}.concurrency_key IS NULL OR ${a}.concurrency_scope IS NULL
      OR (
        SELECT COUNT(*) FROM wf_step_queue r
        WHERE r.status = 'running'
          AND r.concurrency_key IS NOT NULL
          AND r.concurrency_scope = ${a}.concurrency_scope
          AND r.concurrency_key = ${a}.concurrency_key
      ) < ${a}.concurrency_limit
    )`;
    // Group pre-check, likewise: a task whose group is already out of slots
    // or starts never takes a candidate slot, so it cannot crowd out tasks
    // behind it.
    const groupPrecheck = (a: SQL): SQL => sql`(
      ${a}.group_name IS NULL OR (
        (${a}.group_max_concurrent IS NULL OR (
          SELECT COUNT(*) FROM wf_step_queue r
          WHERE r.status = 'running' AND r.group_name = ${a}.group_name
        ) < ${a}.group_max_concurrent)
        AND (${a}.group_max_starts IS NULL OR ${a}.group_window_ms IS NULL OR (
          SELECT COUNT(*) FROM wf_step_queue r
          WHERE r.group_name = ${a}.group_name AND r.status <> 'pending'
            AND r.claimed_at > ${claimedAt}::timestamptz - ${a}.group_window_ms * interval '1 millisecond'
        ) < ${a}.group_max_starts)
      )
    )`;
    const q = sql.raw("q");
    const where = sql.join(
      [...rowFilters(q), concurrencyPrecheck(q), groupPrecheck(q)],
      sql` AND `,
    );

    let admittedOrder: number[] = [];
    const rows = await this.db.transaction(async (txn) => {
      const tx = txn as unknown as DrizzleDb;
      // The claim's statements run in well under a millisecond but carry
      // subplans the planner costs high enough to JIT-compile (~30 ms each).
      if (this.claimStrategy === "lanes") await execRaw(tx, sql`SET LOCAL jit = off`);

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

      // Fair order: within a priority, a keyed task of a group ranks at its
      // position among that `(group, fairness_key)`'s pending peers plus the
      // key's running tasks, so keys interleave round-robin and a key
      // already using the group yields to one that is not. `source` is the
      // pending rows to rank: all of them, or the lane heads.
      const fairRanked = (source: SQL): SQL => sql`
        running AS (
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
          FROM ${source} q
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
        )`;

      // The original claim: one index-ordered scan of every pending row,
      // admission pre-checks per row. Cost grows with a throttled backlog.
      //
      // The status recheck on `q` is what makes a row another claimer took
      // after our snapshot drop out (READ COMMITTED EvalPlanQual).
      const scanCandidates = async (): Promise<Row[]> =>
        fair
          ? execRaw(
              tx,
              sql`
              WITH ${fairRanked(sql`wf_step_queue`)}
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
          : execRaw(
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

      // Lane path. A lane is the set of pending rows sharing every column the
      // pre-checks and fair ranking read (`lane`, a generated column). Rows of
      // one lane are admitted or throttled together, and only its first
      // `wide` rows by claim order can be among the next claimed, so a lane
      // costs one pre-check plus a bounded index range, never its depth.
      //
      // The lane values go into the probe as parameters (one subquery per
      // lane) rather than a lateral join over them: with the value known the
      // planner uses the lane index even for a lane that holds nearly the
      // whole table, where a lateral falls back to scanning the global order.
      const LANES_PER_PROBE = 64;
      const laneHeadIds = async (p: { wide: number; skip: number[] }): Promise<number[]> => {
        const skipLiteral = sql.raw(`'{${p.skip.join(",")}}'::bigint[]`);
        // Every lane with pending rows, by a skip scan of the lane index that
        // carries each lane's first row; keep the lanes that pass the
        // admission pre-checks.
        const l = sql.raw("l");
        const laneRows = await execRaw(
          tx,
          sql`
            WITH RECURSIVE lanes AS (
              (SELECT n.lane, n.namespace, n.concurrency_scope, n.concurrency_key, n.concurrency_limit,
                      n.group_name, n.group_max_concurrent, n.group_max_starts, n.group_window_ms
               FROM wf_step_queue n WHERE n.status = 'pending' ORDER BY n.lane LIMIT 1)
              UNION ALL
              SELECT n.lane, n.namespace, n.concurrency_scope, n.concurrency_key, n.concurrency_limit,
                     n.group_name, n.group_max_concurrent, n.group_max_starts, n.group_window_ms
              FROM lanes l CROSS JOIN LATERAL (
                SELECT x.* FROM wf_step_queue x
                WHERE x.status = 'pending' AND x.lane > l.lane ORDER BY x.lane LIMIT 1
              ) n
            )
            SELECT l.lane FROM lanes l
            WHERE ${sql.join(
              [
                ...(this.namespace ? [sql`l.namespace = ${this.namespace}`] : []),
                concurrencyPrecheck(l),
                groupPrecheck(l),
              ],
              sql` AND `,
            )}
          `,
        );
        const ids: number[] = [];
        for (let i = 0; i < laneRows.length; i += LANES_PER_PROBE) {
          const probes = laneRows.slice(i, i + LANES_PER_PROBE).map(
            (r) => sql`(
              SELECT h.id FROM wf_step_queue h
              WHERE h.lane = ${String(r.lane)} AND ${sql.join(rowFilters(sql.raw("h")), sql` AND `)}
                AND h.id <> ALL(${skipLiteral})
              ORDER BY h.priority DESC, h.created_at ASC, h.id ASC
              LIMIT ${p.wide}
            )`,
          );
          const rows = await execRaw(tx, sql.join(probes, sql` UNION ALL `));
          for (const r of rows) ids.push(Number(r.id));
        }
        return ids;
      };

      // Eligible ids in claim order (a superset of the next `need`, to
      // survive rows other claimers lock meanwhile); the locking step takes
      // the first unlocked ones.
      const candidateIds = async (p: { need: number; wide: number; skip: number[] }) => {
        const skipLiteral = sql.raw(`'{${p.skip.join(",")}}'::bigint[]`);
        const rank = async (ids: number[]): Promise<number[]> => {
          if (ids.length === 0) return [];
          const source = sql`(SELECT * FROM wf_step_queue WHERE id = ANY(${sql.raw(`'{${ids.join(",")}}'::bigint[]`)}))`;
          const rows = fair
            ? await execRaw(
                tx,
                sql`
                  WITH ${fairRanked(source)}
                  SELECT ranked.id FROM ranked
                  WHERE ranked.group_pos <= COALESCE(ranked.avail, 2147483647)
                  ORDER BY ranked.priority DESC, ranked.fair_rank ASC, ranked.created_at ASC, ranked.id ASC
                  LIMIT ${p.wide}
                `,
              )
            : await execRaw(
                tx,
                sql`
                  SELECT q.id FROM ${source} q
                  ORDER BY q.priority DESC, q.created_at ASC, q.id ASC
                  LIMIT ${p.wide}
                `,
              );
          return rows.map((r) => Number(r.id));
        };
        if (fair) return rank(await laneHeadIds(p));
        // Plain order: the head of the global index answers in one cheap
        // range when it holds enough eligible rows (or all of them); only a
        // head clogged by throttled rows falls through to the lane probe.
        const window = Math.max(256, p.wide * 8);
        const head = await execRaw(
          tx,
          sql`
            WITH w AS MATERIALIZED (
              SELECT * FROM wf_step_queue
              WHERE status = 'pending' AND id <> ALL(${skipLiteral})
              ORDER BY priority DESC, created_at ASC, id ASC
              LIMIT ${window}
            )
            SELECT s.n, f.id FROM (SELECT COUNT(*) AS n FROM w) s
            LEFT JOIN LATERAL (
              SELECT q.id FROM w q
              WHERE ${where}
              ORDER BY q.priority DESC, q.created_at ASC, q.id ASC
              LIMIT ${p.wide}
            ) f ON true
          `,
        );
        const found = head.filter((r) => r.id != null).map((r) => Number(r.id));
        if (found.length >= p.need || Number(head[0]?.n ?? 0) < window) return found;
        return rank(await laneHeadIds(p));
      };

      const laneCandidates = async (): Promise<Row[]> => {
        const collected: Row[] = [];
        const seen: number[] = [];
        for (let round = 0; round < 3 && collected.length < limit; round++) {
          const need = limit - collected.length;
          const wide = need * 2 + 8;
          const ids = await candidateIds({ need, wide, skip: seen });
          if (ids.length === 0) break;
          const idsLiteral = sql.raw(`'{${ids.join(",")}}'::bigint[]`);
          // Re-checks status after the lock, like the scan: a row another
          // claimer took in between drops out.
          const locked = await execRaw(
            tx,
            sql`
              SELECT q.id, q.concurrency_scope, q.concurrency_key, q.concurrency_limit,
                     q.group_name, q.group_max_concurrent, q.group_max_starts,
                     q.group_window_ms, q.fairness_key
              FROM wf_step_queue q
              WHERE q.id = ANY(${idsLiteral}) AND q.status = 'pending'
              ORDER BY array_position(${idsLiteral}, q.id)
              LIMIT ${need}
              FOR UPDATE OF q SKIP LOCKED
            `,
          );
          collected.push(...locked);
          seen.push(...ids);
          // Fewer ids than asked for and none lost to another claimer: all found.
          if (ids.length < wide && locked.length === Math.min(ids.length, need)) break;
        }
        return collected;
      };

      const candidates =
        this.claimStrategy === "scan" ? await scanCandidates() : await laneCandidates();
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
    if (rows.length > 0) notifyQuietly(this.wakeups, { topic: "steps" });
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
    if (rows.length > 0) this.notifySettled(params.taskId);
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
    if (rows.length > 0) this.notifySettled(params.taskId);
    return rows.length > 0;
  }

  private notifySettled(taskId: string): void {
    notifyQuietly(this.wakeups, { topic: "results", key: taskId });
    // A settle frees a concurrency or group slot a throttled task waits for.
    notifyQuietly(this.wakeups, { topic: "steps" });
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
    if (rows.length - deadLettered > 0) notifyQuietly(this.wakeups, { topic: "steps" });
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
    // A table from before claim lanes lacks the generated `lane` column.
    const addLane = sql.raw(
      `ADD COLUMN IF NOT EXISTS lane text GENERATED ALWAYS AS (${STEP_QUEUE_LANE_SQL}) STORED`,
    );
    await execRaw(this.db, sql`ALTER TABLE IF EXISTS wf_step_queue ${addLane}`);
    await ensureTableFromSchema(this.db, stepQueue);
    // A table created just now got `lane` as a plain column (the generic
    // creator does not render generated columns): make it generated, and
    // create the lane indexes with their real ordering.
    const generated = await execRaw(
      this.db,
      sql`SELECT attgenerated FROM pg_attribute
          WHERE attrelid = 'wf_step_queue'::regclass AND attname = 'lane' AND NOT attisdropped`,
    );
    if (generated[0]?.attgenerated !== "s") {
      await execRaw(this.db, sql`ALTER TABLE wf_step_queue DROP COLUMN lane`);
      await execRaw(this.db, sql`ALTER TABLE wf_step_queue ${addLane}`);
    }
    await execRaw(
      this.db,
      sql`CREATE INDEX IF NOT EXISTS wf_step_queue_lane_idx
          ON wf_step_queue (lane, priority DESC, created_at, id) WHERE status = 'pending'`,
    );
    await execRaw(
      this.db,
      sql`CREATE INDEX IF NOT EXISTS wf_step_queue_pending_group_idx
          ON wf_step_queue (group_name) WHERE status = 'pending' AND group_name IS NOT NULL`,
    );
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
