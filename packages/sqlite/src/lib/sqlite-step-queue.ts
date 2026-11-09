import { randomUUID } from "node:crypto";
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
import {
  deadLetterError,
  isStepGroupTask,
  percentileCont,
  StepGroupLedger,
} from "@promin/workflow/storage-kit";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent step queue backed by SQLite.
 *
 * Implements idempotent enqueue on (workflowId, stepName), capability /
 * step-name / version routing inside the claim, priority ordering,
 * concurrency keys, heartbeat, release, delivery counting with
 * dead-lettering, and purge.
 *
 * An `active_key` column with a partial unique index enforces the
 * single-unconsumed-task-per-(workflowId, stepName) invariant without a
 * separate lookup table: it is set on enqueue and cleared by `consume`.
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_step_tasks (
 *     id TEXT PRIMARY KEY, workflow_id, step_name, needs TEXT (JSON),
 *     priority, input TEXT (JSON), deps TEXT (JSON), depends_on TEXT (JSON),
 *     timeout_ms, attempt, deliveries, status, version, namespace, created_at,
 *     claimed_at, claimed_by, claim_token, completed_at, result TEXT (JSON),
 *     error, error_tag, step_metadata TEXT (JSON), duration_ms,
 *     last_heartbeat, heartbeat_details TEXT (JSON), run, consumed_at, active_key TEXT UNIQUE WHERE NOT NULL
 *   )
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("tasks.db");
 * const queue = SqliteStepQueue.make({ db });
 * const id = await queue.enqueue({ workflowId: "wf-1", stepName: "charge", input: {} });
 * const [task] = await queue.claim({ workerId: "w-1", limit: 1 });
 * await queue.complete({ taskId: task.id, claimToken: task.claimToken, result: "ok", durationMs: 50 });
 * ```
 */
export class SqliteStepQueue implements StepQueue {
  private readonly _table: string;
  private readonly clock: WallClock;
  private readonly maxDeliveries: number;

  private constructor(
    private readonly db: SqliteDatabase,
    params: { table: string; clock: WallClock; maxDeliveries: number },
  ) {
    this._table = params.table;
    this.clock = params.clock;
    this.maxDeliveries = params.maxDeliveries;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Override the table name (default: `promin_step_tasks`). */
    table?: string;
    /**
     * Time source for `created_at` / `claimed_at` / heartbeat / completion
     * timestamps. Defaults to `SystemWallClock`. Tests pass a `FakeWallClock` so
     * `clock.advance(ms)` drives the queue's time math deterministically.
     */
    clock?: WallClock;
    /**
     * Deliveries after which `requeueStuck` dead-letters a task instead of
     * requeueing it. Default: `DEFAULT_MAX_DELIVERIES` (10).
     */
    maxDeliveries?: number;
  }): SqliteStepQueue {
    return new SqliteStepQueue(params.db, {
      table: params.table ?? "promin_step_tasks",
      clock: params.clock ?? SystemWallClock,
      maxDeliveries: params.maxDeliveries ?? DEFAULT_MAX_DELIVERIES,
    });
  }

  private _setup(): void {
    const t = this._table;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        id             TEXT    NOT NULL PRIMARY KEY,
        workflow_id    TEXT    NOT NULL,
        step_name      TEXT    NOT NULL,
        needs          TEXT    NOT NULL DEFAULT '[]',
        priority       INTEGER NOT NULL DEFAULT 5,
        input          TEXT    NOT NULL,
        deps           TEXT    NOT NULL DEFAULT '{}',
        depends_on     TEXT    NOT NULL DEFAULT '[]',
        timeout_ms     INTEGER,
        attempt        INTEGER NOT NULL DEFAULT 1,
        status         TEXT    NOT NULL DEFAULT 'pending',
        version        TEXT,
        namespace      TEXT,
        metadata       TEXT,
        created_at     INTEGER NOT NULL,
        claimed_at     INTEGER,
        claim_token    TEXT,
        completed_at   INTEGER,
        result         TEXT,
        error          TEXT,
        error_tag      TEXT,
        step_metadata  TEXT,
        duration_ms    INTEGER,
        last_heartbeat INTEGER,
        active_key     TEXT,
        concurrency_key   TEXT,
        concurrency_scope TEXT,
        concurrency_limit INTEGER,
        group_name        TEXT,
        group_max_concurrent INTEGER,
        group_max_starts  INTEGER,
        group_window_ms   INTEGER,
        fairness_key      TEXT,
        claimed_by     TEXT,
        deliveries     INTEGER NOT NULL DEFAULT 0,
        run            INTEGER NOT NULL DEFAULT 1,
        consumed_at    INTEGER,
        heartbeat_details TEXT
      )
    `);
    // Tables created before heartbeat details lack their column.
    const columns = this.db.query<{ name: string }>(`PRAGMA table_info(${t})`).all();
    if (!columns.some((c) => c.name === "heartbeat_details")) {
      this.db.run(`ALTER TABLE ${t} ADD COLUMN heartbeat_details TEXT`);
    }
    // Tables created before step groups lack their columns.
    for (const [name, type] of [
      ["group_name", "TEXT"],
      ["group_max_concurrent", "INTEGER"],
      ["group_max_starts", "INTEGER"],
      ["group_window_ms", "INTEGER"],
      ["fairness_key", "TEXT"],
    ] as const) {
      if (!columns.some((c) => c.name === name)) {
        this.db.run(`ALTER TABLE ${t} ADD COLUMN ${name} ${type}`);
      }
    }
    // `lane` sorts a task into the queue it is claimed from (see `claim`): a
    // generated column, so older tables upgrade in place and a release that
    // predates it can still insert rows. Needs SQLite 3.31+.
    const withHidden = this.db.query<{ name: string }>(`PRAGMA table_xinfo(${t})`).all();
    if (!withHidden.some((c) => c.name === "lane")) {
      this.db.run(
        `ALTER TABLE ${t} ADD COLUMN lane TEXT GENERATED ALWAYS AS (${LANE_EXPR}) VIRTUAL`,
      );
    }
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_group_started ON ${t} (group_name, claimed_at) WHERE group_name IS NOT NULL`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_concurrency_running ON ${t} (concurrency_scope, concurrency_key) WHERE status = 'running' AND concurrency_key IS NOT NULL`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_group_running ON ${t} (group_name, fairness_key) WHERE status = 'running' AND group_name IS NOT NULL`,
    );
    // One partial index per claim lane, in claim order, so a claim reads the
    // head of each lane and never the backlog behind it.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_lane_plain ON ${t} (priority DESC, created_at ASC) WHERE status = 'pending' AND lane = 'p'`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_lane_conc ON ${t} (concurrency_scope, concurrency_key, priority DESC, created_at ASC) WHERE status = 'pending' AND lane = 'c'`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_lane_group ON ${t} (group_name, fairness_key, priority DESC, created_at ASC) WHERE status = 'pending' AND lane = 'g'`,
    );
    // Ranks a task among the pending peers of its fairness key by age.
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_lane_group_age ON ${t} (group_name, fairness_key, created_at ASC, priority DESC) WHERE status = 'pending' AND lane = 'g'`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_throttled ON ${t} (workflow_id) WHERE status = 'pending' AND group_name IS NOT NULL`,
    );
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${t}_active ON ${t} (active_key) WHERE active_key IS NOT NULL`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_status_pri ON ${t} (status, priority DESC, created_at ASC)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_terminal ON ${t} (completed_at) WHERE status IN ('completed', 'failed')`,
    );
  }

  private _activeKey(workflowId: string, stepName: string): string {
    return `${workflowId}::${stepName}`;
  }

  async enqueue(params: StepQueueEnqueueParams): Promise<string> {
    const key = this._activeKey(params.workflowId, params.stepName);

    return this.db.transaction((): string => {
      const existing = this.db
        .query<{ id: string }>(`SELECT id FROM ${this._table} WHERE active_key = ?`)
        .get(key);
      if (existing) return existing.id;

      const id = randomUUID();
      this.db
        .query(
          `INSERT INTO ${this._table}
           (id, workflow_id, step_name, needs, priority, input, deps, depends_on, timeout_ms,
            attempt, run, deliveries, status, version, namespace, metadata,
            concurrency_key, concurrency_scope, concurrency_limit,
            group_name, group_max_concurrent, group_max_starts, group_window_ms, fairness_key,
            created_at, active_key)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          id,
          params.workflowId,
          params.stepName,
          JSON.stringify(params.needs ?? []),
          params.priority ?? 5,
          JSON.stringify(params.input),
          JSON.stringify(params.deps ?? {}),
          JSON.stringify(params.dependsOn ?? []),
          params.timeoutMs ?? null,
          params.attempt ?? 1,
          params.run ?? 1,
          params.version ?? null,
          params.namespace ?? null,
          params.metadata !== undefined ? JSON.stringify(params.metadata) : null,
          params.concurrencyKey ?? null,
          params.concurrencyScope ?? null,
          params.concurrencyLimit ?? null,
          params.groupName ?? null,
          params.groupMaxConcurrent ?? null,
          params.groupMaxStarts ?? null,
          params.groupWindowMs ?? null,
          params.fairnessKey ?? null,
          this.clock.currentTimeMs(),
          key,
        );
      return id;
    })();
  }

  /**
   * Claim in one write transaction, reading only the head of each lane.
   *
   * Pending tasks sit in three lanes (the `lane` column, each with a partial
   * index in claim order): plain tasks, tasks of one (scope, key) with a
   * concurrency limit, and the tasks of one fairness key of a step group. A
   * lane whose cap is already full (key at its limit, group out of slots or
   * starts) is never read, so a throttled backlog costs nothing per claim.
   * The heads of the open lanes are merged in claim order (priority, fairness
   * rank, age); claiming a task reads the next row of its lane.
   *
   * Cost is the number of open lanes plus `limit`, not the pending count.
   * Tasks of one group or one concurrency key are assumed to carry the same
   * limits, as the steps that enqueue them do.
   */
  async claim(params: StepQueueClaimParams): Promise<StepTask[]> {
    if (params.stepNames && params.stepNames.length === 0) return [];
    const limit = Math.floor(params.limit);
    if (!(limit >= 1)) return [];
    const routing = routingFilter(params);

    // One write transaction: no other connection can claim between our
    // reads and the updates below.
    return this.db.transaction((): StepTask[] => {
      const now = this.clock.currentTimeMs();
      const t = this._table;
      const heap = new MinHeap<Candidate>(compareCandidates);
      const lanes = new ClaimLanes();
      const ledgers = new Map<string, StepGroupLedger>();
      const runningPerKey = new Map<string, number>();
      const deadGroups = new Set<string>();
      const deadKeys = new Set<string>();

      const runningOf = (row: Head): number => {
        const k = concurrencyKeyOf(row)!;
        let n = runningPerKey.get(k);
        if (n === undefined) {
          n = this.db
            .query<{ n: number }>(
              `SELECT COUNT(*) AS n FROM ${t}
               WHERE status = 'running' AND concurrency_scope = ? AND concurrency_key = ?`,
            )
            .get(row.concurrency_scope, row.concurrency_key)!.n;
          runningPerKey.set(k, n);
        }
        return n;
      };
      const ledgerOf = (row: Head): StepGroupLedger => {
        let l = ledgers.get(row.group_name!);
        if (!l) {
          l = this._groupLedger({ now, groups: [groupWindow(row)] });
          ledgers.set(row.group_name!, l);
        }
        return l;
      };

      // Push the next row of `lane` after `after` (or its head) onto the heap.
      const advance = (lane: Lane, after?: Head): void => {
        const row = this._nextOf({ lane, after, routing });
        if (row) heap.push(this._candidate({ lane, row, lanes }));
      };

      // Seed: the plain lane, then the open concurrency lanes, then the open groups.
      advance(lanes.plain());
      for (const row of this._concurrencyHeads(routing)) {
        const lane = lanes.conc(row);
        heap.push(this._candidate({ lane, row, lanes }));
      }
      for (const name of this._pendingGroups()) {
        const first = this._groupSample(name);
        if (!first) continue;
        const ledger = ledgerOf(first);
        if (!ledger.check({ ...groupFieldsOfHead(first), groupName: name }).ok) continue;
        for (const { row, lane } of this._groupHeads({ name, routing, lanes })) {
          heap.push(this._candidate({ lane, row, lanes }));
        }
      }

      const claimed: StepTask[] = [];
      while (claimed.length < limit) {
        const c = heap.pop();
        if (!c) break;
        const { row, lane } = c;
        if (row.group_name && deadGroups.has(row.group_name)) continue;
        const ckey = concurrencyKeyOf(row);
        if (ckey !== undefined) {
          if (deadKeys.has(ckey)) continue;
          if (runningOf(row) >= row.concurrency_limit!) {
            // Every row of a concurrency lane has this key; a grouped row's
            // key says nothing about the rest of its group lane.
            if (lane.kind === "c") deadKeys.add(ckey);
            else advance(lane, row);
            continue;
          }
        }
        const group = groupFieldsOfHead(row);
        const ledger = isStepGroupTask(group) ? ledgerOf(row) : undefined;
        if (isStepGroupTask(group) && !ledger!.check(group).ok) {
          deadGroups.add(group.groupName);
          continue;
        }
        if (ckey !== undefined) runningPerKey.set(ckey, runningOf(row) + 1);
        if (isStepGroupTask(group)) ledger!.admit(group);
        lanes.claimedFrom({ lane, row });

        const claimToken = randomUUID();
        this.db
          .query(
            `UPDATE ${t}
             SET status = 'running', claimed_at = ?, claimed_by = ?, last_heartbeat = ?,
                 claim_token = ?, deliveries = deliveries + 1
             WHERE id = ? AND status = 'pending'`,
          )
          .run(now, params.workerId, now, claimToken, row.id);
        const full = this.db.query<TaskRow>(`SELECT * FROM ${t} WHERE id = ?`).get(row.id)!;
        claimed.push(rowToTask(full));
        advance(lane, row);
      }
      return claimed;
    })();
  }

  /** The row of `lane` after `after` (its head when absent), in claim order. */
  private _nextOf(params: { lane: Lane; after?: Head; routing: Routing }): Head | undefined {
    const { lane, after, routing } = params;
    const conds = [`t.status = 'pending'`, `t.lane = '${lane.kind}'`];
    const args: unknown[] = [];
    if (lane.kind === "c") {
      conds.push(`t.concurrency_scope = ? AND t.concurrency_key = ?`);
      args.push(lane.scope, lane.key);
    } else if (lane.kind === "g") {
      conds.push(`t.group_name = ?`);
      args.push(lane.group);
      if (lane.fair === null) conds.push(`t.fairness_key IS NULL`);
      else {
        conds.push(`t.fairness_key = ?`);
        args.push(lane.fair);
      }
    }
    if (after) {
      conds.push(`(t.priority < ? OR (t.priority = ? AND (t.created_at, t.rowid) > (?, ?)))`);
      args.push(after.priority, after.priority, after.created_at, after._rowid);
    }
    conds.push(routing.sql("t"));
    args.push(...routing.args);
    const row = this.db
      .query<Head>(
        `SELECT ${HEAD_COLS} FROM ${this._table} t INDEXED BY ${this._table}_lane_${LANE_INDEX[lane.kind]}
         WHERE ${conds.join(" AND ")}
         ORDER BY t.priority DESC, t.created_at ASC, t.rowid ASC LIMIT 1`,
      )
      .get(...args);
    return row ?? undefined;
  }

  /**
   * Heap entry for `row`: its fairness rank is its position by age among the
   * pending peers of its `(group, fairness key)` (counting what an earlier
   * claim of this call already took) plus the key's running tasks, so keys
   * interleave round-robin and a key already running yields to one that is not.
   */
  private _candidate(params: { lane: Lane; row: Head; lanes: ClaimLanes }): Candidate {
    const { lane, row, lanes } = params;
    let rank = 0;
    if (lane.kind === "g" && lane.fair !== null) {
      const t = this._table;
      const peers = `p.status = 'pending' AND p.lane = 'g' AND p.group_name = ? AND p.fairness_key = ?`;
      const older = this.db
        .query<{ n: number }>(
          `SELECT (SELECT COUNT(*) FROM ${t} p INDEXED BY ${t}_lane_group_age WHERE ${peers} AND p.created_at < ?)
                + (SELECT COUNT(*) FROM ${t} p INDEXED BY ${t}_lane_group_age WHERE ${peers} AND p.created_at = ?
                     AND (p.priority > ? OR (p.priority = ? AND p.rowid < ?))) AS n`,
        )
        .get(
          lane.group,
          lane.fair,
          row.created_at,
          lane.group,
          lane.fair,
          row.created_at,
          row.priority,
          row.priority,
          row._rowid,
        )!.n;
      if (lane.runningAtStart === undefined) {
        lane.runningAtStart = this.db
          .query<{ n: number }>(
            `SELECT COUNT(*) AS n FROM ${t}
             WHERE status = 'running' AND group_name = ? AND fairness_key = ?`,
          )
          .get(lane.group, lane.fair)!.n;
      }
      rank = older + lanes.claimedBefore({ lane, row }) + lane.runningAtStart;
    }
    return { row, lane, rank };
  }

  /** Open concurrency lanes: each routable key's head, keys already at their limit left out. */
  private _concurrencyHeads(routing: Routing): Head[] {
    const t = this._table;
    // Scopes (one per step, a handful) by loose index scan; the keys of one
    // scope the same way, each step seeking past the previous key.
    const scopes = this.db
      .query<{ s: string | null }>(
        `WITH RECURSIVE s(name) AS (
           SELECT (SELECT concurrency_scope FROM ${t} INDEXED BY ${t}_lane_conc
                   WHERE status = 'pending' AND lane = 'c' ORDER BY concurrency_scope LIMIT 1)
           UNION ALL
           SELECT (SELECT concurrency_scope FROM ${t} INDEXED BY ${t}_lane_conc
                   WHERE status = 'pending' AND lane = 'c' AND concurrency_scope > s.name
                   ORDER BY concurrency_scope LIMIT 1)
           FROM s WHERE s.name IS NOT NULL
         )
         SELECT name AS s FROM s WHERE name IS NOT NULL`,
      )
      .all()
      .map((r) => r.s!);
    const keys = this.db.query<Head>(
      `WITH RECURSIVE k(rid) AS (
         SELECT (SELECT t.rowid FROM ${t} t INDEXED BY ${t}_lane_conc
                 WHERE t.status = 'pending' AND t.lane = 'c' AND t.concurrency_scope = ?
                 ORDER BY t.concurrency_key LIMIT 1)
         UNION ALL
         SELECT (SELECT t.rowid FROM ${t} t INDEXED BY ${t}_lane_conc
                 WHERE t.status = 'pending' AND t.lane = 'c' AND t.concurrency_scope = ?
                   AND t.concurrency_key >
                       (SELECT n.concurrency_key FROM ${t} n WHERE n.rowid = k.rid)
                 ORDER BY t.concurrency_key LIMIT 1)
         FROM k WHERE k.rid IS NOT NULL
       )
       SELECT ${HEAD_COLS} FROM k
       JOIN ${t} k0 ON k0.rowid = k.rid
       JOIN ${t} t ON t.rowid = (
         SELECT t2.rowid FROM ${t} t2 INDEXED BY ${t}_lane_conc
         WHERE t2.status = 'pending' AND t2.lane = 'c'
           AND t2.concurrency_scope = k0.concurrency_scope AND t2.concurrency_key = k0.concurrency_key
           AND ${routing.sql("t2")}
         ORDER BY t2.priority DESC, t2.created_at ASC, t2.rowid ASC LIMIT 1)
       WHERE (SELECT COUNT(*) FROM ${t} r
              WHERE r.status = 'running' AND r.concurrency_scope = t.concurrency_scope
                AND r.concurrency_key = t.concurrency_key) < t.concurrency_limit`,
    );
    return scopes.flatMap((scope) => keys.all(scope, scope, ...routing.args));
  }

  /** Names of the groups with pending tasks. */
  private _pendingGroups(): string[] {
    const t = this._table;
    return this.db
      .query<{ g: string | null }>(
        `WITH RECURSIVE g(name) AS (
           SELECT (SELECT group_name FROM ${t} INDEXED BY ${t}_lane_group WHERE status = 'pending' AND lane = 'g'
                   ORDER BY group_name LIMIT 1)
           UNION ALL
           SELECT (SELECT group_name FROM ${t} INDEXED BY ${t}_lane_group
                   WHERE status = 'pending' AND lane = 'g' AND group_name > g.name
                   ORDER BY group_name LIMIT 1)
           FROM g WHERE g.name IS NOT NULL
         )
         SELECT name AS g FROM g WHERE name IS NOT NULL`,
      )
      .all()
      .map((r) => r.g!);
  }

  /** One pending task of a group, for the limits its tasks carry. */
  private _groupSample(name: string): Head | undefined {
    return (
      this.db
        .query<Head>(
          `SELECT ${HEAD_COLS} FROM ${this._table} t INDEXED BY ${this._table}_lane_group
           WHERE t.status = 'pending' AND t.lane = 'g' AND t.group_name = ? LIMIT 1`,
        )
        .get(name) ?? undefined
    );
  }

  /** The head of each routable fairness-key lane of a group, with its lane. */
  private _groupHeads(params: {
    name: string;
    routing: Routing;
    lanes: ClaimLanes;
  }): Array<{ row: Head; lane: Lane }> {
    const { name, routing, lanes } = params;
    const t = this._table;
    const keyed = this.db
      .query<Head>(
        `WITH RECURSIVE k(rid) AS (
           SELECT (SELECT t.rowid FROM ${t} t INDEXED BY ${t}_lane_group
                   WHERE t.status = 'pending' AND t.lane = 'g' AND t.group_name = ?
                     AND t.fairness_key IS NOT NULL
                   ORDER BY t.fairness_key LIMIT 1)
           UNION ALL
           SELECT (SELECT t.rowid FROM ${t} t INDEXED BY ${t}_lane_group
                   WHERE t.status = 'pending' AND t.lane = 'g' AND t.group_name = ?
                     AND t.fairness_key > (SELECT n.fairness_key FROM ${t} n WHERE n.rowid = k.rid)
                   ORDER BY t.fairness_key LIMIT 1)
           FROM k WHERE k.rid IS NOT NULL
         )
         SELECT ${HEAD_COLS} FROM k
         JOIN ${t} k0 ON k0.rowid = k.rid
         JOIN ${t} t ON t.rowid = (
           SELECT t2.rowid FROM ${t} t2 INDEXED BY ${t}_lane_group
           WHERE t2.status = 'pending' AND t2.lane = 'g' AND t2.group_name = k0.group_name
             AND t2.fairness_key = k0.fairness_key AND ${routing.sql("t2")}
           ORDER BY t2.priority DESC, t2.created_at ASC, t2.rowid ASC LIMIT 1)`,
      )
      .all(name, name, ...routing.args);
    const out = keyed.map((row) => ({
      row,
      lane: lanes.group({ group: name, fair: row.fairness_key }),
    }));
    const unkeyed = lanes.group({ group: name, fair: null });
    const head = this._nextOf({ lane: unkeyed, routing });
    if (head) out.push({ row: head, lane: unkeyed });
    return out;
  }

  async throttled(params: { workflowId: string }): Promise<ThrottledTask[]> {
    const rows = this.db
      .query<TaskRow>(
        `SELECT * FROM ${this._table}
         WHERE workflow_id = ? AND status = 'pending' AND group_name IS NOT NULL`,
      )
      .all(params.workflowId);
    const windows = new Map<string, GroupWindow>();
    for (const row of rows) {
      if (row.group_name) windows.set(row.group_name, groupWindow(row));
    }
    const groups = this._groupLedger({
      now: this.clock.currentTimeMs(),
      groups: [...windows.values()],
    });
    const out: ThrottledTask[] = [];
    for (const row of rows) {
      const group = groupFieldsOf(row);
      if (!isStepGroupTask(group)) continue;
      const verdict = groups.check(group);
      if (verdict.ok) continue;
      out.push({
        taskId: row.id,
        workflowId: row.workflow_id,
        run: row.run,
        stepName: row.step_name,
        attempt: row.attempt,
        groupName: group.groupName,
        reason: verdict.reason,
        since: new Date(row.created_at),
        ...(verdict.retryAtMs !== undefined && { retryAt: new Date(verdict.retryAtMs) }),
      });
    }
    return out;
  }

  /**
   * Running tasks and window starts of the given groups, read through the
   * group indexes: the running rows, and the starts inside the window, never
   * the group's settled history.
   */
  private _groupLedger(params: { now: number; groups: readonly GroupWindow[] }): StepGroupLedger {
    const ledger = new StepGroupLedger(params.now);
    for (const g of params.groups) {
      const running = this.db
        .query<{ group_name: string; fairness_key: string | null; claimed_at: number | null }>(
          `SELECT group_name, fairness_key, claimed_at FROM ${this._table}
           WHERE group_name = ? AND status = 'running'`,
        )
        .all(g.name);
      for (const r of running) {
        ledger.record({
          groupName: r.group_name,
          ...(r.fairness_key != null && { fairnessKey: r.fairness_key }),
          running: true,
          ...(r.claimed_at != null && { startedAtMs: r.claimed_at }),
        });
      }
      if (g.windowMs === undefined) continue;
      const settled = this.db
        .query<{ claimed_at: number }>(
          `SELECT claimed_at FROM ${this._table}
           WHERE group_name = ? AND claimed_at > ? AND status NOT IN ('pending', 'running')`,
        )
        .all(g.name, params.now - g.windowMs);
      for (const r of settled) {
        ledger.record({ groupName: g.name, running: false, startedAtMs: r.claimed_at });
      }
    }
    return ledger;
  }

  async release(params: { taskId: string; claimToken: string }): Promise<boolean> {
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'pending', claimed_at = NULL, claimed_by = NULL, last_heartbeat = NULL,
             claim_token = NULL, deliveries = MAX(deliveries - 1, 0)
         WHERE id = ? AND status = 'running' AND claim_token = ?`,
      )
      .run(params.taskId, params.claimToken);
    return this._changes() > 0;
  }

  async get(taskId: string): Promise<StepTaskRecord | undefined> {
    const row = this.db.query<TaskRow>(`SELECT * FROM ${this._table} WHERE id = ?`).get(taskId);
    return row ? rowToRecord(row) : undefined;
  }

  async complete(params: StepQueueCompleteParams): Promise<boolean> {
    const now = this.clock.currentTimeMs();
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'completed', result = ?, step_metadata = ?, duration_ms = ?,
             completed_at = ?
         WHERE id = ? AND status = 'running'
           AND (? IS NULL OR claim_token = ?)`,
      )
      .run(
        JSON.stringify(params.result) ?? null,
        params.stepMetadata !== undefined ? JSON.stringify(params.stepMetadata) : null,
        params.durationMs,
        now,
        params.taskId,
        params.claimToken ?? null,
        params.claimToken ?? null,
      );
    return this._changes() > 0;
  }

  async fail(params: StepQueueFailParams): Promise<boolean> {
    const now = this.clock.currentTimeMs();
    this.db
      .query(
        `UPDATE ${this._table}
         SET status = 'failed', error = ?, error_tag = ?, step_metadata = ?, duration_ms = ?,
             completed_at = ?
         WHERE id = ? AND status = 'running'
           AND (? IS NULL OR claim_token = ?)`,
      )
      .run(
        params.error,
        params.errorTag ?? null,
        params.stepMetadata !== undefined ? JSON.stringify(params.stepMetadata) : null,
        params.durationMs,
        now,
        params.taskId,
        params.claimToken ?? null,
        params.claimToken ?? null,
      );
    return this._changes() > 0;
  }

  async heartbeat(params: StepQueueHeartbeatParams): Promise<boolean> {
    // No details keeps the stored ones; `null` clears them.
    this.db
      .query(
        `UPDATE ${this._table}
         SET last_heartbeat = ?,
             heartbeat_details = CASE WHEN ? THEN ? ELSE heartbeat_details END
         WHERE id = ? AND status = 'running'
           AND (? IS NULL OR claim_token = ?)`,
      )
      .run(
        this.clock.currentTimeMs(),
        params.details !== undefined ? 1 : 0,
        params.details != null ? JSON.stringify(params.details) : null,
        params.taskId,
        params.claimToken ?? null,
        params.claimToken ?? null,
      );
    return this._changes() > 0;
  }

  async requeueStuck(params: StepQueueRequeueParams): Promise<StepQueueRequeueResult> {
    const now = this.clock.currentTimeMs();
    const t = this._table;
    const [match, arg] =
      params.mode === "worker"
        ? [`claimed_by = ?`, params.workerId]
        : // last_heartbeat falls back to claimed_at when no heartbeat was sent
          [`COALESCE(last_heartbeat, claimed_at) < ?`, now - params.olderThanMs];

    return this.db.transaction((): StepQueueRequeueResult => {
      this.db
        .query(
          `UPDATE ${t}
           SET status = 'failed', error = ?, completed_at = ?,
               claim_token = NULL, last_heartbeat = NULL
           WHERE status = 'running' AND ${match} AND deliveries >= ?`,
        )
        .run(deadLetterError(this.maxDeliveries), now, arg, this.maxDeliveries);
      const deadLettered = this._changes();

      this.db
        .query(
          `UPDATE ${t}
           SET status = 'pending', claimed_at = NULL, claimed_by = NULL, last_heartbeat = NULL,
               claim_token = NULL
           WHERE status = 'running' AND ${match}`,
        )
        .run(arg);
      return { requeued: this._changes(), deadLettered };
    })();
  }

  async consume(params: { taskId: string }): Promise<boolean> {
    this.db
      .query(
        `UPDATE ${this._table} SET consumed_at = ?, active_key = NULL
         WHERE id = ? AND status IN ('completed', 'failed') AND consumed_at IS NULL`,
      )
      .run(this.clock.currentTimeMs(), params.taskId);
    return this._changes() > 0;
  }

  async consumeSettled(params: {
    workflowId: string;
    stepNames: readonly string[];
  }): Promise<number> {
    if (params.stepNames.length === 0) return 0;
    const keys = params.stepNames.map((stepName) => this._activeKey(params.workflowId, stepName));
    this.db
      .query(
        `UPDATE ${this._table} SET consumed_at = ?, active_key = NULL
         WHERE active_key IN (${keys.map(() => "?").join(", ")})
           AND status IN ('completed', 'failed') AND consumed_at IS NULL`,
      )
      .run(this.clock.currentTimeMs(), ...keys);
    return this._changes();
  }

  async purge(params: { completedBefore: Date }): Promise<number> {
    this.db
      .query(
        `DELETE FROM ${this._table}
         WHERE status IN ('completed', 'failed') AND completed_at < ?`,
      )
      .run(params.completedBefore.getTime());
    return this._changes();
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
    const sinceMs = params.since.getTime();
    const untilMs = (params.until ?? this.clock.now()).getTime();

    const pending =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'pending' AND created_at >= ? AND created_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    const running =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'running' AND claimed_at >= ? AND claimed_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    const completed =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'completed' AND completed_at >= ? AND completed_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    const failed =
      this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this._table}
           WHERE status = 'failed' AND completed_at >= ? AND completed_at <= ?`,
        )
        .get(sinceMs, untilMs)?.n ?? 0;

    // Latency stats over terminal tasks in the window
    const terminalRows = this.db
      .query<{ created_at: number; claimed_at: number | null; duration_ms: number | null }>(
        `SELECT created_at, claimed_at, duration_ms FROM ${this._table}
         WHERE status IN ('completed', 'failed')
           AND completed_at >= ? AND completed_at <= ?`,
      )
      .all(sinceMs, untilMs);

    if (terminalRows.length === 0) {
      return { pending, running, completed, failed, avgWaitMs: 0, avgExecMs: 0, p95ExecMs: 0 };
    }

    let waitSum = 0;
    let waitN = 0;
    let execSum = 0;
    const execTimes: number[] = [];

    for (const r of terminalRows) {
      if (r.claimed_at != null) {
        waitSum += r.claimed_at - r.created_at;
        waitN++;
      }
      if (r.duration_ms != null) {
        execSum += r.duration_ms;
        execTimes.push(r.duration_ms);
      }
    }

    return {
      pending,
      running,
      completed,
      failed,
      avgWaitMs: waitN > 0 ? waitSum / waitN : 0,
      avgExecMs: execTimes.length > 0 ? execSum / execTimes.length : 0,
      p95ExecMs: execTimes.length > 0 ? percentileCont({ values: execTimes, p: 0.95 }) : 0,
    };
  }

  private _changes(): number {
    return this.db.query<{ changes: number }>(`SELECT changes() AS changes`).get()?.changes ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Claim lanes
// ---------------------------------------------------------------------------

/** Which claim lane a task is read from; the generated `lane` column. */
const LANE_EXPR = `CASE
  WHEN group_name IS NOT NULL AND group_name != '' THEN 'g'
  WHEN concurrency_key IS NOT NULL AND concurrency_key != ''
    AND concurrency_scope IS NOT NULL AND concurrency_scope != ''
    AND concurrency_limit IS NOT NULL THEN 'c'
  ELSE 'p' END`;

/** The index each lane is read through. */
const LANE_INDEX = { p: "plain", c: "conc", g: "group" } as const;

/** What the claim reads of a pending row until it wins its turn. */
const HEAD_COLS = `t.id, t.priority, t.created_at, t.rowid AS _rowid, t.concurrency_key,
  t.concurrency_scope, t.concurrency_limit, t.group_name, t.group_max_concurrent,
  t.group_max_starts, t.group_window_ms, t.fairness_key`;

interface Head {
  id: string;
  priority: number;
  created_at: number;
  _rowid: number;
  concurrency_key: string | null;
  concurrency_scope: string | null;
  concurrency_limit: number | null;
  group_name: string | null;
  group_max_concurrent: number | null;
  group_max_starts: number | null;
  group_window_ms: number | null;
  fairness_key: string | null;
}

type Lane =
  | { kind: "p" }
  | { kind: "c"; scope: string; key: string }
  | { kind: "g"; group: string; fair: string | null; runningAtStart?: number };

interface Candidate {
  row: Head;
  lane: Lane;
  /** Fairness rank within the group; 0 outside a fairness key. */
  rank: number;
}

/** Claim order: priority, fairness rank, age, enqueue order. */
function compareCandidates(a: Candidate, b: Candidate): number {
  return (
    b.row.priority - a.row.priority ||
    a.rank - b.rank ||
    a.row.created_at - b.row.created_at ||
    a.row._rowid - b.row._rowid
  );
}

/** The lanes of one claim, and what the claim has taken from each. */
class ClaimLanes {
  private readonly byId = new Map<string, Lane>();
  private readonly taken = new Map<Lane, Head[]>();

  plain(): Lane {
    return this.get({ id: "p", make: () => ({ kind: "p" }) });
  }

  conc(row: Head): Lane {
    const scope = row.concurrency_scope!;
    const key = row.concurrency_key!;
    return this.get({
      id: `c:${[...scope].length}:${scope}:${key}`,
      make: () => ({ kind: "c", scope, key }),
    });
  }

  group(params: { group: string; fair: string | null }): Lane {
    const { group, fair } = params;
    return this.get({
      id: `g:${[...group].length}:${group}:${fair === null ? "-" : `+${fair}`}`,
      make: () => ({ kind: "g", group, fair }),
    });
  }

  claimedFrom(params: { lane: Lane; row: Head }): void {
    const list = this.taken.get(params.lane) ?? [];
    list.push(params.row);
    this.taken.set(params.lane, list);
  }

  /** Rows this claim already took from `lane` that sort before `row` by age. */
  claimedBefore(params: { lane: Lane; row: Head }): number {
    const { row } = params;
    return (this.taken.get(params.lane) ?? []).filter(
      (c) =>
        c.created_at < row.created_at ||
        (c.created_at === row.created_at &&
          (c.priority > row.priority || (c.priority === row.priority && c._rowid < row._rowid))),
    ).length;
  }

  private get(params: { id: string; make: () => Lane }): Lane {
    let lane = this.byId.get(params.id);
    if (!lane) {
      lane = params.make();
      this.byId.set(params.id, lane);
    }
    return lane;
  }
}

/** Binary min-heap, for merging the heads of many lanes. */
class MinHeap<T> {
  private readonly items: T[] = [];
  constructor(private readonly compare: (a: T, b: T) => number) {}

  push(item: T): void {
    const a = this.items;
    a.push(item);
    let i = a.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.compare(a[i]!, a[parent]!) >= 0) break;
      [a[i], a[parent]] = [a[parent]!, a[i]!];
      i = parent;
    }
  }

  pop(): T | undefined {
    const a = this.items;
    if (a.length === 0) return undefined;
    const top = a[0]!;
    const last = a.pop()!;
    if (a.length > 0) {
      a[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < a.length && this.compare(a[l]!, a[m]!) < 0) m = l;
        if (r < a.length && this.compare(a[r]!, a[m]!) < 0) m = r;
        if (m === i) break;
        [a[i], a[m]] = [a[m]!, a[i]!];
        i = m;
      }
    }
    return top;
  }
}

interface Routing {
  /** Conditions for rows aliased `alias`: step, version and capability routing. */
  sql(alias: string): string;
  args: unknown[];
}

/** What this worker can run, as SQL over the table alias. */
function routingFilter(params: StepQueueClaimParams): Routing {
  const conds: Array<(a: string) => string> = [
    (a) =>
      `(${a}.needs = '[]' OR NOT EXISTS (SELECT 1 FROM json_each(${a}.needs) WHERE value NOT IN (SELECT value FROM json_each(?))))`,
  ];
  const args: unknown[] = [JSON.stringify(params.capabilities ?? [])];
  if (params.stepNames) {
    conds.push((a) => `${a}.step_name IN (SELECT value FROM json_each(?))`);
    args.push(JSON.stringify(params.stepNames));
  }
  if (params.versions) {
    conds.push((a) => `(${a}.version IS NULL OR ${a}.version IN (SELECT value FROM json_each(?)))`);
    args.push(JSON.stringify(params.versions));
  }
  return { sql: (alias) => conds.map((c) => c(alias)).join(" AND "), args };
}

interface GroupWindow {
  name: string;
  windowMs?: number;
}

function groupWindow(row: {
  group_name: string | null;
  group_max_starts: number | null;
  group_window_ms: number | null;
}): GroupWindow {
  return {
    name: row.group_name!,
    ...(row.group_max_starts != null &&
      row.group_window_ms != null && { windowMs: row.group_window_ms }),
  };
}

function concurrencyKeyOf(row: Head): string | undefined {
  return row.concurrency_key && row.concurrency_scope && row.concurrency_limit != null
    ? `${row.concurrency_scope}::${row.concurrency_key}`
    : undefined;
}

function groupFieldsOfHead(row: Head): StepGroupFields {
  return {
    ...(row.group_name != null && { groupName: row.group_name }),
    ...(row.group_max_concurrent != null && { groupMaxConcurrent: row.group_max_concurrent }),
    ...(row.group_max_starts != null && { groupMaxStarts: row.group_max_starts }),
    ...(row.group_window_ms != null && { groupWindowMs: row.group_window_ms }),
    ...(row.fairness_key != null && { fairnessKey: row.fairness_key }),
  };
}

// ---------------------------------------------------------------------------
// Row helpers
// ---------------------------------------------------------------------------

interface TaskRow {
  id: string;
  workflow_id: string;
  step_name: string;
  needs: string;
  priority: number;
  input: string;
  deps: string;
  depends_on: string;
  timeout_ms: number | null;
  attempt: number;
  run: number;
  consumed_at: number | null;
  deliveries: number;
  status: string;
  version: string | null;
  namespace: string | null;
  metadata: string | null;
  concurrency_key: string | null;
  concurrency_scope: string | null;
  concurrency_limit: number | null;
  group_name: string | null;
  group_max_concurrent: number | null;
  group_max_starts: number | null;
  group_window_ms: number | null;
  fairness_key: string | null;
  created_at: number;
  claimed_at: number | null;
  claimed_by: string | null;
  claim_token: string | null;
  completed_at: number | null;
  result: string | null;
  error: string | null;
  error_tag: string | null;
  step_metadata: string | null;
  duration_ms: number | null;
  last_heartbeat: number | null;
  heartbeat_details: string | null;
  active_key: string | null;
}

function rowToTask(row: TaskRow): StepTask {
  return {
    id: row.id,
    workflowId: row.workflow_id,
    stepName: row.step_name,
    needs: JSON.parse(row.needs) as string[],
    priority: row.priority,
    input: JSON.parse(row.input),
    deps: JSON.parse(row.deps) as Record<string, unknown>,
    dependsOn: JSON.parse(row.depends_on) as string[],
    ...(row.timeout_ms != null && { timeoutMs: row.timeout_ms }),
    attempt: row.attempt,
    run: row.run,
    deliveries: row.deliveries,
    status: row.status as StepTask["status"],
    createdAt: new Date(row.created_at),
    claimToken: row.claim_token ?? undefined,
    version: row.version ?? undefined,
    metadata:
      row.metadata != null ? (JSON.parse(row.metadata) as Record<string, unknown>) : undefined,
    concurrencyKey: row.concurrency_key ?? undefined,
    concurrencyScope: row.concurrency_scope ?? undefined,
    concurrencyLimit: row.concurrency_limit ?? undefined,
    ...(row.heartbeat_details != null && {
      lastHeartbeatDetails: JSON.parse(row.heartbeat_details) as unknown,
    }),
    ...groupFieldsOf(row),
  };
}

function groupFieldsOf(row: TaskRow): StepGroupFields {
  return {
    ...(row.group_name != null && { groupName: row.group_name }),
    ...(row.group_max_concurrent != null && { groupMaxConcurrent: row.group_max_concurrent }),
    ...(row.group_max_starts != null && { groupMaxStarts: row.group_max_starts }),
    ...(row.group_window_ms != null && { groupWindowMs: row.group_window_ms }),
    ...(row.fairness_key != null && { fairnessKey: row.fairness_key }),
  };
}

function rowToRecord(row: TaskRow): StepTaskRecord {
  return {
    ...rowToTask(row),
    claimedBy: row.claimed_by ?? undefined,
    claimedAt: row.claimed_at != null ? new Date(row.claimed_at) : undefined,
    heartbeatAt: row.last_heartbeat != null ? new Date(row.last_heartbeat) : undefined,
    completedAt: row.completed_at != null ? new Date(row.completed_at) : undefined,
    result: row.result != null ? JSON.parse(row.result) : undefined,
    error: row.error ?? undefined,
    errorTag: row.error_tag ?? undefined,
    stepMetadata:
      row.step_metadata != null
        ? (JSON.parse(row.step_metadata) as Record<string, unknown>)
        : undefined,
    durationMs: row.duration_ms ?? undefined,
    consumedAt: row.consumed_at != null ? new Date(row.consumed_at) : undefined,
  };
}
