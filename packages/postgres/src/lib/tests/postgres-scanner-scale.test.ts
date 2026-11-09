import { expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { postgresDescribe } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { StepStatusIds, WorkflowStatusIds } from "../workflow-lookups.ts";

// ---------------------------------------------------------------------------
// listDueTimers over a large backlog of due sleeps: a page walks the keyset
// index and stops at its limit, so a page deep in the backlog costs about the
// same as the first. Before, every page sorted every due step after the
// cursor, so the first pages of a big backlog cost most of all.
// ---------------------------------------------------------------------------

const DUE = 60_000;
const PAGE = 100;

postgresDescribe("Postgres timer scan at scale", { migrate }, (pg) => {
  it("costs a page, not the backlog behind it", async () => {
    const ids = (n: number, prefix = "") =>
      `SELECT '${prefix}' || 'run-' || lpad(i::text, 7, '0') AS id FROM generate_series(1, ${n}) i`;
    // Half the suspended runs have a due sleep; the other half sleep far ahead.
    await pg.sql.unsafe(`INSERT INTO wf_workflows (workflow_id, workflow_name, input, status_id)
      SELECT id, 'x', '{}'::jsonb, ${WorkflowStatusIds.id.suspended} FROM (${ids(2 * DUE)}) q`);
    await pg.sql
      .unsafe(`INSERT INTO wf_workflow_steps (workflow_id, step_name, run, status_id, wake_at)
      SELECT id, 'nap', 1, ${StepStatusIds.id.sleeping},
        CASE WHEN right(id, 1)::int % 2 = 0 THEN now() - interval '1 minute' ELSE now() + interval '1 year' END
      FROM (${ids(2 * DUE)}) q`);
    await pg.sql.unsafe("ANALYZE");
    const now = new Date();

    // Tuples a page reads, from the transaction's own table statistics
    // (visible at once, unlike the cumulative ones).
    const readCounter = async (tx: typeof pg.db): Promise<number> => {
      const rows = (await tx.execute(
        sql`SELECT COALESCE(SUM(COALESCE(idx_tup_fetch, 0) + COALESCE(seq_tup_read, 0)), 0)::bigint AS read
          FROM pg_stat_xact_user_tables WHERE relname IN ('wf_workflows', 'wf_workflow_steps')`,
      )) as unknown as Array<{ read: string }>;
      return Number(rows[0]!.read);
    };
    const tuplesRead = async (after?: string): Promise<{ read: number; rows: string[] }> =>
      pg.db.transaction(async (tx) => {
        const storage = await PostgresWorkflowStorage.create({
          db: tx as unknown as typeof pg.db,
          autoSeedLookups: false,
        });
        // The counters carry what earlier transactions of the connection read
        // until they are flushed, so measure the page as a difference.
        const before = await readCounter(tx as unknown as typeof pg.db);
        const page = await storage.listDueTimers({
          now,
          limit: PAGE,
          ...(after !== undefined && { afterWorkflowId: after }),
        });
        const read = (await readCounter(tx as unknown as typeof pg.db)) - before;
        return { read, rows: page.map((r) => r.workflowId) };
      });

    // Walk the whole backlog: each due run once, in order; sample the cost of
    // pages across it.
    const seen: string[] = [];
    const costs: number[] = [];
    let after: string | undefined;
    for (let pageNo = 0; ; pageNo++) {
      const { read, rows } = await tuplesRead(after);
      costs.push(read);
      seen.push(...rows);
      if (rows.length < PAGE) break;
      after = rows.at(-1);
    }
    expect(seen.length).toBe(DUE);
    expect(new Set(seen).size).toBe(DUE);
    expect(seen).toEqual([...seen].sort());

    // A page reads a bounded number of tuples however deep in the backlog it
    // is. (Before, each page read every due step after its cursor: ~80 000 on
    // average here. The last few pages, with only a few thousand due runs
    // left, may read those instead, which is the cheaper plan for them.)
    const sorted = [...costs].sort((a, b) => a - b);
    const p95 = sorted[Math.floor(0.95 * (sorted.length - 1))]!;
    expect(p95).toBeLessThanOrEqual(10 * PAGE);
  }, 300_000);
});
