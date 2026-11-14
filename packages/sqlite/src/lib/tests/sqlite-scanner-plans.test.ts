import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

// ---------------------------------------------------------------------------
// The scanner queries walk the suspended runs in id order through the partial
// index and stop at their LIMIT: a page costs its own rows. A plan that sorts
// the matches (a temp b-tree at the top level, or another index chosen for
// the status filter) reads every match after the cursor on every page, so a
// 100k backlog is paged quadratically.
// ---------------------------------------------------------------------------

interface Captured {
  readonly sql: string;
  readonly args: unknown[];
}

/** A storage over a database that records the statements it runs. */
function spiedStorage(): { storage: SqliteWorkflowStorage; db: Database; captured: Captured[] } {
  const db = new Database(":memory:");
  const captured: Captured[] = [];
  const spy = new Proxy(db, {
    get(target, prop) {
      if (prop === "query") {
        return (sql: string) => {
          const stmt = (target as unknown as { query: (s: string) => any }).query(sql);
          return new Proxy(stmt, {
            get(t, p) {
              const value = t[p];
              if (p !== "all" && p !== "get")
                return typeof value === "function" ? value.bind(t) : value;
              return (...args: unknown[]) => {
                captured.push({ sql, args });
                return value.apply(t, args);
              };
            },
          });
        };
      }
      const value = (target as unknown as Record<string | symbol, unknown>)[prop];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { storage: SqliteWorkflowStorage.make({ db: spy }), db, captured };
}

describe("SQLite scanner query plans", () => {
  it("every scanner page is an ordered walk of the suspended index, without a sort", async () => {
    const { storage, db, captured } = spiedStorage();
    const past = new Date(Date.now() - 60_000);
    await storage.createWorkflow({ workflowId: "w1", workflowName: "w", input: {} });
    await storage.suspendWorkflow({
      workflowId: "w1",
      stepName: "nap",
      stepUpdate: { status: "sleeping", stepType: "sleep", wakeAt: past },
    });
    await storage.deliverSignal({ workflowId: "w1", signalName: "workflow.update:u", payload: 1 });
    captured.length = 0;

    await storage.listDueTimers({ now: new Date(), limit: 10, afterWorkflowId: "a" });
    await storage.listSignalWakeups({ limit: 10, afterWorkflowId: "a" });
    await storage.listUpdateWakeups({ limit: 10, afterWorkflowId: "a" });
    await storage.listChildEndedWaits({ limit: 10, afterWorkflowId: "a" });
    const scans = captured.filter(
      (c) => /status = 'suspended'/.test(c.sql) && /LIMIT \?\s*$/.test(c.sql),
    );
    expect(scans.length).toBeGreaterThanOrEqual(3);

    for (const scan of scans) {
      const plan = db
        .query<{ parent: number; detail: string }, Array<string | number>>(
          `EXPLAIN QUERY PLAN ${scan.sql}`,
        )
        .all(...(scan.args as Array<string | number>));
      const top = plan.filter((row) => row.parent === 0).map((row) => row.detail);
      expect(top.some((d) => /USING (COVERING )?INDEX promin_wf_suspended/.test(d))).toBe(true);
      expect(top.some((d) => /TEMP B-TREE/.test(d))).toBe(false);
    }
  });
});
