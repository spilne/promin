import { succeed, tryPromise } from "@spilne/perfect-core";
import { beforeEach, describe, it, expect } from "bun:test";
import { createWorkflowRunner, runJournaledStep, workflow } from "@promin/workflow";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { PgStateMachineStorage } from "../pg-state-machine-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

// ---------------------------------------------------------------------------
// Postgres-specific regressions for the workflow store: lock exclusion and
// release across one connection pool, the journal on fresh runs and
// continue-as-new, `ctx.child` against the journal CHECK constraint, and
// state-machine lease locks. The portable contract lives in the conformance
// suite; these pin the Postgres mechanics behind it.
// ---------------------------------------------------------------------------

postgresDescribe("PostgresWorkflowStorage store correctness", { migrate }, (pg) => {
  beforeEach(async () => {
    await pg.sql`TRUNCATE TABLE
      wf_workflow_step_tasks, wf_workflow_steps, wf_workflow_signals, wf_workflow_locks,
      wf_workflow_runs, wf_activity_journal, wf_signal_tokens, wf_streams, wf_workflows,
      wf_step_queue, sm_machine_locks
    RESTART IDENTITY CASCADE`;
  });

  const create = () => PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });

  describe("locks", () => {
    it("default locks exclude a second caller on the same pool and issue fence tokens", async () => {
      const a = await create();
      const b = await create();
      const first = await a.tryLock("lk", 30_000);
      const again = await a.tryLock("lk", 30_000);
      const other = await b.tryLock("lk", 30_000);

      expect(first).toMatchObject({ acquired: true });
      expect(first.token).toBeDefined();
      expect(again.acquired).toBe(false);
      expect(other.acquired).toBe(false);

      await a.releaseLock("lk", { fenceToken: first.token });
      expect((await b.tryLock("lk", 30_000)).acquired).toBe(true);
    });

    it("concurrent lock/release pairs leave no lock behind", async () => {
      const s = await create();
      const ids = Array.from({ length: 8 }, (_, i) => `adv-${i}`);
      const held = await Promise.all(ids.map((id) => s.tryLock(id, 1_000)));
      expect(held.every((h) => h.acquired)).toBe(true);
      await Promise.all(ids.map((id, i) => s.releaseLock(id, { fenceToken: held[i]!.token })));

      const [{ rows }] = (await pg.sql`
        SELECT (SELECT count(*) FROM wf_workflow_locks)::int
             + (SELECT count(*) FROM pg_locks WHERE locktype = 'advisory')::int AS rows
      `) as unknown as Array<{ rows: number }>;
      expect(rows).toBe(0);
    });

    it("lease expiry is judged on the server clock", async () => {
      const s = await create();
      await s.tryLock("srv", 60_000);
      const [row] = (await pg.sql`
        SELECT EXTRACT(EPOCH FROM (expires_at - NOW())) AS remaining
        FROM wf_workflow_locks WHERE workflow_id = 'srv'
      `) as unknown as Array<{ remaining: string }>;
      expect(Number(row!.remaining)).toBeGreaterThan(55);
      expect(Number(row!.remaining)).toBeLessThanOrEqual(60);
    });
  });

  describe("journal and fresh runs", () => {
    it("startFreshRun makes the next run re-execute its activities", async () => {
      const s = await create();
      await s.createWorkflow({ workflowId: "can", workflowName: "w", input: 1 });
      let calls = 0;
      const body = function* (ctx: { activity: (n: string, f: () => Promise<number>) => any }) {
        return (yield* ctx.activity("fetch", async () => ++calls)) as number;
      };
      const run = () =>
        runJournaledStep({
          input: 1,
          prev: undefined,
          workflowId: "can",
          stepName: "s",
          storage: s,
          workflowStorage: s,
          body: body as never,
        });

      expect(await run()).toBe(1);
      expect(await run()).toBe(1); // replayed, not re-executed
      await s.startFreshRun("can");
      expect(await run()).toBe(2);
      expect(calls).toBe(2);
    });

    it("continue-as-new re-runs activities on every chain link", async () => {
      const storage = await create();
      const runner = createWorkflowRunner({ storage });
      let activityCalls = 0;
      const wf = workflow<{ count: number }>({ name: "act-counter" })
        .journaled("loop", function* (ctx) {
          const seen = yield* ctx.activity("work", async () => {
            activityCalls++;
            return ctx.input.count;
          });
          if (seen >= 2) return { final: seen };
          ctx.continueAsNew({ count: ctx.input.count + 1 });
        })
        .build();

      const result = await runner.run({ workflow: wf, workflowId: "can-1", input: { count: 0 } });

      expect(result).toEqual({ final: 2 });
      expect(activityCalls).toBe(3);
      const state = await storage.loadWorkflow("can-1");
      expect(state?.status).toBe("completed");
      expect(state?.run).toBe(3);
    });

    it("ctx.child journals and links the child run", async () => {
      const storage = await create();
      let enrichCalls = 0;
      const enrich = workflow<{ userId: string }>({ name: "enrich" })
        .step("fetch", ({ input }) =>
          tryPromise(
            async () => {
              enrichCalls++;
              return { userId: input.userId, tags: ["vip"] };
            },
            (e) => e,
          ).orDie(),
        )
        .build();
      const parent = workflow<{ userId: string }>({ name: "signup" })
        .journaled("setup", function* (ctx, input) {
          return yield* ctx.child(enrich, {
            input: { userId: input.userId },
            workflowId: `enrich-${input.userId}`,
          });
        })
        .build();
      const runner = createWorkflowRunner({ storage });

      const first = await runner.run({
        workflow: parent,
        workflowId: "par-1",
        input: { userId: "u1" },
      });
      const again = await runner.run({
        workflow: parent,
        workflowId: "par-1",
        input: { userId: "u1" },
      });

      expect(first).toEqual({ userId: "u1", tags: ["vip"] });
      expect(again).toEqual(first);
      expect(enrichCalls).toBe(1);
      const journal = await storage.loadJournal("par-1", "setup");
      expect(journal.map((e) => e.stepType)).toEqual(["child"]);
      const child = await storage.loadWorkflow("enrich-u1");
      expect(child?.parentWorkflowId).toBe("par-1");
      expect((await storage.listWorkflows({ parentId: "par-1" })).map((w) => w.workflowId)).toEqual(
        ["enrich-u1"],
      );
    });

    it("cancel cascade reaches child runs", async () => {
      const storage = await create();
      const childWf = workflow<{ x: number }>({ name: "child-wf" })
        .step("double", ({ input }) => succeed(input.x * 2))
        .build();
      await storage.createWorkflow({ workflowId: "root", workflowName: "p", input: {} });
      await storage.createWorkflow({
        workflowId: "kid",
        workflowName: childWf.name,
        input: { x: 1 },
        parentWorkflowId: "root",
      });
      await storage.cancelWorkflow("root", { cascade: true });
      expect((await storage.loadWorkflow("kid"))?.error).toBe("Cancelled");
    });
  });

  describe("PgStateMachineStorage locks", () => {
    it("excludes other instances on the same pool and releases reliably", async () => {
      const a = new PgStateMachineStorage(pg.db);
      const b = new PgStateMachineStorage(pg.db);

      expect(await a.tryLock("m1", 30_000)).toBe(true);
      expect(await a.tryLock("m1", 30_000)).toBe(false);
      expect(await b.tryLock("m1", 30_000)).toBe(false);

      await b.releaseLock("m1"); // not the holder — no effect
      expect(await b.tryLock("m1", 30_000)).toBe(false);

      await a.releaseLock("m1");
      expect(await b.tryLock("m1", 30_000)).toBe(true);
    });

    it("an expired lease can be taken over", async () => {
      const a = new PgStateMachineStorage(pg.db);
      const b = new PgStateMachineStorage(pg.db);
      expect(await a.tryLock("m2", 1)).toBe(true);
      await new Promise((r) => setTimeout(r, 30));
      expect(await b.tryLock("m2", 30_000)).toBe(true);
    });

    it("concurrent tryLock calls produce one winner", async () => {
      const instances = Array.from({ length: 6 }, () => new PgStateMachineStorage(pg.db));
      const results = await Promise.all(instances.map((i) => i.tryLock("m3", 30_000)));
      expect(results.filter(Boolean)).toHaveLength(1);
    });
  });
});
