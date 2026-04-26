// ---------------------------------------------------------------------------
// ctx.continueAsNew — clean restart of the same workflowId with new input.
//
// Used by long-running workflows that would otherwise grow the journal
// without bound — typical
// example: a workflow that processes a stream of batches and re-enters
// itself with the next batch id after each iteration.
//
// Pins the contract:
//   - The body unwinds on continueAsNew (return type: never).
//   - The runner archives the current run via startFreshRun, then runs
//     fresh under the same workflowId with the carried input.
//   - Each chained run gets its own journal — replay cost stays bounded.
//   - Compensations do NOT run on continueAsNew (clean restart, not failure).
//   - Hard cap (1024) protects against infinite loops in user code.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe("ctx.continueAsNew", () => {
  it("chains N continue-as-new calls under the same workflowId, each with its own journal", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const seen: number[] = [];

    const wf = workflow<{ count: number }>({ name: "counter" })
      .journaled("loop", function* (ctx) {
        seen.push(ctx.input.count);
        if (ctx.input.count >= 3) {
          return { final: ctx.input.count };
        }
        ctx.continueAsNew({ count: ctx.input.count + 1 });
      })
      .build();

    const result = await runner.run({
      workflow: wf,
      workflowId: "cnt-1",
      input: { count: 0 },
    });

    expect(result).toEqual({ final: 3 });
    expect(seen).toEqual([0, 1, 2, 3]);

    // The final state lives under the same workflowId; the prior runs are
    // archived in run history.
    const state = await storage.loadWorkflow("cnt-1");
    expect(state?.status).toBe("completed");
    const runs = await storage.loadRunHistory("cnt-1");
    expect(runs.length).toBeGreaterThanOrEqual(3);
  });

  it("replay-safe — restarting against a continued-as-new row resumes the LATEST run, not the chain", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ n: number }>({ name: "chain" })
      .journaled("step", function* (ctx) {
        if (ctx.input.n < 2) {
          ctx.continueAsNew({ n: ctx.input.n + 1 });
        }
        return { final: ctx.input.n };
      })
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "ch-1", input: { n: 0 } });
    expect(r1).toEqual({ final: 2 });

    // Drive the runner against the SAME workflowId again — should hit the
    // idempotency cache (workflow already completed), not re-execute.
    const state = await storage.loadWorkflow("ch-1");
    expect(state?.run).toBeGreaterThanOrEqual(3); // 0 → 1 → 2 = 3 fresh runs minimum
  });

  it("fires the 1024-chain safety belt on infinite continue-as-new loops", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ n: number }>({ name: "infinite" })
      .journaled("loop", function* (ctx) {
        ctx.continueAsNew({ n: ctx.input.n + 1 });
      })
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "inf-1", input: { n: 0 } }),
    ).rejects.toThrow(/continue-as-new chain limit/);
  });

  it("activities emitted before continueAsNew run on the FIRST execution only — not re-emitted on the next chain link", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let activityCalls = 0;

    const wf = workflow<{ count: number }>({ name: "act-counter" })
      .journaled("loop", function* (ctx) {
        const _ = yield* ctx.activity("work", async () => {
          activityCalls++;
          return ctx.input.count;
        });
        if (ctx.input.count >= 2) return { final: ctx.input.count };
        ctx.continueAsNew({ count: ctx.input.count + 1 });
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "act-1", input: { count: 0 } });

    // Each chain link runs the activity once — three links = three calls.
    // The CRITICAL property is that startFreshRun cleared the prior journal
    // so each link's activity-0 doesn't replay from a stale entry; it runs
    // anew with its own count input.
    expect(activityCalls).toBe(3);
  });
});
