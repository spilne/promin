import { afterEach, describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow, type Workflow } from "../../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import type { DagNode } from "../../workflow-dag.ts";
import { createWorkflowRunner, executeWorkflowDag } from "../../workflow-runner.ts";
import type { WorkflowRunEvent } from "../../workflow-state.ts";
import type { WorkflowStorage, NotifyStepStartedParams } from "../../workflow-storage.ts";

const nextMacrotask = () => new Promise<void>((r) => setImmediate(r));

async function waitFor(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !cond(); i++) await nextMacrotask();
  if (!cond()) throw new Error("waitFor: condition never held");
}

function chain(n: number): Workflow<number, number> {
  let b: any = workflow<number>({ name: `chain-${n}` });
  for (let i = 0; i < n; i++) {
    b = b.step(`s${i}`, ({ prev, input }: { prev?: number; input: number }) =>
      succeed((prev ?? input) + 1),
    );
  }
  return b.build();
}

/** Counts calls per storage method; `hide` makes the listed methods absent. */
function countingStorage(params: { hide?: readonly string[] } = {}): {
  storage: WorkflowStorage;
  counts: Record<string, number>;
} {
  const counts: Record<string, number> = {};
  const hidden = new Set(params.hide ?? []);
  const storage = new Proxy(new InMemoryWorkflowStorage(), {
    get(target, key, receiver) {
      if (typeof key === "string" && hidden.has(key)) return undefined;
      const value = Reflect.get(target, key, receiver);
      if (typeof value !== "function" || typeof key !== "string") return value;
      return (...args: unknown[]) => {
        counts[key] = (counts[key] ?? 0) + 1;
        return value.apply(target, args);
      };
    },
  });
  return { storage: storage as WorkflowStorage, counts };
}

// ---------------------------------------------------------------------------
// Scheduling cost grows linearly with the number of steps
// ---------------------------------------------------------------------------

describe("scheduling cost", () => {
  const N = 4_000;

  it("a 4,000-step chain reads each step's dependencies a bounded number of times", async () => {
    const wf = chain(N);
    const steps = wf._definition.steps;
    // Recomputing the ready set every wave reads every pending node's
    // dependencies once per wave: about N²/2 (8M) reads for this chain.
    let dependencyReads = 0;
    const dagNodes: DagNode[] = steps.map((s) => ({
      name: s.name,
      get dependsOn() {
        dependencyReads++;
        return s.dependsOn;
      },
    }));
    const { storage, counts } = countingStorage();
    await storage.createWorkflow({ workflowId: "chain", workflowName: wf.name, input: 0 });

    const result = await executeWorkflowDag(
      { storage, steps },
      {
        workflowId: "chain",
        input: 0,
        dagNodes,
        state: null,
        workflowStartTime: 0,
        stepAttempts: new Map(),
      },
    );

    expect(result).toEqual({ success: true, result: N });
    expect(dependencyReads).toBeLessThanOrEqual(2 * N);
    for (const method of [
      "notifyStepStarted",
      "checkpointStep",
      "saveStepResult",
      "saveStepAttempt",
      "loadWorkflowStatus",
    ]) {
      expect(counts[method] ?? 0).toBeLessThanOrEqual(N);
    }
    expect(counts.loadWorkflow ?? 0).toBe(0);
  });

  it("a 4,000-step run makes a constant number of run-level storage calls", async () => {
    const { storage, counts } = countingStorage();
    const runner = createWorkflowRunner({ storage });

    const result = await runner.run({ workflow: chain(N), workflowId: "run", input: 0 });

    expect(result).toBe(N);
    // The lock and the run's state come in one call.
    expect(counts.tryLockAndLoad).toBe(1);
    expect(counts.tryLock ?? 0).toBe(0);
    // Only the read-back of the freshly created run.
    expect(counts.loadWorkflow ?? 0).toBeLessThanOrEqual(1);
    // One write per step: its row and attempt row together, which also
    // reads the run's status, so no wave reads it separately. The one
    // status read is the cancel check after the completion.
    expect(counts.checkpointStep).toBe(N);
    expect(counts.saveStepResult ?? 0).toBe(0);
    expect(counts.saveStepAttempt ?? 0).toBe(0);
    expect(counts.loadWorkflowStatus ?? 0).toBe(1);
    expect(counts.notifyStepStarted).toBe(N);
  });

  it("a storage without checkpointStep gets the separate writes and a status read per wave", async () => {
    const { storage, counts } = countingStorage({ hide: ["checkpointStep"] });
    const runner = createWorkflowRunner({ storage });

    const result = await runner.run({ workflow: chain(10), workflowId: "split", input: 0 });

    expect(result).toBe(10);
    expect(counts.checkpointStep ?? 0).toBe(0);
    expect(counts.saveStepResult).toBe(10);
    expect(counts.saveStepAttempt).toBe(10);
    // One per wave after the first, plus the check after the completion.
    expect(counts.loadWorkflowStatus).toBe(10);
  });

  it("falls back to tryLock + loadWorkflow on a storage without tryLockAndLoad", async () => {
    const { storage, counts } = countingStorage({ hide: ["tryLockAndLoad"] });
    const runner = createWorkflowRunner({ storage });

    const result = await runner.run({ workflow: chain(3), workflowId: "fallback", input: 0 });

    expect(result).toBe(3);
    expect(counts.tryLock).toBe(1);
    expect(counts.loadWorkflow).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// step-started notices go out together and never hold a step back
// ---------------------------------------------------------------------------

/** In-memory storage whose `notifyStepStarted` waits until the test releases it. */
class GatedNotifyStorage extends InMemoryWorkflowStorage {
  readonly pending: { stepName: string; release: () => void }[] = [];
  autoRelease = false;

  override async notifyStepStarted({
    workflowId,
    stepName,
  }: NotifyStepStartedParams): Promise<void> {
    if (!this.autoRelease) {
      await new Promise<void>((release) => this.pending.push({ stepName, release }));
    }
    super.notifyStepStarted({ workflowId, stepName });
  }

  releaseAll(): void {
    for (const p of this.pending.splice(0)) p.release();
  }
}

describe("step-started notices", () => {
  const originalWarn = console.warn;
  afterEach(() => {
    console.warn = originalWarn;
  });

  it("are sent for a whole wave at once, while the steps run", async () => {
    const storage = new GatedNotifyStorage();
    const runner = createWorkflowRunner({ storage });
    const ran: string[] = [];
    const wf = workflow<number>({ name: "gated" })
      .step("root", ({ input }) => {
        ran.push("root");
        return succeed(input);
      })
      .parallelSteps("fork", {
        a: ({ prev }) => {
          ran.push("a");
          return succeed(prev + 1);
        },
        b: ({ prev }) => {
          ran.push("b");
          return succeed(prev + 2);
        },
        c: ({ prev }) => {
          ran.push("c");
          return succeed(prev + 3);
        },
      })
      .build();

    const eventsP = (async () => {
      const events: WorkflowRunEvent[] = [];
      for await (const ev of runner.subscribe({ workflowId: "gated-1" })) events.push(ev);
      return events;
    })();
    const done = runner.run({ workflow: wf, workflowId: "gated-1", input: 10 });

    // The root step's body runs while its notice is pending; its row waits.
    await waitFor(() => storage.pending.length === 1 && ran.includes("root"));
    expect((await storage.loadWorkflow("gated-1"))?.steps["root"]).toBeUndefined();
    storage.releaseAll();

    // All three branch notices are in flight together, and the branch
    // bodies have already run.
    await waitFor(() => storage.pending.length === 3 && ran.length === 4);
    expect(storage.pending.map((p) => p.stepName).sort()).toEqual(["fork.a", "fork.b", "fork.c"]);
    const mid = await storage.loadWorkflow("gated-1");
    expect(Object.keys(mid?.steps ?? {}).filter((k) => k.startsWith("fork."))).toEqual([]);
    storage.autoRelease = true;
    storage.releaseAll();

    await done;
    const events = await eventsP;
    for (const step of ["root", "fork.a", "fork.b", "fork.c"]) {
      const started = events.findIndex((e) => e.type === "step-started" && e.stepName === step);
      const completed = events.findIndex((e) => e.type === "step-completed" && e.stepName === step);
      expect(started).toBeGreaterThanOrEqual(0);
      expect(completed).toBeGreaterThan(started);
    }
  });

  it("that fail are reported and never fail the run", async () => {
    const warnings: unknown[][] = [];
    console.warn = (...args: unknown[]) => {
      warnings.push(args);
    };
    const storage = new InMemoryWorkflowStorage();
    let calls = 0;
    storage.notifyStepStarted = ({
      workflowId: _workflowId,
      stepName,
    }: NotifyStepStartedParams): any => {
      calls++;
      if (stepName === "a") throw new Error("bus down (sync)");
      return Promise.reject(new Error("bus down (async)"));
    };
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "notify-fails" })
      .step("a", ({ input }) => succeed(input + 1))
      .step("b", ({ prev }) => succeed(prev * 2))
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "nf-1", input: 1 })).toBe(4);
    expect(calls).toBe(2);
    expect(warnings).toHaveLength(2);
    expect(String(warnings[0]![0])).toContain('"a"');
    expect(String(warnings[1]![0])).toContain('"b"');
  });
});
