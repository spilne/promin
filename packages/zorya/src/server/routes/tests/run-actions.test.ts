// ---------------------------------------------------------------------------
// Run actions through the ZoryaServer harness: trigger idempotency and
// conflicts, rerun, mark-success / mark-failed / cancel, signals, and
// own-property definition lookups.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
import {
  InMemoryWorkflowStorage,
  MARKED_FAILED_ERROR_TAG,
  createWorkflowRunner,
  workflow,
  type Workflow,
  type WorkflowRunner,
} from "@promin/workflow";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows, QueuedWorkflows } from "../../services/workflows/index.ts";
import { InMemoryWorkflowAdvertisementRegistry } from "@promin/workflow/distributed";
import { InMemoryWorkflowStartQueue } from "@promin/workflow/distributed";

/** Yield to the event loop until `predicate` holds, bounded by iterations. */
async function until(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("condition never held");
}

function setup(definitions: Record<string, Workflow<unknown, unknown>>) {
  const storage = new InMemoryWorkflowStorage();
  const runner: WorkflowRunner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions,
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const server = new ZoryaServer({ workflows, insecure: true });
  const post = (path: string, body?: unknown) =>
    server.handle(
      new Request(`http://x${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        ...(body !== undefined && { body: JSON.stringify(body) }),
      }),
    );
  const get = (path: string) => server.handle(new Request(`http://x${path}`));
  const status = async (id: string) => (await storage.loadWorkflow(id))?.status;
  return { storage, runner, server, post, get, status };
}

function counted(name: string) {
  const counter = { executions: 0 };
  const wf = workflow<{ n?: number }>({ name })
    .step("body", () =>
      sync(() => {
        counter.executions++;
        return `${name}-result`;
      }),
    )
    .build() as Workflow<unknown, unknown>;
  return { wf, counter };
}

function waiting(name: string) {
  return workflow<unknown>({ name })
    .waitForSignal("wait", { signalName: "go" })
    .build() as Workflow<unknown, unknown>;
}

describe("POST /api/workflows/:name/runs — existing workflowId", () => {
  it("a retried trigger with the same workflowId answers the run and does not re-run it", async () => {
    const charge = counted("charge");
    const { post, status, storage } = setup({ charge: charge.wf });

    const first = await post("/api/workflows/charge/runs", { workflowId: "order-42" });
    expect(first.status).toBe(202);
    expect(await first.json()).toEqual({ workflowId: "order-42" });
    await until(async () => (await status("order-42")) === "completed");

    const retry = await post("/api/workflows/charge/runs", { workflowId: "order-42" });
    expect(retry.status).toBe(200);
    expect(await retry.json()).toEqual({ workflowId: "order-42", existing: true });

    for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));
    expect(charge.counter.executions).toBe(1);
    expect((await storage.loadWorkflow("order-42"))?.run).toBe(1);
  });

  it("a trigger for an in-flight workflowId returns it without starting a second driver", async () => {
    const { post, status } = setup({ w: waiting("w") });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const again = await post("/api/workflows/w/runs", { workflowId: "w-1" });
    expect(again.status).toBe(200);
    expect(await again.json()).toEqual({ workflowId: "w-1", existing: true });
    expect(await status("w-1")).toBe("suspended");
  });

  it("a workflowId of another workflow's run is a 409 and leaves that run alone", async () => {
    const hello = counted("hello");
    const other = counted("other");
    const { post, status, storage } = setup({ hello: hello.wf, other: other.wf });
    await post("/api/workflows/hello/runs", { workflowId: "r1" });
    await until(async () => (await status("r1")) === "completed");

    const res = await post("/api/workflows/other/runs", { workflowId: "r1" });
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: string }).error).toBe("workflow_id_conflict");

    const run = await storage.loadWorkflow("r1");
    expect(run?.workflowName).toBe("hello");
    expect(run?.run).toBe(1);
    expect(run?.result).toBe("hello-result");
    expect(other.counter.executions).toBe(0);
  });

  it("prototype keys are not workflow names", async () => {
    const hello = counted("hello");
    const { post, get, storage } = setup({ hello: hello.wf });

    for (const name of ["constructor", "toString", "__proto__", "hasOwnProperty"]) {
      const res = await post(`/api/workflows/${name}/runs`, { workflowId: `p-${name}` });
      expect(res.status).toBe(404);
      expect(await storage.loadWorkflow(`p-${name}`)).toBeNull();
      expect((await get(`/api/workflows/${name}/definition`)).status).toBe(404);
    }
  });
});

describe("POST /api/runs/:id/rerun", () => {
  it("re-runs an ended run once: one run-number bump, one history row", async () => {
    const charge = counted("charge");
    const { post, get, status, storage } = setup({ charge: charge.wf });
    await post("/api/workflows/charge/runs", { workflowId: "c-1" });
    await until(async () => (await status("c-1")) === "completed");

    const res = await post("/api/runs/c-1/rerun");
    expect(res.status).toBe(202);
    await until(async () => {
      const run = await storage.loadWorkflow("c-1");
      return run?.run === 2 && run.status === "completed";
    });

    expect(charge.counter.executions).toBe(2);
    const history = (await (await get("/api/runs/c-1/history")).json()) as {
      runs: Array<{ run: number; status: string }>;
    };
    expect(history.runs.map((r) => [r.run, r.status])).toEqual([
      [2, "completed"],
      [1, "completed"],
    ]);
  });

  it("refuses an in-flight run with 409 and leaves it untouched", async () => {
    const { post, status, storage } = setup({ w: waiting("w") });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const res = await post("/api/runs/w-1/rerun");
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: string }).error).toBe("run_not_terminal");
    const run = await storage.loadWorkflow("w-1");
    expect(run?.run).toBe(1);
    expect(run?.status).toBe("suspended");
  });

  it("is a 404 for an unknown run", async () => {
    const { post } = setup({});
    expect((await post("/api/runs/nope/rerun")).status).toBe(404);
  });
});

describe("operator actions — mark-success, mark-failed, cancel", () => {
  it("404 for an unknown run, nothing stored", async () => {
    const { post, storage } = setup({});
    for (const action of ["mark-success", "mark-failed", "cancel"]) {
      const res = await post(`/api/runs/nope/${action}`);
      expect(res.status).toBe(404);
    }
    expect(await storage.loadWorkflow("nope")).toBeNull();
  });

  it("409 for an ended run, whose outcome is kept", async () => {
    const charge = counted("charge");
    const { post, status, storage } = setup({ charge: charge.wf });
    await post("/api/workflows/charge/runs", { workflowId: "c-1" });
    await until(async () => (await status("c-1")) === "completed");

    for (const action of ["mark-success", "mark-failed", "cancel"]) {
      const res = await post(`/api/runs/c-1/${action}`);
      expect(res.status).toBe(409);
      expect(((await res.json()) as { error: string }).error).toBe("run_ended");
    }
    const run = await storage.loadWorkflow("c-1");
    expect(run?.status).toBe("completed");
    expect(run?.result).toBe("charge-result");
  });

  it("ends an in-flight run", async () => {
    const { post, status, storage } = setup({ w: waiting("w") });
    for (const id of ["a", "b", "c"]) {
      await post("/api/workflows/w/runs", { workflowId: id });
      await until(async () => (await status(id)) === "suspended");
    }

    expect((await post("/api/runs/a/mark-success")).status).toBe(200);
    expect((await post("/api/runs/b/mark-failed", { reason: "bad input" })).status).toBe(200);
    expect((await post("/api/runs/c/cancel")).status).toBe(200);

    expect((await storage.loadWorkflow("a"))?.status).toBe("completed");
    const b = await storage.loadWorkflow("b");
    expect([b?.status, b?.error]).toEqual(["failed", "bad input"]);
    expect((await storage.loadWorkflow("c"))?.errorTag).toBe("WorkflowCancelledError");
  });

  it("mark-failed tags the run as an operator failure", async () => {
    const { post, status, storage } = setup({ w: waiting("w") });
    await post("/api/workflows/w/runs", { workflowId: "m-1" });
    await until(async () => (await status("m-1")) === "suspended");

    const res = await post("/api/runs/m-1/mark-failed", { reason: "stuck" });
    expect(await res.json()).toEqual({ workflowId: "m-1", status: "failed" });
    const run = await storage.loadWorkflow("m-1");
    expect([run?.error, run?.errorTag]).toEqual(["stuck", MARKED_FAILED_ERROR_TAG]);
  });

  it("an operator-failed child fails its parent with the child's reason", async () => {
    const child = waiting("child");
    const parent = workflow<unknown>({ name: "parent" })
      .subworkflow("kid", child, { input: () => ({}), workflowId: () => "kid-1" })
      .build() as Workflow<unknown, unknown>;
    const { post, status, storage, runner } = setup({ parent, child });
    await post("/api/workflows/parent/runs", { workflowId: "p-1" });
    await until(
      async () => (await status("kid-1")) === "suspended" && (await status("p-1")) === "suspended",
    );

    await post("/api/runs/kid-1/mark-failed", { reason: "bad payload" });
    await runner.runSafe({ workflow: parent, workflowId: "p-1", input: {} });
    const p = await storage.loadWorkflow("p-1");
    expect(p?.status).toBe("failed");
    expect(p?.steps.kid?.status).toBe("failed");
    expect(p?.error).toContain("bad payload");
    const kid = await storage.loadWorkflow("kid-1");
    expect([kid?.run, kid?.status]).toEqual([1, "failed"]);
  });

  // The resumed parent continues from the child's outcome: a completed
  // child completes it, and a cancelled or operator-failed child fails it.
  // An operator's failure is a decision, not a crash, so the parent step
  // observes it instead of running the child again from scratch.
  for (const [action, parentStatus, childRun] of [
    ["mark-success", "completed", 1],
    ["mark-failed", "failed", 1],
    ["cancel", "failed", 1],
  ] as const) {
    it(`${action} on a suspended child wakes its parent`, async () => {
      const child = waiting("child");
      const parent = workflow<unknown>({ name: "parent" })
        .subworkflow("kid", child, { input: () => ({}), workflowId: () => "kid-1" })
        .step("after", () => succeed("parent-done"))
        .build() as Workflow<unknown, unknown>;
      const { post, status, storage, runner } = setup({ parent, child });
      await post("/api/workflows/parent/runs", { workflowId: "p-1" });
      await until(
        async () =>
          (await status("kid-1")) === "suspended" && (await status("p-1")) === "suspended",
      );

      expect((await post(`/api/runs/kid-1/${action}`)).status).toBe(200);

      // The parent got its child-ended wake-up; resuming it (what the
      // signal scanner does) finishes the parent from the child's outcome.
      const signals = await storage.loadSignals("p-1");
      expect(signals.map((s) => s.signalName)).toEqual(["workflow.child-ended:kid-1#1"]);
      await runner.runSafe({ workflow: parent, workflowId: "p-1", input: {} });
      expect(await status("p-1")).toBe(parentStatus);
      expect((await storage.loadWorkflow("kid-1"))?.run).toBe(childRun);
    });
  }
});

describe("POST /api/runs/:id/cancel — cascade", () => {
  function family() {
    const child = waiting("child");
    const parent = workflow<unknown>({ name: "parent" })
      .subworkflow("kid", child, { input: () => ({}), workflowId: () => "kid-1" })
      .build() as Workflow<unknown, unknown>;
    return setup({ parent, child });
  }

  it("cancels the run's in-flight descendants with cascade: true", async () => {
    const { post, status, storage } = family();
    await post("/api/workflows/parent/runs", { workflowId: "p-1" });
    await until(
      async () => (await status("kid-1")) === "suspended" && (await status("p-1")) === "suspended",
    );

    const res = await post("/api/runs/p-1/cancel", { cascade: true });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ workflowId: "p-1", status: "failed" });
    expect((await storage.loadWorkflow("p-1"))?.errorTag).toBe("WorkflowCancelledError");
    expect((await storage.loadWorkflow("kid-1"))?.errorTag).toBe("WorkflowCancelledError");
  });

  it("leaves descendants running without cascade", async () => {
    const { post, status, storage } = family();
    await post("/api/workflows/parent/runs", { workflowId: "p-2" });
    await until(
      async () => (await status("kid-1")) === "suspended" && (await status("p-2")) === "suspended",
    );

    expect((await post("/api/runs/p-2/cancel")).status).toBe(200);
    expect((await storage.loadWorkflow("p-2"))?.errorTag).toBe("WorkflowCancelledError");
    expect(await status("kid-1")).toBe("suspended");
  });

  it("rejects an unknown body field with 400", async () => {
    const { post, status, storage } = family();
    await storage.createWorkflow({ workflowId: "x-1", workflowName: "parent", input: {} });
    expect((await post("/api/runs/x-1/cancel", { cascade: "yes" })).status).toBe(400);
    expect((await post("/api/runs/x-1/cancel", { recursive: true })).status).toBe(400);
    expect(await status("x-1")).toBe("pending");
  });
});

describe("POST /api/runs/:id/resume", () => {
  function pipeline() {
    const runs = { load: 0, transform: 0, publish: 0 };
    let failPublish = true;
    const wf = workflow<unknown>({ name: "pipeline" })
      .step("load", () => sync(() => ++runs.load))
      .step("transform", ({ prev }) => sync(() => (prev as number) * 10 + ++runs.transform))
      .step("publish", ({ prev }) =>
        sync(() => {
          runs.publish++;
          if (failPublish) throw new Error("publish failed");
          return `published ${prev as number}`;
        }),
      )
      .build() as Workflow<unknown, unknown>;
    return {
      wf,
      runs,
      heal: () => {
        failPublish = false;
      },
    };
  }

  function resumableSetup(definitions: Record<string, Workflow<unknown, unknown>>) {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions,
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ workflows, insecure: true });
    const post = (path: string, body?: unknown) =>
      server.handle(
        new Request(`http://x${path}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          ...(body !== undefined && { body: JSON.stringify(body) }),
        }),
      );
    const status = async (id: string) => (await storage.loadWorkflow(id))?.status;
    return { storage, post, status };
  }

  it("continues an ended run from a step, keeping the steps before it", async () => {
    const p = pipeline();
    const { post, status, storage } = resumableSetup({ pipeline: p.wf });
    await post("/api/workflows/pipeline/runs", { workflowId: "r-1" });
    await until(async () => (await status("r-1")) === "failed");
    expect(p.runs).toEqual({ load: 1, transform: 1, publish: 1 });

    p.heal();
    const res = await post("/api/runs/r-1/resume", { fromStep: "transform" });
    expect(res.status).toBe(202);
    expect(res.headers.get("location")).toBe("/api/runs/r-1");
    expect(await res.json()).toEqual({ workflowId: "r-1", fromStep: "transform" });
    await until(async () => (await status("r-1")) === "completed");

    // load kept its result; transform and publish ran again; same run number.
    expect(p.runs).toEqual({ load: 1, transform: 2, publish: 2 });
    const run = await storage.loadWorkflow("r-1");
    expect(run?.run).toBe(1);
    expect(run?.result).toBe("published 12");
  });

  it("is a 404 for an unknown run or step, nothing reset", async () => {
    const p = pipeline();
    const { post, status, storage } = resumableSetup({ pipeline: p.wf });
    expect((await post("/api/runs/nope/resume", { fromStep: "load" })).status).toBe(404);

    await post("/api/workflows/pipeline/runs", { workflowId: "r-2" });
    await until(async () => (await status("r-2")) === "failed");
    const res = await post("/api/runs/r-2/resume", { fromStep: "nope" });
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: string }).error).toBe("step_not_found");
    expect(await status("r-2")).toBe("failed");
    expect(Object.keys((await storage.loadWorkflow("r-2"))!.steps).sort()).toEqual([
      "load",
      "publish",
      "transform",
    ]);
  });

  it("is a 409 for an in-flight run, left untouched", async () => {
    const { post, status, storage } = resumableSetup({ w: waiting("w") });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const res = await post("/api/runs/w-1/resume", { fromStep: "wait" });
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: string }).error).toBe("run_not_terminal");
    expect((await storage.loadWorkflow("w-1"))?.steps.wait?.status).toBe("waiting_for_signal");
  });

  it("is a 409 for a cancelled run, which stays cancelled", async () => {
    const p = pipeline();
    const { post, status, storage } = resumableSetup({ pipeline: p.wf });
    await storage.createWorkflow({ workflowId: "rc-1", workflowName: "pipeline", input: {} });
    await storage.cancelWorkflow({ workflowId: "rc-1" });
    const res = await post("/api/runs/rc-1/resume", { fromStep: "load" });
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: string }).error).toBe("run_cancelled");
    expect(await status("rc-1")).toBe("failed");
  });

  it("is a 400 without fromStep", async () => {
    const { post } = resumableSetup({});
    expect((await post("/api/runs/x/resume", {})).status).toBe(400);
  });

  it("queues a worker-driven run for a hosting worker, which continues it from the step", async () => {
    const p = pipeline();
    const storage = new InMemoryWorkflowStorage();
    const workflowStarts = new InMemoryWorkflowStartQueue();
    const advertisements = new InMemoryWorkflowAdvertisementRegistry();
    await advertisements.upsert({
      workerId: "w1",
      workflows: [
        {
          name: "pipeline",
          steps: p.wf.dag.steps.map((st) => ({
            name: st.name,
            kind: st.kind,
            dependsOn: [...st.dependsOn],
          })),
        },
      ],
    });
    const workflows = new QueuedWorkflows({
      storage,
      workflowStarts,
      advertisements,
      wakeups: { sleepScanIntervalMs: 0, signalScanIntervalMs: 0 },
      maintenance: false,
    });
    const server = new ZoryaServer({ workflows, insecure: true });
    const post = (path: string, body?: unknown) =>
      server.handle(
        new Request(`http://x${path}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          ...(body !== undefined && { body: JSON.stringify(body) }),
        }),
      );
    // The hosting worker: claims each start and drives the run.
    const runner = createWorkflowRunner({ storage });
    const drive = async () => {
      const [start] = await workflowStarts.claim({
        workflowSpecs: [{ name: "pipeline", versions: [] }],
        limit: 1,
      });
      expect(start).toBeDefined();
      await runner.runSafe({ workflow: p.wf, workflowId: start!.workflowId, input: start!.input });
      await workflowStarts.complete({ id: start!.id, claimToken: start!.claimToken! });
    };

    await post("/api/workflows/pipeline/runs", { workflowId: "q-1" });
    await drive();
    expect((await storage.loadWorkflow("q-1"))?.status).toBe("failed");

    p.heal();
    const res = await post("/api/runs/q-1/resume", { fromStep: "publish" });
    expect(res.status).toBe(202);
    await drive();
    const run = await storage.loadWorkflow("q-1");
    expect(run?.status).toBe("completed");
    expect(run?.run).toBe(1);
    expect(p.runs).toEqual({ load: 1, transform: 1, publish: 2 });
  });
});

describe("POST /api/runs/:id/signal", () => {
  it("404 for an unknown run, and no orphan signal is stored", async () => {
    const { post, storage } = setup({});
    const res = await post("/api/runs/nope/signal", { signalName: "go" });
    expect(res.status).toBe(404);
    expect(await storage.loadSignals("nope")).toEqual([]);
  });

  it("409 for an ended run", async () => {
    const charge = counted("charge");
    const { post, status, storage } = setup({ charge: charge.wf });
    await post("/api/workflows/charge/runs", { workflowId: "c-1" });
    await until(async () => (await status("c-1")) === "completed");

    const res = await post("/api/runs/c-1/signal", { signalName: "go" });
    expect(res.status).toBe(409);
    expect(await storage.loadSignals("c-1")).toEqual([]);
  });

  it("delivers to a suspended run", async () => {
    const { post, status, storage } = setup({ w: waiting("w") });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const res = await post("/api/runs/w-1/signal", { signalName: "go", payload: 1 });
    expect(res.status).toBe(202);
    expect((await storage.loadSignals("w-1")).map((s) => s.signalName)).toEqual(["go"]);
  });
});
