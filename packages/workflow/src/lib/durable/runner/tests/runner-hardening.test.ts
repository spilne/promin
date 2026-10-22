// ---------------------------------------------------------------------------
// Runner surface hardening: routed step execution, `start()` under
// concurrency, the polling event stream's edge cases, and the scoped query
// handler registry.
// ---------------------------------------------------------------------------

import { afterEach, describe, it, expect } from "bun:test";
import { FakeWallClock, SystemWallClock } from "../../../shared/wall-clock.ts";
import { workflow } from "../../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import {
  configureQueryRegistry,
  hasQueryHandlers,
  invokeQueryHandler,
  openQueryScope,
  registerQueryHandler,
} from "../../query-registry.ts";
import { createWorkflowRunner, RoutingStepExecutor } from "../../workflow-runner.ts";
import type { WorkflowRunEvent, WorkflowState } from "../../workflow-state.ts";
import type { WorkflowStorage } from "../../workflow-storage.ts";
import type { StepExecutionRequest, StepExecutor } from "../step-executor.ts";
import { pollWorkflowEvents } from "../workflow-observer.ts";

/** A promise and its resolver. */
function gate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((r) => (open = r));
  return { promise, open };
}

describe("RoutingStepExecutor", () => {
  it("runs routed steps beside their local siblings and decodes their results", async () => {
    const storage = new InMemoryWorkflowStorage();
    const localDone = gate();
    const order: string[] = [];
    const wf = workflow({ name: "routed" })
      .stepAsync("root", async () => "root")
      .stepAsync("remote", { dependsOn: ["root"] }, async () => new Date(0))
      .stepAsync("local", { dependsOn: ["root"] }, async () => {
        order.push("local");
        localDone.open();
        return "local";
      })
      .stepAsync("join", { dependsOn: ["remote", "local"] }, async ({ deps }) => ({
        isDate: deps.remote instanceof Date,
        at: (deps.remote as Date).getTime(),
      }))
      .build();
    const remoteDef = wf._definition.steps.find((s) => s.name === "remote")!;
    const requests: StepExecutionRequest[] = [];
    // The remote side reports an encoded result, as a queue worker would.
    const remote: StepExecutor = {
      executeStep: async (req) => {
        requests.push(req);
        await localDone.promise; // only finishes once its local sibling has
        order.push("remote");
        return { ok: true, result: remoteDef.codec.encode(new Date(7)) };
      },
    };
    const runner = createWorkflowRunner({
      storage,
      stepExecutor: new RoutingStepExecutor({ remote, remoteSteps: ["remote"], storage }),
    });

    const result = await runner.run({ workflow: wf, workflowId: "route-1", input: undefined });

    expect(order).toEqual(["local", "remote"]);
    expect(requests.map((r) => r.stepName)).toEqual(["remote"]);
    expect(result).toEqual({ isDate: true, at: 7 });
  });

  it("requires a local executor or a storage for the default one", () => {
    const remote: StepExecutor = { executeStep: async () => ({ ok: true, result: 1 }) };
    expect(() => new RoutingStepExecutor({ remote, remoteSteps: [] })).toThrow();
  });
});

describe("start()", () => {
  const slowWorkflow = (params: { release: Promise<void>; runs: string[]; join?: boolean }) =>
    workflow({ name: "slow-start" })
      .stepAsync("work", async () => {
        params.runs.push("work");
        await params.release;
        return "done";
      })
      .build(params.join ? { idempotency: { ttl: 60_000, onInFlight: "join" } } : undefined);

  it("of two concurrent starts with onInFlight: reject, exactly one starts the run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const release = gate();
    const runs: string[] = [];
    const wf = slowWorkflow({ release: release.promise, runs });

    const settled = await Promise.allSettled([
      runner.start({ workflow: wf, workflowId: "s-1", input: undefined }),
      runner.start({ workflow: wf, workflowId: "s-1", input: undefined }),
    ]);
    release.open();

    const rejected = settled.filter((s) => s.status === "rejected");
    expect(rejected).toHaveLength(1);
    expect((rejected[0] as PromiseRejectedResult).reason).toMatchObject({
      _tag: "WorkflowLockError",
    });
    const handle = (
      settled.find((s) => s.status === "fulfilled") as PromiseFulfilledResult<
        Awaited<ReturnType<typeof runner.start>>
      >
    ).value;
    expect(await handle.result({ timeoutMs: 5_000, intervalMs: 5 })).toBe("done");
    expect(runs).toEqual(["work"]);
  });

  it("with onInFlight: join, both callers get a handle to the one run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const release = gate();
    const runs: string[] = [];
    const wf = slowWorkflow({ release: release.promise, runs, join: true });

    const [a, b] = await Promise.all([
      runner.start({ workflow: wf, workflowId: "s-2", input: undefined }),
      runner.start({ workflow: wf, workflowId: "s-2", input: undefined }),
    ]);
    release.open();
    expect(await a.result({ timeoutMs: 5_000, intervalMs: 5 })).toBe("done");
    expect(await b.result({ timeoutMs: 5_000, intervalMs: 5 })).toBe("done");
    expect(runs).toEqual(["work"]);
  });

  it("returns once the run has started, and rejects a start on a suspended run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "waits" })
      .stepAsync("a", async () => 1)
      .waitForSignal("wait", { signalName: "go" })
      .build();

    await runner.start({ workflow: wf, workflowId: "s-3", input: undefined });
    // The run took its lock and created its row before start() returned.
    expect(await storage.loadWorkflow("s-3")).not.toBeNull();

    for (let i = 0; i < 100; i++) {
      if ((await storage.loadWorkflow("s-3"))?.status === "suspended") break;
      await new Promise<void>((r) => setTimeout(r, 5));
    }
    expect((await storage.loadWorkflow("s-3"))?.status).toBe("suspended");
    await expect(
      runner.start({ workflow: wf, workflowId: "s-3", input: undefined }),
    ).rejects.toMatchObject({ _tag: "WorkflowLockError" });
  });

  it("takes run()'s params: namespace and idempotency key", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "keyed" })
      .stepAsync("a", async () => "ok")
      .build();

    const first = await runner.start({
      workflow: wf,
      workflowId: "k-1",
      input: undefined,
      namespace: "tenant-a",
      idempotencyKey: "order-9",
      idempotencyKeyTTL: 60_000,
    });
    expect(await first.result({ timeoutMs: 5_000, intervalMs: 5 })).toBe("ok");
    expect((await storage.loadWorkflow("k-1"))?.namespace).toBe("tenant-a");

    // Same key, new id: resolves to the first run.
    const second = await runner.start({
      workflow: wf,
      workflowId: "k-2",
      input: undefined,
      namespace: "tenant-a",
      idempotencyKey: "order-9",
      idempotencyKeyTTL: 60_000,
    });
    expect(second.workflowId).toBe("k-1");
    expect(await storage.loadWorkflow("k-2")).toBeNull();
  });
});

describe("polling event stream", () => {
  const stateOf = (run: number, steps: string[], status: WorkflowState["status"] = "running") =>
    ({
      workflowId: "p",
      workflowName: "p",
      status,
      run,
      input: null,
      createdAt: new Date(0),
      updatedAt: new Date(0),
      steps: Object.fromEntries(
        steps.map((name) => [
          name,
          {
            stepName: name,
            run,
            status: "completed",
            dependsOn: [],
            stepType: "single",
            attempt: 1,
            result: `${name}@${run}`,
            completedAt: new Date(run),
          },
        ]),
      ),
    }) as WorkflowState;

  async function collect(stream: AsyncIterable<WorkflowRunEvent>): Promise<WorkflowRunEvent[]> {
    const out: WorkflowRunEvent[] = [];
    for await (const e of stream) out.push(e);
    return out;
  }

  it("ends at once for a signal that is already aborted", async () => {
    let loads = 0;
    const storage = {
      loadWorkflow: async () => {
        loads++;
        return stateOf(1, []);
      },
    } as unknown as WorkflowStorage;
    const controller = new AbortController();
    controller.abort();

    const events = await collect(
      pollWorkflowEvents({
        storage,
        clock: SystemWallClock,
        workflowId: "p",
        options: { signal: controller.signal, pollIntervalMs: 1 },
      }),
    );
    expect(events).toEqual([]);
    expect(loads).toBe(0);
  });

  it("rejects after the error budget of failed reads in a row", async () => {
    let loads = 0;
    const storage = {
      loadWorkflow: async () => {
        loads++;
        throw new Error("storage down");
      },
    } as unknown as WorkflowStorage;

    await expect(
      collect(
        pollWorkflowEvents({
          storage,
          clock: SystemWallClock,
          workflowId: "p",
          options: { pollIntervalMs: 1, maxConsecutiveErrors: 3 },
        }),
      ),
    ).rejects.toThrow("storage down");
    expect(loads).toBe(3);
  });

  it("a successful read resets the error budget", async () => {
    const script: Array<WorkflowState | Error> = [
      new Error("blip"),
      new Error("blip"),
      stateOf(1, ["a"]),
      new Error("blip"),
      new Error("blip"),
      stateOf(1, ["a"], "completed"),
    ];
    const storage = {
      loadWorkflow: async () => {
        const next = script.shift()!;
        if (next instanceof Error) throw next;
        return next;
      },
    } as unknown as WorkflowStorage;

    const events = await collect(
      pollWorkflowEvents({
        storage,
        clock: SystemWallClock,
        workflowId: "p",
        options: { pollIntervalMs: 1, maxConsecutiveErrors: 3 },
      }),
    );
    expect(events.map((e) => e.type)).toEqual(["step-completed", "workflow-completed"]);
  });

  it("reports the steps of a new run again", async () => {
    const script = [stateOf(1, ["a"]), stateOf(2, []), stateOf(2, ["a"], "completed")];
    const storage = {
      loadWorkflow: async () => script.shift() ?? stateOf(2, ["a"], "completed"),
    } as unknown as WorkflowStorage;

    const events = await collect(
      pollWorkflowEvents({
        storage,
        clock: SystemWallClock,
        workflowId: "p",
        options: { pollIntervalMs: 1 },
      }),
    );
    expect(
      events.map((e) => (e.type === "step-completed" ? `${e.type}:${String(e.result)}` : e.type)),
    ).toEqual(["step-completed:a@1", "step-completed:a@2", "workflow-completed"]);
  });
});

describe("query handler registry", () => {
  afterEach(() => configureQueryRegistry({ clock: SystemWallClock }));

  it("only the scope that owns a workflow's handlers can drop them", async () => {
    const stale = openQueryScope("q-own");
    const live = openQueryScope("q-own");
    registerQueryHandler("q-own", "status", () => "busy");

    stale.close();
    expect(await invokeQueryHandler("q-own", "status")).toBe("busy");

    live.close();
    expect(hasQueryHandlers("q-own")).toBe(false);
  });

  it("a suspended run's handlers are dropped after the TTL", async () => {
    const clock = FakeWallClock.create(0);
    configureQueryRegistry({ clock, suspendedTtlMs: 1_000 });
    const scope = openQueryScope("q-ttl");
    registerQueryHandler("q-ttl", "status", () => "waiting");
    scope.close({ suspended: true });

    clock.advance(999);
    expect(await invokeQueryHandler("q-ttl", "status")).toBe("waiting");
    clock.advance(1);
    expect(hasQueryHandlers("q-ttl")).toBe(false);
    configureQueryRegistry({ suspendedTtlMs: 60 * 60 * 1000 });
  });

  it("a resume here takes the suspended handlers back and keeps them past the TTL", async () => {
    const clock = FakeWallClock.create(0);
    configureQueryRegistry({ clock, suspendedTtlMs: 1_000 });
    openQueryScope("q-resume");
    registerQueryHandler("q-resume", "status", () => "v1");
    // Suspend, then resume on this process before the TTL.
    const first = openQueryScope("q-resume");
    first.close({ suspended: true });
    const resumed = openQueryScope("q-resume");
    clock.advance(5_000);
    expect(await invokeQueryHandler("q-resume", "status")).toBe("v1");
    resumed.reset();
    expect(hasQueryHandlers("q-resume")).toBe(false);
    resumed.close();
    configureQueryRegistry({ suspendedTtlMs: 60 * 60 * 1000 });
  });
});
