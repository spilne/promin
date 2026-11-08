// ---------------------------------------------------------------------------
// WorkflowTracer: the order and content of the lifecycle events across a
// normal run, a retry, a failure with compensation, a signal wait and a child
// workflow; a tracer that throws never reaches the run; the trace context
// stored with a run and carried across a step queue.
// ---------------------------------------------------------------------------

import { describe, it, expect, spyOn } from "bun:test";
import { TaggedError, succeed, fail } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { InMemoryWorkflowVersionRegistry } from "../workflow-version-registry.ts";
import { createWorkflowRunner, RecoveryStrategy } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { WorkflowTracer, WorkflowTracerErrorEvent } from "../workflow-tracer.ts";
import { MapStepRegistry } from "../../distributed/step-registry.ts";
import { InMemoryStepQueue } from "../../distributed/in-memory-step-queue.ts";
import { createDistributedWorkflowRunner } from "../../distributed/coordinator.ts";
import { createWorker } from "../../distributed/worker.ts";

class TestError extends TaggedError("TestError")<{ readonly message: string }>() {}

const T0 = "2026-01-01T00:00:00Z";
const NAMES = [
  "lockAcquired",
  "lockLost",
  "runStart",
  "runEnd",
  "stepAttemptStart",
  "stepAttemptEnd",
  "waitStart",
  "waitEnd",
  "compensationStart",
  "compensationStep",
  "compensationEnd",
  "recoveryReclaim",
  "workerTaskStart",
  "workerTaskEnd",
] as const;

type Recorded = { name: (typeof NAMES)[number]; event: Record<string, any> };

/** A tracer that records every callback, with a one-line summary per event. */
function recordingTracer(): {
  tracer: WorkflowTracer;
  events: Recorded[];
  lines: () => string[];
  errors: WorkflowTracerErrorEvent[];
} {
  const events: Recorded[] = [];
  const errors: WorkflowTracerErrorEvent[] = [];
  const tracer: WorkflowTracer = { onTracerError: (e) => errors.push(e) };
  for (const name of NAMES) {
    (tracer as any)[name] = (event: Record<string, any>) => {
      events.push({ name, event });
    };
  }
  const summarize = ({ name, event: e }: Recorded): string => {
    switch (name) {
      case "runStart":
        return `runStart ${e.workflowId}#${e.run}${e.resumed ? " resumed" : ""}${
          e.parentWorkflowId ? ` parent=${e.parentWorkflowId}` : ""
        }`;
      case "runEnd":
        return `runEnd ${e.workflowId} ${e.outcome}${e.failure ? ` ${e.failure.tag}` : ""}`;
      case "stepAttemptStart":
        return `start ${e.workflowId}/${e.stepName}#${e.attempt}`;
      case "stepAttemptEnd":
        return `end ${e.workflowId}/${e.stepName}#${e.attempt} ${e.outcome}${
          e.willRetry ? " retry" : ""
        }`;
      case "waitStart":
      case "waitEnd":
        return `${name} ${e.stepName} ${e.kind}`;
      case "compensationStep":
        return `compensationStep ${e.stepName}#${e.attempt} ${e.outcome}`;
      case "compensationEnd":
        return `compensationEnd ok=${e.compensated} failed=${e.failed}`;
      default:
        return name;
    }
  };
  return { tracer, events, errors, lines: () => events.map(summarize) };
}

function setup() {
  const clock = FakeWallClock.create(T0);
  const storage = new InMemoryWorkflowStorage({ clock });
  return { clock, storage };
}

const fastRetry = { maxRetries: 2, baseDelayMs: 0, jitter: false } as const;

describe("WorkflowTracer — event order", () => {
  it("a normal run: lock, run start, each step's attempt, run end", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const wf = workflow<number>({ name: "plain" })
      .step("a", ({ input }) => succeed(input + 1))
      .step("b", ({ prev }) => succeed(prev * 2))
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "w1", input: 1, namespace: "ns" })).toBe(4);

    expect(rec.lines()).toEqual([
      "lockAcquired",
      "runStart w1#1",
      "start w1/a#1",
      "end w1/a#1 completed",
      "start w1/b#1",
      "end w1/b#1 completed",
      "runEnd w1 completed",
    ]);
    const start = rec.events.find((e) => e.name === "runStart")!.event;
    expect(start).toMatchObject({ workflowName: "plain", namespace: "ns", resumed: false });
    const end = rec.events.find((e) => e.name === "runEnd")!.event;
    expect(end.durationMs).toBe(end.endedAt.getTime() - end.startedAt.getTime());
    expect(rec.errors).toEqual([]);
  });

  it("a retried step reports each attempt, the failed one marked as retried", async () => {
    // Real clock: the retry's zero-length backoff is a real timer.
    const storage = new InMemoryWorkflowStorage();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, tracer: rec.tracer });
    let calls = 0;
    const wf = workflow<number>({ name: "flaky" })
      .step("a", () => (++calls < 2 ? fail(new TestError({ message: "no" })) : succeed(1)), {
        retry: fastRetry,
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "w1", input: 0 });

    const attempts = rec.lines().filter((l) => l.startsWith("start") || l.startsWith("end"));
    expect(attempts).toEqual([
      "start w1/a#1",
      "start w1/a#2",
      "end w1/a#1 failed retry",
      "end w1/a#2 completed",
    ]);
    const failed = rec.events.find((e) => e.name === "stepAttemptEnd")!.event;
    expect(failed.error).toEqual({ tag: "TestError", message: "no" });
  });

  it("negative control: with no retry policy the failed attempt is final", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const wf = workflow<number>({ name: "flaky" })
      .step("a", () => fail(new TestError({ message: "no" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "w1", input: 0 });

    expect(rec.lines()).toContain("end w1/a#1 failed");
    expect(rec.lines()).not.toContain("end w1/a#1 failed retry");
  });

  it("a failure rolls back: compensation start, each compensate attempt, end, then run end", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const wf = workflow<number>({ name: "saga" })
      .step("charge", () => succeed(1), { compensate: () => succeed(undefined as void) })
      .step("ship", () => fail(new TestError({ message: "no" })))
      .build();

    const { failure } = await runner.runSafe({ workflow: wf, workflowId: "w1", input: 0 });

    expect(failure?.tag).toBe("TestError");
    expect(rec.lines()).toEqual([
      "lockAcquired",
      "runStart w1#1",
      "start w1/charge#1",
      "end w1/charge#1 completed",
      "start w1/ship#1",
      "end w1/ship#1 failed",
      "compensationStart",
      "compensationStep charge#1 completed",
      "compensationEnd ok=1 failed=0",
      "runEnd w1 failed TestError",
    ]);
  });

  it("negative control: a failure with nothing to roll back reports no compensate attempt", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const wf = workflow<number>({ name: "no-saga" })
      .step("ship", () => fail(new TestError({ message: "no" })))
      .build();

    await runner.runSafe({ workflowId: "w1", workflow: wf, input: 0 });

    expect(rec.events.some((e) => e.name === "compensationStep")).toBe(false);
  });

  it("a failing compensate is reported as a failed compensation step", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const wf = workflow<number>({ name: "saga" })
      .step("charge", () => succeed(1), {
        compensate: () => fail(new TestError({ message: "refund failed" })),
      })
      .step("ship", () => fail(new TestError({ message: "no" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "w1", input: 0 });

    expect(rec.lines()).toContain("compensationStep charge#1 failed");
    expect(rec.lines()).toContain("compensationEnd ok=0 failed=1");
  });

  it("a signal wait suspends the run, and the resume reports the wait's end", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const wf = workflow<number>({ name: "approval" })
      .waitForSignal<string>("approve", { signalName: "go" })
      .step("done", ({ prev }) => succeed(`${prev}!`))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "w1", input: 0 });
    expect(rec.lines()).toEqual([
      "lockAcquired",
      "runStart w1#1",
      "start w1/approve#1",
      "end w1/approve#1 suspended",
      "waitStart approve signal",
      "runEnd w1 suspended",
    ]);

    clock.advance(5_000);
    await storage.deliverSignal({ workflowId: "w1", signalName: "go", payload: "ok" });
    rec.events.length = 0;
    await runner.run({ workflow: wf, workflowId: "w1", input: 0 });

    const resumed = rec.lines();
    expect(resumed.slice(0, 3)).toEqual([
      "lockAcquired",
      "runStart w1#1 resumed",
      "waitEnd approve signal",
    ]);
    expect(resumed.at(-1)).toBe("runEnd w1 completed");
    const waitEnd = rec.events.find((e) => e.name === "waitEnd")!.event;
    expect(waitEnd.waitedMs).toBe(5_000);
  });

  it("negative control: a fresh run reports no wait end", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const wf = workflow<number>({ name: "sleepy" })
      .sleep("nap", 10)
      .step("done", () => succeed(1))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "w1", input: 0 });

    expect(rec.events.some((e) => e.name === "waitEnd")).toBe(false);
    expect(rec.lines()).toContain("waitStart nap sleep");
  });

  it("a child workflow is a run whose start names its parent, nested in the parent's step", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const child = workflow<{ id: number }>({ name: "child" })
      .step("work", ({ input }) => succeed(input.id * 10))
      .build();
    const parent = workflow<{ n: number }>({ name: "parent" })
      .subworkflow("enrich", child, {
        input: (prev) => ({ id: (prev as { n: number }).n }),
        workflowId: () => "child-1",
      })
      .build();

    await runner.run({ workflow: parent, workflowId: "p", input: { n: 3 } });

    const lines = rec.lines().filter((l) => !l.startsWith("lock"));
    expect(lines).toEqual([
      "runStart p#1",
      "start p/enrich#1",
      "runStart child-1#1 parent=p",
      "start child-1/work#1",
      "end child-1/work#1 completed",
      "runEnd child-1 completed",
      "end p/enrich#1 completed",
      "runEnd p completed",
    ]);
  });

  it("a driver that loses its run reports the lock lost and an abandoned run", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const abandon = new AbortController();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const slow = workflow<number>({ name: "slow" })
      .step("a", async () => {
        await gate;
        return 1;
      })
      .step("b", () => succeed(2))
      .build();

    const running = runner.runSafe({
      workflow: slow,
      workflowId: "w1",
      input: 0,
      signal: abandon.signal,
    });
    for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
    abandon.abort();
    release();
    await running;

    expect(rec.events.map((e) => e.name)).toContain("lockLost");
    const end = rec.events.find((e) => e.name === "runEnd")!.event;
    expect(end.outcome).toBe("abandoned");
  });

  it("recover() reports each reclaimed run", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const wf = workflow<number>({ name: "orphan", version: "1" })
      .step("a", () => succeed(1))
      .build();
    const registry = new InMemoryWorkflowVersionRegistry();
    await registry.register(wf as never);
    const runner = createWorkflowRunner({ storage, clock, registry, tracer: rec.tracer });
    await storage.createWorkflow({
      workflowId: "o1",
      workflowName: "orphan",
      version: "1",
      input: 0,
    });
    clock.advance(1_000);

    const result = await runner.recover(RecoveryStrategy.builder().resumeRecent().build());
    await result.settled;

    const reclaim = rec.events.find((e) => e.name === "recoveryReclaim")!.event;
    expect(reclaim).toMatchObject({ workflowId: "o1", workflowName: "orphan", version: "1" });
    expect((await storage.loadWorkflow("o1"))?.status).toBe("completed");
  });

  it("negative control: recover() with nothing to reclaim reports nothing", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const registry = new InMemoryWorkflowVersionRegistry();
    const runner = createWorkflowRunner({ storage, clock, registry, tracer: rec.tracer });

    await runner.recover(RecoveryStrategy.builder().resumeRecent().build());

    expect(rec.events).toEqual([]);
  });
});

describe("WorkflowTracer — never throws into the engine", () => {
  const build = () =>
    workflow<number>({ name: "guarded" })
      .step("a", () => succeed(1), { compensate: () => succeed(undefined as void) })
      .step("b", () => fail(new TestError({ message: "no" })))
      .build();

  /** A tracer whose every callback throws (or rejects), and whose reporter throws too. */
  function hostileTracer(mode: "throw" | "reject"): WorkflowTracer {
    const tracer: WorkflowTracer = {
      traceparent: () => {
        throw new Error("traceparent");
      },
      onTracerError: () => {
        throw new Error("reporter");
      },
    };
    for (const name of NAMES) {
      (tracer as any)[name] =
        mode === "throw"
          ? () => {
              throw new Error(name);
            }
          : () => Promise.reject(new Error(name));
    }
    return tracer;
  }

  for (const mode of ["throw", "reject"] as const) {
    it(`a tracer whose callbacks ${mode} leaves the run's outcome and storage untouched`, async () => {
      const baseline = setup();
      const plain = createWorkflowRunner({ storage: baseline.storage, clock: baseline.clock });
      const expected = await plain.runSafe({ workflow: build(), workflowId: "w1", input: 0 });

      const traced = setup();
      const runner = createWorkflowRunner({
        storage: traced.storage,
        clock: traced.clock,
        tracer: hostileTracer(mode),
      });
      const actual = await runner.runSafe({ workflow: build(), workflowId: "w1", input: 0 });
      await new Promise<void>((r) => setImmediate(r));

      expect((actual.error as { _tag?: string } | null)?._tag).toBe(
        (expected.error as { _tag?: string } | null)?._tag,
      );
      expect(actual.failure?.tag).toBe(expected.failure?.tag);
      const stored = await traced.storage.loadWorkflow("w1");
      const reference = await baseline.storage.loadWorkflow("w1");
      expect(stored?.status).toBe(reference?.status);
      expect(Object.entries(stored!.steps).map(([k, s]) => [k, s.status])).toEqual(
        Object.entries(reference!.steps).map(([k, s]) => [k, s.status]),
      );
    });
  }

  it("each failure is reported to onTracerError with the callback that failed", async () => {
    const { clock, storage } = setup();
    const errors: WorkflowTracerErrorEvent[] = [];
    const runner = createWorkflowRunner({
      storage,
      clock,
      tracer: {
        onTracerError: (e) => errors.push(e),
        runStart: () => {
          throw new Error("boom");
        },
        stepAttemptEnd: () => Promise.reject(new Error("late")),
      },
    });
    const wf = workflow<number>({ name: "w" })
      .step("a", () => succeed(1))
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "w1", input: 0 })).toBe(1);
    await new Promise<void>((r) => setImmediate(r));

    expect(errors.map((e) => e.callback).sort()).toEqual(["runStart", "stepAttemptEnd"]);
  });

  it("without onTracerError the failure goes to console.error", async () => {
    const { clock, storage } = setup();
    const spy = spyOn(console, "error").mockImplementation(() => {});
    try {
      const runner = createWorkflowRunner({
        storage,
        clock,
        tracer: {
          runEnd: () => {
            throw new Error("boom");
          },
        },
      });
      const wf = workflow<number>({ name: "w" })
        .step("a", () => succeed(1))
        .build();
      await runner.run({ workflow: wf, workflowId: "w1", input: 0 });
      expect(spy).toHaveBeenCalledTimes(1);
    } finally {
      spy.mockRestore();
    }
  });

  it("a tracer with a subset of callbacks gets only those", async () => {
    const { clock, storage } = setup();
    const seen: string[] = [];
    const runner = createWorkflowRunner({
      storage,
      clock,
      tracer: { runEnd: (e) => void seen.push(e.outcome) },
    });
    const wf = workflow<number>({ name: "w" })
      .step("a", () => succeed(1))
      .build();
    await runner.run({ workflow: wf, workflowId: "w1", input: 0 });
    expect(seen).toEqual(["completed"]);
  });
});

describe("WorkflowTracer — trace context", () => {
  const wf = () =>
    workflow<number>({ name: "ctx" })
      .waitForSignal<string>("approve", { signalName: "go" })
      .step("done", () => succeed(1))
      .build();

  it("stores the caller's traceparent with the run so a later resume joins the trace", async () => {
    const { clock, storage } = setup();
    const rec = recordingTracer();
    const runner = createWorkflowRunner({ storage, clock, tracer: rec.tracer });
    const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    await runner.runSafe({ workflow: wf(), workflowId: "w1", input: 0, traceparent });
    expect((await storage.loadWorkflow("w1"))?.metadata?.traceparent).toBe(traceparent);

    await storage.deliverSignal({ workflowId: "w1", signalName: "go", payload: "ok" });
    rec.events.length = 0;
    await runner.run({ workflow: wf(), workflowId: "w1", input: 0 });

    // The resume passed none: the stored one is what runStart carries.
    expect(rec.events.find((e) => e.name === "runStart")!.event.traceparent).toBe(traceparent);
  });

  it("negative control: with no tracer nothing is stored and the run is untouched", async () => {
    const { clock, storage } = setup();
    const runner = createWorkflowRunner({ storage, clock });

    await runner.runSafe({
      workflow: wf(),
      workflowId: "w1",
      input: 0,
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    });

    expect((await storage.loadWorkflow("w1"))?.metadata?.traceparent).toBeUndefined();
  });

  const POLL_MS = 50;
  async function flush(): Promise<void> {
    for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
  }
  async function drive(params: { clock: FakeWallClock; done: () => boolean }): Promise<void> {
    for (let elapsed = 0; elapsed < 10_000; elapsed += POLL_MS) {
      await flush();
      if (params.done()) return;
      params.clock.advance(POLL_MS);
    }
    expect(params.done()).toBe(true);
  }

  async function runDistributed(params: {
    coordinatorTracer?: WorkflowTracer;
    workerTracer?: WorkflowTracer;
  }) {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const registry = new MapStepRegistry();
    registry.register({ stepName: "double", handler: (ctx) => succeed((ctx.input as number) * 2) });
    const dist = workflow<number>({ name: "dist" })
      .step("double", ({ input }) => succeed(input * 2))
      .build();
    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
      ...(params.coordinatorTracer && { tracer: params.coordinatorTracer }),
    });
    const worker = createWorker({
      stepQueue: queue,
      registry,
      pollIntervalMs: POLL_MS,
      clock,
      ...(params.workerTracer && { tracer: params.workerTracer }),
    });
    const box: { result?: unknown } = {};
    void coordinator.startLoop();
    void worker.start();
    void coordinator
      .run({ workflow: dist, workflowId: "d1", input: 4 })
      .then((r) => (box.result = r));
    await drive({ clock, done: () => box.result !== undefined });
    await coordinator.stopLoop();
    await worker.stop();
    return { queue, result: box.result };
  }

  it("carries the step attempt's traceparent across the queue to the worker's span", async () => {
    const worker = recordingTracer();
    const { queue, result } = await runDistributed({
      coordinatorTracer: {
        traceparent: ({ stepName, attempt }) =>
          `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01#${stepName}.${attempt}`,
      },
      workerTracer: worker.tracer,
    });

    expect(result).toBe(8);
    const task = queue.getAllTasks()[0]!;
    expect(task.metadata?.traceparent).toContain("#double.1");
    const start = worker.events.find((e) => e.name === "workerTaskStart")!.event;
    expect(start).toMatchObject({ workflowId: "d1", stepName: "double", attempt: 1, run: 1 });
    expect(start.traceparent).toBe(task.metadata?.traceparent as string);
    expect(worker.events.find((e) => e.name === "workerTaskEnd")!.event.outcome).toBe("completed");
  });

  it("negative control: a coordinator tracer without traceparent stores no task metadata", async () => {
    const worker = recordingTracer();
    const { queue } = await runDistributed({
      coordinatorTracer: recordingTracer().tracer,
      workerTracer: worker.tracer,
    });

    expect(queue.getAllTasks()[0]!.metadata).toBeUndefined();
    expect(
      worker.events.find((e) => e.name === "workerTaskStart")!.event.traceparent,
    ).toBeUndefined();
  });

  it("negative control: no tracer at all stores no task metadata", async () => {
    const { queue, result } = await runDistributed({});
    expect(result).toBe(8);
    expect(queue.getAllTasks()[0]!.metadata).toBeUndefined();
  });
});
