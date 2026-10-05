// ---------------------------------------------------------------------------
// A queued step behaves like the same step run inline:
//
// - the handler's `ctx.prev` / `ctx.deps` are the inline `prev` / `deps`
//   (declared dependencies only — and only those travel on the task);
// - the definition's `retry`, `timeoutMs` and `onFailure` apply exactly as
//   inline (attempt numbers, backoff, rows, run outcome);
// - only the coordinator writes step rows, from the claim-fenced queue
//   outcome, so a worker that lost its claim can't land a row.
//
// Every case runs the same definition through `createWorkflowRunner` and
// through a coordinator + worker, on FakeWallClock, and compares.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { fail, succeed, TaggedError } from "@spilne/perfect-core";
import { workflow } from "../../durable/durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import type { Workflow } from "../../durable/workflow-types.ts";
import type { StepAttemptRecord, WorkflowState } from "../../durable/workflow-state.ts";
import { StepTimeoutError } from "../../durable/durable-pipeline-error.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { createDistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry, type StepHandler, type WorkerStepContext } from "../step-registry.ts";
import type { StepQueueCompleteParams } from "../step-queue.ts";
import { createWorker } from "../worker.ts";
import { StepQueueExecutor } from "../step-queue-executor.ts";
import { RoutingStepExecutor } from "../../durable/runner/routing-step-executor.ts";
import type { StepExecutionResult } from "../../durable/workflow-runner.ts";

const POLL_MS = 50;

class Transient extends TaggedError("Transient")<{ readonly message: string }>() {}
class Permanent extends TaggedError("Permanent")<{ readonly message: string }>() {}

/** Let in-flight async work settle: a bounded number of macrotask turns. */
async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Settle, then advance the clock by `stepMs`, until `done()` holds. */
async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxMs?: number;
}): Promise<void> {
  const { clock, done, stepMs = POLL_MS, maxMs = 60_000 } = params;
  for (let elapsed = 0; ; elapsed += stepMs) {
    await flush();
    if (await done()) return;
    if (elapsed >= maxMs) break;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
}

/** What one run came to, in a shape both paths can be compared on. */
interface RunOutcome {
  readonly result?: unknown;
  readonly error?: string;
  readonly errorTag?: string;
  readonly state: WorkflowState;
  readonly attempts: readonly StepAttemptRecord[];
}

async function outcomeOf(params: {
  storage: InMemoryWorkflowStorage;
  workflowId: string;
  run: Promise<unknown>;
}): Promise<RunOutcome> {
  const settled = await params.run.then(
    (result) => ({ result }),
    (error: unknown) => ({
      error: error instanceof Error ? error.message : String(error),
      errorTag: (error as { _tag?: string })._tag,
    }),
  );
  const state = (await params.storage.loadWorkflow(params.workflowId))!;
  const attempts = await params.storage.loadStepAttempts({ workflowId: params.workflowId });
  return { ...settled, state, attempts };
}

/** Run `wf` inline on a fake clock. */
async function runInline(params: { wf: Workflow<any, any>; input: unknown }): Promise<RunOutcome> {
  const clock = FakeWallClock.create(0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock });
  let settled = false;
  const run = runner.run({ workflow: params.wf, workflowId: "run", input: params.input });
  run.then(
    () => (settled = true),
    () => (settled = true),
  );
  await driveUntil({ clock, done: () => settled });
  return outcomeOf({ storage, workflowId: "run", run });
}

/**
 * Run `wf` on a coordinator with one worker hosting `handlers`, all on one
 * fake clock.
 */
async function runDistributed(params: {
  wf: Workflow<any, any>;
  input: unknown;
  handlers: Record<string, StepHandler>;
}): Promise<RunOutcome & { queue: InMemoryStepQueue }> {
  const clock = FakeWallClock.create(0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const queue = new InMemoryStepQueue({ clock });
  const coordinator = createDistributedWorkflowRunner({
    storage,
    stepQueue: queue,
    pollIntervalMs: POLL_MS,
    stepPollIntervalMs: POLL_MS,
    clock,
  });
  const registry = new MapStepRegistry();
  for (const [stepName, handler] of Object.entries(params.handlers)) {
    registry.register({ stepName, handler });
  }
  const worker = createWorker({
    stepQueue: queue,
    registry,
    pollIntervalMs: POLL_MS,
    clock,
    workerId: "worker-1",
  });
  void worker.start();
  let settled = false;
  const run = coordinator.run({ workflow: params.wf, workflowId: "run", input: params.input });
  run.then(
    () => (settled = true),
    () => (settled = true),
  );
  await driveUntil({ clock, done: () => settled });
  const stop = worker.stop();
  clock.advance(POLL_MS);
  await stop;
  return { ...(await outcomeOf({ storage, workflowId: "run", run })), queue };
}

/** A step row without its timestamps and durations, for comparing paths. */
function rowsOf(state: WorkflowState): Record<string, unknown> {
  const rows: Record<string, unknown> = {};
  for (const [name, step] of Object.entries(state.steps)) {
    rows[name] = {
      status: step.status,
      result: step.result,
      error: step.error,
      errorTag: step.errorTag,
    };
  }
  return rows;
}

/** Attempt rows as `[step, attempt, status, error]`, sorted. */
function attemptRowsOf(attempts: readonly StepAttemptRecord[]): unknown[] {
  return attempts
    .map((a) => [a.stepName, a.attempt, a.status, a.error ?? null])
    .sort((x, y) => JSON.stringify(x).localeCompare(JSON.stringify(y)));
}

// ---------------------------------------------------------------------------
// 1. ctx.prev / ctx.deps
// ---------------------------------------------------------------------------

describe("queued step context — ctx.prev / ctx.deps match the inline runner", () => {
  /** What every step body saw, by step name. */
  type Seen = Record<string, { prev?: unknown; deps?: Record<string, unknown> }>;

  /**
   * a → b (chain), c depends on a only (DAG, skipping b), d on [b, c],
   * then a parallel block over d, and e after the block.
   */
  function definition(seen: Seen) {
    return workflow<number>({ name: "ctx-parity" })
      .step("a", ({ prev }) => ((seen["a"] = { prev }), succeed(prev + 1)))
      .step("b", ({ prev }) => ((seen["b"] = { prev }), succeed(prev * 10)))
      .step("c", { dependsOn: ["a"] }, ({ deps }) => {
        seen["c"] = { deps: { ...deps } };
        return succeed(deps.a + 100);
      })
      .step("d", { dependsOn: ["b", "c"] }, ({ deps }) => {
        seen["d"] = { deps: { ...deps } };
        return succeed(deps.b + deps.c);
      })
      .parallelSteps("p", {
        x: ({ prev }) => ((seen["p.x"] = { prev }), succeed(prev + 1)),
        y: ({ prev }) => ((seen["p.y"] = { prev }), succeed(prev - 1)),
      })
      .step("e", ({ prev }) => ((seen["e"] = { prev }), succeed(prev.x * prev.y)))
      .build();
  }

  it("chain, DAG and parallel steps see the same prev and deps, distributed or inline", async () => {
    const inlineSeen: Seen = {};
    const inline = await runInline({ wf: definition(inlineSeen), input: 1 });

    const workerSeen: Record<string, WorkerStepContext> = {};
    const record =
      (fn: (ctx: WorkerStepContext) => unknown): StepHandler =>
      async (ctx) => {
        workerSeen[ctx.stepName] = ctx;
        return fn(ctx);
      };
    const num = (v: unknown): number => v as number;
    const distributed = await runDistributed({
      wf: definition({}),
      input: 1,
      handlers: {
        a: record((ctx) => num(ctx.prev) + 1),
        b: record((ctx) => num(ctx.prev) * 10),
        c: record((ctx) => num(ctx.deps["a"]) + 100),
        d: record((ctx) => num(ctx.deps["b"]) + num(ctx.deps["c"])),
        "p.x": record((ctx) => num(ctx.prev) + 1),
        "p.y": record((ctx) => num(ctx.prev) - 1),
        // The join gets its branches as declared deps, like the inline join.
        p: record((ctx) => ({ x: ctx.deps["p.x"], y: ctx.deps["p.y"] })),
        e: record((ctx) => {
          const prev = ctx.prev as { x: number; y: number };
          return prev.x * prev.y;
        }),
      },
    });

    expect(distributed.result).toEqual(inline.result);
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));

    // Linear steps: the worker's prev is the inline prev, and its deps are
    // just the declared dependency.
    for (const [name, dep] of [
      ["a", undefined],
      ["b", "a"],
      ["p.x", "d"],
      ["p.y", "d"],
      ["e", "p"],
    ] as const) {
      expect(workerSeen[name]!.prev).toEqual(inlineSeen[name]!.prev);
      expect(workerSeen[name]!.deps).toEqual(
        dep === undefined ? {} : { [dep]: inline.state.steps[dep]!.result },
      );
    }
    // DAG steps: the worker's deps are the inline deps (declared only), and
    // prev is the first declared dependency.
    expect(workerSeen["c"]!.deps).toEqual(inlineSeen["c"]!.deps!);
    expect(workerSeen["c"]!.prev).toEqual(inlineSeen["c"]!.deps!["a"]);
    expect(workerSeen["d"]!.deps).toEqual(inlineSeen["d"]!.deps!);
    expect(workerSeen["d"]!.prev).toEqual(inlineSeen["d"]!.deps!["b"]);
    // A root step's prev is the input.
    expect(workerSeen["a"]!.prev).toBe(1);
    expect(workerSeen["a"]!.input).toBe(1);
    expect(workerSeen["a"]!.attempt).toBe(1);
  });

  it("each task carries only the step's declared dependencies", async () => {
    const distributed = await runDistributed({
      wf: definition({}),
      input: 1,
      handlers: {
        a: async (ctx) => (ctx.prev as number) + 1,
        b: async (ctx) => (ctx.prev as number) * 10,
        c: async (ctx) => (ctx.deps["a"] as number) + 100,
        d: async (ctx) => (ctx.deps["b"] as number) + (ctx.deps["c"] as number),
        "p.x": async (ctx) => (ctx.prev as number) + 1,
        "p.y": async (ctx) => (ctx.prev as number) - 1,
        p: async (ctx) => ({ x: ctx.deps["p.x"], y: ctx.deps["p.y"] }),
        e: async (ctx) => {
          const prev = ctx.prev as { x: number; y: number };
          return prev.x * prev.y;
        },
      },
    });
    const shipped = Object.fromEntries(
      distributed.queue.getAllTasks().map((t) => [t.stepName, [Object.keys(t.deps), t.dependsOn]]),
    );
    expect(shipped).toEqual({
      a: [[], []],
      b: [["a"], ["a"]],
      c: [["a"], ["a"]],
      d: [
        ["b", "c"],
        ["b", "c"],
      ],
      "p.x": [["d"], ["d"]],
      "p.y": [["d"], ["d"]],
      p: [
        ["p.x", "p.y"],
        ["p.x", "p.y"],
      ],
      e: [["p"], ["p"]],
    });
  });
});

// ---------------------------------------------------------------------------
// 2. retry / timeoutMs / onFailure from the definition
// ---------------------------------------------------------------------------

describe("queued step policies — the definition's retry, timeoutMs and onFailure apply as inline", () => {
  /**
   * A body that fails with `failWith(attempt)` while it returns an error,
   * recording the attempt number and clock time of every invocation.
   */
  function flaky(params: {
    clock: () => FakeWallClock | undefined;
    calls: { attempt: number; at: number }[];
    failWith: (attempt: number) => Transient | Permanent | undefined;
  }) {
    return (attempt: number) => {
      params.calls.push({ attempt, at: params.clock()?.currentTimeMs() ?? 0 });
      const error = params.failWith(attempt);
      return error ? fail(error) : succeed(`ok@${attempt}`);
    };
  }

  /**
   * Run `build(body)` inline and distributed; the worker handler calls the
   * same `body`. Returns both outcomes plus each path's invocation log.
   */
  async function compare(params: {
    build: (body: (attempt: number) => ReturnType<ReturnType<typeof flaky>>) => Workflow<any, any>;
    failWith: (attempt: number) => Transient | Permanent | undefined;
    extraHandlers?: Record<string, StepHandler>;
  }) {
    const inlineCalls: { attempt: number; at: number }[] = [];
    const inline = await runInline({
      wf: params.build(flaky({ clock: () => undefined, calls: inlineCalls, ...params })),
      input: 1,
    });
    const workerCalls: { attempt: number; at: number }[] = [];
    const body = flaky({ clock: () => undefined, calls: workerCalls, ...params });
    const distributed = await runDistributed({
      wf: params.build(body),
      input: 1,
      handlers: { s: (ctx) => body(ctx.attempt), ...params.extraHandlers },
    });
    return { inline, distributed, inlineCalls, workerCalls };
  }

  it("retry: transient failures are retried per the definition, with the same attempts and rows", async () => {
    const { inline, distributed, inlineCalls, workerCalls } = await compare({
      build: (body) =>
        workflow<number>({ name: "retry" })
          .step("s", ({ attempt }) => body(attempt), {
            retry: { maxRetries: 3, baseDelayMs: 100 },
          })
          .build(),
      failWith: (attempt) =>
        attempt < 3 ? new Transient({ message: `transient ${attempt}` }) : undefined,
    });

    expect(distributed.result).toBe("ok@3");
    expect(distributed.result).toEqual(inline.result);
    expect(workerCalls.map((c) => c.attempt)).toEqual(inlineCalls.map((c) => c.attempt));
    expect(workerCalls.map((c) => c.attempt)).toEqual([1, 2, 3]);
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));
    expect(attemptRowsOf(distributed.attempts)).toEqual(attemptRowsOf(inline.attempts));
    expect(attemptRowsOf(distributed.attempts)).toEqual([
      ["s", 1, "failed", "transient 1"],
      ["s", 2, "failed", "transient 2"],
      ["s", 3, "completed", null],
    ]);
    // Every attempt row names the worker that ran it.
    expect(distributed.attempts.map((a) => a.executorId)).toEqual([
      "worker-1",
      "worker-1",
      "worker-1",
    ]);
    // One task per attempt, numbered like the inline attempts.
    expect(distributed.queue.getAllTasks().map((t) => [t.attempt, t.status])).toEqual([
      [1, "failed"],
      [2, "failed"],
      [3, "completed"],
    ]);
  });

  it("retry: backoff runs on the coordinator's clock like the inline backoff", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const enqueuedAt: number[] = [];
    const enqueue = queue.enqueue.bind(queue);
    queue.enqueue = (p) => (enqueuedAt.push(clock.currentTimeMs()), enqueue(p));
    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 10,
      stepPollIntervalMs: 10,
      clock,
    });
    const wf = workflow<number>({ name: "backoff" })
      .step("s", () => succeed(0), { retry: { maxRetries: 2, baseDelayMs: 1_000 } })
      .build();
    const registry = new MapStepRegistry();
    let calls = 0;
    registry.register({
      stepName: "s",
      handler: () => (++calls < 3 ? fail(new Transient({ message: "again" })) : succeed("done")),
    });
    const worker = createWorker({ stepQueue: queue, registry, pollIntervalMs: 10, clock });
    void worker.start();
    let result: unknown;
    void coordinator.run({ workflow: wf, workflowId: "b", input: 0 }).then((r) => (result = r));
    await driveUntil({ clock, done: () => result !== undefined, stepMs: 10 });
    const stop = worker.stop();
    clock.advance(10);
    await stop;

    expect(result).toBe("done");
    // 1s, then 2s of backoff between the attempts' dispatches (doubling, no jitter).
    expect(enqueuedAt[1]! - enqueuedAt[0]!).toBeGreaterThanOrEqual(1_000);
    expect(enqueuedAt[2]! - enqueuedAt[1]!).toBeGreaterThanOrEqual(2_000);
  });

  it("retry.when: a non-retryable error fails the step after one attempt, with its tag", async () => {
    const { inline, distributed, workerCalls } = await compare({
      build: (body) =>
        workflow<number>({ name: "when" })
          .step("s", ({ attempt }) => body(attempt), {
            retry: {
              maxRetries: 5,
              baseDelayMs: 10,
              // A tag check behaves the same on both paths: the queued
              // failure keeps the worker error's `_tag`.
              when: (e) => e._tag !== "Permanent",
            },
          })
          .build(),
      failWith: () => new Permanent({ message: "card declined" }),
    });

    expect(workerCalls).toHaveLength(1);
    expect(distributed.error).toBe(inline.error);
    expect(distributed.error).toBe("card declined");
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));
    expect(distributed.state.steps["s"]!.errorTag).toBe("Permanent");
    expect(distributed.state.status).toBe("failed");
    expect(attemptRowsOf(distributed.attempts)).toEqual(attemptRowsOf(inline.attempts));
  });

  it("onFailure skip: the step completes with undefined and the run continues", async () => {
    const { inline, distributed } = await compare({
      build: (body) =>
        workflow<number>({ name: "skip" })
          .step("s", ({ attempt }) => body(attempt), { onFailure: "skip" })
          .step("after", ({ prev }) => succeed(`after:${String(prev)}`))
          .build(),
      failWith: () => new Transient({ message: "down" }),
      extraHandlers: { after: async (ctx) => `after:${String(ctx.prev)}` },
    });

    expect(distributed.result).toBe("after:undefined");
    expect(distributed.result).toEqual(inline.result);
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));
    expect(attemptRowsOf(distributed.attempts)).toEqual(attemptRowsOf(inline.attempts));
  });

  it("onFailure fallback: the fallback value is the step's result, after retries", async () => {
    const { inline, distributed, workerCalls } = await compare({
      build: (body) =>
        workflow<number>({ name: "fallback" })
          .step("s", ({ attempt }) => body(attempt), {
            retry: { maxRetries: 1, baseDelayMs: 10 },
            onFailure: { fallback: (e) => `fallback:${(e as Error).message}` },
          })
          .build(),
      failWith: (attempt) => new Transient({ message: `down ${attempt}` }),
    });

    expect(workerCalls.map((c) => c.attempt)).toEqual([1, 2]);
    expect(distributed.result).toBe("fallback:down 2");
    expect(distributed.result).toEqual(inline.result);
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));
    expect(attemptRowsOf(distributed.attempts)).toEqual(attemptRowsOf(inline.attempts));
  });

  it("timeoutMs: the worker fails an attempt past the timeout with StepTimeoutError and aborts ctx.signal", async () => {
    // Inline: the body never settles on its own; the timeout interrupts it.
    const inline = await runInline({
      wf: workflow<number>({ name: "timeout" })
        .stepAsync("s", () => new Promise<string>(() => {}), {
          timeoutMs: 100,
          retry: { maxRetries: 1, baseDelayMs: 10 },
        })
        .build(),
      input: 1,
    });

    const signals: AbortSignal[] = [];
    const distributed = await runDistributed({
      wf: workflow<number>({ name: "timeout" })
        .stepAsync("s", () => new Promise<string>(() => {}), {
          timeoutMs: 100,
          retry: { maxRetries: 1, baseDelayMs: 10 },
        })
        .build(),
      input: 1,
      handlers: {
        s: (ctx) => {
          signals.push(ctx.signal);
          return new Promise<string>(() => {});
        },
      },
    });

    expect(distributed.error).toBe('Step "s" timed out after 100ms');
    expect(distributed.error).toBe(inline.error);
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));
    expect(distributed.state.steps["s"]!.errorTag).toBe("StepTimeoutError");
    expect(attemptRowsOf(distributed.attempts)).toEqual(attemptRowsOf(inline.attempts));
    expect(attemptRowsOf(distributed.attempts)).toEqual([
      ["s", 1, "failed", 'Step "s" timed out after 100ms'],
      ["s", 2, "failed", 'Step "s" timed out after 100ms'],
    ]);
    // Each attempt's handler saw its signal aborted by the timeout.
    expect(signals).toHaveLength(2);
    for (const signal of signals) {
      expect(signal.aborted).toBe(true);
      expect(signal.reason).toBeInstanceOf(StepTimeoutError);
    }
    expect(distributed.queue.getAllTasks().map((t) => t.timeoutMs)).toEqual([100, 100]);
  });

  it("an untagged failure is a defect, as inline: no retry, no onFailure", async () => {
    const definition = () =>
      workflow<number>({ name: "defect" })
        .stepAsync(
          "s",
          async () => {
            throw new Error("plain throw");
          },
          {
            retry: { maxRetries: 3, baseDelayMs: 10 },
            onFailure: { fallback: () => "unused" },
          },
        )
        .build();
    const inline = await runInline({ wf: definition(), input: 1 });
    let calls = 0;
    const distributed = await runDistributed({
      wf: definition(),
      input: 1,
      handlers: {
        s: async () => {
          calls++;
          throw new Error("plain throw");
        },
      },
    });

    expect(calls).toBe(1);
    expect(distributed.error).toBe("plain throw");
    expect(distributed.error).toBe(inline.error);
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));
    expect(attemptRowsOf(distributed.attempts)).toEqual(attemptRowsOf(inline.attempts));
  });

  it("no policy: a failure fails the step and the run with the worker's error and tag", async () => {
    const { inline, distributed } = await compare({
      build: (body) =>
        workflow<number>({ name: "plain" })
          .step("s", ({ attempt }) => body(attempt))
          .build(),
      failWith: () => new Permanent({ message: "nope" }),
    });

    expect(distributed.error).toBe(inline.error);
    expect(rowsOf(distributed.state)).toEqual(rowsOf(inline.state));
    expect(distributed.state.status).toBe(inline.state.status);
    expect(distributed.state.errorTag).toBe(inline.state.errorTag);
    expect(attemptRowsOf(distributed.attempts)).toEqual(attemptRowsOf(inline.attempts));
  });
});

// ---------------------------------------------------------------------------
// 3. Fenced worker writes — a zombie worker can't land a step row
// ---------------------------------------------------------------------------

describe("fenced worker outcomes — a worker that lost its claim can't land a row", () => {
  async function zombieScenario(params: { zombieCommitsFirst: boolean }) {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const completes: { worker: string; settled: boolean }[] = [];
    const complete = queue.complete.bind(queue);
    queue.complete = async (p: StepQueueCompleteParams) => {
      const settled = await complete(p);
      completes.push({ worker: String(p.result).split(":")[0]!, settled });
      return settled;
    };
    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 100,
      stepPollIntervalMs: 100,
      workerTimeoutMs: 1_000,
      clock,
    });

    const wf = workflow<number>({ name: "zombie" })
      .step("charge", ({ input }) => succeed(`inline:${input}`))
      .step("receipt", ({ prev }) => succeed(`receipt for ${prev}`))
      .build();

    // Worker A stalls mid-handler: it never heartbeats (its interval is far
    // past the coordinator's stale timeout) and finishes only when released.
    let releaseA!: () => void;
    const gateA = new Promise<void>((r) => (releaseA = r));
    let aStarted = false;
    const registryA = new MapStepRegistry();
    registryA.register({
      stepName: "charge",
      handler: async () => {
        aStarted = true;
        await gateA;
        return "A:charged";
      },
    });
    const workerA = createWorker({
      stepQueue: queue,
      registry: registryA,
      pollIntervalMs: 100,
      heartbeatIntervalMs: 3_600_000,
      clock,
      workerId: "worker-a",
    });

    // Worker B runs both steps; for `charge` it waits for its own gate.
    let releaseB!: () => void;
    const gateB = new Promise<void>((r) => (releaseB = r));
    let bStarted = false;
    const registryB = new MapStepRegistry();
    registryB.register({
      stepName: "charge",
      handler: async () => {
        bStarted = true;
        await gateB;
        return "B:charged";
      },
    });
    registryB.register({ stepName: "receipt", handler: async (ctx) => `receipt for ${ctx.prev}` });
    const workerB = createWorker({
      stepQueue: queue,
      registry: registryB,
      pollIntervalMs: 100,
      heartbeatIntervalMs: 200,
      clock,
      workerId: "worker-b",
    });

    void coordinator.startLoop();
    let result: unknown;
    void coordinator.run({ workflow: wf, workflowId: "z", input: 1 }).then((r) => (result = r));

    // A claims `charge` first and stalls.
    void workerA.start();
    await driveUntil({ clock, done: () => aStarted, stepMs: 100 });
    // The coordinator's stale sweep takes the task back; B claims it.
    void workerB.start();
    await driveUntil({ clock, done: () => bStarted, stepMs: 100 });
    const [task] = queue.getAllTasks();
    expect(task!.claimedBy).toBe("worker-b");
    expect(task!.deliveries).toBe(2);

    if (params.zombieCommitsFirst) {
      // A wakes up while B still runs: its outcome is rejected at the queue.
      releaseA();
      await driveUntil({ clock, done: () => completes.length === 1, stepMs: 100 });
      expect((await storage.loadWorkflow("z"))?.steps["charge"]).toBeUndefined();
      releaseB();
    } else {
      releaseB();
      await driveUntil({
        clock,
        done: async () => (await storage.loadWorkflow("z"))?.steps["charge"] !== undefined,
        stepMs: 100,
      });
      // A's late write comes after B's row has landed.
      releaseA();
      await driveUntil({ clock, done: () => completes.length === 2, stepMs: 100 });
    }
    await driveUntil({ clock, done: () => result !== undefined, stepMs: 100 });

    const stops = [workerA.stop(), workerB.stop(), coordinator.stopLoop()];
    clock.advance(100);
    await Promise.all(stops);
    return { storage, queue, completes, result };
  }

  it("A loses its claim mid-handler, B completes, A's late write is rejected and B's row stands", async () => {
    const { storage, queue, completes, result } = await zombieScenario({
      zombieCommitsFirst: false,
    });

    expect(completes.filter((c) => c.worker !== "receipt for B")).toEqual([
      { worker: "B", settled: true },
      { worker: "A", settled: false },
    ]);
    const state = await storage.loadWorkflow("z");
    expect(state?.steps["charge"]?.result).toBe("B:charged");
    expect(result).toBe("receipt for B:charged");
    const charge = queue.getAllTasks().find((t) => t.stepName === "charge")!;
    expect(charge.result).toBe("B:charged");
    expect(charge.claimedBy).toBe("worker-b");
    const attempts = await storage.loadStepAttempts({ workflowId: "z", stepName: "charge" });
    expect(attempts.map((a) => [a.attempt, a.status, a.executorId])).toEqual([
      [1, "completed", "worker-b"],
    ]);
  });

  it("a zombie that commits before the new claimant is rejected too, and never reaches storage", async () => {
    const { storage, completes, result } = await zombieScenario({ zombieCommitsFirst: true });

    expect(completes[0]).toEqual({ worker: "A", settled: false });
    expect((await storage.loadWorkflow("z"))?.steps["charge"]?.result).toBe("B:charged");
    expect(result).toBe("receipt for B:charged");
  });
});

// ---------------------------------------------------------------------------
// 4. Adopting a settled outcome instead of dispatching again
// ---------------------------------------------------------------------------

describe("adopting settled outcomes — a crashed coordinator's settled task is reused once", () => {
  /** A queue holding one task for `wf/s` settled by "w-old", and an executor over it. */
  async function leftover(params: { run: number; attempt: number }) {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const id = await queue.enqueue({
      workflowId: "wf",
      stepName: "s",
      input: {},
      run: params.run,
      attempt: params.attempt,
    });
    const [task] = await queue.claim({ workerId: "w-old", limit: 1 });
    await queue.complete({
      taskId: id,
      claimToken: task!.claimToken!,
      result: "old",
      durationMs: 1,
    });
    const executor = new StepQueueExecutor({
      stepQueue: queue,
      storage,
      clock,
      pollIntervalMs: POLL_MS,
    });
    const execute = (run: number) => {
      const box: { result?: StepExecutionResult } = {};
      void executor
        .executeStep({
          workflowId: "wf",
          stepName: "s",
          input: {},
          prevResults: {},
          attempt: 1,
          runtime: { run },
        })
        .then((r) => (box.result = r));
      return box;
    };
    return { clock, queue, id, execute };
  }

  it("takes the settled outcome of the same run, with its attempt and worker, and consumes it", async () => {
    const { clock, queue, id, execute } = await leftover({ run: 1, attempt: 2 });
    const box = execute(1);
    await driveUntil({ clock, done: () => box.result !== undefined });

    expect(box.result).toEqual({
      ok: true,
      result: "old",
      attempt: 2,
      failedAttempts: [],
      executorId: "w-old",
    });
    expect(queue.getAllTasks()).toHaveLength(1);
    expect((await queue.get(id))?.consumedAt).toBeInstanceOf(Date);
  });

  it("never takes a settled task of an earlier run: it dispatches the step afresh", async () => {
    const { clock, queue, id, execute } = await leftover({ run: 1, attempt: 1 });
    const box = execute(2);
    await driveUntil({ clock, done: () => queue.getAllTasks().length === 2 });

    expect((await queue.get(id))?.consumedAt).toBeInstanceOf(Date);
    expect(queue.getAllTasks()[1]).toMatchObject({ status: "pending", run: 2, attempt: 1 });
    expect(box.result).toBeUndefined();
  });

  it("discardSettled (runner.resume) consumes a leftover so a reset step runs again", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = createWorkflowRunner({
      storage,
      clock,
      stepExecutor: new RoutingStepExecutor({
        remote: new StepQueueExecutor({
          stepQueue: queue,
          storage,
          clock,
          pollIntervalMs: POLL_MS,
        }),
        remoteSteps: ["s"],
        storage,
      }),
    });
    const wf = workflow<number>({ name: "reset" })
      .step("s", ({ input }) => succeed(String(input)))
      .build();
    let calls = 0;
    const registry = new MapStepRegistry();
    registry.register({ stepName: "s", handler: async () => `run-${++calls}` });
    const worker = createWorker({ stepQueue: queue, registry, pollIntervalMs: POLL_MS, clock });
    void worker.start();

    let first: unknown;
    void runner.run({ workflow: wf, workflowId: "r", input: 1 }).then((r) => (first = r));
    await driveUntil({ clock, done: () => first !== undefined });
    expect(first).toBe("run-1");

    // A settled task nobody consumed (its coordinator crashed before
    // reading it) is still in the queue when the step is reset.
    const leftoverId = await queue.enqueue({ workflowId: "r", stepName: "s", input: 1, run: 1 });
    await driveUntil({
      clock,
      done: async () => (await queue.get(leftoverId))?.status === "completed",
    });
    expect(calls).toBe(2);

    let resumed: unknown;
    void runner.resume({ workflow: wf, workflowId: "r", fromStep: "s" }).then((r) => (resumed = r));
    await driveUntil({ clock, done: () => resumed !== undefined });
    const stop = worker.stop();
    clock.advance(POLL_MS);
    await stop;

    // The reset step ran again instead of taking the leftover outcome.
    expect(resumed).toBe("run-3");
    expect((await queue.get(leftoverId))?.consumedAt).toBeInstanceOf(Date);
  });

  it("a retry after a failed attempt dispatches a new task: every outcome is consumed as read", async () => {
    const distributed = await runDistributed({
      wf: workflow<number>({ name: "retry-consume" })
        .step("s", () => succeed("unused"), { retry: { maxRetries: 2, baseDelayMs: 10 } })
        .build(),
      input: 1,
      handlers: {
        s: (ctx) => (ctx.attempt < 3 ? fail(new Transient({ message: "again" })) : succeed("ok")),
      },
    });
    expect(distributed.result).toBe("ok");
    const tasks = distributed.queue.getAllTasks();
    expect(tasks.map((t) => [t.attempt, t.status])).toEqual([
      [1, "failed"],
      [2, "failed"],
      [3, "completed"],
    ]);
    expect(tasks.every((t) => t.consumedAt instanceof Date)).toBe(true);
  });
});
