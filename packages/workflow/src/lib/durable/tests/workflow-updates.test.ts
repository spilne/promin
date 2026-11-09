// ---------------------------------------------------------------------------
// Workflow updates — `ctx.setUpdateHandler` + `handle.update`.
//
// Level 1 drives `runJournaledStep` directly (the journal and the update
// points); level 2 drives a runner with the signal scanner (a suspended run
// is woken to apply an update), one FakeWallClock for all of it.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  workflow,
  InMemoryWorkflowStorage,
  defineUpdate,
  s,
  submitUpdate,
  awaitUpdate,
  UpdateRejectedError,
  UpdateFailedError,
  UpdateTimeoutError,
  updateResultSignalName,
  UPDATE_HANDLERS_SIGNAL,
} from "../../../index.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { createSignalScanner } from "../../distributed/signal-scanner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { runJournaledStep, completeSignal, type JournaledContext } from "../journaled-step.ts";
import { WorkflowSuspendedError } from "../durable-pipeline-error.ts";
import type { WorkflowTracer, WorkflowTraceUpdate } from "../workflow-tracer.ts";

const SetLimit = defineUpdate({
  name: "setLimit",
  input: s.object({ limit: s.integer() }),
  output: s.object({ previous: s.integer() }),
});

type Ctx = JournaledContext<unknown, unknown>;

/** A body: handler `setLimit`, two activities, then a signal wait; returns the limit it ends with. */
function limitBody(seen: string[] = []) {
  return function* (ctx: Ctx) {
    let limit = 10;
    ctx.setUpdateHandler(
      SetLimit,
      ({ limit: next }) => {
        seen.push(`handler:${next}`);
        const previous = limit;
        limit = next;
        return { previous };
      },
      { validate: ({ limit: next }) => (next < 0 ? "limit must not be negative" : undefined) },
    );
    ctx.setUpdateHandler("boom", () => {
      throw new Error("handler exploded");
    });
    yield* ctx.activity("first", () => {
      seen.push("activity:first");
      return 1;
    });
    yield* ctx.activity("second", () => {
      seen.push(`activity:second(limit=${limit})`);
      return 2;
    });
    yield* ctx.signal<string>("go");
    return limit;
  };
}

async function setup(workflowId = "wf-u") {
  const storage = new InMemoryWorkflowStorage();
  await storage.createWorkflow({ workflowId, workflowName: "u", input: {} });
  const run = <T>(body: (ctx: Ctx) => Generator<never, T, never>) =>
    runJournaledStep<unknown, unknown, T>({
      input: {},
      prev: {},
      workflowId,
      stepName: "main",
      storage,
      workflowStorage: storage,
      body: body as never,
    });
  return { storage, run, workflowId };
}

describe("update handlers at update points", () => {
  it("applies a pending update between yields, in order, journals it, and replays it identically", async () => {
    const { storage, run, workflowId } = await setup();
    const seen: string[] = [];
    // Park on the first run so the run is suspended and updatable.
    await expect(run(limitBody(seen) as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    seen.length = 0;

    // Two updates delivered while suspended; applied in delivery order at the
    // update point of the (re-suspending) wait.
    await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 20 },
      updateId: "u1",
    });
    await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 30 },
      updateId: "u2",
    });
    await expect(run(limitBody(seen) as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);

    expect(await awaitUpdate({ storage, workflowId, updateId: "u1", update: SetLimit })).toEqual({
      previous: 10,
    });
    expect(await awaitUpdate({ storage, workflowId, updateId: "u2", update: SetLimit })).toEqual({
      previous: 20,
    });

    // Journal: first, second, the wait, then the two updates in that order.
    const journal = await storage.loadJournal({ workflowId, stepName: "main" });
    expect(journal.map((e) => e.activityName)).toEqual([
      "first",
      "second",
      "go",
      "workflow.update:u1",
      "workflow.update:u2",
    ]);

    // Replay after the signal: the handlers re-run in the recorded order (state
    // rebuilt: the body ends with limit 30) and the activities do not re-run.
    seen.length = 0;
    await completeSignal({ storage, workflowId, stepName: "main", signalName: "go", value: "x" });
    const out = await run(limitBody(seen) as never);
    expect(out).toBe(30);
    expect(seen).toEqual(["handler:20", "handler:30"]);
  });

  it("applies an update that arrives while the run is not suspended at the next yield", async () => {
    const { storage, run, workflowId } = await setup();
    const seen: string[] = [];
    let delivered = Promise.resolve<unknown>(undefined);
    const body = function* (ctx: Ctx) {
      let limit = 1;
      ctx.setUpdateHandler(SetLimit, ({ limit: next }) => {
        limit = next;
        return { previous: 0 };
      });
      yield* ctx.activity("a", () => {
        // Delivered mid-activity: not applied until the activity has settled.
        delivered = submitUpdate({
          storage,
          workflowId,
          update: SetLimit,
          args: { limit: 5 },
          updateId: "mid",
        });
        seen.push("a");
        return 1;
      });
      seen.push(`after-a:limit=${limit}`);
      yield* ctx.activity("b", () => {
        seen.push("b");
        return 2;
      });
      return limit;
    };
    // The handler list is published when the body yields; deliver after the first publish.
    await storage.deliverSignal({
      workflowId,
      signalName: UPDATE_HANDLERS_SIGNAL,
      payload: { updates: [{ name: "setLimit" }] },
    });
    const out = await run(body as never);
    await delivered;
    expect(seen).toEqual(["a", "after-a:limit=5", "b"]);
    expect(out).toBe(5);
    expect(await awaitUpdate({ storage, workflowId, updateId: "mid", timeoutMs: 1 })).toEqual({
      previous: 0,
    });
  });

  it("is idempotent by update id and rejects a conflicting id", async () => {
    const { storage, run, workflowId } = await setup();
    await expect(run(limitBody() as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 7 },
      updateId: "same",
    });
    await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 7 },
      updateId: "same",
    });
    await expect(
      submitUpdate({ storage, workflowId, update: SetLimit, args: { limit: 8 }, updateId: "same" }),
    ).rejects.toMatchObject({ _tag: "UpdateRejectedError", reason: "id_conflict" });
    await expect(run(limitBody() as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);

    // Applied once; a repeated submit afterwards answers with the recorded outcome.
    const journal = await storage.loadJournal({ workflowId, stepName: "main" });
    expect(journal.filter((e) => e.activityName === "workflow.update:same")).toHaveLength(1);
    const again = await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 7 },
      updateId: "same",
    });
    expect(again.outcome).toEqual({ status: "completed", result: { previous: 10 } });
  });

  it("rejects without running the handler: bad args, failed validator, unknown name", async () => {
    const { storage, run, workflowId } = await setup();
    const seen: string[] = [];
    await expect(run(limitBody(seen) as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    seen.length = 0;

    // Schema mismatch is caught by the caller before delivery.
    await expect(
      submitUpdate({ storage, workflowId, update: "setLimit", args: { limit: "x" } }),
    ).rejects.toMatchObject({ reason: "validation" });
    // Unknown name likewise.
    await expect(
      submitUpdate({ storage, workflowId, update: "nope", args: {} }),
    ).rejects.toMatchObject({ reason: "unknown_update" });

    // The handler's own validator runs in the body: rejected, not journaled.
    const { updateId } = await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: -1 },
    });
    await expect(run(limitBody(seen) as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    await expect(awaitUpdate({ storage, workflowId, updateId })).rejects.toMatchObject({
      _tag: "UpdateRejectedError",
      reason: "validation",
      message: "limit must not be negative",
    });
    expect(seen.some((x) => x.startsWith("handler:"))).toBe(false);
    const journal = await storage.loadJournal({ workflowId, stepName: "main" });
    expect(journal.some((e) => e.activityName.startsWith("workflow.update:"))).toBe(false);
  });

  it("records a throwing handler as failed, and replay keeps it failed without stalling the body", async () => {
    const { storage, run, workflowId } = await setup();
    await expect(run(limitBody() as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    const { updateId } = await submitUpdate({ storage, workflowId, update: "boom", args: null });
    await expect(run(limitBody() as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    await expect(awaitUpdate({ storage, workflowId, updateId })).rejects.toBeInstanceOf(
      UpdateFailedError,
    );
    await completeSignal({ storage, workflowId, stepName: "main", signalName: "go", value: 1 });
    expect(await run<number>(limitBody() as never)).toBe(10);
    await expect(awaitUpdate({ storage, workflowId, updateId })).rejects.toMatchObject({
      message: "handler exploded",
    });
  });

  it("rejects when the run is terminal, unknown, or serves no handlers; pending ones get not_applied when the step ends", async () => {
    const { storage, run, workflowId } = await setup();
    await expect(
      submitUpdate({ storage, workflowId: "ghost", update: "x", args: null }),
    ).rejects.toMatchObject({ reason: "run_not_found" });
    // No body has published handlers yet.
    await expect(
      submitUpdate({ storage, workflowId, update: "x", args: null }),
    ).rejects.toMatchObject({ reason: "no_handlers" });

    await expect(run(limitBody() as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    // Delivered but not applied before the step ends: answered with not_applied.
    const { updateId } = await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 3 },
    });
    await completeSignal({ storage, workflowId, stepName: "main", signalName: "go", value: 1 });
    // The step finishes via a body that never reaches an update point after the delivery.
    await storage.deliverSignal({
      workflowId,
      signalName: updateResultSignalName("unrelated"),
      payload: { status: "completed", result: 1 },
    });
    const finishing = function* (ctx: Ctx) {
      ctx.setUpdateHandler("other", () => 1);
      // No update point: the body finishes without a yield.
      yield* [] as never[];
      return "done";
    };
    await run(finishing as never);
    await expect(awaitUpdate({ storage, workflowId, updateId })).rejects.toMatchObject({
      reason: "not_applied",
    });
    // The published list is empty now: callers stop sending.
    await expect(
      submitUpdate({ storage, workflowId, update: "other", args: null }),
    ).rejects.toMatchObject({ reason: "no_handlers" });

    await storage.completeWorkflow({ workflowId, result: null });
    await expect(
      submitUpdate({ storage, workflowId, update: "other", args: null }),
    ).rejects.toBeInstanceOf(UpdateRejectedError);
  });

  it("finishes an update the run was in the middle of applying when it stopped (pending journal entry)", async () => {
    const { storage, run, workflowId } = await setup();
    const seen: string[] = [];
    const body = function* (ctx: Ctx) {
      let limit = 10;
      ctx.setUpdateHandler(SetLimit, ({ limit: next }) => {
        seen.push(`handler:${next}`);
        const previous = limit;
        limit = next;
        return { previous };
      });
      yield* ctx.signal<string>("go");
      return limit;
    };
    await expect(run(body as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 4 },
      updateId: "crash",
    });
    // The previous run journaled the application as pending, then died before completing it.
    await storage.appendPendingEntry({
      workflowId,
      stepName: "main",
      activityIndex: 1,
      branchPath: "",
      activityName: "workflow.update:crash",
      stepType: "activity",
    });
    await expect(run(body as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    expect(seen).toEqual(["handler:4"]);
    expect(await awaitUpdate({ storage, workflowId, updateId: "crash" })).toEqual({ previous: 10 });
    const journal = await storage.loadJournal({ workflowId, stepName: "main" });
    expect(journal[1]).toMatchObject({ activityName: "workflow.update:crash", phase: "completed" });
  });

  it("awaitUpdate reports an ended run and a timeout", async () => {
    const { storage, run, workflowId } = await setup();
    await expect(run(limitBody() as never)).rejects.toBeInstanceOf(WorkflowSuspendedError);
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { updateId } = await submitUpdate({
      storage,
      workflowId,
      update: SetLimit,
      args: { limit: 1 },
    });

    const waiting = awaitUpdate({
      storage,
      workflowId,
      updateId,
      timeoutMs: 500,
      intervalMs: 100,
      clock,
    });
    const caught = waiting.catch((e: unknown) => e);
    for (let i = 0; i < 10; i++) {
      await new Promise<void>((r) => setImmediate(r));
      clock.advance(100);
    }
    expect(await caught).toBeInstanceOf(UpdateTimeoutError);

    await storage.completeWorkflow({ workflowId, result: null });
    await expect(awaitUpdate({ storage, workflowId, updateId })).rejects.toMatchObject({
      reason: "run_terminal",
    });
  });
});

describe("updates through the runner and the signal scanner", () => {
  const SCAN_MS = 50;
  async function flush(): Promise<void> {
    for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
  }
  async function driveUntil(params: {
    clock: FakeWallClock;
    done: () => boolean | Promise<boolean>;
  }): Promise<void> {
    for (let i = 0; i < 400; i++) {
      await flush();
      if (await params.done()) return;
      params.clock.advance(SCAN_MS);
    }
    expect(await params.done()).toBe(true);
  }

  const batch = workflow<{ n: number }>({ name: "batch", version: "1" })
    .journaled("main", function* (ctx) {
      let limit = ctx.input.n;
      ctx.setUpdateHandler(SetLimit, ({ limit: next }) => {
        const previous = limit;
        limit = next;
        return { previous };
      });
      yield* ctx.signal<string>("go");
      return { limit };
    })
    .build();

  it("wakes a suspended run to apply an update, answers the caller, and traces it", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const events: WorkflowTraceUpdate[] = [];
    const tracer: WorkflowTracer = { update: (e) => void events.push(e) };
    const runner = createWorkflowRunner({ storage, clock, tracer });
    const handle = await runner.start({ workflow: batch, workflowId: "b-1", input: { n: 4 } });
    await flush();
    expect((await storage.loadWorkflowStatus("b-1"))?.status).toBe("suspended");

    const scanner = createSignalScanner({
      storage,
      clock,
      scanIntervalMs: SCAN_MS,
      runner,
      resolveWorkflow: () => batch as never,
    });
    void scanner.start();

    const settle = (p: Promise<unknown>): (() => boolean) => {
      let settled = false;
      void p.then(
        () => (settled = true),
        () => (settled = true),
      );
      return () => settled;
    };
    const updated = handle.update({
      update: SetLimit,
      args: { limit: 9 },
      intervalMs: SCAN_MS,
      clock,
    });
    await driveUntil({ clock, done: settle(updated) });
    expect(await updated).toEqual({ previous: 4 });
    // Still suspended: an update does not end the wait, and the scanner stops re-waking it.
    expect((await storage.loadWorkflowStatus("b-1"))?.status).toBe("suspended");
    const journalLen = (await storage.loadJournal({ workflowId: "b-1", stepName: "main" })).length;
    for (let i = 0; i < 6; i++) {
      await flush();
      clock.advance(SCAN_MS);
    }
    expect((await storage.loadJournal({ workflowId: "b-1", stepName: "main" })).length).toBe(
      journalLen,
    );

    // Typed result on a second update; then the signal ends the run with the handler's state.
    const second = handle.update({
      update: SetLimit,
      args: { limit: 12 },
      intervalMs: SCAN_MS,
      clock,
    });
    await driveUntil({ clock, done: settle(second) });
    expect(await second).toEqual({ previous: 9 });
    await handle.signal({ signalName: "go", payload: "now" });
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflowStatus("b-1"))?.status === "completed",
    });
    expect(await handle.result({ intervalMs: SCAN_MS })).toEqual({ limit: 12 });
    await scanner.stop();

    expect(events.map((e) => [e.updateName, e.outcome])).toEqual([
      ["setLimit", "completed"],
      ["setLimit", "completed"],
    ]);

    // The run has ended: typed rejection.
    await expect(handle.update({ update: SetLimit, args: { limit: 1 } })).rejects.toMatchObject({
      _tag: "UpdateRejectedError",
      reason: "run_terminal",
    });
  });
});
