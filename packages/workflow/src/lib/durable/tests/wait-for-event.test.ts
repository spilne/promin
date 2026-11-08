// ---------------------------------------------------------------------------
// ctx.waitForEvent / deliverEvent — buffered, addressed, idempotent events on
// top of the signal store: early delivery, ordered consumption, keys,
// predicate, timeout, idempotent redelivery, the park/deliver race, replay
// stability, waitingOn and tracing. The signal scanner (runner mode) resumes
// parked runs, as in production.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { createSignalScanner } from "../../distributed/signal-scanner.ts";
import { createSleepScanner } from "../../distributed/sleep-scanner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { deliverEvent } from "../wait-for-event.ts";
import { deriveWaitingOn, summarizeWaits } from "../workflow-waits.ts";
import type { Workflow } from "../durable-pipeline.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { WorkflowTracer } from "../workflow-tracer.ts";
import { defineSignal } from "../../signals/define-signal.ts";
import { s } from "../../schema/builder.ts";

const T0 = "2026-01-01T00:00:00.000Z";
const SCAN_MS = 50;
const tagOf = (e: unknown) => (e as { _tag?: string } | null | undefined)?._tag;

type Payload = { amount: number };

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

function setup(params?: { storage?: (clock: FakeWallClock) => InMemoryWorkflowStorage }) {
  const clock = FakeWallClock.create(T0);
  const storage = params?.storage?.(clock) ?? new InMemoryWorkflowStorage({ clock });
  const tracerEvents: Array<{ name: string; event: Record<string, unknown> }> = [];
  const tracer = new Proxy(
    {},
    {
      get: (_t, name) => (event: Record<string, unknown>) => {
        tracerEvents.push({ name: String(name), event });
      },
    },
  ) as WorkflowTracer;
  const runner = createWorkflowRunner({ storage, clock, tracer });
  return { clock, storage, runner, tracerEvents };
}

/** Run scanners (signal + sleep) over the workflow until `done` holds. */
async function drive(params: {
  clock: FakeWallClock;
  storage: WorkflowStorage;
  runner: ReturnType<typeof setup>["runner"];
  workflow: unknown;
  done: () => Promise<boolean>;
}) {
  const target = {
    runner: params.runner,
    resolveWorkflow: () => params.workflow as unknown as Workflow<unknown, unknown>,
  };
  const common = { storage: params.storage, clock: params.clock, scanIntervalMs: SCAN_MS };
  const signals = createSignalScanner({ ...common, ...target });
  const sleeps = createSleepScanner({ ...common, ...target });
  void signals.start();
  void sleeps.start();
  try {
    for (let i = 0; i < 400; i++) {
      await flush();
      if (await params.done()) return;
      params.clock.advance(SCAN_MS);
    }
    expect(await params.done()).toBe(true);
  } finally {
    await signals.stop();
    await sleeps.stop();
  }
}

const statusOf = async (storage: WorkflowStorage, id: string) =>
  (await storage.loadWorkflowStatus(id))?.status;

const oneWait = () =>
  workflow<number>({ name: "one" })
    .journaled("wait", function* (ctx) {
      return yield* ctx.waitForEvent<Payload>("payment");
    })
    .build();

describe("ctx.waitForEvent", () => {
  it("parks, shows in waitingOn and the tracer, and a delivery resumes the run", async () => {
    const { clock, storage, runner, tracerEvents } = setup();
    const wf = oneWait();
    const first = await runner.runSafe({ workflow: wf, workflowId: "e1", input: 1 });
    expect(tagOf(first.error)).toBe("WorkflowSuspendedError");

    const waits = deriveWaitingOn((await storage.loadWorkflow("e1"))!);
    expect(waits).toEqual([
      expect.objectContaining({ kind: "event", eventName: "payment", stepName: "wait" }),
    ]);
    expect(summarizeWaits({ waits, now: new Date(T0) })).toContain("event payment");
    expect(tracerEvents.find((e) => e.name === "waitStart")?.event).toMatchObject({
      kind: "event",
      eventName: "payment",
    });

    const sent = await deliverEvent({
      storage,
      workflowId: "e1",
      event: "payment",
      payload: { amount: 5 },
    });
    expect(sent).toMatchObject({ duplicate: false, woken: 1 });
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e1")) === "completed",
    });
    expect((await storage.loadWorkflow("e1"))?.result).toEqual({ amount: 5 });
    expect(tracerEvents.find((e) => e.name === "waitEnd")?.event).toMatchObject({
      kind: "event",
      eventName: "payment",
    });
  });

  it("an event delivered before the wait starts is buffered, and consumed in delivery order", async () => {
    const { clock, storage, runner } = setup();
    const wf = workflow<number>({ name: "early" })
      .journaled("wait", function* (ctx) {
        yield* ctx.sleep(1_000);
        const a = yield* ctx.waitForEvent<Payload>("payment");
        const b = yield* ctx.waitForEvent<Payload>("payment");
        return [a.amount, b.amount];
      })
      .build();
    await runner.runSafe({ workflow: wf, workflowId: "e2", input: 1 });
    // The run is asleep: nobody is waiting for the events yet.
    await deliverEvent({
      storage,
      workflowId: "e2",
      event: "payment",
      payload: { amount: 1 },
      eventId: "z-first",
    });
    clock.advance(1);
    const second = await deliverEvent({
      storage,
      workflowId: "e2",
      event: "payment",
      payload: { amount: 2 },
      eventId: "a-second",
    });
    expect(second.woken).toBe(0);
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e2")) === "completed",
    });
    // Delivery order, not event-id order.
    expect((await storage.loadWorkflow("e2"))?.result).toEqual([1, 2]);
  });

  it("redelivery of an eventId is a no-op: one event, one wait satisfied", async () => {
    const { clock, storage, runner } = setup();
    const wf = workflow<number>({ name: "idem" })
      .journaled("wait", function* (ctx) {
        const a = yield* ctx.waitForEvent<Payload>("payment", { timeout: 5_000 });
        const b = yield* ctx.waitForEvent<Payload>("payment", { timeout: 5_000 });
        return { a, b };
      })
      .build();
    await runner.runSafe({ workflow: wf, workflowId: "e3", input: 1 });
    const send = () =>
      deliverEvent({
        storage,
        workflowId: "e3",
        event: "payment",
        payload: { amount: 7 },
        eventId: "evt-1",
      });
    expect((await send()).duplicate).toBe(false);
    expect((await send()).duplicate).toBe(true);
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e3")) === "completed",
    });
    expect((await storage.loadWorkflow("e3"))?.result).toEqual({
      a: { ok: true, value: { amount: 7 } },
      b: { ok: false, error: "timeout" },
    });
  });

  it("a retry of a delivery that buffered but did not wake still wakes the wait", async () => {
    const { clock, storage, runner } = setup();
    const wf = oneWait();
    await runner.runSafe({ workflow: wf, workflowId: "e4", input: 1 });
    // First attempt crashed after buffering: only the buffer write landed.
    await storage.deliverSignal({
      workflowId: "e4",
      signalName: "event:payment#@evt-9",
      payload: { eventId: "evt-9", payload: { amount: 3 } },
    });
    const retry = await deliverEvent({
      storage,
      workflowId: "e4",
      event: "payment",
      payload: { amount: 3 },
      eventId: "evt-9",
    });
    expect(retry).toMatchObject({ duplicate: true, woken: 1 });
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e4")) === "completed",
    });
    expect((await storage.loadWorkflow("e4"))?.result).toEqual({ amount: 3 });
  });

  it("a delivery landing between the wait's look and its park is not lost", async () => {
    const { clock, storage, runner } = setup({
      storage: (c) => {
        const inner = new InMemoryWorkflowStorage({ clock: c });
        const suspend = inner.suspendWorkflow.bind(inner);
        // The sender buffers the event after the wait looked (empty) and
        // before it parks, so it finds nobody to wake.
        inner.suspendWorkflow = async (p) => {
          await inner.deliverSignal({
            workflowId: p.workflowId,
            signalName: "event:payment#@raced",
            payload: { eventId: "raced", payload: { amount: 11 } },
          });
          return suspend(p);
        };
        return inner;
      },
    });
    const wf = oneWait();
    await runner.runSafe({ workflow: wf, workflowId: "e5", input: 1 });
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e5")) === "completed",
    });
    expect((await storage.loadWorkflow("e5"))?.result).toEqual({ amount: 11 });
  });

  it("matches on key: other keys and unkeyed events are not taken", async () => {
    const { clock, storage, runner } = setup();
    const wf = workflow<number>({ name: "keyed" })
      .journaled("wait", function* (ctx) {
        return yield* ctx.waitForEvent<Payload>("payment", { key: "order-2" });
      })
      .build();
    await runner.runSafe({ workflow: wf, workflowId: "e6", input: 1 });
    const send = (key: string | undefined, amount: number) =>
      deliverEvent({
        storage,
        workflowId: "e6",
        event: "payment",
        ...(key !== undefined && { key }),
        payload: { amount },
      });
    expect((await send(undefined, 1)).woken).toBe(0);
    expect((await send("order-1", 2)).woken).toBe(0);
    expect((await send("order-2", 3)).woken).toBe(1);
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e6")) === "completed",
    });
    expect((await storage.loadWorkflow("e6"))?.result).toEqual({ amount: 3 });
  });

  it("`where` skips non-matching events, which stay available to a later wait", async () => {
    const { clock, storage, runner } = setup();
    const wf = workflow<number>({ name: "where" })
      .journaled("wait", function* (ctx) {
        const big = yield* ctx.waitForEvent<Payload>("payment", {
          where: (p) => p.amount >= 100,
        });
        const any = yield* ctx.waitForEvent<Payload>("payment");
        return [big.amount, any.amount];
      })
      .build();
    await runner.runSafe({ workflow: wf, workflowId: "e7", input: 1 });
    for (const [id, amount] of [
      ["a", 5],
      ["b", 150],
    ] as const) {
      await deliverEvent({
        storage,
        workflowId: "e7",
        event: "payment",
        payload: { amount },
        eventId: id,
      });
      clock.advance(1);
    }
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e7")) === "completed",
    });
    expect((await storage.loadWorkflow("e7"))?.result).toEqual([150, 5]);
  });

  it("times out at a deadline fixed on the first call, across resumes", async () => {
    const { clock, storage, runner } = setup();
    const wf = workflow<number>({ name: "to" })
      .journaled("wait", function* (ctx) {
        return yield* ctx.waitForEvent<Payload>("payment", { timeout: 1_000 });
      })
      .build();
    await runner.runSafe({ workflow: wf, workflowId: "e8", input: 1 });
    const deadline = "2026-01-01T00:00:01.000Z";
    const stored = async () =>
      (await storage.loadWorkflow("e8"))?.steps["wait"]?.signalTimeoutAt?.toISOString();
    expect(await stored()).toBe(deadline);
    clock.advance(500);
    await runner.runSafe({ workflow: wf, workflowId: "e8", input: 1 });
    expect(await stored()).toBe(deadline);
    clock.advance(500);
    const late = await runner.runSafe({ workflow: wf, workflowId: "e8", input: 1 });
    expect(late.error).toBeNull();
    expect(late.data).toEqual({ ok: false, error: "timeout" });
  });

  it("replays the journaled choice: a later event cannot change a completed wait", async () => {
    const { clock, storage, runner } = setup();
    const wf = workflow<number>({ name: "replay" })
      .journaled("wait", function* (ctx) {
        const a = yield* ctx.waitForEvent<Payload>("payment");
        yield* ctx.sleep(1_000);
        return a.amount;
      })
      .build();
    await runner.runSafe({ workflow: wf, workflowId: "e9", input: 1 });
    await deliverEvent({
      storage,
      workflowId: "e9",
      event: "payment",
      payload: { amount: 1 },
      eventId: "first",
    });
    clock.advance(1);
    await deliverEvent({
      storage,
      workflowId: "e9",
      event: "payment",
      payload: { amount: 2 },
      eventId: "second",
    });
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e9")) === "completed",
    });
    // The step re-ran at least twice (wake, then the sleep): always 1.
    expect((await storage.loadWorkflow("e9"))?.result).toBe(1);
    const journal = await storage.loadJournal({ workflowId: "e9", stepName: "wait" });
    expect(journal.filter((j) => j.phase !== "completed")).toEqual([]);
  });

  it("takes a SignalType and returns its payload type", async () => {
    const { clock, storage, runner } = setup();
    const Paid = defineSignal({ name: "paid", schema: s.object({ amount: s.number() }) });
    const wf = workflow<number>({ name: "typed" })
      .journaled("wait", function* (ctx) {
        const paid = yield* ctx.waitForEvent(Paid);
        return paid.amount;
      })
      .build();
    await runner.runSafe({ workflow: wf, workflowId: "e10", input: 1 });
    const step = (await storage.loadWorkflow("e10"))?.steps["wait"];
    expect(step?.signalJsonSchema).toEqual(Paid.schema.jsonSchema);
    await deliverEvent({ storage, workflowId: "e10", event: Paid, payload: { amount: 4 } });
    await drive({
      clock,
      storage,
      runner,
      workflow: wf,
      done: async () => (await statusOf(storage, "e10")) === "completed",
    });
    expect((await storage.loadWorkflow("e10"))?.result).toBe(4);
  });
});
