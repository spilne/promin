// ---------------------------------------------------------------------------
// The OpenTelemetry adapter against the real `@opentelemetry/api` and SDK:
// span parentage, status, timestamps and attributes of exported spans.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import * as api from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
} from "@opentelemetry/sdk-trace-base";
import { TaggedError, succeed, fail } from "@spilne/perfect-core";
import { workflow } from "../../durable/workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { MapStepRegistry } from "../../distributed/step-registry.ts";
import { InMemoryStepQueue } from "../../distributed/in-memory-step-queue.ts";
import { createDistributedWorkflowRunner } from "../../distributed/coordinator.ts";
import { createWorker } from "../../distributed/worker.ts";
import {
  createOpenTelemetryWorkflowTracer,
  type OpenTelemetryWorkflowTracerOptions,
  type OtelApiLike,
} from "../otel-workflow-tracer.ts";

class TestError extends TaggedError("TestError")<{ readonly message: string }>() {}

const T0 = "2026-01-01T00:00:00Z";
const T0_MS = Date.parse(T0);
const REMOTE_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
const REMOTE_SPAN_ID = "00f067aa0ba902b7";
const REMOTE_TRACEPARENT = `00-${REMOTE_TRACE_ID}-${REMOTE_SPAN_ID}-01`;

/** The real API, with `propagation.extract` reading `traceparent` by hand (no propagator is registered). */
const otelApi: OtelApiLike = {
  trace: api.trace,
  context: api.context,
  ROOT_CONTEXT: api.ROOT_CONTEXT,
  propagation: {
    extract(context, carrier) {
      const [, traceId, spanId, flags] = (carrier.traceparent ?? "").split("-");
      if (!traceId || !spanId) return context;
      return api.trace.setSpanContext(context as api.Context, {
        traceId,
        spanId,
        traceFlags: Number.parseInt(flags ?? "00", 16),
        isRemote: true,
      });
    },
  },
};

function harness(options?: Partial<OpenTelemetryWorkflowTracerOptions>) {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  const tracer = createOpenTelemetryWorkflowTracer({
    tracer: provider.getTracer("test"),
    api: otelApi,
    ...options,
  });
  const clock = FakeWallClock.create(T0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock, tracer });
  const spans = (): ReadableSpan[] => exporter.getFinishedSpans();
  const named = (prefix: string): ReadableSpan[] =>
    spans().filter((s) => s.name.startsWith(prefix));
  return { exporter, tracer, clock, storage, runner, spans, named };
}

const ms = (time: [number, number]): number => time[0] * 1000 + time[1] / 1e6;
const parentOf = (span: ReadableSpan): string | undefined => span.parentSpanContext?.spanId;
const idOf = (span: ReadableSpan): string => span.spanContext().spanId;

describe("OpenTelemetry workflow tracer — span tree", () => {
  const saga = () =>
    workflow<number>({ name: "saga" })
      .step("charge", () => succeed(1), { compensate: () => succeed(undefined as void) })
      .step("ship", () => fail(new TestError({ message: "no" })))
      .build();

  it("a failed saga is one trace: root, step attempts, and compensation as its children", async () => {
    const h = harness();
    await h.runner.runSafe({ workflow: saga(), workflowId: "w1", input: 0 });

    const [root] = h.named("workflow.run");
    const steps = h.named("workflow.step");
    const [compensation] = h.spans().filter((s) => s.name === "workflow.compensation");
    const [compensationStep] = h.named("workflow.compensation.step");
    expect(h.spans()).toHaveLength(5);

    expect(parentOf(root!)).toBeUndefined();
    expect(steps.map((s) => parentOf(s))).toEqual([idOf(root!), idOf(root!)]);
    expect(parentOf(compensation!)).toBe(idOf(root!));
    expect(parentOf(compensationStep!)).toBe(idOf(compensation!));
    expect(new Set(h.spans().map((s) => s.spanContext().traceId)).size).toBe(1);

    // ERROR status carries the tag, not the message.
    expect(root!.status).toEqual({ code: api.SpanStatusCode.ERROR, message: "TestError" });
    const ship = steps.find((s) => s.name === "workflow.step ship")!;
    expect(ship.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(ship.attributes["workflow.step.error.tag"]).toBe("TestError");
    expect(steps.find((s) => s.name === "workflow.step charge")!.status.code).toBe(
      api.SpanStatusCode.UNSET,
    );
    expect(root!.attributes["workflow.outcome"]).toBe("failed");
    expect(compensation!.attributes["workflow.compensation.compensated"]).toBe(1);
  });

  it("negative control: a successful run has no error status and no compensation span", async () => {
    const h = harness();
    const wf = workflow<number>({ name: "ok" })
      .step("a", () => succeed(1))
      .build();
    await h.runner.run({ workflow: wf, workflowId: "w1", input: 0 });

    expect(h.spans().map((s) => s.status.code)).toEqual([
      api.SpanStatusCode.UNSET,
      api.SpanStatusCode.UNSET,
    ]);
    expect(h.spans().filter((s) => s.name.startsWith("workflow.compensation"))).toEqual([]);
    expect(h.named("workflow.run")[0]!.attributes["workflow.outcome"]).toBe("completed");
  });

  it("span timestamps are the engine's: a step that takes 250ms is a 250ms span inside the run's", async () => {
    const h = harness();
    const wf = workflow<number>({ name: "timed" })
      .step("slow", () => {
        h.clock.advance(250);
        return succeed(1);
      })
      .build();
    await h.runner.run({ workflow: wf, workflowId: "w1", input: 0 });

    const [root] = h.named("workflow.run");
    const [step] = h.named("workflow.step");
    expect(ms(root!.startTime)).toBe(T0_MS);
    expect(ms(root!.endTime) - ms(root!.startTime)).toBe(250);
    expect(ms(step!.startTime)).toBe(T0_MS);
    expect(ms(step!.endTime)).toBe(T0_MS + 250);
    expect(step!.attributes["workflow.duration_ms"]).toBe(250);
  });

  it("a retried step is one span per attempt, the failed one in ERROR and marked retried", async () => {
    const h = harness();
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage, tracer: h.tracer });
    let calls = 0;
    const wf = workflow<number>({ name: "flaky" })
      .step("a", () => (++calls < 2 ? fail(new TestError({ message: "no" })) : succeed(1)), {
        retry: { maxRetries: 2, baseDelayMs: 0, jitter: false },
      })
      .build();
    await runner.run({ workflow: wf, workflowId: "w1", input: 0 });

    const attempts = h
      .named("workflow.step")
      .sort(
        (a, b) =>
          Number(a.attributes["workflow.step.attempt"]) -
          Number(b.attributes["workflow.step.attempt"]),
      );
    expect(attempts.map((s) => [s.attributes["workflow.step.attempt"], s.status.code])).toEqual([
      [1, api.SpanStatusCode.ERROR],
      [2, api.SpanStatusCode.UNSET],
    ]);
    expect(attempts[0]!.attributes["workflow.step.will_retry"]).toBe(true);
    expect(attempts[1]!.attributes["workflow.step.will_retry"]).toBe(false);
  });

  it("a child workflow's root is a child of the parent's, in the same trace", async () => {
    const h = harness();
    const child = workflow<{ id: number }>({ name: "child" })
      .step("work", ({ input }) => succeed(input.id))
      .build();
    const parent = workflow<{ n: number }>({ name: "parent" })
      .subworkflow("enrich", child, {
        input: (prev) => ({ id: (prev as { n: number }).n }),
        workflowId: () => "child-1",
      })
      .build();
    await h.runner.run({ workflow: parent, workflowId: "p", input: { n: 1 } });

    const parentRoot = h.named("workflow.run parent")[0]!;
    const childRoot = h.named("workflow.run child")[0]!;
    expect(parentOf(childRoot)).toBe(idOf(parentRoot));
    expect(childRoot.spanContext().traceId).toBe(parentRoot.spanContext().traceId);
    expect(childRoot.attributes["workflow.parent.id"]).toBeDefined();
  });

  it("negative control: two unrelated runs are separate traces", async () => {
    const h = harness();
    const wf = workflow<number>({ name: "ok" })
      .step("a", () => succeed(1))
      .build();
    await h.runner.run({ workflow: wf, workflowId: "w1", input: 0 });
    await h.runner.run({ workflow: wf, workflowId: "w2", input: 0 });

    const roots = h.named("workflow.run");
    expect(roots[0]!.spanContext().traceId).not.toBe(roots[1]!.spanContext().traceId);
  });

  it("a signal wait ends the run's span as suspended; the resume joins the stored trace and records the wait", async () => {
    const h = harness();
    const wf = () =>
      workflow<number>({ name: "approval" })
        .waitForSignal<string>("approve", { signalName: "go" })
        .step("done", () => succeed(1))
        .build();

    await h.runner.runSafe({
      workflow: wf(),
      workflowId: "w1",
      input: 0,
      traceparent: REMOTE_TRACEPARENT,
    });
    expect(h.named("workflow.run")[0]!.events.map((e) => e.name)).toContain("workflow.wait.start");
    expect(h.named("workflow.run")[0]!.attributes["workflow.outcome"]).toBe("suspended");

    h.clock.advance(5_000);
    await h.storage.deliverSignal({ workflowId: "w1", signalName: "go", payload: "ok" });
    await h.runner.run({ workflow: wf(), workflowId: "w1", input: 0 });

    const roots = h.named("workflow.run");
    expect(roots).toHaveLength(2);
    // Both orchestrations hang off the caller's trace, not two new ones.
    for (const root of roots) {
      expect(root.spanContext().traceId).toBe(REMOTE_TRACE_ID);
      expect(parentOf(root)).toBe(REMOTE_SPAN_ID);
    }
    const [wait] = h.named("workflow.wait");
    expect(wait!.attributes["workflow.wait.kind"]).toBe("signal");
    expect(ms(wait!.endTime) - ms(wait!.startTime)).toBe(5_000);
    expect(parentOf(wait!)).toBe(idOf(roots[1]!));
  });

  it("negative control: without a traceparent a run starts its own trace", async () => {
    const h = harness();
    const wf = workflow<number>({ name: "ok" })
      .step("a", () => succeed(1))
      .build();
    await h.runner.run({ workflow: wf, workflowId: "w1", input: 0 });

    const [root] = h.named("workflow.run");
    expect(root!.spanContext().traceId).not.toBe(REMOTE_TRACE_ID);
    expect(parentOf(root!)).toBeUndefined();
  });

  it("a parentContext option wins over the traceparent", async () => {
    const forced = api.trace.setSpanContext(api.ROOT_CONTEXT, {
      traceId: "11111111111111111111111111111111",
      spanId: "2222222222222222",
      traceFlags: 1,
    });
    const h = harness({ parentContext: () => forced });
    const wf = workflow<number>({ name: "ok" })
      .step("a", () => succeed(1))
      .build();
    await h.runner.run({
      workflow: wf,
      workflowId: "w1",
      input: 0,
      traceparent: REMOTE_TRACEPARENT,
    });

    expect(h.named("workflow.run")[0]!.spanContext().traceId).toBe(
      "11111111111111111111111111111111",
    );
  });
});

describe("OpenTelemetry workflow tracer — what reaches a span", () => {
  const leaky = () =>
    workflow<{ secret: string }>({ name: "leaky" })
      .step("a", ({ input }) => succeed({ echoed: input.secret }))
      .step("b", () => fail(new TestError({ message: "SECRET-ERROR-MESSAGE" })))
      .build();
  const input = { secret: "SECRET-INPUT-VALUE" };

  it("never exports inputs, results or error messages, and hashes the ids", async () => {
    const h = harness();
    await h.runner.runSafe({
      workflow: leaky(),
      workflowId: "customer-4242",
      input,
      namespace: "tenant-acme",
    });

    const dump = JSON.stringify(
      h.spans().map((s) => ({
        name: s.name,
        attributes: s.attributes,
        status: s.status,
        events: s.events,
      })),
    );
    expect(dump).not.toContain("SECRET");
    expect(dump).not.toContain("customer-4242");
    expect(dump).not.toContain("tenant-acme");
    const [root] = h.named("workflow.run");
    expect(root!.attributes["workflow.id"]).toMatch(/^[0-9a-f]{16}$/);
    expect(root!.attributes["workflow.namespace"]).toMatch(/^[0-9a-f]{16}$/);
  });

  it("negative control: includeIdentifiers exports the raw ids, and still no content", async () => {
    const h = harness({ includeIdentifiers: true });
    await h.runner.runSafe({
      workflow: leaky(),
      workflowId: "customer-4242",
      input,
      namespace: "tenant-acme",
    });

    const [root] = h.named("workflow.run");
    expect(root!.attributes["workflow.id"]).toBe("customer-4242");
    expect(root!.attributes["workflow.namespace"]).toBe("tenant-acme");
    expect(JSON.stringify(h.spans().map((s) => s.attributes))).not.toContain("SECRET");
  });

  it("the id hash is stable per salt and differs between salts", async () => {
    const idUnder = async (salt: string | undefined) => {
      const h = harness(salt !== undefined ? { identifierSalt: salt } : {});
      await h.runner.runSafe({ workflow: leaky(), workflowId: "w1", input });
      return h.named("workflow.run")[0]!.attributes["workflow.id"];
    };
    const a1 = await idUnder("a");
    expect(await idUnder("a")).toBe(a1);
    expect(await idUnder("b")).not.toBe(a1);
    expect(await idUnder(undefined)).not.toBe(a1);
  });

  it("filterAttribute can drop an attribute", async () => {
    const h = harness({
      filterAttribute: (key, value) => (key === "workflow.name" ? undefined : value),
    });
    await h.runner.runSafe({ workflow: leaky(), workflowId: "w1", input });

    expect(h.named("workflow.run")[0]!.attributes["workflow.name"]).toBeUndefined();
    expect(h.named("workflow.run")[0]!.attributes["workflow.outcome"]).toBe("failed");
  });
});

describe("OpenTelemetry workflow tracer — failure isolation and lifecycle", () => {
  it("a tracer whose spans throw does not affect the run, and the errors are reported", async () => {
    const errors: unknown[] = [];
    const exploding = createOpenTelemetryWorkflowTracer({
      tracer: {
        startSpan() {
          throw new Error("exporter down");
        },
      },
      api: otelApi,
      onError: (e) => errors.push(e),
    });
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock, tracer: exploding });
    const wf = workflow<number>({ name: "ok" })
      .step("a", () => succeed(7))
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "w1", input: 0 })).toBe(7);
    expect(errors.length).toBeGreaterThan(0);
  });

  it("every span ends: a run that suspends and one that fails leave nothing open", async () => {
    const h = harness();
    const sleepy = workflow<number>({ name: "sleepy" })
      .sleep("nap", 10)
      .step("done", () => succeed(1))
      .build();
    await h.runner.runSafe({ workflow: sleepy, workflowId: "s1", input: 0 });
    await h.runner.runSafe({
      workflow: workflow<number>({ name: "bad" })
        .step("a", () => fail(new TestError({ message: "x" })))
        .build(),
      workflowId: "f1",
      input: 0,
    });

    // The in-memory exporter only holds ended spans: root + step per run
    // (the failed run also opened, and closed, an empty compensation).
    expect(
      h
        .spans()
        .map((s) => s.name)
        .sort(),
    ).toEqual([
      "workflow.compensation",
      "workflow.run bad",
      "workflow.run sleepy",
      "workflow.step a",
      "workflow.step nap",
    ]);
  });
});

describe("OpenTelemetry workflow tracer — traceparent across the queue", () => {
  const POLL_MS = 50;
  async function flush(): Promise<void> {
    for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
  }

  async function runDistributed(options?: Partial<OpenTelemetryWorkflowTracerOptions>) {
    const h = harness(options);
    const queue = new InMemoryStepQueue({ clock: h.clock });
    const registry = new MapStepRegistry();
    registry.register({ stepName: "double", handler: (ctx) => succeed((ctx.input as number) * 2) });
    const dist = workflow<number>({ name: "dist" })
      .step("double", ({ input }) => succeed(input * 2))
      .build();
    const coordinator = createDistributedWorkflowRunner({
      storage: h.storage,
      stepQueue: queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock: h.clock,
      tracer: h.tracer,
    });
    const worker = createWorker({
      stepQueue: queue,
      registry,
      pollIntervalMs: POLL_MS,
      clock: h.clock,
      tracer: h.tracer,
    });
    const box: { result?: unknown } = {};
    void coordinator.startLoop();
    void worker.start();
    void coordinator
      .run({ workflow: dist, workflowId: "d1", input: 4 })
      .then((r) => (box.result = r));
    for (let elapsed = 0; box.result === undefined && elapsed < 10_000; elapsed += POLL_MS) {
      await flush();
      h.clock.advance(POLL_MS);
    }
    await coordinator.stopLoop();
    await worker.stop();
    return { h, result: box.result };
  }

  it("the worker's span is a child of the coordinator's step attempt span", async () => {
    const { h, result } = await runDistributed();

    expect(result).toBe(8);
    const [step] = h.named("workflow.step");
    const [task] = h.named("workflow.worker.task");
    expect(parentOf(task!)).toBe(idOf(step!));
    expect(task!.spanContext().traceId).toBe(step!.spanContext().traceId);
    expect(task!.kind).toBe(api.SpanKind.CONSUMER);
    // Worker and runner ids are hashed like the rest.
    expect(task!.attributes["workflow.worker.id"]).toMatch(/^[0-9a-f]{16}$/);
  });

  it("negative control: with injectTraceparent off the worker's span starts its own trace", async () => {
    const { h } = await runDistributed({ injectTraceparent: false });

    const [step] = h.named("workflow.step");
    const [task] = h.named("workflow.worker.task");
    expect(parentOf(task!)).toBeUndefined();
    expect(task!.spanContext().traceId).not.toBe(step!.spanContext().traceId);
  });

  it("traceparent() names the open step attempt's span in W3C form", async () => {
    const h = harness();
    let seen: string | undefined;
    const wf = workflow<number>({ name: "probe" })
      .step("a", () => {
        seen = h.tracer.traceparent?.({ workflowId: "w1", stepName: "a", attempt: 1 });
        return succeed(1);
      })
      .build();
    await h.runner.run({ workflow: wf, workflowId: "w1", input: 0 });

    const [step] = h.named("workflow.step");
    expect(seen).toBe(`00-${step!.spanContext().traceId}-${idOf(step!)}-01`);
    // Once the run is over there is nothing to name.
    expect(h.tracer.traceparent?.({ workflowId: "w1", stepName: "a", attempt: 1 })).toBeUndefined();
  });
});
