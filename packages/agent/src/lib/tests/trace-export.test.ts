import { describe, expect, it } from "bun:test";
import {
  ROOT_CONTEXT,
  SpanStatusCode,
  context as otelContext,
  trace as otelTrace,
  type Context,
  type ContextManager,
} from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { agentAction } from "../agent-action.ts";
import {
  createOpenTelemetryAgentTraceExporter,
  type OpenTelemetrySpanLike,
  type OpenTelemetryTracerLike,
} from "../trace-export.ts";
import type { TurnTrace } from "../trace.ts";
import { freshRunner, reactiveLLM } from "./engine-harness.ts";

const SAMPLE_TRACE: TurnTrace = {
  workflowId: "wf-1",
  startedAt: 100,
  endedAt: 170,
  durationMs: 70,
  finishReason: "stop",
  steps: 1,
  usage: { inputTokens: 8, outputTokens: 3 },
  spans: [
    { kind: "think", step: 0, startedAt: 100, durationMs: 20 },
    {
      kind: "tool",
      step: 0,
      name: "lookup",
      toolCallId: "call-1",
      failed: true,
      startedAt: 120,
      durationMs: 50,
    },
  ],
};
const SAMPLE_CONTEXT = {
  agentName: "helper",
  workflowId: "wf-1",
  scope: { namespaceId: "tenant", resourceId: "secret", threadId: "thread-1" },
};

/** Synchronous stack-based context manager: enough for the sync exporter. */
class StackContextManager implements ContextManager {
  private stack: Context[] = [];
  active(): Context {
    return this.stack.at(-1) ?? ROOT_CONTEXT;
  }
  with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(
    ctx: Context,
    fn: F,
    thisArg?: ThisParameterType<F>,
    ...args: A
  ): ReturnType<F> {
    this.stack.push(ctx);
    try {
      return fn.call(thisArg, ...args);
    } finally {
      this.stack.pop();
    }
  }
  bind<T>(_ctx: Context, target: T): T {
    return target;
  }
  enable(): this {
    return this;
  }
  disable(): this {
    this.stack = [];
    return this;
  }
}

describe("agent trace export", () => {
  it("exports correlated timing and usage without content, and applies the attribute filter", () => {
    const spans: Array<{
      name: string;
      attributes: Record<string, string | number | boolean>;
      parent: string | null;
      endedAt?: number;
    }> = [];
    const stack: string[] = [];
    const tracer = {
      startActiveSpan(
        name: string,
        options: { attributes: Record<string, string | number | boolean> },
        ...rest: unknown[]
      ) {
        const fn = rest.at(-1) as (span: OpenTelemetrySpanLike) => never;
        const item = {
          name,
          attributes: options.attributes,
          parent: stack.at(-1) ?? null,
          endedAt: undefined as number | undefined,
        };
        spans.push(item);
        stack.push(name);
        const span: OpenTelemetrySpanLike = {
          end(at) {
            item.endedAt = at?.getTime();
          },
        };
        try {
          return fn(span);
        } finally {
          stack.pop();
        }
      },
    } as OpenTelemetryTracerLike;
    const trace: TurnTrace = {
      workflowId: "wf-1",
      startedAt: 100,
      endedAt: 170,
      durationMs: 70,
      finishReason: "stop",
      steps: 1,
      usage: { inputTokens: 8, outputTokens: 3 },
      spans: [
        { kind: "think", step: 0, startedAt: 100, durationMs: 20 },
        {
          kind: "tool",
          step: 0,
          name: "lookup",
          toolCallId: "call-1",
          failed: false,
          startedAt: 120,
          durationMs: 50,
        },
      ],
    };
    createOpenTelemetryAgentTraceExporter({
      tracer,
      filterAttribute: (key, value) => (key === "agent.resource_id" ? undefined : value),
    }).exportTurn(trace, {
      agentName: "helper",
      workflowId: "wf-1",
      scope: { namespaceId: "tenant", resourceId: "secret", threadId: "thread-1" },
    });

    expect(spans.map((s) => [s.name, s.parent, s.endedAt])).toEqual([
      ["agent.turn", null, 170],
      ["agent.think", "agent.turn", 120],
      ["agent.tool", "agent.turn", 170],
    ]);
    expect(spans[0]!.attributes).toMatchObject({
      "agent.workflow_id": "wf-1",
      "agent.usage.input_tokens": 8,
    });
    expect(spans[0]!.attributes["agent.resource_id"]).toBeUndefined();
    expect(spans[0]!.attributes["agent.thread_id"]).toMatch(/^[0-9a-f]{16}$/);
    expect(JSON.stringify(spans)).not.toContain("secret");
    expect(JSON.stringify(spans)).not.toContain("thread-1");
  });

  it("exports once for a completed workflow and ignores telemetry failures", async () => {
    const { runner } = freshRunner();
    const exported: string[] = [];
    const errors: unknown[] = [];
    const workflow = agentAction({
      name: "trace-export",
      llm: reactiveLLM(() => ({ content: "done", finishReason: "stop" })),
      traceExporter: {
        exportTurn(trace) {
          exported.push(trace.workflowId);
          throw new Error("collector unavailable");
        },
        onError(error) {
          errors.push(error);
        },
      },
    });
    const first = await runner.run({ workflow, workflowId: "export-once", input: { task: "go" } });
    const replay = await runner.run({ workflow, workflowId: "export-once", input: { task: "go" } });
    expect(first.answer).toBe("done");
    expect(replay.answer).toBe("done");
    expect(exported).toEqual(["export-once"]);
    expect(errors).toHaveLength(1);
  });

  it("hashes identifiers by default, salts them, and exports them raw only on request", () => {
    const rootAttrs = (options: { includeIdentifiers?: boolean; identifierSalt?: string }) => {
      const exporter = new InMemorySpanExporter();
      const provider = new BasicTracerProvider({
        spanProcessors: [new SimpleSpanProcessor(exporter)],
      });
      createOpenTelemetryAgentTraceExporter({
        tracer: provider.getTracer("t"),
        ...options,
      }).exportTurn(SAMPLE_TRACE, SAMPLE_CONTEXT);
      return exporter.getFinishedSpans().find((s) => s.name === "agent.turn")!.attributes;
    };
    const plain = rootAttrs({});
    const salted = rootAttrs({ identifierSalt: "pepper" });
    expect(plain["agent.resource_id"]).not.toBe("secret");
    expect(plain["agent.resource_id"]).toBe(rootAttrs({})["agent.resource_id"]);
    expect(salted["agent.resource_id"]).not.toBe(plain["agent.resource_id"]);
    const raw = rootAttrs({ includeIdentifiers: true });
    expect(raw["agent.resource_id"]).toBe("secret");
    expect(raw["agent.thread_id"]).toBe("thread-1");
  });

  describe("with the real OpenTelemetry API", () => {
    it("links children to the root, joins the caller's trace and marks failed tools", () => {
      const manager = new StackContextManager();
      otelContext.setGlobalContextManager(manager);
      try {
        const exporter = new InMemorySpanExporter();
        const provider = new BasicTracerProvider({
          spanProcessors: [new SimpleSpanProcessor(exporter)],
        });
        const traceId = "0af7651916cd43dd8448eb211c80319c";
        const parentSpanId = "b7ad6b7169203331";
        createOpenTelemetryAgentTraceExporter({
          tracer: provider.getTracer("test"),
          // propagation.extract needs a registered propagator; parse by hand here.
          parentContext: (ctx) => {
            const [, tid, sid, flags] = ctx.traceparent!.split("-");
            return otelTrace.setSpanContext(ROOT_CONTEXT, {
              traceId: tid!,
              spanId: sid!,
              traceFlags: Number.parseInt(flags!, 16),
              isRemote: true,
            });
          },
        }).exportTurn(SAMPLE_TRACE, {
          ...SAMPLE_CONTEXT,
          traceparent: `00-${traceId}-${parentSpanId}-01`,
        });
        const spans = exporter.getFinishedSpans();
        const root = spans.find((s) => s.name === "agent.turn")!;
        const tool = spans.find((s) => s.name === "agent.tool")!;
        const think = spans.find((s) => s.name === "agent.think")!;
        expect(root.spanContext().traceId).toBe(traceId);
        expect(root.parentSpanContext?.spanId).toBe(parentSpanId);
        for (const child of [tool, think]) {
          expect(child.spanContext().traceId).toBe(traceId);
          expect(child.parentSpanContext?.spanId).toBe(root.spanContext().spanId);
        }
        expect(root.startTime).toEqual([0, 100_000_000]);
        expect(root.endTime).toEqual([0, 170_000_000]);
        expect(tool.startTime).toEqual([0, 120_000_000]);
        expect(tool.endTime).toEqual([0, 170_000_000]);
        expect(tool.attributes).toMatchObject({
          "agent.tool.name": "lookup",
          "agent.tool.failed": true,
        });
        expect(tool.status.code).toBe(SpanStatusCode.ERROR);
        expect(think.status.code).toBe(SpanStatusCode.UNSET);
        expect(root.attributes["agent.usage.input_tokens"]).toBe(8);
      } finally {
        otelContext.disable();
        otelTrace.disable();
      }
    });
  });

  it("does not let a hung exporter hold the turn, and reports the timeout", async () => {
    const { runner } = freshRunner();
    const errors: unknown[] = [];
    const workflow = agentAction({
      name: "trace-timeout",
      llm: reactiveLLM(() => ({ content: "done", finishReason: "stop" })),
      traceExporter: {
        timeoutMs: 30,
        exportTurn: () => new Promise<void>(() => {}),
        onError: (error) => {
          errors.push(error);
        },
      },
    });
    const started = Date.now();
    const result = await runner.run({ workflow, workflowId: "export-hang", input: { task: "go" } });
    expect(result.answer).toBe("done");
    expect(Date.now() - started).toBeLessThan(2000);
    expect(errors).toHaveLength(1);
    expect(String(errors[0])).toContain("timed out");
  });
});
