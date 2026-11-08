import { describe, expect, it } from "bun:test";
import { agentAction } from "../agent-action.ts";
import {
  createOpenTelemetryAgentTraceExporter,
  type OpenTelemetrySpanLike,
  type OpenTelemetryTracerLike,
} from "../trace-export.ts";
import type { TurnTrace } from "../trace.ts";
import { freshRunner, reactiveLLM } from "./engine-harness.ts";

describe("agent trace export", () => {
  it("exports correlated timing and usage without content, and applies the attribute filter", () => {
    const spans: Array<{
      name: string;
      attributes: Record<string, string | number | boolean>;
      parent: string | null;
      endedAt?: number;
    }> = [];
    const stack: string[] = [];
    const tracer: OpenTelemetryTracerLike = {
      startActiveSpan(name, options, fn) {
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
    };
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
      "agent.thread_id": "thread-1",
      "agent.usage.input_tokens": 8,
    });
    expect(spans[0]!.attributes["agent.resource_id"]).toBeUndefined();
    expect(JSON.stringify(spans)).not.toContain("secret");
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
});
