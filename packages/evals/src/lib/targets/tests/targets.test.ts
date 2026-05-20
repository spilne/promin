import { describe, expect, it } from "bun:test";
import type { AgentRunOutput, RegisteredAgent, ResolveLocalAgentDeps } from "@promin/agent";
import { FakeClock } from "@promin/core";
import { fnTarget } from "../fn-target.ts";
import { recipeTarget } from "../recipe-target.ts";
import { toEvalOutput } from "../to-eval-output.ts";

function fakeRun(opts: {
  text?: string;
  output?: unknown;
  usage?: { inputTokens: number; outputTokens: number };
  finishReason?: string;
}): AgentRunOutput {
  async function* empty(): AsyncGenerator<never> {}
  return {
    textStream: empty(),
    fullStream: empty(),
    text: Promise.resolve(opts.text ?? ""),
    output: Promise.resolve(opts.output),
    toolCalls: Promise.resolve([]),
    toolResults: Promise.resolve([]),
    steps: Promise.resolve([]),
    usage: Promise.resolve(opts.usage ?? { inputTokens: 0, outputTokens: 0 }),
    finishReason: Promise.resolve(opts.finishReason ?? "stop"),
    messages: Promise.resolve([]),
    cancel: async () => {},
  } as AgentRunOutput;
}

describe("toEvalOutput", () => {
  it("maps text + usage and builds a trace", async () => {
    const result = await toEvalOutput(
      fakeRun({ text: "hello", usage: { inputTokens: 10, outputTokens: 5 } }),
      { latencyMs: 42 },
    );
    expect(result.output).toBe("hello");
    expect(result.metrics.latencyMs).toBe(42);
    expect(result.metrics.inputTokens).toBe(10);
    expect(result.trace?.summary.turns).toBe(0);
    expect(result.error).toBeUndefined();
  });

  it("prefers structured output over text", async () => {
    const result = await toEvalOutput(fakeRun({ text: "ignored", output: { ok: true } }), {
      latencyMs: 1,
    });
    expect(result.output).toEqual({ ok: true });
  });

  it("computes costUsd when rates are supplied", async () => {
    const result = await toEvalOutput(
      fakeRun({ usage: { inputTokens: 1_000_000, outputTokens: 0 } }),
      {
        latencyMs: 1,
        rates: { inputPerMillion: 3 },
      },
    );
    expect(result.metrics.costUsd).toBeCloseTo(3);
  });

  it("surfaces a finished-in-error run", async () => {
    const result = await toEvalOutput(fakeRun({ finishReason: "error" }), { latencyMs: 1 });
    expect(result.error).toBeDefined();
  });
});

describe("fnTarget", () => {
  it("runs the function and carries the configured id", async () => {
    const target = fnTarget((input) => `echo:${String(input)}`, {
      id: "echo",
      clock: FakeClock.create(1000),
    });
    expect(target.id).toBe("echo");
    const result = await target.run({ id: "c", input: "hi" });
    expect(result.output).toBe("echo:hi");
    expect(result.error).toBeUndefined();
  });

  it("catches a throwing function into EvalOutput.error", async () => {
    const target = fnTarget(() => {
      throw new Error("boom");
    });
    const result = await target.run({ id: "c", input: 1 });
    expect(result.output).toBeUndefined();
    expect(result.error).toContain("boom");
  });
});

describe("recipeTarget", () => {
  const recipe: RegisteredAgent = {
    id: "support-bot",
    version: "v3",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-x" },
      systemPrompt: null,
      tools: ["ghostTool"],
    },
    metadata: { description: null, capabilities: [], tags: [] },
    createdAt: 0,
    updatedAt: 0,
  };
  const deps = {
    runner: undefined,
    llm: () => {
      throw new Error("llm factory should not run");
    },
    tools: {},
  } as unknown as ResolveLocalAgentDeps;

  it("takes id + version from the recipe", () => {
    const target = recipeTarget({ recipe, deps });
    expect(target.id).toBe("support-bot");
    expect(target.version).toBe("v3");
  });

  it("catches a resolve failure into EvalOutput.error", async () => {
    const target = recipeTarget({ recipe, deps });
    const result = await target.run({ id: "c1", input: "help" });
    expect(result.output).toBeUndefined();
    expect(result.error).toContain("ghostTool");
  });
});
