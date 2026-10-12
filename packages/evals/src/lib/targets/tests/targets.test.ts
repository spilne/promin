import { describe, expect, it } from "bun:test";
import {
  InMemoryMemoryStore,
  resolveLocalAgent,
  type AgentRunOutput,
  type FinishReason,
  type RegisteredAgent,
  type ResolveLocalAgentDeps,
} from "@promin/agent";
import { mockLLM } from "@promin/agent/testing";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentTarget } from "../agent-target.ts";
import { fnTarget } from "../fn-target.ts";
import { recipeTarget } from "../recipe-target.ts";
import { toEvalOutput } from "../to-eval-output.ts";

function fakeRun(opts: {
  text?: string;
  output?: unknown;
  usage?: { inputTokens: number; outputTokens: number };
  finishReason?: FinishReason;
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
  };
}

const recipe: RegisteredAgent = {
  id: "support-bot",
  version: "v3",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-x" },
    role: { inline: { systemPrompt: null, tools: [] } },
  },
  metadata: { description: null, capabilities: [], tags: [] },
  createdAt: 0,
  active: true,
  enabled: true,
  trust: "trusted",
};

function localDeps(llm: ResolveLocalAgentDeps["llm"]): ResolveLocalAgentDeps {
  const storage = new InMemoryWorkflowStorage();
  return {
    runner: createWorkflowRunner({ storage }),
    memory: new InMemoryMemoryStore(),
    llm,
    tools: {},
  };
}

describe("toEvalOutput", () => {
  it("maps text + usage and builds a trace", async () => {
    const result = await toEvalOutput({
      run: fakeRun({ text: "hello", usage: { inputTokens: 10, outputTokens: 5 } }),
      latencyMs: 42,
    });
    expect(result.output).toBe("hello");
    expect(result.metrics.latencyMs).toBe(42);
    expect(result.metrics.inputTokens).toBe(10);
    expect(result.trace?.summary.turns).toBe(0);
    expect(result.error).toBeUndefined();
  });

  it("prefers structured output over text", async () => {
    const result = await toEvalOutput({
      run: fakeRun({ text: "ignored", output: { ok: true } }),
      latencyMs: 1,
    });
    expect(result.output).toEqual({ ok: true });
  });

  it("computes costUsd when rates are supplied", async () => {
    const result = await toEvalOutput({
      run: fakeRun({ usage: { inputTokens: 1_000_000, outputTokens: 0 } }),
      latencyMs: 1,
      rates: { inputPerMillion: 3 },
    });
    expect(result.metrics.costUsd).toBeCloseTo(3);
  });

  it("surfaces a finished-in-error run", async () => {
    const result = await toEvalOutput({ run: fakeRun({ finishReason: "error" }), latencyMs: 1 });
    expect(result.error).toBeDefined();
  });
});

describe("fnTarget", () => {
  it("runs the function and carries the configured id", async () => {
    const target = fnTarget({
      fn: (input) => `echo:${String(input)}`,
      id: "echo",
      clock: FakeWallClock.create(1000),
    });
    expect(target.id).toBe("echo");
    const result = await target.run({ id: "c", input: "hi" });
    expect(result.output).toBe("echo:hi");
    expect(result.error).toBeUndefined();
  });

  it("catches a throwing function into EvalOutput.error", async () => {
    const target = fnTarget({
      fn: () => {
        throw new Error("boom");
      },
    });
    const result = await target.run({ id: "c", input: 1 });
    expect(result.output).toBeUndefined();
    expect(result.error).toContain("boom");
  });
});

describe("agentTarget", () => {
  it("invokes a fresh agent per case with the runner's signal and maps the run", async () => {
    let resolved = 0;
    const target = agentTarget({
      id: "bot",
      version: "v1",
      agent: async () => {
        resolved += 1;
        return resolveLocalAgent(
          recipe,
          localDeps(() =>
            mockLLM([
              {
                content: "the answer",
                finishReason: "stop",
                usage: { inputTokens: 3, outputTokens: 2 },
              },
            ]),
          ),
        );
      },
    });
    const first = await target.run({ id: "c1", input: "q" });
    const second = await target.run({ id: "c2", input: { nested: true } });
    expect(resolved).toBe(2);
    expect(first.output).toBe("the answer");
    expect(first.metrics.inputTokens).toBe(3);
    expect(first.trace?.summary.turns).toBe(1);
    expect(second.error).toBeUndefined();
  });

  it("catches a failing agent factory into EvalOutput.error", async () => {
    const target = agentTarget({
      id: "bot",
      agent: () => {
        throw new Error("no such agent");
      },
    });
    const result = await target.run({ id: "c", input: "x" });
    expect(result.error).toBe("no such agent");
  });
});

describe("recipeTarget", () => {
  it("takes id + version from the recipe", () => {
    const target = recipeTarget({
      recipe,
      deps: localDeps(() => mockLLM([])),
    });
    expect(target.id).toBe("support-bot");
    expect(target.version).toBe("v3");
  });

  it("catches a resolve failure into EvalOutput.error", async () => {
    const ghost: RegisteredAgent = {
      ...recipe,
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-x" },
        role: { inline: { systemPrompt: null, tools: ["ghostTool"] } },
      },
    };
    const target = recipeTarget({
      recipe: ghost,
      deps: localDeps(() => {
        throw new Error("llm factory should not run");
      }),
    });
    const result = await target.run({ id: "c1", input: "help" });
    expect(result.output).toBeUndefined();
    expect(result.error).toContain("ghostTool");
  });
});
