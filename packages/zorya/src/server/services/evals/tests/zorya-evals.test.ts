// ---------------------------------------------------------------------------
// ZoryaEvals runs are runs of the eval-run workflow it defines at boot, so
// a run whose process died is resumed by orphan recovery in the next one
// (finished cases are not run again), and a run whose workflow ended
// another way (failed, cancelled) is reconciled in the run store.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  resolveLocalAgent,
  tool,
  type LLMProvider,
  type Message,
  type ResolveLocalAgentDeps,
} from "@promin/agent";
import { z } from "zod";
import { InMemoryEvalRunStore, InMemoryEvalSuiteRegistry } from "@promin/evals";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type WorkflowRunner,
} from "@promin/workflow";
import { ZoryaAgents } from "../../agents/zorya-agents.ts";
import { UnknownWorkflowError } from "../../workflows/errors.ts";
import { LocalWorkflows } from "../../workflows/local-workflows.ts";
import { EVAL_RUN_WORKFLOW_NAME } from "../eval-run-workflow.ts";
import { ZoryaEvals } from "../zorya-evals.ts";

/** Past the in-memory lock lease and the orphan grace period. */
const PAST_LEASE_MS = 121_000;

function lastUser(messages: ReadonlyArray<Message>): string {
  const last = messages.findLast((m) => m.role === "user");
  return typeof last?.content === "string" ? last.content : "";
}

async function yieldUntil(condition: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 5_000; i++) {
    if (await condition()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("condition never held");
}

async function fixtures() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const registry = new InMemoryAgentRegistry({ clock });
  await registry.register({
    id: "bot",
    backend: {
      type: "local",
      model: { provider: "test", id: "m" },
      role: { inline: { systemPrompt: null, tools: [] } },
    },
  });
  const suites = new InMemoryEvalSuiteRegistry({ clock });
  const dataset = {
    kind: "inline" as const,
    id: "letters",
    cases: ["a", "b", "c"].map((letter) => ({
      id: letter,
      input: letter,
      expected: letter.toUpperCase(),
    })),
  };
  await suites.register({ id: "upper", dataset, scorers: [{ kind: "exactMatch" }] });
  await suites.register({
    id: "upper-idempotent",
    dataset,
    scorers: [{ kind: "exactMatch" }],
    idempotentCases: true,
  });
  const runs = new InMemoryEvalRunStore();
  return { clock, storage, registry, suites, runs };
}

type Fixtures = Awaited<ReturnType<typeof fixtures>>;

/** One host process over the shared stores. */
function boot(params: { fx: Fixtures; llm: LLMProvider; tools?: ResolveLocalAgentDeps["tools"] }) {
  const { fx } = params;
  const runner: WorkflowRunner = createWorkflowRunner({ storage: fx.storage, clock: fx.clock });
  const workflows = new LocalWorkflows({
    storage: fx.storage,
    runner,
    definitions: {},
    clock: fx.clock,
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const agents = new ZoryaAgents({
    registry: fx.registry,
    resolve: (recipe) =>
      resolveLocalAgent(recipe, { runner, llm: () => params.llm, tools: params.tools ?? {} }),
  });
  const evals = new ZoryaEvals({
    suites: fx.suites,
    runs: fx.runs,
    workflows,
    agents,
    clock: fx.clock,
  });
  return { workflows, evals };
}

const SCOPE = { namespaceId: "acme", resourceId: "evals" };

describe("ZoryaEvals — durable runs", () => {
  /** Run the suite, kill the process while case "b" is in flight, resume in a new one. */
  async function restartMidCase(params: { suiteId: string }) {
    const fx = await fixtures();
    const calls = new Map<string, number>();
    let releaseStalled!: () => void;
    const stalled = new Promise<void>((r) => {
      releaseStalled = r;
    });
    const llm: LLMProvider = {
      chat: async (params) => {
        const task = lastUser(params.messages);
        calls.set(task, (calls.get(task) ?? 0) + 1);
        // The first process dies while case "b" is in flight.
        if (task === "b" && calls.get(task) === 1) await stalled;
        return { content: task.toUpperCase(), finishReason: "stop" };
      },
    };

    const first = boot({ fx, llm });
    const started = await first.evals.startRun({
      suiteId: params.suiteId,
      agentId: "bot",
      scope: SCOPE,
    });
    if ("error" in started) throw new Error(started.message);
    await yieldUntil(() => calls.get("b") === 1);
    const midway = await first.evals.getRun(started.runId);
    expect(midway?.status).toBe("running");
    expect(midway?.caseResults.map((c) => c.caseId)).toEqual(["a"]);

    fx.clock.set(fx.clock.currentTimeMs() + PAST_LEASE_MS);
    const second = boot({ fx, llm });
    expect(await second.workflows.recoverOrphans()).toEqual({ resumed: 1 });
    await yieldUntil(async () => (await fx.runs.get(started.runId))?.status === "completed");
    releaseStalled();

    const done = await second.evals.getRun(started.runId);
    expect(done?.caseResults.map((c) => c.caseId)).toEqual(["a", "b", "c"]);
    expect(done?.targetVersion).toBe("v1");
    expect(done?.suiteVersion).toBe("v1");
    expect((await fx.storage.loadWorkflow(started.runId))?.status).toBe("completed");
    return { done, calls: Object.fromEntries(calls) };
  }

  it("records a case a restart interrupted as failed instead of re-running its side effects", async () => {
    const { done, calls } = await restartMidCase({ suiteId: "upper" });
    // "a" finished before the restart; "b" is not run again.
    expect(calls).toEqual({ a: 1, b: 1, c: 1 });
    expect(done?.caseResults.map((c) => c.passed)).toEqual([true, false, true]);
    expect(done?.caseResults[1]?.samples[0]?.output.error).toContain("interrupted");
    expect(done?.totals?.passRate).toBeCloseTo(2 / 3);
  });

  it("re-runs an interrupted case when the suite declares its cases idempotent", async () => {
    const { done, calls } = await restartMidCase({ suiteId: "upper-idempotent" });
    expect(calls).toEqual({ a: 1, b: 2, c: 1 });
    expect(done?.totals?.passRate).toBe(1);
  });

  it("refuses approval-gated tools and keeps to the suite's tool allowlist", async () => {
    const fx = await fixtures();
    const ran: string[] = [];
    const guarded = tool({
      name: "guarded",
      description: "needs approval",
      parameters: z.object({}),
      requireApproval: true,
      execute: async () => {
        ran.push("guarded");
        return "ran";
      },
    });
    const other = tool({
      name: "other",
      description: "free",
      parameters: z.object({}),
      execute: async () => {
        ran.push("other");
        return "ran";
      },
    });
    await fx.registry.register({
      id: "tooly",
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { inline: { systemPrompt: null, tools: ["guarded", "other"] } },
      },
    });
    await fx.suites.register({
      id: "tools",
      dataset: {
        kind: "inline",
        id: "t",
        cases: [
          { id: "guarded", input: "guarded" },
          { id: "other", input: "other" },
        ],
      },
      scorers: [{ kind: "regexMatch", pattern: "." }],
      allowedTools: ["guarded"],
    });
    // Calls the tool the task names, then answers with what it got back.
    const llm: LLMProvider = {
      chat: async (params) => {
        const last = params.messages.at(-1);
        if (last?.role === "tool") return { content: `got: ${last.content}`, finishReason: "stop" };
        return {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc", name: lastUser(params.messages), input: {} }],
        };
      },
    };
    const host = boot({ fx, llm, tools: { guarded, other } });
    const started = await host.evals.startRun({ suiteId: "tools", agentId: "tooly", scope: SCOPE });
    if ("error" in started) throw new Error(started.message);
    await yieldUntil(async () => (await fx.runs.get(started.runId))?.status === "completed");
    const run = await fx.runs.get(started.runId);
    // Neither ran: the gated call was refused (no one can approve it), the
    // other tool is outside the allowlist.
    expect(ran).toEqual([]);
    expect(String(run?.caseResults[0]?.samples[0]?.output.output)).toContain(
      "not available in eval runs",
    );
  });

  it("runs each case in a resource of its own per run by default", async () => {
    const fx = await fixtures();
    const host = boot({ fx, llm: { chat: async () => ({ content: "A", finishReason: "stop" }) } });
    const started = await host.evals.startRun({
      suiteId: "upper",
      agentId: "bot",
      scope: { namespaceId: "acme" },
    });
    if ("error" in started) throw new Error(started.message);
    const run = await fx.runs.get(started.runId);
    expect(run?.namespaceId).toBe("acme");
    expect(run?.metadata?.resourceId).toBe(`eval-${started.runId}`);
  });

  it("marks a run failed when its workflow fails outside the run (an operator, a stale sweep)", async () => {
    const fx = await fixtures();
    const llm: LLMProvider = {
      chat: () => new Promise(() => {}),
    };
    const host = boot({ fx, llm });
    const started = await host.evals.startRun({ suiteId: "upper", agentId: "bot", scope: SCOPE });
    if ("error" in started) throw new Error(started.message);
    await fx.storage.failWorkflow({ workflowId: started.runId, error: "marked failed" });
    const run = await host.evals.getRun(started.runId);
    expect(run?.status).toBe("failed");
    expect(run?.error).toBe("marked failed");
    expect((await host.evals.listRuns()).map((r) => r.status)).toEqual(["failed"]);
  });

  it("cancels a run: the workflow is cancelled and the run recorded as cancelled", async () => {
    const fx = await fixtures();
    let calls = 0;
    const llm: LLMProvider = {
      chat: () => {
        calls += 1;
        return new Promise(() => {});
      },
    };
    const host = boot({ fx, llm });
    const started = await host.evals.startRun({ suiteId: "upper", agentId: "bot", scope: SCOPE });
    if ("error" in started) throw new Error(started.message);
    await yieldUntil(() => calls === 1);
    expect(await host.evals.cancelRun(started.runId)).toBe("cancelled");
    expect((await host.evals.getRun(started.runId))?.status).toBe("cancelled");
    expect(await host.evals.cancelRun(started.runId)).toBe("ended");
    const status = await fx.storage.loadWorkflowStatus(started.runId);
    expect(status?.status).toBe("failed");
  });

  it("refuses unknown suites and agents, and keeps the workflow internal", async () => {
    const fx = await fixtures();
    const host = boot({ fx, llm: { chat: async () => ({ content: "x", finishReason: "stop" }) } });
    expect(
      await host.evals.startRun({ suiteId: "ghost", agentId: "bot", scope: SCOPE }),
    ).toMatchObject({ error: "suite_not_found" });
    expect(
      await host.evals.startRun({
        suiteId: "upper",
        agentId: "bot",
        agentVersion: "v9",
        scope: SCOPE,
      }),
    ).toMatchObject({ error: "agent_not_found" });
    expect(host.workflows.isInternal(EVAL_RUN_WORKFLOW_NAME)).toBe(true);
    await expect(host.workflows.trigger(EVAL_RUN_WORKFLOW_NAME, {})).rejects.toBeInstanceOf(
      UnknownWorkflowError,
    );
  });
});
