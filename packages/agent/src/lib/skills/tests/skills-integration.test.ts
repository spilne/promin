// ---------------------------------------------------------------------------
// End-to-end skills integration: registry → resolveSkillCatalog →
// resolveLocalAgent → a real agent run that loads a skill via the
// auto-attached `loadSkill` tool, plus a replay leg proving the loaded body
// is journaled (Option A determinism).
//
// Mirrors the example artifacts under packages/zorya/examples/skills/ — the
// skill manifests are defined inline here because @promin/agent must not
// depend on the zorya examples package.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  runJournaledStep,
  type JournaledContext,
} from "@promin/workflow";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import { resolveSkillCatalog } from "../resolve-skill-catalog.ts";
import { createLoadSkillTool, type LoadSkillOutput } from "../load-skill-tool.ts";
import { InMemorySkillRegistry } from "../in-memory-skill-registry.ts";
import type { RegisteredAgent } from "../../registry/types.ts";
import type { LLMProvider, LLMChatParams, LLMResponse } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";

const DEBUG_BODY = "# Structured debugging\n\n1. Reproduce. 2. Bisect. 3. Hypothesize the cause.";

async function seedRegistry(): Promise<InMemorySkillRegistry> {
  const registry = new InMemorySkillRegistry();
  await registry.register({
    id: "structured-debugging",
    description: "A disciplined debugging loop.",
    whenToUse: "A bug resists a quick fix.",
    body: DEBUG_BODY,
  });
  await registry.register({
    id: "plain-writing",
    description: "A plain-language writing rubric.",
    whenToUse: "Drafting prose for people to read.",
    body: "# Plain writing\n\nLead with the point.",
  });
  return registry;
}

const recipe: RegisteredAgent = {
  id: "skilled-bot",
  version: "v1",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: "You are a helpful engineering assistant.",
    tools: [],
    skills: [{ id: "structured-debugging" }, { id: "plain-writing" }],
  },
  metadata: { description: null, capabilities: ["skills"], tags: [] },
  createdAt: 0,
  updatedAt: 0,
};

describe("skills end-to-end", () => {
  it("injects the catalog, auto-attaches loadSkill, and delivers the body to the model", async () => {
    const registry = await seedRegistry();
    const catalog = await resolveSkillCatalog({ recipe, registry });

    // Capture what the model sees on each step.
    const seenSystemPrompts: string[] = [];
    const seenToolNames: string[][] = [];
    const seenMessages: Message[][] = [];
    let call = 0;
    const llm: LLMProvider = {
      chat: async (params: LLMChatParams): Promise<LLMResponse> => {
        seenSystemPrompts.push(params.messages.find((m) => m.role === "system")?.content ?? "");
        seenToolNames.push((params.tools ?? []).map((t) => t.name));
        seenMessages.push(params.messages);
        call++;
        if (call === 1) {
          // First step: load the debugging skill.
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "loadSkill", input: { id: "structured-debugging" } }],
          };
        }
        // Second step: answer using the loaded instructions.
        return { content: "Let's reproduce the bug first.", finishReason: "stop" };
      },
    };

    const agent = resolveLocalAgent(recipe, {
      runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
      llm: () => llm,
      tools: {},
      skills: registry,
      skillCatalog: catalog,
      namespaceId: "acme",
    });

    const out = await agent.invoke({ task: "There's a flaky test, why?" });
    expect(await out.text).toBe("Let's reproduce the bug first.");

    // (1) Catalog block reached the system prompt — descriptions only, no body.
    const systemPrompt = seenSystemPrompts[0]!;
    expect(systemPrompt).toContain("## Skills");
    expect(systemPrompt).toContain("`structured-debugging`");
    expect(systemPrompt).toContain("`plain-writing`");
    expect(systemPrompt).not.toContain("Reproduce. 2. Bisect"); // body must not leak

    // (2) loadSkill was auto-attached and offered to the model.
    expect(seenToolNames[0]).toContain("loadSkill");

    // (3) The body was delivered as a tool result the model saw on step 2.
    const toolResult = seenMessages[1]!.find((m) => m.role === "tool");
    expect(toolResult?.content).toContain("1. Reproduce. 2. Bisect");

    // (4) And it shows up in the run's tool results.
    const results = await out.toolResults;
    expect(results.some((r) => String(r.content ?? "").includes("Reproduce"))).toBe(true);
  });

  it("loadSkill body survives replay bit-identically (journaled, not re-fetched)", async () => {
    const registry = await seedRegistry();
    const catalog = await resolveSkillCatalog({ recipe, registry });
    const loadSkill = createLoadSkillTool({ registry, catalog });

    const storage = new InMemoryWorkflowStorage();
    let execCount = 0;
    // Mirror how the agent loop runs a tool: inside a journaled activity.
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const loaded = yield* ctx.activity("load-skill", async () => {
        execCount++;
        return (await loadSkill.execute({ id: "structured-debugging" })) as LoadSkillOutput;
      });
      return loaded;
    };

    // First run executes the activity and journals the body.
    const first = (await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-skill-replay",
      stepName: "body",
      storage,
      // biome-ignore lint/suspicious/noExplicitAny: generator body typing in test
      body: body as any,
    })) as LoadSkillOutput;
    expect(first.version).toBe("v1");
    expect(first.body).toBe(DEBUG_BODY);
    expect(execCount).toBe(1);

    // Now remove the pinned version from the registry — a re-fetch would fail.
    await registry.unregister("structured-debugging", first.version);

    // Replay against the same journal: the body comes back bit-identically
    // and loadSkill is NOT re-executed.
    const replayed = (await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-skill-replay",
      stepName: "body",
      storage,
      // biome-ignore lint/suspicious/noExplicitAny: generator body typing in test
      body: body as any,
    })) as LoadSkillOutput;
    expect(replayed).toEqual(first);
    expect(execCount).toBe(1); // journal hit — execute did not fire again
  });
});
