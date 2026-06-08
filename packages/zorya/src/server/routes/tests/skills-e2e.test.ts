// ---------------------------------------------------------------------------
// Skills end-to-end — drives the whole feature through a real ZoryaServer:
// create a skill → list it in the catalog → attach it to an agent recipe →
// read it back → resolve + RUN the agent (it loads the skill via loadSkill,
// the body reaches the model) → delete the skill → catalog empties and a
// resolve still works (onMissing: 'skip').
//
// No browser: this is HTTP + the agent runtime, the same wiring the demo
// host uses (resolveSkillCatalog → resolveLocalAgent with skills +
// skillCatalog). Covers promin-q5px.8 (API), .9 (resolve wiring), and the
// runtime half of the feature.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemorySkillRegistry,
  inlineRoleDefinition,
  resolveLocalAgent,
  resolveSkillCatalog,
  type Agent,
  type LLMProvider,
  type Message,
  type RegisteredAgent,
} from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents, ZoryaSkills } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

const SKILL_BODY = "# Structured debugging\n\n1. Reproduce. 2. Bisect. 3. Hypothesize.";

async function boot() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });

  const skillRegistry = new InMemorySkillRegistry();
  const agentRegistry = new InMemoryAgentRegistry();

  // Capture what the model sees so we can assert the catalog reached the
  // system prompt and the loaded body reached the conversation.
  const seen = { systemPrompt: "", secondTurn: [] as Message[] };
  let turn = 0;
  const llm: LLMProvider = {
    chat: async (params) => {
      const t = turn++;
      if (t === 0) {
        seen.systemPrompt = params.messages.find((m) => m.role === "system")?.content ?? "";
        // Only call loadSkill when the tool is actually offered (i.e. the
        // recipe still has a resolvable skill). After deletion it's absent,
        // so just answer — proving graceful degradation.
        if ((params.tools ?? []).some((tool) => tool.name === "loadSkill")) {
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "loadSkill", input: { id: "structured-debugging" } }],
          };
        }
      }
      if (t === 1) seen.secondTurn = params.messages;
      return { content: "Reproduce the failure first.", finishReason: "stop" };
    },
  };

  // Mirror the demo host's resolve wiring: resolve the bound role first,
  // then hand its skills to the catalog resolver, then skills +
  // skillCatalog to resolveLocalAgent.
  const resolve = async (recipe: RegisteredAgent): Promise<Agent> => {
    const roleSkills =
      recipe.backend.type === "local"
        ? (inlineRoleDefinition(recipe.backend.role)?.skills ?? [])
        : [];
    const skillCatalog = await resolveSkillCatalog({
      recipe,
      registry: skillRegistry,
      skills: roleSkills,
      onMissing: "skip",
    });
    return resolveLocalAgent(recipe, {
      runner,
      llm: () => llm,
      tools: {},
      skills: skillRegistry,
      skillCatalog,
    });
  };

  const agents = new ZoryaAgents({ registry: agentRegistry, resolve });
  const skills = new ZoryaSkills({ registry: skillRegistry });
  const server = new ZoryaServer({ workflows, agents, skills });
  await server.handle(
    new Request("http://t/api/namespaces", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: "acme", displayName: "Acme" }),
    }),
  );
  return { server, seen };
}

async function jsonOf<T>(res: Response): Promise<T> {
  return (await res.json()) as T;
}

describe("skills e2e — create → catalog → attach → resolve → run → delete", () => {
  it("drives the full flow through the server", async () => {
    const { server, seen } = await boot();

    // 1. Create a custom skill.
    const created = await server.handle(
      new Request("http://t/api/skills", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: "structured-debugging",
          description: "A disciplined debugging loop.",
          whenToUse: "A bug resists a quick fix.",
          body: SKILL_BODY,
        }),
      }),
    );
    expect(created.status).toBe(201);

    // 2. It shows up in the agent editor's catalog.
    const catalog = await jsonOf<{ skills: Array<{ id: string }> }>(
      await server.handle(new Request("http://t/api/agents/_catalog/skills")),
    );
    expect(catalog.skills.map((s) => s.id)).toContain("structured-debugging");

    // 3. Attach it to an agent recipe.
    const agentRes = await server.handle(
      new Request("http://t/api/agents", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: "skilled-bot",
          backend: {
            type: "local",
            model: { provider: "anthropic", id: "claude-sonnet-4-6" },
            role: {
              inline: {
                systemPrompt: "You are a helpful engineering assistant.",
                tools: [],
                skills: [{ id: "structured-debugging" }],
              },
            },
          },
          metadata: { capabilities: ["skills"] },
        }),
      }),
    );
    expect(agentRes.status).toBe(201);

    // 4. Read it back — the recipe pins the skill.
    const recipe = await jsonOf<RegisteredAgent>(
      await server.handle(new Request("http://t/api/agents/skilled-bot")),
    );
    expect(recipe.backend.type).toBe("local");
    if (recipe.backend.type === "local") {
      expect(inlineRoleDefinition(recipe.backend.role)?.skills).toEqual([
        { id: "structured-debugging" },
      ]);
    }

    // 5. Resolve + RUN: the agent loads the skill, the body reaches the model.
    const invoke = await server.handle(
      new Request("http://t/api/agents/skilled-bot/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task: "A test is flaky — why?",
          namespaceId: "acme",
          resourceId: "alice",
        }),
      }),
    );
    expect(invoke.status).toBe(200);
    const out = await jsonOf<{ text: string }>(invoke);
    expect(out.text).toBe("Reproduce the failure first.");

    // Catalog block reached the system prompt (descriptions only, no body).
    expect(seen.systemPrompt).toContain("## Skills");
    expect(seen.systemPrompt).toContain("structured-debugging");
    expect(seen.systemPrompt).not.toContain("1. Reproduce. 2. Bisect");
    // The loaded body arrived as a tool result the model saw on the 2nd turn.
    const toolMsg = seen.secondTurn.find((m) => m.role === "tool");
    expect(toolMsg?.content).toContain("1. Reproduce. 2. Bisect");

    // 6. Delete the skill — catalog empties.
    const del = await server.handle(
      new Request("http://t/api/skills/structured-debugging", { method: "DELETE" }),
    );
    expect(del.status).toBe(204);
    const after = await jsonOf<{ skills: unknown[] }>(
      await server.handle(new Request("http://t/api/agents/_catalog/skills")),
    );
    expect(after.skills).toHaveLength(0);

    // 7. The agent still pins the (now-deleted) skill, but resolve with
    //    onMissing:'skip' degrades gracefully — invoke still succeeds.
    const invoke2 = await server.handle(
      new Request("http://t/api/agents/skilled-bot/invoke", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "still working?", namespaceId: "acme", resourceId: "alice" }),
      }),
    );
    expect(invoke2.status).toBe(200);
  });
});
