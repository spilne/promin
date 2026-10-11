// ---------------------------------------------------------------------------
// Approval round trip over SSE: a thread turn that calls an approval-gated
// tool streams `approval-requested` (tool name + input) and `suspended`;
// approving it streams the resumed turn — the tool's result and the answer
// — and ends with `finish`, without repeating the approval request.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { tool, type LLMProvider } from "@promin/agent";
import { bootAgents, localBackend, req } from "./agent-gateway-harness.ts";

interface SseFrame {
  readonly event: string | null;
  readonly data: unknown;
}

async function readSse(res: Response): Promise<SseFrame[]> {
  const text = await res.text();
  return text
    .split("\n\n")
    .filter((chunk) => chunk.trim() !== "")
    .map((chunk) => {
      let event: string | null = null;
      let data = "";
      for (const line of chunk.split("\n")) {
        if (line.startsWith("event: ")) event = line.slice(7);
        else if (line.startsWith("data: ")) data += line.slice(6);
      }
      return { event, data: JSON.parse(data) as unknown };
    });
}

const scope = { namespaceId: "ns", resourceId: "alice" };

describe("agent SSE — approval resume", () => {
  it("streams the resumed turn to finish without re-sending the approval request", async () => {
    let deployed = 0;
    const deploy = tool({
      name: "deploy",
      description: "deploy a service",
      parameters: z.object({ env: z.string() }),
      requireApproval: true,
      execute: async ({ env }) => {
        deployed++;
        return `deployed to ${env}`;
      },
    });
    const llm: LLMProvider = {
      chat: async ({ messages }) => {
        const last = messages[messages.length - 1];
        return last?.role === "tool"
          ? { content: `Done: ${last.content}`, finishReason: "stop" }
          : {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [{ id: "call-1", name: "deploy", input: { env: "prod" } }],
            };
      },
    };
    const { server, registry } = await bootAgents({ llm, tools: { deploy } });
    await registry.register({
      id: "deployer",
      backend: {
        ...localBackend,
        role: { inline: { systemPrompt: "Deploys things", tools: ["deploy"] } },
      },
    });

    const first = await readSse(
      await server.handle(
        req("POST", "/api/agents/deployer/threads/t1/stream", { task: "ship it", ...scope }),
      ),
    );
    const firstEvents = first.map((f) => f.event);
    expect(first.find((f) => f.event === "approval-requested")?.data).toEqual({
      toolCallId: "call-1",
      toolName: "deploy",
      input: { env: "prod" },
    });
    expect(firstEvents).toContain("suspended");
    expect(firstEvents).not.toContain("finish");
    expect(deployed).toBe(0);

    const resumed = await readSse(
      await server.handle(
        req("POST", "/api/agents/deployer/threads/t1/approve", {
          toolCallId: "call-1",
          approved: true,
          ...scope,
        }),
      ),
    );
    const resumedEvents = resumed.map((f) => f.event);
    expect(resumedEvents).not.toContain("approval-requested");
    expect(resumedEvents).not.toContain("suspended");
    expect(resumedEvents[resumedEvents.length - 1]).toBe("finish");
    expect(resumed.find((f) => f.event === "finish")?.data).toMatchObject({
      text: "Done: deployed to prod",
      finishReason: "stop",
    });
    expect(deployed).toBe(1);
  });
});
