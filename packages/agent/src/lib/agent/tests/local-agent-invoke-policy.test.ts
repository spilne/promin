// ---------------------------------------------------------------------------
// One-shot invoke policies: `onApprovalRequired` decides an approval-gated
// call instead of suspending the turn, and `allowedTools` leaves every other
// tool out of the turn.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { createWorkflowRunner, InMemoryWorkflowStorage } from "@promin/workflow";
import { z } from "zod";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";
import { tool } from "../../tool.ts";
import { LocalAgent } from "../local-agent.ts";

const ran: string[] = [];
const guarded = tool({
  name: "guarded",
  description: "Requires approval before running",
  parameters: z.object({ x: z.string() }),
  execute: async ({ x }) => {
    ran.push(`guarded:${x}`);
    return `ran: ${x}`;
  },
  requireApproval: true,
});
const free = tool({
  name: "free",
  description: "Runs freely",
  parameters: z.object({ x: z.string() }),
  execute: async ({ x }) => {
    ran.push(`free:${x}`);
    return `ran: ${x}`;
  },
});

/** Calls `toolName` once, then answers with the tool result it saw. */
function callThenEcho(toolName: string): LLMProvider {
  return {
    chat: async (params): Promise<LLMResponse> => {
      const last: Message | undefined = params.messages.at(-1);
      if (last?.role === "tool") return { content: `saw: ${last.content}`, finishReason: "stop" };
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "tc-1", name: toolName, input: { x: "h" } }],
      };
    },
  };
}

function agentCalling(toolName: string): LocalAgent {
  return new LocalAgent({
    namespaceId: "acme",
    runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
    agent: { name: "policy", llm: callThenEcho(toolName), tools: { guarded, free } },
  });
}

describe("LocalAgent.invoke — policies", () => {
  it("onApprovalRequired refuses an approval-gated call instead of suspending", async () => {
    ran.length = 0;
    const run = await agentCalling("guarded").invoke(
      { task: "go" },
      { onApprovalRequired: async () => ({ approved: false, reason: "no reviewer" }) },
    );
    expect(await run.finishReason).toBe("stop");
    expect(await run.text).toContain("no reviewer");
    expect(ran).toEqual([]);
  });

  it("allowedTools leaves the other tools out of the turn", async () => {
    ran.length = 0;
    const run = await agentCalling("free").invoke({ task: "go" }, { allowedTools: ["guarded"] });
    expect(await run.finishReason).toBe("stop");
    expect(ran).toEqual([]);
    const allowed = await agentCalling("free").invoke({ task: "go" }, { allowedTools: ["free"] });
    expect(await allowed.text).toContain("ran: h");
    expect(ran).toEqual(["free:h"]);
  });
});
