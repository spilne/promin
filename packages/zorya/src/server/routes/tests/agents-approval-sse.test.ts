// ---------------------------------------------------------------------------
// Approval round trip over SSE: a thread turn that calls an approval-gated
// tool streams `approval-requested` (tool name + input) and `suspended`;
// approving it streams the resumed turn — the tool's result and the answer
// — and ends with `finish`, without repeating the approval request.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { tool, type LLMProvider } from "@promin/agent";
import { bootAgents, jsonOf, localBackend, req } from "./agent-gateway-harness.ts";

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

/** A deployer agent whose model asks to deploy on "ship it" and answers "hi" plainly. */
async function bootDeployer() {
  let llmCalls = 0;
  const deploy = tool({
    name: "deploy",
    description: "deploy a service",
    parameters: z.object({ env: z.string() }),
    requireApproval: true,
    execute: async ({ env }) => `deployed to ${env}`,
  });
  const llm: LLMProvider = {
    chat: async ({ messages }) => {
      llmCalls++;
      const last = messages[messages.length - 1];
      if (last?.role === "tool") return { content: `Done: ${last.content}`, finishReason: "stop" };
      if (last?.role === "user" && last.content === "ship it") {
        return {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "call-1", name: "deploy", input: { env: "prod" } }],
        };
      }
      return { content: "hello", finishReason: "stop" };
    },
  };
  const booted = await bootAgents({ llm, tools: { deploy } });
  await booted.registry.register({
    id: "deployer",
    backend: {
      ...localBackend,
      role: { inline: { systemPrompt: "Deploys things", tools: ["deploy"] } },
    },
  });
  return { ...booted, llmCalls: () => llmCalls };
}

const query = "namespaceId=ns&resourceId=alice";

describe("agent routes — pending-approval index", () => {
  it("lists the pending approval, refuses a new send, and records the approver", async () => {
    const { server, approvals } = await bootDeployer();
    await readSse(
      await server.handle(
        req("POST", "/api/agents/deployer/threads/t1/stream", { task: "ship it", ...scope }),
      ),
    );

    const listed = await server.handle(
      req("GET", `/api/agents/deployer/threads/t1/approvals?${query}`),
    );
    expect(listed.status).toBe(200);
    const body = await jsonOf<{ approvals: Array<Record<string, unknown>> }>(listed);
    expect(body.approvals).toMatchObject([
      { toolCallId: "call-1", toolName: "deploy", input: { env: "prod" }, expiresAt: null },
    ]);

    const blocked = await server.handle(
      req("POST", "/api/agents/deployer/threads/t1", { task: "hi", ...scope }),
    );
    expect(blocked.status).toBe(409);
    expect((await jsonOf<{ error: string }>(blocked)).error).toBe("approval_pending");
    const blockedStream = await server.handle(
      req("POST", "/api/agents/deployer/threads/t1/stream", { task: "hi", ...scope }),
    );
    expect(blockedStream.status).toBe(409);

    await readSse(
      await server.handle(
        req("POST", "/api/agents/deployer/threads/t1/approve", {
          toolCallId: "call-1",
          approved: true,
          ...scope,
        }),
      ),
    );
    expect(
      await approvals.get({ namespaceId: "ns", threadId: "t1", toolCallId: "call-1" }),
    ).toMatchObject({ status: "approved", decidedBy: "alice" });
    const after = await server.handle(
      req("GET", `/api/agents/deployer/threads/t1/approvals?${query}`),
    );
    expect((await jsonOf<{ approvals: unknown[] }>(after)).approvals).toEqual([]);
    const next = await server.handle(
      req("POST", "/api/agents/deployer/threads/t1", { task: "hi", ...scope }),
    );
    expect(next.status).toBe(200);
  });

  it("does not approve another thread's turn when thread ids prefix each other", async () => {
    const { server } = await bootDeployer();
    // `t` exists (a plain turn); `t-1` suspends on call-1.
    await server.handle(req("POST", "/api/agents/deployer/threads/t", { task: "hi", ...scope }));
    await readSse(
      await server.handle(
        req("POST", "/api/agents/deployer/threads/t-1/stream", { task: "ship it", ...scope }),
      ),
    );
    const wrong = await server.handle(
      req("POST", "/api/agents/deployer/threads/t/approve", {
        toolCallId: "call-1",
        approved: true,
        ...scope,
      }),
    );
    expect(wrong.status).toBe(404);
    expect((await jsonOf<{ error: string }>(wrong)).error).toBe("approval_not_found");
  });

  it("a send retried with the same Idempotency-Key runs the turn once", async () => {
    const { server, memory, llmCalls } = await bootDeployer();
    const send = () =>
      server.handle(
        req(
          "POST",
          "/api/agents/deployer/threads/t2",
          { task: "hi", ...scope },
          { "idempotency-key": "client-req-1" },
        ),
      );
    const first = await send();
    const retry = await send();
    expect(first.status).toBe(200);
    expect(retry.status).toBe(200);
    expect((await jsonOf<{ text: string }>(retry)).text).toBe("hello");
    expect(llmCalls()).toBe(1);
    const stored = await memory.getMessages({ namespaceId: "ns", threadId: "t2" });
    expect(stored.map((m) => m.role)).toEqual(["user", "assistant"]);
  });
});

describe("agent routes — turn edge cases", () => {
  it("a non-stream send whose turn suspends answers 202 with the pending approval", async () => {
    const { server } = await bootDeployer();
    const res = await server.handle(
      req("POST", "/api/agents/deployer/threads/t3", { task: "ship it", ...scope }),
    );
    expect(res.status).toBe(202);
    expect(await jsonOf<Record<string, unknown>>(res)).toMatchObject({
      threadId: "t3",
      suspended: true,
      approvals: [{ toolCallId: "call-1", toolName: "deploy", status: "pending" }],
    });
  });

  it("the same Idempotency-Key with a different message answers 422", async () => {
    const { server } = await bootDeployer();
    const send = (task: string) =>
      server.handle(
        req(
          "POST",
          "/api/agents/deployer/threads/t4",
          { task, ...scope },
          { "idempotency-key": "client-req-2" },
        ),
      );
    expect((await send("hi")).status).toBe(200);
    const mismatch = await send("hi again");
    expect(mismatch.status).toBe(422);
    expect((await jsonOf<{ error: string }>(mismatch)).error).toBe("idempotency_key_mismatch");
  });

  it("refuses to deliver an agent turn's approval signal directly", async () => {
    const { server, approvals } = await bootDeployer();
    await readSse(
      await server.handle(
        req("POST", "/api/agents/deployer/threads/t5/stream", { task: "ship it", ...scope }),
      ),
    );
    const row = await approvals.get({ namespaceId: "ns", threadId: "t5", toolCallId: "call-1" });
    const signals = await jsonOf<{ signals: Array<Record<string, unknown>> }>(
      await server.handle(req("GET", "/api/signals")),
    );
    expect(signals.signals.find((s) => s.workflowId === row!.workflowId)).toMatchObject({
      agentThread: { namespaceId: "ns", threadId: "t5", agentId: "deployer" },
    });

    const direct = await server.handle(
      req("POST", `/api/runs/${row!.workflowId}/signal`, {
        signalName: "approve:call-1",
        payload: { approved: true },
      }),
    );
    expect(direct.status).toBe(409);
    expect((await jsonOf<{ error: string }>(direct)).error).toBe("agent_approval");
    const token = await server.handle(
      req("POST", `/api/runs/${row!.workflowId}/signals/approve:call-1/token`, {
        expiresInMs: 60_000,
      }),
    );
    expect(token.status).toBe(409);
    expect((await jsonOf<{ error: string }>(token)).error).toBe("agent_approval");
    expect(
      (await approvals.get({ namespaceId: "ns", threadId: "t5", toolCallId: "call-1" }))?.status,
    ).toBe("pending");
  });
});
