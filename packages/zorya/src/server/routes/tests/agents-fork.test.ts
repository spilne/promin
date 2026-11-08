import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { tool, type LLMProvider } from "@promin/agent";
import { bootAgents, jsonOf, localBackend, req } from "./agent-gateway-harness.ts";

const scope = { namespaceId: "ns", resourceId: "alice" };

describe("agent thread forks", () => {
  it("branches at a retained sequence, isolates later turns, and exposes lineage", async () => {
    const h = await bootAgents({ reply: () => "answer" });
    for (const task of ["first", "second"]) {
      expect(
        (
          await h.server.handle(
            req("POST", "/api/agents/support/threads/parent", { ...scope, task }),
          )
        ).status,
      ).toBe(200);
    }
    const path = "/api/agents/support/threads/parent/fork";
    const fork = await h.server.handle(
      req("POST", path, { ...scope, newThreadId: "child", atSeq: 2 }),
    );
    expect(fork.status).toBe(201);
    const childMessages = await h.memory.getMessages({ namespaceId: "ns", threadId: "child" });
    expect(childMessages.map((m) => m.content)).toEqual(["first", "answer"]);
    const lineage = await h.server.handle(
      req("GET", "/api/agents/support/threads/child/lineage?namespaceId=ns&resourceId=alice"),
    );
    expect(await jsonOf<{ parentThreadId: string; branchAtSeq: number }>(lineage)).toMatchObject({
      parentThreadId: "parent",
      branchAtSeq: 2,
    });
    expect(
      (
        await h.server.handle(
          req("POST", "/api/agents/support/threads/child", { ...scope, task: "alternative" }),
        )
      ).status,
    ).toBe(200);
    expect(
      (await h.memory.getMessages({ namespaceId: "ns", threadId: "parent" })).map((m) => m.content),
    ).toEqual(["first", "answer", "second", "answer"]);
    expect(
      (await h.memory.getMessages({ namespaceId: "ns", threadId: "child" })).map((m) => m.content),
    ).toEqual(["first", "answer", "alternative", "answer"]);
    expect(
      (
        await h.server.handle(
          req("GET", "/api/agents/support/threads/child/trace?namespaceId=ns&resourceId=alice"),
        )
      ).status,
    ).toBe(200);
    expect(
      (
        await h.server.handle(
          req("POST", path, { ...scope, newThreadId: "foreign", atSeq: 2, resourceId: "bob" }),
        )
      ).status,
    ).toBe(404);
  });

  it("copies current scratchpad at the head but clears it at an earlier fork", async () => {
    const h = await bootAgents();
    await h.server.handle(
      req("POST", "/api/agents/support/threads/source", { ...scope, task: "hello" }),
    );
    await h.memory.setThreadWorking({ namespaceId: "ns", threadId: "source" }, "scratch");
    await h.memory.appendThreadFact(
      { namespaceId: "ns", threadId: "source" },
      { text: "remember this", source: "user" },
    );
    await h.memory.appendThreadEpisode(
      { namespaceId: "ns", threadId: "source" },
      {
        summary: "first turn",
        sourceThreadId: "source",
        sourceMessageRange: { fromSeq: 1, toSeq: 2 },
      },
    );
    expect(
      (
        await h.server.handle(
          req("POST", "/api/agents/support/threads/source/fork", { ...scope, newThreadId: "head" }),
        )
      ).status,
    ).toBe(201);
    expect((await h.memory.getThread({ namespaceId: "ns", threadId: "head" }))?.workingMemory).toBe(
      "scratch",
    );
    expect(await h.memory.listThreadFacts({ namespaceId: "ns", threadId: "head" })).toHaveLength(1);
    expect(
      (await h.memory.listThreadEpisodes({ namespaceId: "ns", threadId: "head" }))[0]
        ?.sourceThreadId,
    ).toBe("head");
    expect(
      (
        await h.server.handle(
          req("POST", "/api/agents/support/threads/source/fork", {
            ...scope,
            newThreadId: "early",
            atSeq: 1,
          }),
        )
      ).status,
    ).toBe(201);
    expect(
      (await h.memory.getThread({ namespaceId: "ns", threadId: "early" }))?.workingMemory,
    ).toBeNull();
    expect(await h.memory.listThreadFacts({ namespaceId: "ns", threadId: "early" })).toHaveLength(
      0,
    );
    expect(
      await h.memory.listThreadEpisodes({ namespaceId: "ns", threadId: "early" }),
    ).toHaveLength(0);
  });
});

describe("agent thread forks — refusals", () => {
  const forkPath = (id: string) => `/api/agents/support/threads/${id}/fork`;
  const errorOf = async (res: Response) => (await jsonOf<{ error: string }>(res)).error;

  async function bootWithParent() {
    const h = await bootAgents({ reply: () => "answer" });
    await h.server.handle(
      req("POST", "/api/agents/support/threads/parent", { ...scope, task: "hi" }),
    );
    // Seqs: 1 user, 2 assistant, 3 assistant tool call, 4 tool result, 5 assistant.
    await h.memory.appendMessages({ namespaceId: "ns", resourceId: "alice", threadId: "parent" }, [
      { role: "assistant", content: null, toolCalls: [{ id: "c1", name: "t", input: {} }] },
      { role: "tool", toolCallId: "c1", content: "r" },
      { role: "assistant", content: "done" },
    ]);
    return h;
  }

  it("answers 409 for a taken id, 400 for a bad atSeq or a cut inside a tool exchange", async () => {
    const h = await bootWithParent();
    await h.server.handle(
      req("POST", "/api/agents/support/threads/taken", { ...scope, task: "x" }),
    );
    const fork = (newThreadId: string, atSeq?: number) =>
      h.server.handle(req("POST", forkPath("parent"), { ...scope, newThreadId, atSeq }));

    const taken = await fork("taken");
    expect([taken.status, await errorOf(taken)]).toEqual([409, "thread_exists"]);
    const beyond = await fork("beyond", 99);
    expect([beyond.status, await errorOf(beyond)]).toEqual([400, "invalid_branch_seq"]);
    const cut = await fork("cut", 3);
    expect([cut.status, await errorOf(cut)]).toEqual([400, "invalid_branch_seq"]);
    expect((await fork("after", 4)).status).toBe(201);
    for (const id of ["beyond", "cut"]) {
      expect(await h.memory.getThread({ namespaceId: "ns", threadId: id })).toBeNull();
    }
  });

  it("removes the child when the copy fails", async () => {
    const h = await bootWithParent();
    const original = h.memory.appendMessages.bind(h.memory);
    h.memory.appendMessages = (async (key: Parameters<typeof original>[0], ...rest: unknown[]) => {
      if (key.threadId === "doomed") throw new Error("disk full");
      return (original as (...a: unknown[]) => unknown)(key, ...rest);
    }) as typeof original;
    const res = await h.server.handle(
      req("POST", forkPath("parent"), { ...scope, newThreadId: "doomed" }),
    );
    expect(res.status).toBe(500);
    expect(await h.memory.getThread({ namespaceId: "ns", threadId: "doomed" })).toBeNull();
  });

  it("answers 409 while the parent waits for an approval", async () => {
    const deploy = tool({
      name: "deploy",
      description: "deploy",
      parameters: z.object({ env: z.string() }),
      requireApproval: true,
      execute: async ({ env }) => `deployed to ${env}`,
    });
    const llm: LLMProvider = {
      chat: async () => ({
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "call-1", name: "deploy", input: { env: "prod" } }],
      }),
    };
    const h = await bootAgents({ llm, tools: { deploy } });
    await h.registry.register({
      id: "deployer",
      backend: {
        ...localBackend,
        role: { inline: { systemPrompt: "Deploys", tools: ["deploy"] } },
      },
    });
    await (
      await h.server.handle(
        req("POST", "/api/agents/deployer/threads/t1/stream", { ...scope, task: "ship it" }),
      )
    ).text();
    const res = await h.server.handle(
      req("POST", "/api/agents/deployer/threads/t1/fork", { ...scope, newThreadId: "child" }),
    );
    expect([res.status, await errorOf(res)]).toEqual([409, "approval_pending"]);
    expect(await h.memory.getThread({ namespaceId: "ns", threadId: "child" })).toBeNull();
  });
});
