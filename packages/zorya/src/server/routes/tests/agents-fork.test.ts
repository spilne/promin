import { describe, expect, it } from "bun:test";
import { bootAgents, jsonOf, req } from "./agent-gateway-harness.ts";

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
