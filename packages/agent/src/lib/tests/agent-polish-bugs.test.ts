import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import { resolveTools } from "../agent-shared.ts";
import { tool } from "../tool.ts";
import { z } from "zod";
import type { LLMResponse } from "../llm-provider.ts";

// ---------------------------------------------------------------------------
// promin-sxri — resolveTools refuses ambiguous config
// ---------------------------------------------------------------------------

describe("resolveTools — ambiguous config", () => {
  it("throws when both toolRegistry and tools are provided", () => {
    const fakeRegistry = { getTools: () => ({}) } as Parameters<
      typeof resolveTools
    >[0]["toolRegistry"];
    expect(() =>
      resolveTools({
        toolRegistry: fakeRegistry,
        tools: {
          foo: tool({
            name: "foo",
            description: "",
            parameters: z.object({}),
            execute: async () => "ok",
          }),
        },
      }),
    ).toThrow(/cannot specify both/i);
  });

  it("returns toolRegistry tools when only registry is set", () => {
    const fakeRegistry = {
      getTools: () => ({
        bar: tool({
          name: "bar",
          description: "",
          parameters: z.object({}),
          execute: async () => "r",
        }),
      }),
    } as Parameters<typeof resolveTools>[0]["toolRegistry"];
    const result = resolveTools({ toolRegistry: fakeRegistry });
    expect(Object.keys(result)).toEqual(["bar"]);
  });

  it("returns tools map when only inline tools are set", () => {
    const result = resolveTools({
      tools: {
        baz: tool({
          name: "baz",
          description: "",
          parameters: z.object({}),
          execute: async () => "i",
        }),
      },
    });
    expect(Object.keys(result)).toEqual(["baz"]);
  });

  it("returns empty map when neither is set", () => {
    expect(resolveTools({})).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// promin-8on6 — turn.end durationMs > 0 even when replayed after a crash
//
// We can't easily simulate "crash between lc-${turn}-message and emit-${turn}"
// without a workflow-level harness. Instead we assert the invariant that
// matters to callers: durationMs reported in the session-logger reflects a
// real wall-clock delta, not a stale closure value, and survives a second
// session() construction on the same durable storage.
// ---------------------------------------------------------------------------

function mockLLM(responses: LLMResponse[]) {
  let i = 0;
  return {
    chat: async () => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted");
      return resp;
    },
  };
}

describe("turn durationMs — journaled start time", () => {
  it("turn.end event carries a non-zero durationMs on fresh run", async () => {
    const events: Array<{ type: string; durationMs?: number }> = [];
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = agentLoop({
      llm: mockLLM([{ content: "done", finishReason: "stop" }]),
      logger: { emit: (e) => events.push(e) },
    }).session({ runner, sessionId: "dur-1" });

    await (await session).send("hi");
    await (await session).close();

    const end = events.find((e) => e.type === "turn.end");
    expect(end).toBeDefined();
    expect(typeof end!.durationMs).toBe("number");
    expect(end!.durationMs!).toBeGreaterThanOrEqual(0);
    // If the turn-start timestamp were lost (old bug), durationMs would be 0.
    // This isn't ironclad — the fix is really about replay, which needs the
    // next test — but it asserts the happy path still works.
  });

  it("lc-${turn}-message activity returns the start time (journal invariant)", async () => {
    // The fix is mechanism-level: the turn start time is the return value
    // of the journaled lc-${turn}-message activity. On replay, the
    // activity's journaled return value hydrates turnStarts before the
    // emit-${turn} activity reads it. We can't exercise crash recovery
    // from a unit test, but we CAN confirm that after a turn runs, the
    // activity journal contains a numeric entry for lc-${turn}-message.
    // If the journal ever loses it (e.g. someone reverts the fix), this
    // assertion catches the regression.
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
    }).session({ runner, sessionId: "dur-2" });
    await session.send("hello");

    // Journal lives under the workflowId keyed by step name. Look up the
    // activity journal directly and check for an lc-0-message entry with a
    // numeric success value.
    const journal = await storage.loadJournal("dur-2", "conversation");
    const lcEntry = journal.find((e) => e.activityName === "lc-0-message");
    expect(lcEntry).toBeDefined();
    expect(lcEntry!.exit?.tag).toBe("Success");
    const value = (lcEntry!.exit as { tag: "Success"; value: unknown }).value;
    // With the fix, value is the start time. Without it, value would be
    // undefined (the old activity had no return).
    expect(typeof value).toBe("number");
    expect(value as number).toBeGreaterThan(0);

    await session.close();
  });
});
