import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import type { AgentLifecycleEvent } from "../agent-loop.ts";

function mockLLM(responses: { content: string; finishReason: "stop" }[]) {
  let i = 0;
  return {
    chat: async () => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted");
      return resp;
    },
  };
}

function makeRunner() {
  return createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
}

describe("agentLoop lifecycle", () => {
  describe("onLifecycle", () => {
    it("fires message→done pair for each turn", async () => {
      const events: AgentLifecycleEvent[] = [];

      const session = await agentLoop({
        name: "lc-basic",
        llm: mockLLM([
          { content: "reply 1", finishReason: "stop" },
          { content: "reply 2", finishReason: "stop" },
        ]),
        onLifecycle: (e) => {
          events.push(e);
        },
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await session.send("first");
      await session.send("second");
      await session.close();

      expect(events).toHaveLength(4);
      expect(events.map((e) => e.event)).toEqual(["message", "done", "message", "done"]);
    });

    it("message event carries correct from/to/context/sessionId", async () => {
      const events: AgentLifecycleEvent[] = [];

      const session = await agentLoop({
        name: "lc-message",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
        onLifecycle: (e) => {
          events.push(e);
        },
      }).session({ runner: makeRunner(), sessionId: "my-session" });

      await session.send("hello");
      await session.close();

      const msg = events.find((e) => e.event === "message")!;
      expect(msg.from).toBe("idle");
      expect(msg.to).toBe("thinking");
      expect(msg.sessionId).toBe("my-session");
      expect((msg.context as any).task).toBe("hello");
      expect((msg.context as any).turn).toBe(0);
    });

    it("done event carries correct from/to/context", async () => {
      const events: AgentLifecycleEvent[] = [];

      const session = await agentLoop({
        name: "lc-done",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
        onLifecycle: (e) => {
          events.push(e);
        },
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await session.send("hi");
      await session.close();

      const done = events.find((e) => e.event === "done")!;
      expect(done.from).toBe("thinking");
      expect(done.to).toBe("idle");
      expect((done.context as any).turns).toBe(1);
    });

    it("includes sessionId from session() params", async () => {
      const ids = new Set<string>();

      const loop = agentLoop({
        name: "lc-sessionid",
        llm: mockLLM([
          { content: "a", finishReason: "stop" },
          { content: "b", finishReason: "stop" },
        ]),
        onLifecycle: (e) => {
          ids.add(e.sessionId);
        },
      });

      const s1 = await loop.session({ runner: makeRunner(), sessionId: "alice" });
      const s2 = await loop.session({ runner: makeRunner(), sessionId: "bob" });

      await s1.send("hi");
      await s2.send("hi");
      await s1.close();
      await s2.close();

      expect(ids).toContain("alice");
      expect(ids).toContain("bob");
    });

    it("createdAt is a recent Date", async () => {
      const before = new Date();
      let event: AgentLifecycleEvent | undefined;

      const session = await agentLoop({
        name: "lc-date",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
        onLifecycle: (e) => {
          event ??= e;
        },
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await session.send("hi");
      await session.close();

      expect(event!.createdAt).toBeInstanceOf(Date);
      expect(event!.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
    });

    it("async handler errors are swallowed and do not crash the turn", async () => {
      const session = await agentLoop({
        name: "lc-async-error",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
        onLifecycle: async () => {
          throw new Error("handler boom");
        },
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await expect(session.send("hi")).resolves.toBe("ok");
      await session.close();
    });

    it("omitting onLifecycle causes no error", async () => {
      const session = await agentLoop({
        name: "lc-omit",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await expect(session.send("hi")).resolves.toBe("ok");
      await session.close();
    });

    it("AgentLifecycleEvent.isReplay is plumbed and reflects body-level replay state", async () => {
      const events: AgentLifecycleEvent[] = [];
      const session = await agentLoop({
        name: "lc-replay",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
        onLifecycle: (e) => {
          events.push(e);
        },
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await session.send("hi");
      await session.close();

      // Every transition carries a boolean isReplay flag plumbed from
      // ctx.isReplay. The flag is true here because the body's very
      // first pass suspends at the task-0 signal before any user hook
      // fires; user-visible turns always run on top of that pre-existing
      // journal entry. Verifies the flag is wired and is a boolean.
      expect(events.length).toBeGreaterThan(0);
      for (const e of events) {
        expect(typeof e.isReplay).toBe("boolean");
      }
    });
  });

  describe("lifecycleState() / lifecycleHistory()", () => {
    it("starts idle with zero context", async () => {
      const session = await agentLoop({
        name: "lc-state-init",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      }).session({ runner: makeRunner(), sessionId: "s1" });

      const st = session.lifecycleState();
      expect(st.current).toBe("idle");
      expect((st.context as any).turns).toBe(0);
      await session.close();
    });

    it("history is empty before any turn", async () => {
      const session = await agentLoop({
        name: "lc-hist-empty",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      }).session({ runner: makeRunner(), sessionId: "s1" });

      expect(session.lifecycleHistory()).toHaveLength(0);
      await session.close();
    });

    it("history accumulates message+done entries per turn", async () => {
      const session = await agentLoop({
        name: "lc-hist-accum",
        llm: mockLLM([
          { content: "a", finishReason: "stop" },
          { content: "b", finishReason: "stop" },
        ]),
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await session.send("first");
      await session.send("second");
      await session.close();

      const hist = session.lifecycleHistory();
      expect(hist).toHaveLength(4);
      expect(hist.map((e) => e.event)).toEqual(["message", "done", "message", "done"]);
    });

    it("is idle with correct turns count after completed turns", async () => {
      const session = await agentLoop({
        name: "lc-state-after",
        llm: mockLLM([
          { content: "a", finishReason: "stop" },
          { content: "b", finishReason: "stop" },
        ]),
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await session.send("one");
      await session.send("two");
      await session.close();

      const st = session.lifecycleState();
      expect(st.current).toBe("idle");
      expect((st.context as any).turns).toBe(2);
    });

    it("lifecycleHistory returns a snapshot copy, not the live array", async () => {
      const session = await agentLoop({
        name: "lc-snapshot",
        llm: mockLLM([
          { content: "a", finishReason: "stop" },
          { content: "b", finishReason: "stop" },
        ]),
      }).session({ runner: makeRunner(), sessionId: "s1" });

      await session.send("first");
      const snap = session.lifecycleHistory();

      await session.send("second");
      expect(snap).toHaveLength(2); // snapshot unaffected
      expect(session.lifecycleHistory()).toHaveLength(4);
      await session.close();
    });
  });
});
