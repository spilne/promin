import { describe, expect, it } from "bun:test";
import { CursorAgent } from "../cursor-agent.ts";
import { resolveCursorAgent } from "../resolve-cursor-agent.ts";
import type { CursorChild, CursorTransport } from "../session.ts";
import type { RegisteredAgent } from "../../registry/types.ts";

function transportYielding(stdout: ReadonlyArray<string>, exitCode = 0): CursorTransport {
  return {
    spawn() {
      const enc = new TextEncoder();
      async function* outIter() {
        for (const line of stdout) yield enc.encode(`${line}\n`);
      }
      async function* empty() {}
      const child: CursorChild = {
        stdout: outIter(),
        stderr: empty(),
        exited: Promise.resolve(exitCode),
        kill: () => {},
      };
      return child;
    },
  };
}

describe("CursorAgent — invoke / stream", () => {
  it("invoke drains the run and resolves text from the result frame", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"s"}',
      '{"type":"assistant","session_id":"s","timestamp_ms":1,"message":{"role":"assistant","content":[{"type":"text","text":"Hello"}]}}',
      '{"type":"result","subtype":"success","is_error":false,"result":"Hello!","session_id":"s"}',
    ]);
    const agent = new CursorAgent({ transport });
    const out = await agent.invoke({ task: "say hi" });
    expect(await out.text).toBe("Hello!");
    expect(await out.finishReason).toBe("stop");
  });

  it("stream yields text deltas (recovered from cumulative frames) in order on textStream", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"s"}',
      '{"type":"assistant","session_id":"s","timestamp_ms":1,"message":{"role":"assistant","content":[{"type":"text","text":"Hel"}]}}',
      '{"type":"assistant","session_id":"s","timestamp_ms":2,"message":{"role":"assistant","content":[{"type":"text","text":"Hello!"}]}}',
      '{"type":"result","subtype":"success","is_error":false,"result":"Hello!","session_id":"s"}',
    ]);
    const agent = new CursorAgent({ transport });
    const out = agent.stream({ task: "x" });
    const seen: string[] = [];
    for await (const delta of out.textStream) seen.push(delta);
    expect(seen).toEqual(["Hel", "lo!"]);
    expect(await out.text).toBe("Hello!");
  });

  it("fullStream surfaces tool-call and tool-result events", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"s"}',
      '{"type":"tool_call","subtype":"started","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"a.txt"}}},"session_id":"s"}',
      '{"type":"tool_call","subtype":"completed","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"a.txt"},"result":"contents"}},"session_id":"s"}',
      '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}',
    ]);
    const agent = new CursorAgent({ transport });
    const out = agent.stream({ task: "x" });
    const types: string[] = [];
    for await (const ev of out.fullStream) types.push(ev.type);
    expect(types).toContain("tool-call");
    expect(types).toContain("tool-result");
    const calls = await out.toolCalls;
    expect(calls).toHaveLength(1);
    expect(calls[0]!.id).toBe("c1");
    expect(calls[0]!.name).toBe("read");
  });

  it("finishReason is 'error' when the result is_error flag is true", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"s"}',
      '{"type":"result","subtype":"error","is_error":true,"result":"oops","session_id":"s"}',
    ]);
    const agent = new CursorAgent({ transport });
    const out = await agent.invoke({ task: "x" });
    expect(await out.finishReason).toBe("error");
  });

  it("messages promise carries the assistant text from the result frame", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"my-session"}',
      '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"my-session"}',
    ]);
    const agent = new CursorAgent({ transport });
    const out = await agent.invoke({ task: "x" });
    const msgs = await out.messages;
    expect(msgs).toHaveLength(1);
    expect((msgs[0] as { role: string; content: unknown }).role).toBe("assistant");
    expect((msgs[0] as { content: string }).content).toBe("ok");
  });

  it("withScope returns a new CursorAgent that retains the original config", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"s"}',
      '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s"}',
    ]);
    const a = new CursorAgent({ transport, model: "auto" });
    const scoped = a.withScope({ namespaceId: "acme" });
    expect(scoped).not.toBe(a);
    const out = await scoped.invoke({ task: "x" });
    expect(await out.text).toBe("ok");
  });

  it("compactThread / distillThread throw — Cursor doesn't expose these surfaces", async () => {
    const agent = new CursorAgent({ transport: transportYielding([]) });
    await expect(agent.compactThread("t1")).rejects.toThrow(/not supported/);
    await expect(agent.distillThread("t1")).rejects.toThrow(/not supported/);
  });
});

describe("CursorAgentThread", () => {
  it("send returns the resolved result for a single turn", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"s1"}',
      '{"type":"result","subtype":"success","is_error":false,"result":"first","session_id":"s1"}',
    ]);
    const agent = new CursorAgent({ transport });
    const thread = await agent.thread("t1");
    const out = await thread.send({ task: "first" });
    expect(await out.text).toBe("first");
  });

  it("send() on a second turn passes --resume with the Cursor session id from the first turn", async () => {
    // Drives the multi-turn contract: we capture session_id from turn 1
    // (any frame), then turn 2's spawn args must include `--resume <id>`.
    const seenArgs: string[][] = [];
    const transport = {
      spawn(args: ReadonlyArray<string>) {
        seenArgs.push([...args]);
        const enc = new TextEncoder();
        // Emit a session_id on each call so the thread can capture it.
        const turn = seenArgs.length;
        async function* outIter() {
          yield enc.encode(`{"type":"system","subtype":"init","session_id":"sess-${turn}"}\n`);
          yield enc.encode(
            `{"type":"result","subtype":"success","is_error":false,"result":"turn ${turn}","session_id":"sess-${turn}"}\n`,
          );
        }
        async function* empty() {}
        return {
          stdout: outIter(),
          stderr: empty(),
          exited: Promise.resolve(0),
          kill: () => {},
        };
      },
    };
    const agent = new CursorAgent({ transport });
    const thread = await agent.thread("t1");
    const out1 = await thread.send({ task: "first" });
    expect(await out1.text).toBe("turn 1");
    const out2 = await thread.send({ task: "second" });
    expect(await out2.text).toBe("turn 2");
    expect(seenArgs[0]).not.toContain("--resume");
    expect(seenArgs[1]).toContain("--resume");
    expect(seenArgs[1]).toContain("sess-1");
  });

  it("workingMemory + metadata + title APIs no-op so callers don't have to special-case the backend", async () => {
    const agent = new CursorAgent({ transport: transportYielding([]) });
    const t = await agent.thread("t1");
    expect(await t.workingMemory()).toBeNull();
    expect(await t.metadata()).toEqual({});
    expect(await t.title()).toBeNull();
    // Calls don't throw.
    await t.setWorkingMemory("hi");
    await t.setMetadata({ x: 1 });
    await t.setTitle("name");
    await t.delete();
  });
});

describe("resolveCursorAgent", () => {
  function recipe(): RegisteredAgent {
    return {
      id: "code-bot",
      version: "v1",
      backend: { type: "cursor", model: "auto" },
      metadata: { description: null, capabilities: [], tags: [] },
      createdAt: 0,
      updatedAt: 0,
    };
  }

  it("rejects a recipe whose backend isn't 'cursor'", () => {
    const r: RegisteredAgent = {
      ...recipe(),
      backend: {
        type: "local",
        model: { provider: "x", id: "y" },
        systemPrompt: null,
        tools: [],
      },
    };
    expect(() => resolveCursorAgent(r)).toThrow(/expected backend.type "cursor"/);
  });

  it("throws a clear error when CURSOR_API_KEY is missing", () => {
    expect(() => resolveCursorAgent(recipe(), { env: {} })).toThrow(/CURSOR_API_KEY/);
  });

  it("honors a custom requiredEnv list — missing the named var throws", () => {
    const r: RegisteredAgent = {
      ...recipe(),
      backend: { type: "cursor", model: "auto", requiredEnv: ["MY_KEY"] },
    };
    expect(() => resolveCursorAgent(r, { env: {} })).toThrow(/MY_KEY/);
  });

  it("constructs a CursorAgent that uses the injected transport when env is set", async () => {
    const transport = transportYielding([
      '{"type":"system","subtype":"init","session_id":"s"}',
      '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s"}',
    ]);
    const agent = resolveCursorAgent(recipe(), {
      transport,
      env: { CURSOR_API_KEY: "k" },
    });
    expect(agent).toBeInstanceOf(CursorAgent);
    const out = await agent.invoke({ task: "x" });
    expect(await out.text).toBe("ok");
  });
});
