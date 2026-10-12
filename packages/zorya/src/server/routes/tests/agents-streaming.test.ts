// ---------------------------------------------------------------------------
// Agent thread streams over HTTP: event ids from the turn's workflow id,
// heartbeats on the gateway clock, a disconnect that detaches (the turn
// runs on and commits once), reattach with Last-Event-ID, the cancel
// route, input caps, and the turn trace on the trace endpoint.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { threadTurnWorkflowId, type LLMChatParams, type LLMProvider } from "@promin/agent";
import { MAX_AGENT_TASK_CHARS } from "../agents/agent-scope.ts";
import { MAX_THREAD_TITLE_CHARS } from "../agents/agent-threads.ts";
import { bootAgents, jsonOf, req } from "./agent-gateway-harness.ts";

const scope = { namespaceId: "ns", resourceId: "alice" };
const query = "namespaceId=ns&resourceId=alice";
const key = { namespaceId: "ns", threadId: "t1" };

/**
 * An LLM that streams `chunks`: the first right away, the rest once
 * `release()` is called (or the call is aborted).
 */
function gatedLlm(chunks: ReadonlyArray<string>) {
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  let calls = 0;
  const llm: LLMProvider = {
    chat: async () => ({ content: chunks.join(""), finishReason: "stop" }),
    async *chatStream(params: LLMChatParams) {
      calls++;
      yield { delta: chunks[0]! };
      await Promise.race([
        gate,
        new Promise<void>((r) => params.signal?.addEventListener("abort", () => r())),
      ]);
      if (params.signal?.aborted) {
        const err = new Error("aborted");
        err.name = "AbortError";
        throw err;
      }
      for (const delta of chunks.slice(1)) yield { delta };
      yield { delta: "", finishReason: "stop", usage: { inputTokens: 3, outputTokens: 4 } };
    },
  };
  return { llm, release: () => release(), calls: () => calls };
}

interface Frame {
  id?: string;
  event?: string;
  data?: Record<string, unknown>;
  comment?: string;
}

function parseFrame(raw: string): Frame {
  const frame: Frame = {};
  for (const line of raw.split("\n")) {
    if (line.startsWith(":")) frame.comment = line.slice(1).trim();
    else if (line.startsWith("id: ")) frame.id = line.slice(4);
    else if (line.startsWith("event: ")) frame.event = line.slice(7);
    else if (line.startsWith("data: ")) frame.data = JSON.parse(line.slice(6));
  }
  return frame;
}

function frameReader(res: Response) {
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  return {
    async next(): Promise<Frame | null> {
      for (;;) {
        const at = buffer.indexOf("\n\n");
        if (at >= 0) {
          const raw = buffer.slice(0, at);
          buffer = buffer.slice(at + 2);
          return parseFrame(raw);
        }
        const { value, done } = await reader.read();
        if (done) return null;
        buffer += decoder.decode(value, { stream: true });
      }
    },
    async rest(): Promise<Frame[]> {
      const frames: Frame[] = [];
      for (let f = await this.next(); f; f = await this.next()) frames.push(f);
      return frames;
    },
    cancel: () => reader.cancel(),
  };
}

async function waitFor(cond: () => Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await cond()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("condition never held");
}

const deltasOf = (frames: ReadonlyArray<Frame>): string =>
  frames
    .filter((f) => f.event === undefined && typeof f.data?.delta === "string")
    .map((f) => f.data!.delta as string)
    .join("");

describe("thread stream — event ids", () => {
  it("ids every event with the turn's workflow id and a sequence", async () => {
    const { server } = await bootAgents({ reply: () => "hello" });
    const res = await server.handle(
      req("POST", "/api/agents/support/threads/t1/stream", { task: "hi", ...scope, turnKey: "k1" }),
    );
    expect(res.status).toBe(200);
    const turnId = threadTurnWorkflowId({ ...key, turnKey: "k1" });
    expect(res.headers.get("x-turn-id")).toBe(turnId);
    const frames = await frameReader(res).rest();
    expect(frames.map((f) => f.id)).toEqual(frames.map((_, i) => `${turnId}:${i}`));
    expect(frames[0]).toMatchObject({ event: "thread", data: { threadId: "t1", turnId } });
    expect(frames[frames.length - 1]).toMatchObject({ event: "finish", data: { text: "hello" } });
  });
});

describe("thread stream — heartbeat", () => {
  it("sends a heartbeat comment on the gateway clock while a tool call runs", async () => {
    const llm = gatedLlm(["thinking", "...done"]);
    const { server, clock } = await bootAgents({
      llm: llm.llm,
      streaming: { heartbeatMs: 5_000 },
    });
    const res = await server.handle(
      req("POST", "/api/agents/support/threads/t1/stream", { task: "hi", ...scope }),
    );
    const reader = frameReader(res);
    expect((await reader.next())!.event).toBe("thread");
    expect((await reader.next())!.data).toEqual({ delta: "thinking" });

    let next: Frame | null | undefined;
    const pending = reader.next().then((f) => (next = f));
    for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));
    expect(next).toBeUndefined();
    clock.advance(5_000);
    await pending;
    expect(next).toEqual({ comment: "heartbeat" });

    llm.release();
    const rest = await reader.rest();
    expect(deltasOf(rest)).toBe("...done");
    expect(rest[rest.length - 1]!.event).toBe("finish");
  });
});

describe("thread stream — detach and reattach", () => {
  it("a disconnect detaches; the turn completes once and a reattach resumes after Last-Event-ID", async () => {
    const llm = gatedLlm(["Hel", "lo", " world"]);
    const { server, leaseStore } = await bootAgents({ llm: llm.llm, turnGate: true });
    const res = await server.handle(
      req("POST", "/api/agents/support/threads/t1/stream", { task: "hi", ...scope, turnKey: "k" }),
    );
    const turnId = threadTurnWorkflowId({ ...key, turnKey: "k" });
    const first = frameReader(res);
    const seen = [(await first.next())!, (await first.next())!];
    expect(seen.map((f) => f.id)).toEqual([`${turnId}:0`, `${turnId}:1`]);
    // The network drops.
    await first.cancel();
    for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));

    // Reattach while the turn still runs, from the last event received.
    const again = await server.handle(
      req("GET", `/api/agents/support/threads/t1/stream?${query}`, undefined, {
        "last-event-id": `${turnId}:1`,
      }),
    );
    expect(again.status).toBe(200);
    llm.release();
    const tail = await frameReader(again).rest();
    expect(tail[0]!.id).toBe(`${turnId}:2`);
    // No text twice: what the first connection saw plus the tail is the answer.
    expect(deltasOf(seen) + deltasOf(tail)).toBe("Hello world");
    expect(tail[tail.length - 1]).toMatchObject({
      event: "finish",
      data: { text: "Hello world", finishReason: "stop" },
    });

    // The turn ran once and committed once; the lease is free again.
    await waitFor(async () => (await leaseStore.get(key)) === null);
    expect(llm.calls()).toBe(1);
    const listed = await server.handle(
      req("GET", `/api/agents/support/threads/t1/messages?${query}`),
    );
    const { messages } = await jsonOf<{ messages: Array<{ role: string; content: string }> }>(
      listed,
    );
    expect(messages.map((m) => [m.role, m.content])).toEqual([
      ["user", "hi"],
      ["assistant", "Hello world"],
    ]);

    // A reattach after the turn finished replays from the start.
    const replay = await server.handle(
      req("GET", `/api/agents/support/threads/t1/stream?${query}&turnId=${turnId}`),
    );
    const replayed = await frameReader(replay).rest();
    expect(replayed[0]).toMatchObject({ id: `${turnId}:0`, event: "thread" });
    expect(deltasOf(replayed)).toBe("Hello world");
  });

  it("a send retried with the same turn key while it runs attaches instead of running twice", async () => {
    const llm = gatedLlm(["a", "b"]);
    const { server } = await bootAgents({ llm: llm.llm, turnGate: true });
    const body = { task: "hi", ...scope, turnKey: "same" };
    const first = frameReader(
      await server.handle(req("POST", "/api/agents/support/threads/t1/stream", body)),
    );
    await first.next();
    await first.next();
    await first.cancel();

    const retry = await server.handle(req("POST", "/api/agents/support/threads/t1/stream", body));
    expect(retry.status).toBe(200);
    llm.release();
    const frames = await frameReader(retry).rest();
    expect(deltasOf(frames)).toBe("ab");
    expect(frames[frames.length - 1]!.event).toBe("finish");
    expect(llm.calls()).toBe(1);
  });

  it("a retry with the same turn key but another task answers 422, not the running turn", async () => {
    const llm = gatedLlm(["a", "b"]);
    const { server } = await bootAgents({ llm: llm.llm, turnGate: true });
    const first = frameReader(
      await server.handle(
        req("POST", "/api/agents/support/threads/t1/stream", {
          task: "hi",
          ...scope,
          turnKey: "k",
        }),
      ),
    );
    await first.next();
    const other = await server.handle(
      req("POST", "/api/agents/support/threads/t1/stream", {
        task: "something else",
        ...scope,
        turnKey: "k",
      }),
    );
    expect(other.status).toBe(422);
    expect((await jsonOf<{ error: string }>(other)).error).toBe("idempotency_key_mismatch");
    llm.release();
    expect(deltasOf(await first.rest())).toBe("ab");
  });

  it("only the owner can reattach; an unknown turn answers 404", async () => {
    const { server } = await bootAgents({ reply: () => "x" });
    await frameReader(
      await server.handle(
        req("POST", "/api/agents/support/threads/t1/stream", { task: "hi", ...scope }),
      ),
    ).rest();
    const bob = await server.handle(
      req("GET", "/api/agents/support/threads/t1/stream?namespaceId=ns&resourceId=bob"),
    );
    expect(bob.status).toBe(404);
    const unknown = await server.handle(
      req("GET", `/api/agents/support/threads/t1/stream?${query}&turnId=nope`),
    );
    expect(unknown.status).toBe(404);
    expect((await jsonOf<{ error: string }>(unknown)).error).toBe("turn_stream_not_found");
  });
});

describe("thread stream — cancel", () => {
  it("only the cancel route cancels a running turn", async () => {
    const llm = gatedLlm(["partial", " never"]);
    const { server, leaseStore } = await bootAgents({ llm: llm.llm, turnGate: true });
    const reader = frameReader(
      await server.handle(
        req("POST", "/api/agents/support/threads/t1/stream", { task: "hi", ...scope }),
      ),
    );
    await reader.next();
    await reader.next();

    const foreign = await server.handle(
      req("POST", "/api/agents/support/threads/t1/cancel", {
        namespaceId: "ns",
        resourceId: "bob",
      }),
    );
    expect(foreign.status).toBe(404);

    const cancel = await server.handle(req("POST", "/api/agents/support/threads/t1/cancel", scope));
    expect(cancel.status).toBe(200);
    expect(await jsonOf<{ cancelled: boolean }>(cancel)).toMatchObject({ cancelled: true });
    const rest = await reader.rest();
    expect(rest[rest.length - 1]).toMatchObject({
      event: "finish",
      data: { finishReason: "cancelled" },
    });
    await waitFor(async () => (await leaseStore.get(key)) === null);

    const again = await server.handle(req("POST", "/api/agents/support/threads/t1/cancel", scope));
    expect(again.status).toBe(404);
    expect((await jsonOf<{ error: string }>(again)).error).toBe("turn_not_running");
  });
});

describe("thread stream — cancel without a turn gate", () => {
  it("requires turnId when several turns run on the thread", async () => {
    const llm = gatedLlm(["x", "y"]);
    const { server } = await bootAgents({ llm: llm.llm, turnGate: false });
    const open = async (turnKey: string) => {
      const res = await server.handle(
        req("POST", "/api/agents/support/threads/t1/stream", { task: "hi", ...scope, turnKey }),
      );
      const reader = frameReader(res);
      await reader.next();
      return { reader, turnId: res.headers.get("x-turn-id")! };
    };
    const a = await open("a");
    const b = await open("b");

    const guess = await server.handle(req("POST", "/api/agents/support/threads/t1/cancel", scope));
    expect(guess.status).toBe(400);
    expect((await jsonOf<{ error: string }>(guess)).error).toBe("turnId_required");

    const named = await server.handle(
      req("POST", "/api/agents/support/threads/t1/cancel", { ...scope, turnId: a.turnId }),
    );
    expect(named.status).toBe(200);
    expect(await jsonOf<{ turnId: string }>(named)).toMatchObject({ turnId: a.turnId });
    const aRest = await a.reader.rest();
    expect(aRest[aRest.length - 1]).toMatchObject({ data: { finishReason: "cancelled" } });

    llm.release();
    const bRest = await b.reader.rest();
    expect(bRest[bRest.length - 1]).toMatchObject({ data: { finishReason: "stop" } });
  });
});

describe("input caps", () => {
  it("rejects a task over the cap with 413 on every invoke route", async () => {
    const { server } = await bootAgents({ reply: () => "ok" });
    const task = "x".repeat(MAX_AGENT_TASK_CHARS + 1);
    for (const path of [
      "/api/agents/support/invoke",
      "/api/agents/support/stream",
      "/api/agents/support/threads/t1",
      "/api/agents/support/threads/t1/stream",
    ]) {
      const res = await server.handle(req("POST", path, { task, ...scope }));
      expect([path, res.status]).toEqual([path, 413]);
      expect(await jsonOf<{ error: string }>(res)).toMatchObject({ error: "task_too_large" });
    }
    const atCap = await server.handle(
      req("POST", "/api/agents/support/invoke", {
        task: "x".repeat(MAX_AGENT_TASK_CHARS),
        ...scope,
      }),
    );
    expect(atCap.status).toBe(200);
  });

  it("rejects a thread title over the cap with 400", async () => {
    const { server } = await bootAgents({ reply: () => "ok" });
    expect(
      (await server.handle(req("POST", "/api/agents/support/threads/t1", { task: "hi", ...scope })))
        .status,
    ).toBe(200);
    const long = await server.handle(
      req("PATCH", "/api/agents/support/threads/t1", {
        ...scope,
        title: "t".repeat(MAX_THREAD_TITLE_CHARS + 1),
      }),
    );
    expect(long.status).toBe(400);
    expect(await jsonOf<{ error: string }>(long)).toMatchObject({ error: "title_too_long" });
    const ok = await server.handle(
      req("PATCH", "/api/agents/support/threads/t1", {
        ...scope,
        title: `  ${"t".repeat(MAX_THREAD_TITLE_CHARS)}  `,
      }),
    );
    expect(ok.status).toBe(200);
  });
});

describe("turn trace", () => {
  it("the trace endpoint shows each turn's spans, usage and finish reason", async () => {
    const { server } = await bootAgents({ reply: () => "answer" });
    expect(
      (await server.handle(req("POST", "/api/agents/support/threads/t1", { task: "hi", ...scope })))
        .status,
    ).toBe(200);
    const res = await server.handle(req("GET", `/api/agents/support/threads/t1/trace?${query}`));
    expect(res.status).toBe(200);
    const body = await jsonOf<{
      trace: {
        turns: Array<{
          timing?: { finishReason: string; steps: number; spans: Array<{ kind: string }> };
        }>;
      };
    }>(res);
    const timing = body.trace.turns[0]!.timing!;
    expect(timing.finishReason).toBe("stop");
    expect(timing.steps).toBe(1);
    expect(timing.spans.map((s) => s.kind)).toEqual(["think"]);
  });
});
