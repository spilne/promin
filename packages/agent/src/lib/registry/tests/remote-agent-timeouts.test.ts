// ---------------------------------------------------------------------------
// RemoteAgent timeouts run on the injected WallClock: every non-turn call
// (thread list, messages, title, archive) has a deadline even when the
// recipe sets none, turns use the recipe's `timeoutMs`, and a finished
// call leaves no timer behind.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { DEFAULT_REMOTE_REQUEST_TIMEOUT_MS, RemoteAgent } from "../remote-agent.ts";

/** A transport that never answers until the request is aborted. */
function hangingFetch(seen: string[]) {
  return (url: string, init: RequestInit): Promise<Response> =>
    new Promise<Response>((_resolve, reject) => {
      seen.push(`${init.method} ${url}`);
      init.signal?.addEventListener("abort", () => reject(init.signal?.reason));
    });
}

async function settle<T>(p: Promise<T>): Promise<{ value?: T; error?: unknown }> {
  return p.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
}

async function collect<T>(stream: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of stream) values.push(value);
  return values;
}

/** An SSE response which sends one delta, then stays open until aborted. */
function openStreamFetch(init: RequestInit): Promise<Response> {
  const signal = init.signal;
  return Promise.resolve(
    new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('data: {"delta":"partial"}\n\n'));
          signal?.addEventListener("abort", () => controller.error(signal.reason), { once: true });
        },
      }),
      { headers: { "content-type": "text/event-stream" } },
    ),
  );
}

/** Yield until the code under test has registered its timer (bounded by iterations). */
async function waitForTimer(clock: FakeWallClock): Promise<void> {
  for (let i = 0; i < 100 && clock.pendingCount() === 0; i++) {
    await new Promise((r) => setTimeout(r, 0));
  }
}

describe("RemoteAgent timeouts", () => {
  it("a GET with no recipe timeout still gives up after the default request timeout", async () => {
    const clock = FakeWallClock.create(0);
    const seen: string[] = [];
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      fetch: hangingFetch(seen),
      clock,
    }).withScope({ namespaceId: "acme" });
    const listing = settle(agent.listThreads());
    await waitForTimer(clock);
    clock.advance(DEFAULT_REMOTE_REQUEST_TIMEOUT_MS);
    const { error } = await listing;
    expect(String(error)).toContain("timed out");
    expect(seen).toEqual([
      "GET https://peer.example/api/agents/peer/threads?namespaceId=acme&archived=all",
    ]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("a PATCH (setTitle) times out on requestTimeoutMs and reports the failure", async () => {
    const clock = FakeWallClock.create(0);
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      requestTimeoutMs: 500,
      fetch: hangingFetch([]),
      clock,
    });
    const thread = await agent.thread("t1");
    const renaming = settle(thread.setTitle("New title"));
    await waitForTimer(clock);
    clock.advance(499);
    expect(clock.pendingCount()).toBe(1);
    clock.advance(1);
    expect(String((await renaming).error)).toContain("timed out");
  });

  it("a completed call clears its timer", async () => {
    const clock = FakeWallClock.create(0);
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      timeoutMs: 60_000,
      fetch: async () => Response.json({ text: "hi", finishReason: "stop", usage: {} }),
      clock,
    });
    const out = await agent.invoke({ task: "hello" });
    expect(await out.text).toBe("hi");
    expect(clock.pendingCount()).toBe(0);
  });

  it("a turn without timeoutMs has no deadline", async () => {
    const clock = FakeWallClock.create(0);
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      fetch: hangingFetch([]),
      clock,
    });
    const ctrl = new AbortController();
    const turn = settle(agent.invoke({ task: "long" }, { signal: ctrl.signal }));
    await new Promise((r) => setTimeout(r, 0));
    expect(clock.pendingCount()).toBe(0);
    ctrl.abort();
    expect((await turn).error).toBeDefined();
  });

  it("a stream's deadline runs until the stream ends and then clears", async () => {
    const clock = FakeWallClock.create(0);
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      timeoutMs: 1_000,
      fetch: async () =>
        new Response(
          'data: {"delta":"he"}\n\nevent: finish\ndata: {"text":"he","finishReason":"stop","usage":{"inputTokens":1,"outputTokens":1}}\n\n',
          { headers: { "content-type": "text/event-stream" } },
        ),
      clock,
    });
    const out = agent.stream({ task: "hi" });
    expect(await out.text).toBe("he");
    for (let i = 0; i < 100 && clock.pendingCount() > 0; i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    expect(clock.pendingCount()).toBe(0);
  });

  it("caller abort settles every stream surface as cancelled and preserves received text", async () => {
    const clock = FakeWallClock.create(0);
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      timeoutMs: 60_000,
      fetch: async (_url, init) => openStreamFetch(init),
      clock,
    });
    const abort = new AbortController();
    const out = agent.stream({ task: "hello" }, { signal: abort.signal });
    const textDeltas = out.textStream[Symbol.asyncIterator]();
    const events = out.fullStream[Symbol.asyncIterator]();

    await waitForTimer(clock);
    expect(await textDeltas.next()).toEqual({ value: "partial", done: false });
    expect(await events.next()).toEqual({
      value: { type: "text-delta", delta: "partial" },
      done: false,
    });
    abort.abort();

    expect(await out.text).toBe("partial");
    expect(await out.finishReason).toBe("cancelled");
    expect(await out.usage).toEqual({ inputTokens: 0, outputTokens: 0 });
    expect(await textDeltas.next()).toEqual({ value: undefined, done: true });
    expect(await events.next()).toEqual({
      value: {
        type: "finish",
        reason: "cancelled",
        usage: { inputTokens: 0, outputTokens: 0 },
      },
      done: false,
    });
    expect(await events.next()).toEqual({ value: undefined, done: true });
    expect(clock.pendingCount()).toBe(0);
  });

  it("stream timeout settles as cancelled, not an empty successful stop", async () => {
    const clock = FakeWallClock.create(0);
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      timeoutMs: 500,
      fetch: async (_url, init) => openStreamFetch(init),
      clock,
    });
    const out = agent.stream({ task: "hello" });
    const textDeltas = out.textStream[Symbol.asyncIterator]();
    const events = out.fullStream[Symbol.asyncIterator]();

    await waitForTimer(clock);
    expect(await textDeltas.next()).toEqual({ value: "partial", done: false });
    expect(await events.next()).toEqual({
      value: { type: "text-delta", delta: "partial" },
      done: false,
    });
    clock.advance(500);

    expect(await out.text).toBe("partial");
    expect(await out.finishReason).toBe("cancelled");
    expect(await out.usage).toEqual({ inputTokens: 0, outputTokens: 0 });
    expect(await textDeltas.next()).toEqual({ value: undefined, done: true });
    expect(await events.next()).toEqual({
      value: {
        type: "finish",
        reason: "cancelled",
        usage: { inputTokens: 0, outputTokens: 0 },
      },
      done: false,
    });
    expect(await events.next()).toEqual({ value: undefined, done: true });
    expect(clock.pendingCount()).toBe(0);
  });

  it("explicit cancel settles a one-shot stream as cancelled", async () => {
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      fetch: async (_url, init) => openStreamFetch(init),
    });
    const out = agent.stream({ task: "hello" });
    const textDeltas = out.textStream[Symbol.asyncIterator]();
    const events = out.fullStream[Symbol.asyncIterator]();

    expect(await textDeltas.next()).toEqual({ value: "partial", done: false });
    expect(await events.next()).toEqual({
      value: { type: "text-delta", delta: "partial" },
      done: false,
    });
    await out.cancel();

    expect(await out.text).toBe("partial");
    expect(await out.finishReason).toBe("cancelled");
    expect(await out.usage).toEqual({ inputTokens: 0, outputTokens: 0 });
    expect(await textDeltas.next()).toEqual({ value: undefined, done: true });
    expect(await events.next()).toEqual({
      value: expect.objectContaining({ type: "finish", reason: "cancelled" }),
      done: false,
    });
    expect(await events.next()).toEqual({ value: undefined, done: true });
  });

  it("transport and protocol termination settle as error, never stop", async () => {
    for (const fetcher of [
      async () => {
        throw new Error("connection lost");
      },
      async () =>
        new Response('data: {"delta":"unterminated"}\n\n', {
          headers: { "content-type": "text/event-stream" },
        }),
    ]) {
      const agent = new RemoteAgent({
        endpoint: "https://peer.example",
        remoteAgentId: "peer",
        fetch: fetcher,
      });
      const out = agent.stream({ task: "hello" });
      const events = collect(out.fullStream);

      expect((await settle(out.text)).error).toBeDefined();
      expect(await out.finishReason).toBe("error");
      expect(await out.usage).toEqual({ inputTokens: 0, outputTokens: 0 });
      expect((await events).at(-1)?.type).toBe("error");
    }
  });
});
