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

async function until(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 100; i++) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("condition never held");
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

      await expect(out.text).rejects.toThrow();
      expect(await out.finishReason).toBe("error");
      expect(await out.usage).toEqual({ inputTokens: 0, outputTokens: 0 });
      expect((await events).at(-1)?.type).toBe("error");
    }
  });

  it("settles a suspended approval stream as a successful stop", async () => {
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      fetch: async () =>
        new Response(
          'data: {"delta":"partial"}\n\nevent: approval-requested\ndata: {}\n\nevent: suspended\ndata: {}\n\n',
          { headers: { "content-type": "text/event-stream" } },
        ),
    });
    const out = agent.stream({ task: "hello" });

    expect(await out.text).toBe("partial");
    expect(await out.finishReason).toBe("stop");
    expect((await collect(out.fullStream)).at(-1)).toMatchObject({
      type: "finish",
      reason: "stop",
    });
  });

  it("does not leak an unhandled text rejection when error surfaces are fully consumed", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
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

        expect((await collect(out.fullStream)).at(-1)?.type).toBe("error");
        expect(await out.finishReason).toBe("error");
        // Deliberately do not read `out.text`: fullStream and finishReason are
        // valid complete consumption paths for a failed run.
      }
      for (let i = 0; i < 10; i++) await new Promise((resolve) => setTimeout(resolve, 0));
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  it("rejects semantically invalid finish frames instead of producing undefined success", async () => {
    const invalidFinishData = [
      {},
      {
        text: 42,
        finishReason: "stop",
        usage: { inputTokens: 1, outputTokens: 2 },
      },
      {
        text: "done",
        finishReason: "unknown",
        usage: { inputTokens: 1, outputTokens: 2 },
      },
      {
        text: "done",
        finishReason: "stop",
        usage: { inputTokens: "1", outputTokens: 2 },
      },
      {
        text: "done",
        finishReason: "stop",
        usage: { inputTokens: 1, outputTokens: 2, cacheReadTokens: "3" },
      },
    ];

    for (const finish of invalidFinishData) {
      const agent = new RemoteAgent({
        endpoint: "https://peer.example",
        remoteAgentId: "peer",
        fetch: async () =>
          new Response(`event: finish\ndata: ${JSON.stringify(finish)}\n\n`, {
            headers: { "content-type": "text/event-stream" },
          }),
      });
      const out = agent.stream({ task: "hello" });
      const textDeltas = collect(out.textStream);
      const events = collect(out.fullStream);

      expect(String((await settle(out.text)).error)).toContain("invalid finish data");
      expect(await out.finishReason).toBe("error");
      expect(await out.usage).toEqual({ inputTokens: 0, outputTokens: 0 });
      expect(await textDeltas).toEqual([]);
      expect((await events).at(-1)).toMatchObject({ type: "error" });
    }
  });

  it("thread cancel settles before initial response headers and sends one best-effort cancel", async () => {
    const seen: Array<{ readonly url: string; readonly body: string | undefined }> = [];
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      fetch: (url, init) => {
        seen.push({ url, body: typeof init.body === "string" ? init.body : undefined });
        if (url.endsWith("/stream")) return new Promise<Response>(() => {});
        return Promise.resolve(Response.json({}));
      },
    });
    const out = (await agent.thread("t1")).stream({ task: "hello" });
    const textDeltas = collect(out.textStream);
    const events = collect(out.fullStream);

    await Promise.all([out.cancel(), out.cancel()]);

    expect(await out.text).toBe("");
    expect(await out.finishReason).toBe("cancelled");
    expect(await out.usage).toEqual({ inputTokens: 0, outputTokens: 0 });
    expect(await textDeltas).toEqual([]);
    expect((await events).at(-1)).toMatchObject({ type: "finish", reason: "cancelled" });
    await until(() => seen.length === 2);
    expect(seen).toEqual([
      {
        url: "https://peer.example/api/agents/peer/threads/t1/stream",
        body: '{"task":"hello"}',
      },
      {
        url: "https://peer.example/api/agents/peer/threads/t1/cancel",
        body: "{}",
      },
    ]);
  });

  it("a hanging remote cancel is bounded and never delays local settlement", async () => {
    const clock = FakeWallClock.create(0);
    let cancelRequestAborted = false;
    const seen: string[] = [];
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      requestTimeoutMs: 25,
      clock,
      fetch: (url, init) => {
        seen.push(url);
        if (url.endsWith("/cancel")) {
          return new Promise<Response>((_resolve, reject) => {
            init.signal?.addEventListener(
              "abort",
              () => {
                cancelRequestAborted = true;
                reject(init.signal?.reason);
              },
              { once: true },
            );
          });
        }
        const signal = init.signal;
        return Promise.resolve(
          new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                signal?.addEventListener("abort", () => controller.error(signal.reason), {
                  once: true,
                });
              },
            }),
            {
              headers: { "content-type": "text/event-stream", "x-turn-id": "turn-1" },
            },
          ),
        );
      },
    });
    const out = (await agent.thread("t1")).stream({ task: "hello" });
    await new Promise((resolve) => setTimeout(resolve, 0));

    await out.cancel();

    expect(await out.text).toBe("");
    expect(await out.finishReason).toBe("cancelled");
    expect(seen).toHaveLength(2);
    await waitForTimer(clock);
    expect(clock.pendingCount()).toBe(1);
    clock.advance(25);
    await until(() => cancelRequestAborted);
    await until(() => clock.pendingCount() === 0);
  });

  it("a terminal frame ends streams and cancels an HTTP body that stays open", async () => {
    const clock = FakeWallClock.create(0);
    let bodyCancelled = false;
    const abort = new AbortController();
    const agent = new RemoteAgent({
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      timeoutMs: 1_000,
      clock,
      fetch: async () =>
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode(
                  'event: finish\ndata: {"text":"done","finishReason":"stop","usage":{"inputTokens":1,"outputTokens":2}}\n\n',
                ),
              );
            },
            cancel() {
              bodyCancelled = true;
              return new Promise<void>(() => {});
            },
          }),
          { headers: { "content-type": "text/event-stream" } },
        ),
    });
    const out = agent.stream({ task: "hello" }, { signal: abort.signal });
    const textDeltas = collect(out.textStream);
    const events = collect(out.fullStream);

    expect(await out.text).toBe("done");
    expect(await out.finishReason).toBe("stop");
    expect(await out.usage).toEqual({ inputTokens: 1, outputTokens: 2 });
    expect(await textDeltas).toEqual([]);
    expect(await events).toEqual([
      {
        type: "finish",
        reason: "stop",
        usage: { inputTokens: 1, outputTokens: 2 },
      },
    ]);
    await until(() => bodyCancelled);
    expect(clock.pendingCount()).toBe(0);

    // A terminal frame wins a later abort/cancel race and remains stable.
    abort.abort();
    await Promise.all([out.cancel(), out.cancel()]);
    expect(await out.finishReason).toBe("stop");
    expect(clock.pendingCount()).toBe(0);
  });
});
