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
    expect(seen).toEqual(["GET https://peer.example/api/agents/peer/threads?namespaceId=acme"]);
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
});
