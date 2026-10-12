import { describe, expect, it } from "bun:test";
import { OutboundRequestError, createOutboundFetch } from "../../outbound/outbound-fetch.ts";
import { resolveRemoteAgent } from "../resolve-remote-agent.ts";
import type { RegisteredAgent } from "../types.ts";

function remoteRecipe(endpoint: string): RegisteredAgent {
  return {
    id: "peer",
    backend: { type: "remote", endpoint, remoteAgentId: "peer" },
  } as unknown as RegisteredAgent;
}

/** Bounded wait on event-loop turns. */
async function until(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (cond()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("condition never held");
}

/**
 * A peer whose thread stream stays open (naming its turn `turn-9`) until
 * the client aborts, and whose cancel route answers `cancelStatus`.
 */
function fakePeer(opts: { timeoutMs?: number; cancelStatus?: number } = {}) {
  const seen: Array<{ url: string; body: unknown }> = [];
  let aborted = false;
  const recipe = {
    id: "peer",
    backend: {
      type: "remote",
      endpoint: "https://peer.example",
      remoteAgentId: "peer",
      ...(opts.timeoutMs !== undefined && { timeoutMs: opts.timeoutMs }),
    },
  } as unknown as RegisteredAgent;
  const agent = resolveRemoteAgent({
    agent: recipe,
    fetch: async (url, init) => {
      seen.push({ url, body: JSON.parse(String(init?.body ?? "null")) });
      if (url.endsWith("/cancel")) {
        return Response.json({}, { status: opts.cancelStatus ?? 200 });
      }
      const signal = init?.signal;
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          signal?.addEventListener("abort", () => {
            aborted = true;
            controller.error(signal.reason);
          });
        },
      });
      return new Response(body, {
        headers: { "content-type": "text/event-stream", "x-turn-id": "turn-9" },
      });
    },
  });
  return { agent, seen, streamAborted: () => aborted };
}

describe("resolveRemoteAgent", () => {
  it("sends every call through the injected transport", async () => {
    const seen: string[] = [];
    const agent = resolveRemoteAgent({
      agent: remoteRecipe("https://peer.example/"),
      fetch: async (url) => {
        seen.push(url);
        return Response.json({ text: "hi", steps: [], usage: {}, finishReason: "stop" });
      },
    });
    await agent.invoke({ task: "hello" });
    expect(seen).toEqual(["https://peer.example/api/agents/peer/invoke"]);
  });

  it("cancelling a thread stream cancels the turn it named on the server, then closes", async () => {
    const peer = fakePeer();
    const thread = await peer.agent.thread("t1");
    const out = thread.stream({ task: "hello" });
    out.text.catch(() => {});
    await out.cancel();
    expect(peer.seen).toEqual([
      { url: "https://peer.example/api/agents/peer/threads/t1/stream", body: { task: "hello" } },
      { url: "https://peer.example/api/agents/peer/threads/t1/cancel", body: { turnId: "turn-9" } },
    ]);
    expect(peer.streamAborted()).toBe(true);
  });

  it("the caller's signal cancels the remote turn, not just the connection", async () => {
    const peer = fakePeer();
    const thread = await peer.agent.thread("t1");
    const abort = new AbortController();
    const out = thread.stream({ task: "hello" }, { signal: abort.signal });
    out.text.catch(() => {});
    await until(() => peer.seen.length === 1);
    abort.abort();
    await until(() => peer.seen.length === 2);
    expect(peer.seen[1]).toEqual({
      url: "https://peer.example/api/agents/peer/threads/t1/cancel",
      body: { turnId: "turn-9" },
    });
    await until(() => peer.streamAborted());
  });

  it("the call timeout cancels the remote turn", async () => {
    const peer = fakePeer({ timeoutMs: 1 });
    const thread = await peer.agent.thread("t1");
    const out = thread.stream({ task: "hello" });
    out.text.catch(() => {});
    await until(() => peer.seen.length === 2);
    expect(peer.seen[1]!.url).toBe("https://peer.example/api/agents/peer/threads/t1/cancel");
  });

  it("cancel() rejects when the server refuses the cancel; a turn already ended is fine", async () => {
    const refused = fakePeer({ cancelStatus: 500 });
    const out = (await refused.agent.thread("t1")).stream({ task: "hello" });
    out.text.catch(() => {});
    await expect(out.cancel()).rejects.toThrow("RemoteAgent cancel failed: 500");
    expect(refused.streamAborted()).toBe(true);

    const ended = fakePeer({ cancelStatus: 404 });
    const out2 = (await ended.agent.thread("t1")).stream({ task: "hello" });
    out2.text.catch(() => {});
    await out2.cancel();
  });

  it("refuses an endpoint that re-resolves to a private address under an outbound policy", async () => {
    const agent = resolveRemoteAgent({
      agent: remoteRecipe("https://peer.example"),
      fetch: createOutboundFetch({ policy: { resolveHost: async () => ["169.254.169.254"] } }),
    });
    const err = await agent.invoke({ task: "hello" }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(OutboundRequestError);
  });
});
