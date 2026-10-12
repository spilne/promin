import { describe, expect, it } from "bun:test";
import {
  parseSseFrame,
  readResumableSse,
  readSseBody,
  SseHttpError,
  subscribeSse,
  type SseMessage,
} from "../sse.ts";

function bodyOf(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const c of chunks) controller.enqueue(encoder.encode(c));
      controller.close();
    },
  });
}

describe("parseSseFrame", () => {
  it("reads event and multi-line data, skipping comments", () => {
    expect(parseSseFrame(': heartbeat\nevent: finish\ndata: {"a":\ndata: 1}')).toEqual({
      event: "finish",
      data: '{"a":\n1}',
    });
  });
  it("reads the event id", () => {
    expect(parseSseFrame("id: turn-1:3\ndata: x")).toEqual({
      event: "message",
      data: "x",
      id: "turn-1:3",
    });
  });
  it("defaults the event name and ignores data-less frames", () => {
    expect(parseSseFrame("data: x")).toEqual({ event: "message", data: "x" });
    expect(parseSseFrame(": heartbeat")).toBeNull();
  });
});

describe("readSseBody", () => {
  it("reassembles frames split across chunks and CRLF line endings", async () => {
    const messages: SseMessage[] = [];
    await readSseBody({
      body: bodyOf(["data: o", "ne\r\n\r\n: hb\n\ndata: two\n", "\n"]),
      onMessage: (m) => messages.push(m),
    });
    expect(messages.map((m) => m.data)).toEqual(["one", "two"]);
  });
});

describe("subscribeSse", () => {
  it("sends fresh headers per connection and stops reconnecting when told", async () => {
    const seen: string[] = [];
    let connections = 0;
    const fetchImpl = (async (_url: string, init: RequestInit) => {
      connections += 1;
      seen.push(new Headers(init.headers).get("authorization") ?? "");
      return new Response(bodyOf([`data: ${connections}\n\n`]), {
        headers: { "content-type": "text/event-stream" },
      });
    }) as unknown as typeof fetch;
    let key = "k1";
    const got: string[] = [];
    let done!: () => void;
    const finished = new Promise<void>((r) => (done = r));
    const unsubscribe = subscribeSse({
      url: "http://x/events",
      headers: () => ({ authorization: `Bearer ${key}` }),
      initialRetryDelayMs: 1,
      fetchImpl,
      shouldReconnect: () => got.length < 2,
      onMessage: (m) => {
        got.push(m.data);
        key = "k2";
        if (got.length === 2) done();
      },
    });
    await finished;
    unsubscribe();
    expect(got).toEqual(["1", "2"]);
    expect(seen).toEqual(["Bearer k1", "Bearer k2"]);
  });

  it("gives up on 401", async () => {
    let connections = 0;
    const fetchImpl = (async () => {
      connections += 1;
      return new Response("no", { status: 401 });
    }) as unknown as typeof fetch;
    const unsubscribe = subscribeSse({
      url: "http://x/events",
      initialRetryDelayMs: 1,
      fetchImpl,
      onMessage: () => {},
    });
    for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 1));
    unsubscribe();
    expect(connections).toBe(1);
  });
});

/** A body that delivers `chunks`, then fails like a dropped connection. */
function droppingBodyOf(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  let sent = false;
  return new ReadableStream({
    pull(controller) {
      if (!sent) {
        sent = true;
        for (const c of chunks) controller.enqueue(encoder.encode(c));
        return;
      }
      controller.error(new TypeError("network error"));
    },
  });
}

describe("readResumableSse", () => {
  const isTerminal = (m: SseMessage) => m.event === "finish";

  it("resumes a dropped stream after the last event id without repeating events", async () => {
    const requests: Array<{ url: string; method: string; lastEventId: string | null }> = [];
    const fetchImpl = (async (url: string, init: RequestInit) => {
      const headers = new Headers(init.headers);
      requests.push({
        url,
        method: init.method ?? "GET",
        lastEventId: headers.get("last-event-id"),
      });
      if (requests.length === 1) {
        return new Response(
          droppingBodyOf([
            "id: t:0\nevent: thread\ndata: {}\n\n",
            'id: t:1\ndata: {"delta":"Hel"}\n\n',
          ]),
        );
      }
      return new Response(
        bodyOf([
          ": heartbeat\n\n",
          'id: t:2\ndata: {"delta":"lo"}\n\n',
          'id: t:3\nevent: finish\ndata: {"text":"Hello"}\n\n',
        ]),
      );
    }) as unknown as typeof fetch;
    const got: SseMessage[] = [];
    const outcome = await readResumableSse({
      start: { url: "http://x/stream", init: { method: "POST", body: "{}" } },
      resumeUrl: "http://x/stream?scope",
      headers: () => ({ authorization: "Bearer k" }),
      signal: new AbortController().signal,
      isTerminal,
      onMessage: (m) => got.push(m),
      fetchImpl,
      sleep: async () => {},
    });
    expect(outcome).toBe("ended");
    expect(requests).toEqual([
      { url: "http://x/stream", method: "POST", lastEventId: null },
      { url: "http://x/stream?scope", method: "GET", lastEventId: "t:1" },
    ]);
    expect(got.map((m) => m.id)).toEqual(["t:0", "t:1", "t:2", "t:3"]);
    const text = got
      .filter((m) => m.event === "message")
      .map((m) => (JSON.parse(m.data) as { delta: string }).delta)
      .join("");
    expect(text).toBe("Hello");
  });

  it("gives up when the server no longer has the stream", async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls += 1;
      return calls === 1
        ? new Response(droppingBodyOf(['id: t:0\ndata: {"delta":"a"}\n\n']))
        : new Response("gone", { status: 404 });
    }) as unknown as typeof fetch;
    const outcome = await readResumableSse({
      start: { url: "http://x/stream", init: { method: "POST" } },
      resumeUrl: "http://x/stream?scope",
      signal: new AbortController().signal,
      isTerminal,
      onMessage: () => {},
      fetchImpl,
      sleep: async () => {},
    });
    expect(outcome).toBe("lost");
    expect(calls).toBe(2);
  });

  it("stops after maxReconnects failures in a row", async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls += 1;
      if (calls === 1) return new Response(droppingBodyOf(["id: t:0\ndata: {}\n\n"]));
      throw new TypeError("offline");
    }) as unknown as typeof fetch;
    const outcome = await readResumableSse({
      start: { url: "http://x/stream", init: { method: "POST" } },
      resumeUrl: "http://x/stream?scope",
      signal: new AbortController().signal,
      isTerminal,
      onMessage: () => {},
      maxReconnects: 3,
      fetchImpl,
      sleep: async () => {},
    });
    expect(outcome).toBe("lost");
    expect(calls).toBe(4);
  });

  it("rejects when the first request is refused", async () => {
    const fetchImpl = (async () => new Response("no", { status: 409 })) as unknown as typeof fetch;
    const failed = readResumableSse({
      start: { url: "http://x/stream", init: { method: "POST" } },
      resumeUrl: "http://x/stream?scope",
      signal: new AbortController().signal,
      isTerminal,
      onMessage: () => {},
      fetchImpl,
    });
    await expect(failed).rejects.toBeInstanceOf(SseHttpError);
  });
});
