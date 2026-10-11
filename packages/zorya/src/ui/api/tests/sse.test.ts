import { describe, expect, it } from "bun:test";
import { parseSseFrame, readSseBody, subscribeSse, type SseMessage } from "../sse.ts";

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
