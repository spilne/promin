// ---------------------------------------------------------------------------
// TurnStreamHub: event ids, bounded buffering and backpressure, heartbeats,
// detach vs cancel on disconnect, and retention of finished turns.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { AgentEvent, AgentRunOutput } from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import { parseEventId, TurnStreamHub, type TurnStreamOwner } from "../agent-sse.ts";

const owner: TurnStreamOwner = {
  agentId: "support",
  namespaceId: "ns",
  resourceId: "alice",
  threadId: "t1",
};

/** A run output fed by hand: `push` events, then `finish` or `fail`. */
function manualRun() {
  const events: AgentEvent[] = [];
  let ended = false;
  let wake: (() => void) | null = null;
  let settle!: (text: string) => void;
  let reject!: (err: unknown) => void;
  const text = new Promise<string>((res, rej) => {
    settle = res;
    reject = rej;
  });
  let cancelled = 0;
  const fullStream: AsyncIterable<AgentEvent> = {
    [Symbol.asyncIterator]() {
      let i = 0;
      return {
        next: async (): Promise<IteratorResult<AgentEvent>> => {
          while (i >= events.length && !ended) {
            await new Promise<void>((r) => (wake = r));
          }
          if (i < events.length) return { value: events[i++]!, done: false };
          return { value: undefined, done: true };
        },
      };
    },
  };
  const poke = () => {
    const w = wake;
    wake = null;
    w?.();
  };
  const out: AgentRunOutput = {
    textStream: {
      [Symbol.asyncIterator]: () => ({ next: async () => ({ value: "", done: true }) }),
    },
    fullStream,
    text,
    output: text.then(() => undefined),
    toolCalls: text.then(() => []),
    toolResults: text.then(() => []),
    steps: text.then(() => []),
    usage: text.then(() => ({ inputTokens: 1, outputTokens: 2 })),
    finishReason: text.then(() => "stop" as const),
    messages: text.then(() => []),
    cancel: async () => {
      cancelled++;
      ended = true;
      poke();
      settle("cancelled");
    },
  };
  return {
    out,
    push(delta: string) {
      events.push({ type: "text-delta", delta });
      poke();
    },
    finish(answer: string) {
      ended = true;
      poke();
      settle(answer);
    },
    fail(err: unknown) {
      ended = true;
      poke();
      reject(err);
    },
    cancelled: () => cancelled,
  };
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

async function settle(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));
}

describe("TurnStreamHub — event ids and replay", () => {
  it("numbers every frame `<turnId>:<seq>` and replays after a Last-Event-ID", async () => {
    const hub = new TurnStreamHub({ clock: FakeWallClock.create(0) });
    const run = manualRun();
    const stream = hub.start({
      id: "turn-a",
      owner,
      out: run.out,
      meta: { threadId: "t1" },
      announce: true,
    });
    run.push("Hel");
    run.push("lo");
    run.finish("Hello");
    await stream.settled;

    const all = await frameReader(
      hub.respond({ stream, fromSeq: 0, onDisconnect: "detach" }),
    ).rest();
    expect(all.map((f) => f.id)).toEqual(["turn-a:0", "turn-a:1", "turn-a:2", "turn-a:3"]);
    expect(all.map((f) => f.event ?? "delta")).toEqual(["thread", "delta", "delta", "finish"]);
    expect(all[3]!.data).toMatchObject({ threadId: "t1", text: "Hello", finishReason: "stop" });

    const resumed = await frameReader(
      hub.respond({ stream, fromSeq: parseEventId("turn-a:1")!.seq + 1, onDisconnect: "detach" }),
    ).rest();
    expect(resumed.map((f) => f.id)).toEqual(["turn-a:2", "turn-a:3"]);
  });

  it("parses event ids whose turn id contains colons", () => {
    expect(parseEventId("wf:a/approve/c:12")).toEqual({ turnId: "wf:a/approve/c", seq: 12 });
    expect(parseEventId("nope")).toBeNull();
    expect(parseEventId("x:-1")).toBeNull();
    expect(parseEventId(null)).toBeNull();
  });
});

describe("TurnStreamHub — disconnect", () => {
  it("a detaching client leaves the run going; a cancelling one cancels it", async () => {
    const hub = new TurnStreamHub({ clock: FakeWallClock.create(0) });
    const detached = manualRun();
    const kept = hub.start({ id: "d", owner, out: detached.out, meta: {}, announce: true });
    const reader = frameReader(hub.respond({ stream: kept, fromSeq: 0, onDisconnect: "detach" }));
    expect((await reader.next())!.event).toBe("thread");
    await reader.cancel();
    await settle();
    expect(detached.cancelled()).toBe(0);
    detached.push("still running");
    detached.finish("still running");
    await kept.settled;
    expect(kept.done).toBe(true);

    const oneShot = manualRun();
    const gone = hub.start({ id: "c", out: oneShot.out, meta: {}, announce: true });
    const r2 = frameReader(hub.respond({ stream: gone, fromSeq: 0, onDisconnect: "cancel" }));
    await r2.next();
    await r2.cancel();
    await settle();
    expect(oneShot.cancelled()).toBe(1);
  });
});

describe("TurnStreamHub — backpressure", () => {
  it("bounds the turn buffer and closes a client that falls behind it", async () => {
    // Each delta frame is ~40 bytes; the buffer keeps about ten of them.
    const hub = new TurnStreamHub({
      clock: FakeWallClock.create(0),
      limits: { maxBufferBytes: 400, queueFrames: 2 },
    });
    const run = manualRun();
    const stream = hub.start({ id: "bp", owner, out: run.out, meta: {}, announce: false });
    // The client connects but does not read while the run streams.
    const reader = frameReader(hub.respond({ stream, fromSeq: 0, onDisconnect: "detach" }));
    for (let i = 0; i < 200; i++) run.push(`chunk-${i}`);
    run.finish("done");
    await stream.settled;

    // The turn kept only its newest frames.
    expect(stream.firstSeq).toBeGreaterThan(150);
    expect(stream.nextSeq).toBe(201);
    // The client got what fit in its queue, then a slow-consumer error —
    // not all 200 deltas.
    const frames = await reader.rest();
    const deltas = frames.filter((f) => f.event === undefined && f.data?.delta !== undefined);
    expect(deltas.length).toBeLessThanOrEqual(3);
    expect(frames[frames.length - 1]).toMatchObject({
      event: "error",
      data: { error: "slow_consumer" },
    });
  });

  it("cancels a one-shot run whose only client it closes as a slow consumer", async () => {
    const hub = new TurnStreamHub({
      clock: FakeWallClock.create(0),
      limits: { maxBufferBytes: 300, queueFrames: 2 },
    });
    const run = manualRun();
    const stream = hub.start({ id: "invoke-x", out: run.out, meta: {}, announce: false });
    const reader = frameReader(hub.respond({ stream, fromSeq: 0, onDisconnect: "cancel" }));
    for (let i = 0; i < 100; i++) run.push(`chunk-${i}`);
    await settle();
    const frames = await reader.rest();
    expect(frames[frames.length - 1]).toMatchObject({ data: { error: "slow_consumer" } });
    await stream.settled;
    expect(run.cancelled()).toBe(1);
  });

  it("closing a detached client as a slow consumer leaves the run going", async () => {
    const hub = new TurnStreamHub({
      clock: FakeWallClock.create(0),
      limits: { maxBufferBytes: 300, queueFrames: 2 },
    });
    const run = manualRun();
    const stream = hub.start({ id: "kept", owner, out: run.out, meta: {}, announce: false });
    const reader = frameReader(hub.respond({ stream, fromSeq: 0, onDisconnect: "detach" }));
    for (let i = 0; i < 100; i++) run.push(`chunk-${i}`);
    await settle();
    await reader.rest();
    expect(run.cancelled()).toBe(0);
    run.finish("done");
    await stream.settled;
  });

  it("answers replay_unavailable when asked to resume from frames already dropped", async () => {
    const hub = new TurnStreamHub({
      clock: FakeWallClock.create(0),
      limits: { maxBufferBytes: 200 },
    });
    const run = manualRun();
    const stream = hub.start({ id: "gone", owner, out: run.out, meta: {}, announce: false });
    for (let i = 0; i < 50; i++) run.push(`chunk-${i}`);
    run.finish("done");
    await stream.settled;
    const frames = await frameReader(
      hub.respond({ stream, fromSeq: 1, onDisconnect: "detach" }),
    ).rest();
    expect(frames).toEqual([{ event: "error", data: { error: "replay_unavailable" } }]);
  });

  it("a client keeping up reads every frame", async () => {
    const hub = new TurnStreamHub({
      clock: FakeWallClock.create(0),
      limits: { maxBufferBytes: 400, queueFrames: 2 },
    });
    const run = manualRun();
    const stream = hub.start({ id: "fast", owner, out: run.out, meta: {}, announce: false });
    const reader = frameReader(hub.respond({ stream, fromSeq: 0, onDisconnect: "detach" }));
    const read: Frame[] = [];
    for (let i = 0; i < 100; i++) {
      run.push(`chunk-${i}`);
      read.push((await reader.next())!);
    }
    run.finish("done");
    read.push(...(await reader.rest()));
    expect(read.filter((f) => f.data?.delta !== undefined)).toHaveLength(100);
    expect(read[read.length - 1]!.event).toBe("finish");
  });
});

describe("TurnStreamHub — memory of finished turns", () => {
  it("a finished turn no longer holds its run", async () => {
    const hub = new TurnStreamHub({ clock: FakeWallClock.create(0) });
    const run = manualRun();
    const stream = hub.start({ id: "done", owner, out: run.out, meta: {}, announce: true });
    run.finish("ok");
    await stream.settled;
    await stream.cancel();
    expect(run.cancelled()).toBe(0);
  });

  it("drops the oldest finished turns beyond the shared byte budget, never a running one", async () => {
    // Each turn below buffers ~60 bytes (its finish frame).
    const hub = new TurnStreamHub({
      clock: FakeWallClock.create(0),
      limits: { maxRetainedBytes: 250 },
    });
    const ownerOf = (i: number): TurnStreamOwner => ({ ...owner, threadId: `th${i}` });
    const live = manualRun();
    const running = hub.start({
      id: "live",
      owner: ownerOf(99),
      out: live.out,
      meta: {},
      announce: false,
    });
    for (let i = 0; i < 6; i++) {
      const run = manualRun();
      const stream = hub.start({
        id: `t${i}`,
        owner: ownerOf(i),
        out: run.out,
        meta: {},
        announce: false,
      });
      run.finish("ok");
      await stream.settled;
    }
    const kept = [0, 1, 2, 3, 4, 5].filter((i) => hub.find({ id: `t${i}`, owner: ownerOf(i) }));
    expect(kept.length).toBeGreaterThan(0);
    expect(kept.length).toBeLessThan(6);
    // The newest are the ones kept.
    expect(kept).toEqual([0, 1, 2, 3, 4, 5].slice(6 - kept.length));
    expect(hub.find({ id: "live", owner: ownerOf(99) })).toBe(running);
    live.finish("ok");
    await running.settled;
  });
});

describe("TurnStreamHub — heartbeats and retention", () => {
  it("sends a heartbeat comment per interval on the hub clock while idle", async () => {
    const clock = FakeWallClock.create(0);
    const hub = new TurnStreamHub({ clock, limits: { heartbeatMs: 1_000 } });
    const run = manualRun();
    const stream = hub.start({ id: "hb", owner, out: run.out, meta: {}, announce: true });
    const reader = frameReader(hub.respond({ stream, fromSeq: 0, onDisconnect: "detach" }));
    expect((await reader.next())!.event).toBe("thread");

    let next: Frame | null | undefined;
    const pending = reader.next().then((f) => (next = f));
    await settle();
    expect(next).toBeUndefined();
    clock.advance(999);
    await settle();
    expect(next).toBeUndefined();
    clock.advance(1);
    await pending;
    expect(next).toEqual({ comment: "heartbeat" });

    run.finish("ok");
    const rest = await reader.rest();
    expect(rest[rest.length - 1]!.event).toBe("finish");
    // The heartbeat stops with the response; the retention timer remains.
    expect(clock.pendingCount()).toBe(1);
  });

  it("keeps a finished turn for the retention window, then drops it", async () => {
    const clock = FakeWallClock.create(0);
    const hub = new TurnStreamHub({ clock, limits: { retentionMs: 60_000 } });
    const run = manualRun();
    const stream = hub.start({ id: "keep", owner, out: run.out, meta: {}, announce: true });
    run.finish("ok");
    await stream.settled;
    expect(hub.find({ id: "keep", owner })).toBe(stream);
    expect(hub.latest(owner)).toBe(stream);
    // Another owner never sees it.
    expect(hub.find({ id: "keep", owner: { ...owner, resourceId: "bob" } })).toBeUndefined();
    clock.advance(59_999);
    expect(hub.find({ id: "keep", owner })).toBe(stream);
    clock.advance(1);
    expect(hub.find({ id: "keep", owner })).toBeUndefined();
  });

  it("runs onSettled once the run settles, also after every client left", async () => {
    const hub = new TurnStreamHub({ clock: FakeWallClock.create(0) });
    const run = manualRun();
    let released = 0;
    const stream = hub.start({
      id: "lease",
      owner,
      out: run.out,
      meta: {},
      announce: true,
      onSettled: async () => {
        released++;
      },
    });
    const reader = frameReader(hub.respond({ stream, fromSeq: 0, onDisconnect: "detach" }));
    await reader.next();
    await reader.cancel();
    await settle();
    expect(released).toBe(0);
    run.fail(new Error("model down"));
    await stream.settled;
    expect(released).toBe(1);
  });
});
