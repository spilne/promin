// ---------------------------------------------------------------------------
// Unit tests for RedisStream — uses a minimal fake RedisClient that only
// implements XREADGROUP + XACK so we can assert batching behaviour without
// booting a real Redis. The xack-per-batch expectation is the load-bearing
// one: the subscribe-mode loop used to ack per-message (N round-trips per
// poll), and now acks all ids in a single XACK call.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { RedisClient } from "../redis-client.ts";
import { RedisStream } from "../redis-stream.ts";

/**
 * Build a fake Redis client that replays a canned sequence of XREADGROUP
 * batches. Each call to xreadgroup returns one entry from `batches`, then
 * returns null forever (causing the poll loop to spin harmlessly while the
 * test runs `.take(N)` to collect its needed messages).
 *
 * Records every xack call into `ackCalls` so tests can assert "one ack call
 * per poll with N ids" instead of "N ack calls with one id each".
 */
function makeFakeRedis(
  batches: ReadonlyArray<Array<{ id: string; data: unknown; key?: string }>>,
): {
  client: RedisClient;
  ackCalls: Array<{ key: string; group: string; ids: string[] }>;
} {
  let batchIdx = 0;
  const ackCalls: Array<{ key: string; group: string; ids: string[] }> = [];

  const fake: Partial<RedisClient> = {
    async xreadgroup(...args: unknown[]): Promise<unknown> {
      void args;
      if (batchIdx >= batches.length) {
        // Emulate a long BLOCK timeout with nothing to return.
        await new Promise((r) => setTimeout(r, 50));
        return null;
      }
      const batch = batches[batchIdx]!;
      batchIdx++;
      const streamName = (args[args.indexOf("STREAMS") + 1] as string) ?? "stream";
      // ioredis-shape: [[stream, [[id, [field, val, field, val, ...]], ...]]]
      const messages = batch.map((m) => {
        const fields: string[] = ["data", JSON.stringify(m.data)];
        if (m.key !== undefined) fields.push("key", m.key);
        return [m.id, fields];
      });
      return [[streamName, messages]];
    },
    async xack(key: string, group: string, ...ids: string[]): Promise<number> {
      ackCalls.push({ key, group, ids });
      return ids.length;
    },
    async xgroup(...args: unknown[]): Promise<unknown> {
      void args;
      return "OK";
    },
  };

  return { client: fake as RedisClient, ackCalls };
}

describe("RedisStream — batch emit via emit.chunk()", () => {
  it("subscribe() returns every message from the poll batch in order", async () => {
    const { client } = makeFakeRedis([
      [
        { id: "1-0", data: { n: 1 } },
        { id: "1-1", data: { n: 2 } },
        { id: "1-2", data: { n: 3 } },
        { id: "1-3", data: { n: 4 } },
      ],
    ]);

    const stream = new RedisStream<{ n: number }>({
      redis: client,
      stream: "s",
      group: "g",
      count: 10,
      blockMs: 10,
    });

    const values = await stream.subscribe().take(4).collect();
    expect(values.map((v) => v.n)).toEqual([1, 2, 3, 4]);
  });

  it("subscribe() acks the whole batch in a SINGLE xack call (one round-trip per poll)", async () => {
    const { client, ackCalls } = makeFakeRedis([
      [
        { id: "10-0", data: { n: 1 } },
        { id: "10-1", data: { n: 2 } },
        { id: "10-2", data: { n: 3 } },
      ],
    ]);

    const stream = new RedisStream<{ n: number }>({
      redis: client,
      stream: "s",
      group: "g",
      count: 10,
      blockMs: 10,
    });

    await stream.subscribe().take(3).collect();
    // Give the background poll one tick to run the ack after emitting.
    await new Promise((r) => setTimeout(r, 20));

    expect(ackCalls).toHaveLength(1);
    expect(ackCalls[0]!.ids).toEqual(["10-0", "10-1", "10-2"]);
    expect(ackCalls[0]!.group).toBe("g");
  });

  it("subscribeAck() delivers envelopes in order, leaves acking to the caller", async () => {
    const { client, ackCalls } = makeFakeRedis([
      [
        { id: "a-0", data: { n: 1 } },
        { id: "a-1", data: { n: 2 } },
      ],
    ]);

    const stream = new RedisStream<{ n: number }>({
      redis: client,
      stream: "s",
      group: "g",
      count: 10,
      blockMs: 10,
    });

    const envelopes = await stream.subscribeAck().take(2).collect();
    expect(envelopes.map((e) => e.value.n)).toEqual([1, 2]);
    // Manual-ack mode: no auto-ack happened during emit.
    expect(ackCalls).toEqual([]);

    // Manually ack the second envelope — confirms the closure still works.
    await envelopes[1]!.ack();
    expect(ackCalls).toEqual([{ key: "s", group: "g", ids: ["a-1"] }]);
  });

  it("empty XREADGROUP result does not emit (no zero-size chunks)", async () => {
    // Start with a batch that has messages, then nothing. If an empty chunk
    // were emitted, take(1) would still resolve but the test would still
    // pass — so we also assert no ack fired, since the empty batch wouldn't
    // produce any ids.
    const { client, ackCalls } = makeFakeRedis([[{ id: "b-0", data: { n: 99 } }]]);

    const stream = new RedisStream<{ n: number }>({
      redis: client,
      stream: "s",
      group: "g",
      count: 10,
      blockMs: 10,
    });

    const values = await stream.subscribe().take(1).collect();
    expect(values.map((v) => v.n)).toEqual([99]);
    await new Promise((r) => setTimeout(r, 20));
    // Exactly one ack, for the one real batch — no spurious acks from null polls.
    expect(ackCalls).toHaveLength(1);
    expect(ackCalls[0]!.ids).toEqual(["b-0"]);
  });
});
