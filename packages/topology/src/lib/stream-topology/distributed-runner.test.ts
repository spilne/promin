import { describe, it, expect } from "bun:test";
import { StreamTopology } from "./stream-topology.ts";
import { DistributedRunner } from "./distributed-runner.ts";
import { planStages } from "./stage-planner.ts";
import { StreamPipeline } from "@promin/core";
import type { Streamable, Acknowledgeable, Sinkable } from "@promin/core";
import type { Codec } from "@promin/core";
import type { ShuffleTransport } from "./shuffle-transport.ts";

// ---------------------------------------------------------------------------
// In-memory ShuffleTransport for testing
// ---------------------------------------------------------------------------

function createInMemoryTransport(): ShuffleTransport {
  const topics = new Map<string, unknown[]>();

  function getTopic(name: string) {
    if (!topics.has(name)) topics.set(name, []);
    return topics.get(name)!;
  }

  return {
    async getOrCreateRepartitionChannel(params) {
      const items = getTopic(params.name);
      return {
        source: {
          codec: params.codec,
          subscribe: () => StreamPipeline.fromIterable(items),
          subscribeAck: () =>
            StreamPipeline.fromIterable(
              items.map((value) => ({
                value,
                ack: async () => {},
                nack: async () => {},
                metadata: {},
              })),
            ),
        },
        sink: {
          codec: params.codec,
          publish: async (value: unknown) => {
            items.push(value);
          },
        },
      };
    },
  };
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function createTestSource<T>(items: T[]): Streamable<T> & Acknowledgeable<T> {
  const codec: Codec<T> = { encode: (v) => v, decode: (v) => v as T };
  return {
    codec,
    subscribe: () => StreamPipeline.fromIterable(items),
    subscribeAck: () =>
      StreamPipeline.fromIterable(
        items.map((value) => ({
          value,
          ack: async () => {},
          nack: async () => {},
          metadata: {},
        })),
      ),
  };
}

function createTestSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return {
    items,
    codec: { encode: (v) => v, decode: (v) => v as T },
    publish: async (value: T) => {
      items.push(value);
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("DistributedRunner", () => {
  it("single stage (no shuffle) delegates to TopologyRunner", async () => {
    const source = createTestSource([{ v: 1 }, { v: 2 }, { v: 3 }]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((e) => e.v * 2)
      .to(sink);

    const handle = await DistributedRunner.run(topology, {
      group: "no-shuffle",
      shuffleTransport: createInMemoryTransport(),
    });

    // Wait for processing
    await new Promise((r) => setTimeout(r, 200));
    await handle.shutdown();

    expect(sink.items.sort((a, b) => a - b)).toEqual([2, 4, 6]);
  });

  it("two-stage: planner correctly identifies stages", () => {
    const source = createTestSource([
      { userId: "u1", v: 10 },
      { userId: "u2", v: 20 },
      { userId: "u1", v: 30 },
    ]);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.userId)
      .shuffle()
      .map((e) => e)
      .to(createTestSink());

    const plan = planStages({ compiled: topology.compiled, group: "two-stage" });

    expect(plan.stages).toHaveLength(2);
    expect(plan.repartitionTopics).toEqual(["two-stage-repartition-0"]);

    // Stage 0 reads from original, writes to repartition
    expect(plan.stages[0]!.source).toBe("original");
    expect(plan.stages[0]!.sink).toEqual({ repartitionTopic: "two-stage-repartition-0" });
    expect(plan.stages[0]!.keyFn).toBeDefined();

    // Stage 1 reads from repartition, writes to terminal
    expect(plan.stages[1]!.source).toEqual({ repartitionTopic: "two-stage-repartition-0" });
    expect(plan.stages[1]!.sink).toBe("terminal");
  });

  it("metrics are aggregated across stages", async () => {
    const source = createTestSource([{ v: 1 }]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((e) => e.v)
      .to(sink);

    const handle = await DistributedRunner.run(topology, {
      group: "metrics-test",
      shuffleTransport: createInMemoryTransport(),
    });

    await new Promise((r) => setTimeout(r, 200));

    const metrics = handle.metrics();
    expect(metrics.itemsProcessed).toBeGreaterThanOrEqual(0);

    await handle.shutdown();
    expect(handle.isRunning()).toBe(false);
  });
});
