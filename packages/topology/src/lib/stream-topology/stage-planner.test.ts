import { describe, it, expect } from "bun:test";
import { StreamTopology } from "./stream-topology.ts";
import { planStages } from "./stage-planner.ts";
import { StreamPipeline } from "@promin/core";
import type { Streamable, Acknowledgeable, Sinkable } from "@promin/core";
import type { Codec } from "@promin/core";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function mockSource<T>(): Streamable<T> & Acknowledgeable<T> {
  const codec: Codec<T> = { encode: (v) => v, decode: (v) => v as T };
  return {
    codec,
    subscribe: () => StreamPipeline.fromIterable([]),
    subscribeAck: () => StreamPipeline.fromIterable([]),
  };
}

function mockSink<T>(): Sinkable<T> {
  return {
    publish: async () => {},
    codec: { encode: (v) => v, decode: (v) => v as T },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("planStages", () => {
  it("no shuffles → single stage", () => {
    const topology = StreamTopology.source(mockSource<{ v: number }>())
      .map((e) => e.v)
      .to(mockSink());

    const plan = planStages({ compiled: topology.compiled, group: "test" });

    expect(plan.stages).toHaveLength(1);
    expect(plan.repartitionTopics).toHaveLength(0);
    expect(plan.stages[0]!.source).toBe("original");
    expect(plan.stages[0]!.sink).toBe("terminal");
  });

  it("one shuffle → two stages with repartition topic", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .map((e) => ({ ...e, v: e.v * 2 }))
      .keyBy((e) => e.userId)
      .shuffle()
      .tumbling(60_000)
      .count()
      .to(mockSink());

    const plan = planStages({ compiled: topology.compiled, group: "counter" });

    expect(plan.stages).toHaveLength(2);
    expect(plan.repartitionTopics).toHaveLength(1);
    expect(plan.repartitionTopics[0]).toBe("counter-repartition-0");

    // Stage 0: source → map → keyBy → publish to repartition
    const stage0 = plan.stages[0]!;
    expect(stage0.source).toBe("original");
    expect(stage0.sink).toEqual({ repartitionTopic: "counter-repartition-0" });
    expect(stage0.keyFn).toBeDefined();

    // Stage 1: repartition → window → aggregate → sink
    const stage1 = plan.stages[1]!;
    expect(stage1.source).toEqual({ repartitionTopic: "counter-repartition-0" });
    expect(stage1.sink).toBe("terminal");
    expect(stage1.sinkNodes).toHaveLength(1);
  });

  it("two shuffles → three stages", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .shuffle()
      .map((e) => ({ ...e, enriched: true as const }))
      .shuffle()
      .tumbling(60_000)
      .count()
      .to(mockSink());

    const plan = planStages({ compiled: topology.compiled, group: "multi" });

    expect(plan.stages).toHaveLength(3);
    expect(plan.repartitionTopics).toHaveLength(2);
    expect(plan.repartitionTopics[0]).toBe("multi-repartition-0");
    expect(plan.repartitionTopics[1]).toBe("multi-repartition-1");
  });

  it("user-specified topic name is respected", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string }>())
      .keyBy((e) => e.userId)
      .shuffle({ topicName: "my-custom-topic" })
      .tumbling(60_000)
      .count()
      .to(mockSink());

    const plan = planStages({ compiled: topology.compiled, group: "custom" });

    expect(plan.repartitionTopics).toEqual(["my-custom-topic"]);
  });
});
