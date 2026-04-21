import { describe, it, expect } from "bun:test";
import { StreamTopology } from "../stream-topology.ts";
import { analyze } from "../topology-analyzer.ts";
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

describe("analyzeTopology", () => {
  it("detects keyBy → stateful op without shuffle", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .tumbling(60_000)
      .count()
      .to(mockSink());

    const warnings = analyze(topology.compiled);

    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.type).toBe("missing-shuffle");
    expect(warnings[0]!.message).toContain("aggregate");
    expect(warnings[0]!.message).toContain("shuffle");
  });

  it("no warning when shuffle is present", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .shuffle()
      .tumbling(60_000)
      .count()
      .to(mockSink());

    const warnings = analyze(topology.compiled);

    expect(warnings).toHaveLength(0);
  });

  it("no warning for keyBy → stateless → sink (no stateful op)", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .map((e) => e.v)
      .to(mockSink());

    const warnings = analyze(topology.compiled);

    expect(warnings).toHaveLength(0);
  });

  it("detects keyBy → process without shuffle", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .process<{ total: number }, number>({
        init: () => ({ total: 0 }),
        process: (state, value) => ({
          state: { total: state.total + value.v },
          emit: state.total + value.v,
        }),
      })
      .to(mockSink());

    const warnings = analyze(topology.compiled);

    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.type).toBe("missing-shuffle");
    expect(warnings[0]!.message).toContain("process");
  });

  it("detects keyBy → dedupe without shuffle", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; id: string }>())
      .keyBy((e) => e.userId)
      .dedupe((e) => e.id)
      .to(mockSink());

    const warnings = analyze(topology.compiled);

    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.message).toContain("dedupe");
  });
});
