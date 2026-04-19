// ---------------------------------------------------------------------------
// Ensures that step results carrying non-JSON-native values (Date, BigInt,
// Map, Set, Error) survive the round trip through the storage layer and
// come out as the same type on the replay side.
//
// The bug this test prevents: fresh run returns a real Date; replay via
// Postgres/Redis returns a string because JSON.parse strips type info. The
// LosslessJsonCodec default closes that gap. We simulate the round trip by
// putting the in-memory storage through the same JSON.stringify → JSON.parse
// step that real storage backends do.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { createWorkflowRunner } from "./workflow-runner.ts";
import { JsonCodec } from "@promin/core";

/**
 * Wraps an InMemoryWorkflowStorage so every step result round-trips through
 * JSON.stringify / JSON.parse — exactly what Postgres and Redis do under the
 * hood. If the codec is doing its job, the decoded shape downstream is
 * identical to the fresh-run shape.
 */
class JsonRoundTripStorage extends InMemoryWorkflowStorage {
  async saveStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt?: Date;
  }): Promise<void> {
    const roundTripped = JSON.parse(JSON.stringify(params.result));
    await super.saveStepResult({ ...params, result: roundTripped });
  }
}

describe("workflow — step result codec round-trip on replay", () => {
  it("Date survives replay through JSON storage with default codec", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "date-rt" })
      .stepAsync("produce", async () => new Date("2026-04-16T12:00:00Z"))
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "date-rt-1", input: {} });
    const r2 = await runner.run({ workflow: wf, workflowId: "date-rt-1", input: {} });

    expect(r1).toBeInstanceOf(Date);
    expect(r2).toBeInstanceOf(Date);
    expect((r1 as Date).getTime()).toBe((r2 as Date).getTime());
  });

  it("BigInt survives replay", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const big = 12345678901234567890n;

    const wf = workflow({ name: "bigint-rt" })
      .stepAsync("produce", async () => big)
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "bi-1", input: {} });
    const r2 = await runner.run({ workflow: wf, workflowId: "bi-1", input: {} });

    expect(typeof r1).toBe("bigint");
    expect(typeof r2).toBe("bigint");
    expect(r1).toBe(big);
    expect(r2).toBe(big);
  });

  it("Map survives replay with Date keys and BigInt values", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "map-rt" })
      .stepAsync(
        "produce",
        async () =>
          new Map<Date, bigint>([
            [new Date("2026-01-01T00:00:00Z"), 1n],
            [new Date("2026-06-01T00:00:00Z"), 2n],
          ]),
      )
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "map-1", input: {} });
    const r2 = await runner.run({ workflow: wf, workflowId: "map-1", input: {} });

    expect(r1).toBeInstanceOf(Map);
    expect(r2).toBeInstanceOf(Map);
    const firstEntries1 = [...(r1 as Map<Date, bigint>).entries()];
    const firstEntries2 = [...(r2 as Map<Date, bigint>).entries()];
    expect(firstEntries1.length).toBe(2);
    expect(firstEntries2.length).toBe(2);
    expect(firstEntries2[0]![0]).toBeInstanceOf(Date);
    expect(typeof firstEntries2[0]![1]).toBe("bigint");
  });

  it("Set survives replay", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "set-rt" })
      .stepAsync("produce", async () => new Set(["a", "b", "c"]))
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "set-1", input: {} });
    const r2 = await runner.run({ workflow: wf, workflowId: "set-1", input: {} });

    expect(r1).toBeInstanceOf(Set);
    expect(r2).toBeInstanceOf(Set);
    expect((r2 as Set<string>).has("b")).toBe(true);
  });

  it("undefined / NaN / ±Infinity / -0 survive replay", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "special-rt" })
      .stepAsync("produce", async () => ({
        u: undefined,
        nan: Number.NaN,
        inf: Number.POSITIVE_INFINITY,
        ninf: Number.NEGATIVE_INFINITY,
        nz: -0,
      }))
      .build();

    const r2 = (await runner.run({ workflow: wf, workflowId: "special-1", input: {} })) as any;
    await runner.run({ workflow: wf, workflowId: "special-1", input: {} }); // trigger replay path
    const r3 = (await runner.run({ workflow: wf, workflowId: "special-1", input: {} })) as any;

    expect(r3.u).toBeUndefined();
    expect(Number.isNaN(r3.nan)).toBe(true);
    expect(r3.inf).toBe(Infinity);
    expect(r3.ninf).toBe(-Infinity);
    expect(1 / r3.nz).toBe(-Infinity); // confirms -0, not +0
  });

  it("downstream step receives decoded value on fresh run (not encoded shape)", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    let firstStepSawDate = false;
    let secondStepSawDate = false;

    const wf = workflow({ name: "downstream-rt" })
      .stepAsync("produce", async () => {
        const d = new Date("2026-04-16T00:00:00Z");
        firstStepSawDate = d instanceof Date;
        return d;
      })
      .stepAsync(
        "consume",
        async ({ prev }: { prev: unknown }) => {
          secondStepSawDate = prev instanceof Date;
          return (prev as Date).toISOString();
        },
        { codec: JsonCodec },
      )
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "ds-1", input: {} });
    expect(firstStepSawDate).toBe(true);
    // Key assertion: fresh-run downstream step received a Date, not the
    // encoded `{ __t: "date", v: "..." }` object.
    expect(secondStepSawDate).toBe(true);
    expect(result).toBe("2026-04-16T00:00:00.000Z");
  });
});

describe("workflow — explicit JsonCodec opt-out still works", () => {
  it("identity codec skips round-trip; Date becomes string under JSON storage", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "identity-codec" })
      .stepAsync("produce", async () => new Date("2026-04-16T00:00:00Z"), {
        codec: JsonCodec,
      })
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "id-1", input: {} });
    const r2 = await runner.run({ workflow: wf, workflowId: "id-1", input: {} });

    // Fresh run: Date is preserved (no JSON round-trip happened yet on
    // return path because identity codec does nothing).
    expect(r1).toBeInstanceOf(Date);
    // Replay: storage JSON-stringified the Date, parsed it back as a string,
    // and identity decode did nothing — so we get the string back.
    expect(typeof r2).toBe("string");
  });
});

describe("workflow — pipeline-level default codec", () => {
  it("workflow({ codec: JsonCodec }) applies identity to every step by default", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "pipeline-default", codec: JsonCodec })
      .stepAsync("a", async () => new Date("2026-04-16T00:00:00Z"))
      .stepAsync("b", async ({ prev }: { prev: unknown }) => prev)
      .build();

    await runner.run({ workflow: wf, workflowId: "pdc-1", input: {} });
    const replay = await runner.run({ workflow: wf, workflowId: "pdc-1", input: {} });

    // Every step inherited identity JsonCodec → Date lost on replay via JSON
    // storage. Confirms the pipeline-level default propagates.
    expect(typeof replay).toBe("string");
  });

  it("per-step codec still overrides the pipeline-level default", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "pipeline-override", codec: JsonCodec })
      .stepAsync("lossy", async () => new Date("2026-04-16T00:00:00Z"))
      .stepAsync(
        "lossless",
        async ({ prev: _ }: { prev: unknown }) => new Date("2026-05-01T00:00:00Z"),
        { codec: (await import("@promin/core")).LosslessJsonCodec },
      )
      .build();

    await runner.run({ workflow: wf, workflowId: "po-1", input: {} });
    const replay = await runner.run({ workflow: wf, workflowId: "po-1", input: {} });

    // Last step's output (Date from "lossless") must survive because that
    // step overrode the identity default with LosslessJsonCodec.
    expect(replay).toBeInstanceOf(Date);
  });

  it("no pipeline codec set ⇒ default is LosslessJsonCodec (backward compat)", async () => {
    const storage = new JsonRoundTripStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "no-pipeline-codec" })
      .stepAsync("produce", async () => new Date("2026-04-16T00:00:00Z"))
      .build();

    const replay = await runner.run({ workflow: wf, workflowId: "npc-1", input: {} });
    // With no pipeline codec, the step picks up LosslessJsonCodec and the
    // Date survives — unchanged behaviour from before this feature landed.
    await runner.run({ workflow: wf, workflowId: "npc-1", input: {} });
    const r2 = await runner.run({ workflow: wf, workflowId: "npc-1", input: {} });
    expect(replay).toBeInstanceOf(Date);
    expect(r2).toBeInstanceOf(Date);
  });
});
