// ---------------------------------------------------------------------------
// `.map()` — a pure step after the head. The head step keeps its own result,
// codec and options; the transform applies to whatever the head produced
// (a fresh result, a skip value, a fallback) and is checkpointed on its own
// row. Also: `cache.key` of a DAG step sees `prev`.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { MemoryCache } from "../../shared/cache-store.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { workflow } from "../workflow-builder.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

class Boom extends TaggedError("Boom")<{ readonly message: string }>() {}

function setup() {
  const storage = new InMemoryWorkflowStorage();
  return { storage, runner: createWorkflowRunner({ storage }) };
}

describe(".map()", () => {
  it("adds a transform step after the head; the head keeps its unmapped result", async () => {
    const { storage, runner } = setup();
    const wf = workflow<number>({ name: "map-rows" })
      .step("load", ({ input }) => succeed({ n: input }))
      .map((v) => v.n * 2)
      .map((n) => `#${n}`)
      .build();

    const out = await runner.run({ workflow: wf, workflowId: "m1", input: 5 });

    expect(out).toBe("#10");
    expect(wf.dag.steps.map((s) => [s.name, s.kind])).toEqual([
      ["load", "normal"],
      ["load.map", "transform"],
      ["load.map.map", "transform"],
    ]);
    const steps = storage.getWorkflow("m1")?.steps;
    expect(steps?.["load"]?.result).toEqual({ n: 5 });
    expect(steps?.["load.map"]?.result).toBe(10);
    expect(steps?.["load.map.map"]?.result).toBe("#10");
  });

  it("maps the head's prev pass-through when the head is skipped", async () => {
    const { runner } = setup();
    const wf = workflow<number>({ name: "map-skip" })
      .step("seed", ({ input }) => succeed(input))
      .step("double", ({ prev }) => succeed(prev * 2), { skipWhen: (prev) => prev > 5 })
      .map((n) => `mapped:${n}`)
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "s1", input: 7 })).toBe("mapped:7");
    expect(await runner.run({ workflow: wf, workflowId: "s2", input: 2 })).toBe("mapped:4");
  });

  it("maps the head's skipValue and onFailure fallback", async () => {
    const { runner } = setup();
    const skipped = workflow<number>({ name: "map-skip-value" })
      .step("v", () => succeed(1), { skipWhen: () => true, skipValue: () => 40 })
      .map((n) => n + 2)
      .build();
    expect(await runner.run({ workflow: skipped, workflowId: "sv", input: 0 })).toBe(42);

    const fallback = workflow<number>({ name: "map-fallback" })
      .step("v", () => fail(new Boom({ message: "no" })), {
        onFailure: { fallback: () => 20 },
      })
      .map((n) => n + 1)
      .build();
    expect(await runner.run({ workflow: fallback, workflowId: "fb", input: 0 })).toBe(21);
  });

  it("encodes the head with its own codec and the mapped value with the workflow codec", async () => {
    const { storage, runner } = setup();
    const upper: Codec<string> = {
      encode: (v) => v.toUpperCase(),
      decode: (raw) => String(raw).toLowerCase(),
    };
    const wf = workflow<number>({ name: "map-codec" })
      .step("name", () => succeed("ada"), { codec: upper })
      .map((s) => ({ name: s, length: s.length }))
      .build();

    const out = await runner.run({ workflow: wf, workflowId: "c1", input: 0 });

    expect(out).toEqual({ name: "ada", length: 3 });
    const steps = storage.getWorkflow("c1")?.steps;
    expect(steps?.["name"]?.result).toBe("ADA");
    expect(steps?.["name.map"]?.result).toEqual({ name: "ada", length: 3 });
  });

  it("the head's compensate receives the unmapped result", async () => {
    const { runner } = setup();
    const compensated: unknown[] = [];
    const wf = workflow<number>({ name: "map-compensate" })
      .step("reserve", () => succeed({ id: "r1" }), {
        compensate: ({ result }) => {
          compensated.push(result);
          return succeed(undefined);
        },
      })
      .map((r) => r.id)
      .step("charge", () => fail(new Boom({ message: "declined" })))
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "cp", input: 0 });

    expect(r.error).toBeInstanceOf(Boom);
    expect(compensated).toEqual([{ id: "r1" }]);
  });

  it("a later dependsOn on the head sees the unmapped value", async () => {
    const { runner } = setup();
    const wf = workflow<number>({ name: "map-deps" })
      .step("load", ({ input }) => succeed({ n: input }))
      .map((v) => v.n)
      .step("both", { dependsOn: ["load"] }, ({ deps }) => succeed(deps.load))
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "d1", input: 3 })).toEqual({ n: 3 });
  });

  it("picks a free name when <head>.map is taken", async () => {
    const { runner } = setup();
    const wf = workflow<number>({ name: "map-names" })
      .parallelSteps("p", { map: () => succeed(1) })
      .map((rec) => rec.map + 1)
      .build();

    expect(wf.dag.steps.map((s) => s.name)).toEqual(["p.map", "p", "p.map.2"]);
    expect(await runner.run({ workflow: wf, workflowId: "n1", input: 0 })).toBe(2);
  });

  it("fails on an empty workflow", () => {
    expect(() => workflow<number>({ name: "empty" }).map((n) => n)).toThrow(
      "Cannot call .map() on a workflow with no steps",
    );
  });
});

describe("cache.key context", () => {
  it("a DAG step's cache key sees prev (its first dependency's result)", async () => {
    const { runner } = setup();
    const store = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
    const seen: unknown[] = [];
    const wf = workflow<number>({ name: "dag-cache" })
      .step("a", ({ input }) => succeed(input * 10))
      .step("b", () => succeed("b"))
      .step("c", { dependsOn: ["a", "b"] }, ({ deps }) => succeed(deps.a + 1), {
        cache: {
          key: (ctx) => {
            seen.push(ctx.prev);
            return String(ctx.prev);
          },
          ttlMs: 60_000,
          store,
        },
      })
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "k1", input: 4 })).toBe(41);
    expect(seen).toEqual([40]);
  });
});
