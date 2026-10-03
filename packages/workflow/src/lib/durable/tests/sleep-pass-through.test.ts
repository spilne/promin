// ---------------------------------------------------------------------------
// `.sleep()` is transparent to data flow: the step after a sleep sees the
// value from before it as `prev` (matching the builder types), and the
// sleep's own checkpointed result is that value, round-tripped with the
// predecessor's codec so replay yields the same shape.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const tagOf = (e: unknown) => (e as { _tag?: string } | null | undefined)?._tag;

function setup() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00.000Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, runner };
}

describe(".sleep() — passes the predecessor value through", () => {
  it("the step after a sleep receives the pre-sleep value as prev", async () => {
    const { clock, storage, runner } = setup();
    let seen: unknown = "unset";
    const build = () =>
      workflow<number>({ name: "sleep-prev" })
        .step("a", ({ input }) => succeed(input * 2))
        .sleep("nap", 1_000)
        .step("b", ({ prev }) => {
          seen = prev;
          return succeed(prev.toFixed(1));
        })
        .build();

    const first = await runner.runSafe({ workflow: build(), workflowId: "sp-1", input: 5 });
    expect(tagOf(first.error)).toBe("WorkflowSuspendedError");
    clock.advance(1_000);
    const done = await runner.runSafe({ workflow: build(), workflowId: "sp-1", input: 5 });

    expect(done.error).toBeNull();
    expect(done.data).toBe("10.0");
    expect(seen).toBe(10);
    const state = await storage.loadWorkflow("sp-1");
    expect(state?.steps["nap"]?.status).toBe("completed");
    expect(state?.steps["nap"]?.result).toBe(10);
  });

  it("a leading sleep passes the workflow input through", async () => {
    const { clock, runner } = setup();
    const build = () =>
      workflow<string>({ name: "sleep-first" })
        .sleep("nap", 10)
        .step("echo", ({ prev }) => succeed(`got ${prev}`))
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "sf-1", input: "hi" });
    clock.advance(10);
    const r = await runner.runSafe({ workflow: build(), workflowId: "sf-1", input: "hi" });
    expect(r.data).toBe("got hi");
  });

  it("the sleep row uses the predecessor's codec", async () => {
    const { clock, storage, runner } = setup();
    // A codec that stores Dates as epoch numbers — the default codec would
    // not decode a bare number back to a Date.
    const epochCodec = {
      encode: (d: Date) => d.getTime(),
      decode: (raw: unknown) => new Date(raw as number),
    };
    const build = () =>
      workflow<number>({ name: "sleep-codec" })
        .step("when", ({ input }) => succeed(new Date(input)), { codec: epochCodec })
        .sleep("nap", 10)
        .step("iso", ({ prev }) => succeed(prev.toISOString()))
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "sc-1", input: 0 });
    clock.advance(10);
    const r = await runner.runSafe({ workflow: build(), workflowId: "sc-1", input: 0 });

    expect(r.data).toBe("1970-01-01T00:00:00.000Z");
    const state = await storage.loadWorkflow("sc-1");
    expect(state?.steps["nap"]?.result).toBe(0); // encoded with epochCodec
  });

  it("a replayed sleep row feeds the next sleep and the step after it", async () => {
    const { clock, runner } = setup();
    const build = () =>
      workflow<number>({ name: "sleep-replay" })
        .step("a", ({ input }) => succeed({ at: new Date(input), n: 41 }))
        .sleep("nap-1", 10)
        .sleep("nap-2", 10)
        .step("b", ({ prev }) => succeed(`${prev.at.toISOString()} ${prev.n + 1}`))
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "sr-1", input: 0 });
    clock.advance(10);
    // nap-1 wakes and checkpoints the pass-through value; nap-2 suspends.
    const mid = await runner.runSafe({ workflow: build(), workflowId: "sr-1", input: 0 });
    expect(tagOf(mid.error)).toBe("WorkflowSuspendedError");
    clock.advance(10);
    // nap-1's row is now replayed from storage (decoded) and passed on.
    const r = await runner.runSafe({ workflow: build(), workflowId: "sr-1", input: 0 });
    expect(r.error).toBeNull();
    expect(r.data).toBe("1970-01-01T00:00:00.000Z 42");
  });
});
