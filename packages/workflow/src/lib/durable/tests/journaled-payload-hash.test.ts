// ---------------------------------------------------------------------------
// Payload-hash tests for ctx.activity.
//
// Covers:
//   * 2-arg ctx.activity(name, fn) still works unchanged.
//   * 3-arg ctx.activity(name, input, fn) passes the input through.
//   * Opting in via options.payloadHash writes a hash to the journal row.
//   * Pipeline-level runJournaledStep `payloadHash: true` hashes every 3-arg
//     call by default.
//   * Per-activity `payloadHash: false` opts out even when the pipeline
//     default is on.
//   * Requesting `payloadHash` with the 2-arg form throws.
//   * Equal inputs hash identically; different inputs hash differently.
//   * Replay with matching input hash returns the recorded value.
//   * Replay with drifted input hash throws JournalNonDeterminismError.
//   * Asymmetric opt-in across runs (hash recorded but current opted out,
//     or vice versa) does NOT throw — hashing is a monitor, not a gate.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { runJournaledStep, JournalNonDeterminismError } from "../journaled-step.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";

describe("ctx.activity — 3-arg overload", () => {
  it("passes the explicit input to the activity fn", async () => {
    const storage = new InMemoryWorkflowStorage();
    const result = await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-3arg",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return yield* ctx.activity("double", 21, async (x: number) => x * 2);
      },
    });
    expect(result).toBe(42);
  });

  it("2-arg form still works (no input reification, no hash)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const result = await runJournaledStep<unknown, unknown, string>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-2arg",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return yield* ctx.activity("greet", async () => "hi");
      },
    });
    expect(result).toBe("hi");
    const [entry] = await storage.loadJournal("wf-2arg", "s");
    expect(entry!.payloadHash).toBeUndefined();
  });
});

describe("ctx.activity — payloadHash per-activity opt-in", () => {
  it("options.payloadHash:true writes a 64-char hex fingerprint", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-hash",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return yield* ctx.activity("inc", 1, async (x: number) => x + 1, {
          payloadHash: true,
        });
      },
    });
    const [entry] = await storage.loadJournal("wf-hash", "s");
    expect(entry!.payloadHash).toMatch(/^[0-9a-f]{64}$/);
  });

  it("same input hashes identically across runs", async () => {
    const input = { orderId: "ord-1", qty: 3 };
    const run = async (wid: string): Promise<string | undefined> => {
      const storage = new InMemoryWorkflowStorage();
      await runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: wid,
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("charge", input, async () => 1, {
            payloadHash: true,
          });
        },
      });
      const [entry] = await storage.loadJournal(wid, "s");
      return entry!.payloadHash;
    };
    const h1 = await run("wf-eq-1");
    const h2 = await run("wf-eq-2");
    expect(h1).toBe(h2);
    expect(h1).toBeDefined();
  });

  it("different input hashes differently", async () => {
    const hashFor = async (orderId: string): Promise<string | undefined> => {
      const storage = new InMemoryWorkflowStorage();
      await runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: `wf-${orderId}`,
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("charge", { orderId }, async () => 1, {
            payloadHash: true,
          });
        },
      });
      const [entry] = await storage.loadJournal(`wf-${orderId}`, "s");
      return entry!.payloadHash;
    };
    const a = await hashFor("A");
    const b = await hashFor("B");
    expect(a).not.toBe(b);
  });

  it("throws when payloadHash is requested with the 2-arg form", async () => {
    const storage = new InMemoryWorkflowStorage();
    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-bad",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("bad", async () => 1, {
            payloadHash: true,
          });
        },
      }),
    ).rejects.toThrow(/3-arg form/);
  });
});

describe("ctx.activity — pipeline-level payloadHash default", () => {
  it("runJournaledStep payloadHash:true hashes every 3-arg activity", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-pipe",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        yield* ctx.activity("a", { v: 1 }, async (x: { v: number }) => x.v);
        return yield* ctx.activity("b", { v: 2 }, async (x: { v: number }) => x.v);
      },
    });
    const journal = await storage.loadJournal("wf-pipe", "s");
    expect(journal).toHaveLength(2);
    for (const entry of journal) {
      expect(entry.payloadHash).toMatch(/^[0-9a-f]{64}$/);
    }
    // The two activities have different inputs → different hashes.
    expect(journal[0]!.payloadHash).not.toBe(journal[1]!.payloadHash);
  });

  it("2-arg activities are unaffected by the pipeline default", async () => {
    // Mixing a 2-arg call under a pipeline default of `true` is allowed —
    // only 3-arg calls hash; the 2-arg one stays without a fingerprint
    // rather than throwing. Throwing would punish existing codebases that
    // opt into pipeline-level hashing.
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-mix",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        const a = yield* ctx.activity("two-arg", async () => 1);
        const b = yield* ctx.activity("three-arg", 5, async (x: number) => x + a);
        return b;
      },
    });
    const journal = await storage.loadJournal("wf-mix", "s");
    const twoArg = journal.find((e) => e.activityName === "two-arg")!;
    const threeArg = journal.find((e) => e.activityName === "three-arg")!;
    expect(twoArg.payloadHash).toBeUndefined();
    expect(threeArg.payloadHash).toMatch(/^[0-9a-f]{64}$/);
  });

  it("per-activity payloadHash:false opts out under a pipeline default of true", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-optout",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        return yield* ctx.activity("skip", { v: 1 }, async (x: { v: number }) => x.v, {
          payloadHash: false,
        });
      },
    });
    const [entry] = await storage.loadJournal("wf-optout", "s");
    expect(entry!.payloadHash).toBeUndefined();
  });
});

describe("ctx.activity — 3-arg under ctx.parallel", () => {
  it("each branch's activity gets its own hash", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, unknown>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-par",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        yield* ctx.parallel([
          ctx.activity("x", { side: "L" }, async (i: { side: string }) => i.side),
          ctx.activity("y", { side: "R" }, async (i: { side: string }) => i.side),
        ]);
        return null;
      },
    });
    const journal = await storage.loadJournal("wf-par", "s");
    const x = journal.find((e) => e.activityName === "x")!;
    const y = journal.find((e) => e.activityName === "y")!;
    expect(x.payloadHash).toMatch(/^[0-9a-f]{64}$/);
    expect(y.payloadHash).toMatch(/^[0-9a-f]{64}$/);
    expect(x.payloadHash).not.toBe(y.payloadHash);
  });
});

describe("ctx.activity — replay guard", () => {
  it("replay with the same input returns the recorded value", async () => {
    const storage = new InMemoryWorkflowStorage();
    let ran = 0;

    const run = async (input: { id: string }): Promise<number> =>
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-ok",
        stepName: "s",
        storage,
        payloadHash: true,
        body: function* (ctx) {
          return yield* ctx.activity("charge", input, async (i: { id: string }) => {
            ran++;
            return i.id.length;
          });
        },
      });

    const fresh = await run({ id: "abc" });
    const replay = await run({ id: "abc" });
    expect(fresh).toBe(3);
    expect(replay).toBe(3);
    expect(ran).toBe(1); // replay hit the journal
  });

  it("replay with a drifted input throws JournalNonDeterminismError", async () => {
    const storage = new InMemoryWorkflowStorage();

    // First run records `{ id: "abc" }`'s fingerprint.
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-drift",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        return yield* ctx.activity(
          "charge",
          { id: "abc" },
          async (i: { id: string }) => i.id.length,
        );
      },
    });

    // Second run passes a DIFFERENT input for the same activity name.
    // The recorded hash disagrees → throw.
    let thrown: unknown;
    try {
      await runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-drift",
        stepName: "s",
        storage,
        payloadHash: true,
        body: function* (ctx) {
          return yield* ctx.activity(
            "charge",
            { id: "xyz" },
            async (i: { id: string }) => i.id.length,
          );
        },
      });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(JournalNonDeterminismError);
    const e = thrown as JournalNonDeterminismError;
    expect(e.stepName).toBe("s");
    expect(e.activityIndex).toBe(0);
    expect(e.expected).toMatch(/^payloadHash=[0-9a-f]{64}$/);
    expect(e.actual).toMatch(/^payloadHash=[0-9a-f]{64}$/);
    expect(e.expected).not.toBe(e.actual);
    expect(e.message).toContain("payloadHash");
  });

  it("hash recorded but replay opted out — no check, no throw", async () => {
    const storage = new InMemoryWorkflowStorage();

    // Fresh run: pipeline default ON → hash recorded.
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-off-replay",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        return yield* ctx.activity("a", { v: 1 }, async (i: { v: number }) => i.v);
      },
    });
    const recorded = (await storage.loadJournal("wf-off-replay", "s"))[0]!;
    expect(recorded.payloadHash).toBeDefined();

    // Replay: pipeline default OFF and caller feeds a DIFFERENT input.
    // Replay should still succeed — the engine returns the recorded value
    // without looking at the hash because the current run didn't compute one.
    const replay = await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-off-replay",
      stepName: "s",
      storage,
      // NO payloadHash here — pipeline default is off on replay.
      body: function* (ctx) {
        return yield* ctx.activity("a", { v: 999 }, async (i: { v: number }) => i.v);
      },
    });
    expect(replay).toBe(1); // came from the journal, not from the new input
  });

  it("no hash recorded but replay opted in — no check, no throw", async () => {
    const storage = new InMemoryWorkflowStorage();

    // Fresh run: no hashing.
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-only-replay",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return yield* ctx.activity("a", { v: 1 }, async (i: { v: number }) => i.v);
      },
    });
    const recorded = (await storage.loadJournal("wf-only-replay", "s"))[0]!;
    expect(recorded.payloadHash).toBeUndefined();

    // Replay: hashing turned on. Recorded has no hash to compare against,
    // so we skip the check and return the recorded value.
    const replay = await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-only-replay",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        return yield* ctx.activity("a", { v: 1 }, async (i: { v: number }) => i.v);
      },
    });
    expect(replay).toBe(1);
  });

  it("hash mismatch inside a ctx.parallel branch throws from that branch", async () => {
    const storage = new InMemoryWorkflowStorage();

    await runJournaledStep<unknown, unknown, unknown>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-par-drift",
      stepName: "s",
      storage,
      payloadHash: true,
      body: function* (ctx) {
        yield* ctx.parallel([
          ctx.activity("left", { side: "L" }, async (i: { side: string }) => i.side),
          ctx.activity("right", { side: "R" }, async (i: { side: string }) => i.side),
        ]);
        return null;
      },
    });

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-par-drift",
        stepName: "s",
        storage,
        payloadHash: true,
        body: function* (ctx) {
          yield* ctx.parallel([
            ctx.activity("left", { side: "L" }, async (i: { side: string }) => i.side),
            // Drifted input for the right branch.
            ctx.activity("right", { side: "DIFFERENT" }, async (i: { side: string }) => i.side),
          ]);
          return null;
        },
      }),
    ).rejects.toThrow(/payloadHash/);
  });
});
