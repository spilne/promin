// ---------------------------------------------------------------------------
// Tests for instrumentNonDeterminism() — verifies global patching detects
// Date.now / Math.random / setTimeout / fetch calls inside journaled step
// bodies, passes them through untouched outside the scope, and restores
// the originals when .stop() is called.
// ---------------------------------------------------------------------------

import { afterEach, describe, it, expect } from "bun:test";
import { runJournaledStep } from "../lib/durable/journaled-step.ts";
import { InMemoryWorkflowStorage } from "../lib/durable/in-memory-storage.ts";
import { instrumentNonDeterminism, isInJournaledBody } from "../dev.ts";

// Each test starts without instrumentation active.
let activeHandles: ReturnType<typeof instrumentNonDeterminism>[] = [];

afterEach(() => {
  for (const h of activeHandles) h.stop();
  activeHandles = [];
});

function install(options: Parameters<typeof instrumentNonDeterminism>[0] = {}) {
  const h = instrumentNonDeterminism(options);
  activeHandles.push(h);
  return h;
}

describe("isInJournaledBody", () => {
  it("is false outside any journaled step", () => {
    expect(isInJournaledBody()).toBe(false);
  });

  it("is true inside a journaled step body between yields", async () => {
    const storage = new InMemoryWorkflowStorage();
    let sawInBody = false;

    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-scope",
      stepName: "s",
      storage,
      body: function* (ctx) {
        // Body code runs inside the scope
        sawInBody = isInJournaledBody();
        yield* ctx.activity("a", async () => 1);
      },
    });

    expect(sawInBody).toBe(true);
    // Scope ends once the runner returns
    expect(isInJournaledBody()).toBe(false);
  });
});

describe("instrumentNonDeterminism — warn mode (default)", () => {
  it("reports Date.now / Math.random calls from inside a journaled body", async () => {
    const events: Array<{ api: string; stepName: string }> = [];
    install({ onCall: (e) => events.push({ api: e.api, stepName: e.stepName }) });

    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-warn",
      stepName: "checkout",
      storage,
      body: function* (ctx) {
        Date.now(); // <- flagged
        Math.random(); // <- flagged
        yield* ctx.activity("price", async () => 100);
      },
    });

    const apis = events.map((e) => e.api).sort();
    expect(apis).toEqual(["Date.now", "Math.random"]);
    expect(events[0]!.stepName).toBe("checkout");
  });

  it("does NOT report calls from outside any journaled body", async () => {
    const events: string[] = [];
    install({ onCall: (e) => events.push(e.api) });

    // No workflow — just plain user code.
    Date.now();
    Math.random();

    expect(events).toEqual([]);
  });

  it("does NOT report calls from inside an activity fn (side effects are allowed)", async () => {
    const events: string[] = [];
    install({ onCall: (e) => events.push(e.api) });

    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-activity-ok",
      stepName: "s",
      storage,
      body: function* (ctx) {
        yield* ctx.activity("external", async () => {
          // Inside an activity fn, random + time are FINE — they're the
          // journaled value.
          return Date.now() + Math.random();
        });
      },
    });

    expect(events).toEqual([]);
  });

  it("reports setTimeout and fetch from inside the body", async () => {
    const events: string[] = [];
    install({ onCall: (e) => events.push(e.api) });

    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-timers",
      stepName: "s",
      storage,
      body: function* (ctx) {
        // Indirect call — a helper defined elsewhere uses setTimeout.
        setTimeout(() => {}, 0);
        // fetch may or may not be defined depending on runtime; we still
        // want the patching to be a no-op if not.
        if (typeof fetch === "function") fetch("http://example.test").catch(() => undefined);
        yield* ctx.activity("noop", async () => 1);
      },
    });

    expect(events).toContain("setTimeout");
    if (typeof fetch === "function") expect(events).toContain("fetch");
  });
});

describe("instrumentNonDeterminism — strict mode", () => {
  it("throws when a non-deterministic global is invoked inside a body", async () => {
    install({ mode: "strict" });

    const storage = new InMemoryWorkflowStorage();
    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "wf-strict",
        stepName: "s",
        storage,
        body: function* (ctx) {
          Date.now();
          yield* ctx.activity("unreached", async () => 1);
        },
      }),
    ).rejects.toThrow(/non-deterministic call Date.now/);
  });
});

describe("instrumentNonDeterminism — handle.stop()", () => {
  it("restores originals so subsequent calls behave normally", async () => {
    const events: string[] = [];
    const h = install({ onCall: (e) => events.push(e.api) });
    h.stop();

    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-stopped",
      stepName: "s",
      storage,
      body: function* (ctx) {
        Date.now(); // should NOT be reported — handle stopped
        yield* ctx.activity("a", async () => 1);
      },
    });

    expect(events).toEqual([]);
  });

  it("stop() is idempotent", () => {
    const h = install();
    h.stop();
    expect(() => h.stop()).not.toThrow();
  });
});

describe("instrumentNonDeterminism — custom onCall", () => {
  it("suppresses the default warning / throw when onCall is provided", async () => {
    const events: Array<{ api: string; mode: string }> = [];
    install({
      mode: "strict", // would normally throw — but onCall overrides
      onCall: (e) => events.push({ api: e.api, mode: e.mode }),
    });

    const storage = new InMemoryWorkflowStorage();
    // Should NOT throw — onCall swallows the default strict behaviour.
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-oncall",
      stepName: "s",
      storage,
      body: function* (ctx) {
        Date.now();
        yield* ctx.activity("a", async () => 1);
      },
    });

    expect(events.length).toBe(1);
    expect(events[0]!.api).toBe("Date.now");
    expect(events[0]!.mode).toBe("strict");
  });
});
