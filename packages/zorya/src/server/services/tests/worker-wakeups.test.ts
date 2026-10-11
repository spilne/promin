// ---------------------------------------------------------------------------
// WorkerWakeups — which connected workers are told that work is available,
// how many, and how notices coalesce and follow up. Sockets are an
// in-memory double; the clock is a FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import {
  InMemoryWorkflowAdvertisementRegistry,
  type AdvertisedWorkflow,
  type WorkflowAdvertisementRegistry,
} from "@promin/workflow/distributed";
import { WorkerWakeups, type WakeupSockets } from "../worker-wakeups.ts";
import type { WorkerConnectionEvent, WorkerWsOutbound } from "../worker-ws-server.ts";

class SocketsDouble implements WakeupSockets {
  /** Connected workers that claim starts. */
  readonly connected = new Set<string>();
  /** Connected sockets that never claim (agent, query or control-only). */
  readonly nonClaiming = new Set<string>();
  readonly sent: Array<{ workerId: string; message: WorkerWsOutbound }> = [];
  private readonly handlers = new Set<(e: WorkerConnectionEvent) => void>();
  claimingWorkers(): string[] {
    return [...this.connected];
  }
  send(params: { workerId: string; message: WorkerWsOutbound }): boolean {
    if (!this.connected.has(params.workerId) && !this.nonClaiming.has(params.workerId)) {
      return false;
    }
    this.sent.push(params);
    return true;
  }
  onConnection(handler: (e: WorkerConnectionEvent) => void): () => void {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }
  /** Simulate a worker identifying on its socket. */
  identify(workerId: string, params: { claimsStarts?: boolean } = {}): void {
    const claimsStarts = params.claimsStarts ?? true;
    (claimsStarts ? this.connected : this.nonClaiming).add(workerId);
    for (const h of this.handlers) h({ workerId, connected: true, claimsStarts });
  }
  /** Work messages per worker. */
  counts(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const { workerId, message } of this.sent) {
      if (message.kind === "work") out[workerId] = (out[workerId] ?? 0) + 1;
    }
    return out;
  }
  reset(): void {
    this.sent.length = 0;
  }
}

const ad = (name: string, version?: string): AdvertisedWorkflow => ({
  name,
  ...(version !== undefined && { version }),
  steps: [],
});

let seq = 0;
const start = (workflowName: string, version?: string) => ({
  workflowId: `run-${++seq}`,
  workflowName,
  ...(version !== undefined && { version }),
});

function countingRegistry(clock: FakeWallClock) {
  const registry = new InMemoryWorkflowAdvertisementRegistry({ clock });
  const state = { reads: 0, fail: false };
  const counting: WorkflowAdvertisementRegistry = {
    upsert: (p) => registry.upsert(p),
    remove: (id) => registry.remove(id),
    list: async () => {
      state.reads++;
      if (state.fail) throw new Error("db down");
      return registry.list();
    },
    distinct: () => registry.distinct(),
    prune: () => registry.prune(),
  };
  return { registry: counting, state };
}

function setup(params: { coalesceMs?: number; followUpMs?: number; cacheMs?: number } = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const sockets = new SocketsDouble();
  const { registry, state } = countingRegistry(clock);
  const errors: unknown[] = [];
  const wakeups = new WorkerWakeups({
    sockets,
    advertisements: registry,
    clock,
    coalesceMs: params.coalesceMs ?? 20,
    followUpMs: params.followUpMs ?? 250,
    advertisementCacheMs: params.cacheMs ?? 0,
    onError: (e) => errors.push(e),
  });
  const connect = async (workerId: string, workflows: AdvertisedWorkflow[]) => {
    await registry.upsert({ workerId, workflows });
    sockets.connected.add(workerId);
  };
  /** Send `n` (one per window) so each send goes out at once. */
  const notifyApart = async (n: ReturnType<typeof start>) => {
    clock.advance(1_000);
    wakeups.notify(n);
    await wakeups.idle();
  };
  return { clock, sockets, registry, reads: state, wakeups, errors, connect, notifyApart };
}

describe("WorkerWakeups — who is woken", () => {
  it("wakes only connected workers that advertise the workflow", async () => {
    const { sockets, wakeups, connect, registry } = setup();
    await connect("alpha-worker", [ad("alpha")]);
    await connect("beta-worker", [ad("beta")]);
    // Advertises alpha but has no socket on this instance.
    await registry.upsert({ workerId: "elsewhere", workflows: [ad("alpha")] });

    wakeups.notify(start("alpha"));
    await wakeups.idle();

    expect(sockets.counts()).toEqual({ "alpha-worker": 1 });
    expect(sockets.sent[0]!.message).toEqual({ kind: "work" });
    wakeups.stop();
  });

  it("routes by version like the claim filter (an empty version is no version)", async () => {
    const { sockets, connect, notifyApart, wakeups } = setup();
    await connect("v1", [ad("greet", "1")]);
    await connect("v2", [ad("greet", "2")]);
    await connect("any", [ad("greet")]);
    const woken = async (s: ReturnType<typeof start>) => {
      sockets.reset();
      await notifyApart(s);
      wakeups.claimed([s]);
      return Object.keys(sockets.counts()).sort();
    };

    // One start: wakes at most two (1 + margin) of the matching workers.
    expect(await woken(start("greet", "2"))).toEqual(["any", "v2"]);
    expect(await woken(start("greet", "9"))).toEqual(["any"]);
    for (const version of [undefined, ""]) {
      const got = await woken(start("greet", version));
      expect(got).toHaveLength(2);
      for (const id of got) expect(["v1", "v2", "any"]).toContain(id);
    }
    wakeups.stop();
  });

  it("wakes a connected worker it has no advertisement for (its claim filter decides)", async () => {
    const { sockets, connect, wakeups, notifyApart, reads } = setup({ cacheMs: 60_000 });
    await connect("known", [ad("other")]);
    await notifyApart(start("x"));
    expect(sockets.counts()).toEqual({});
    expect(reads.reads).toBe(1);

    // Connects (or the registry forgot it) after the index was read: woken,
    // and the miss re-reads the index.
    sockets.connected.add("new");
    await notifyApart(start("x"));
    expect(sockets.counts()).toEqual({ new: 1 });
    expect(reads.reads).toBe(2);
    wakeups.stop();
  });

  it("never wakes a socket that does not claim starts, nor counts it in the rotation", async () => {
    const { sockets, connect, wakeups, notifyApart } = setup({ cacheMs: 60_000 });
    sockets.identify("agent-only", { claimsStarts: false });
    await connect("w1", [ad("x")]);
    await connect("w2", [ad("x")]);
    for (let i = 0; i < 4; i++) {
      const s = start("x");
      await notifyApart(s);
      wakeups.claimed([s]);
    }
    expect(sockets.counts()["agent-only"]).toBeUndefined();
    // Every send woke two claiming workers (one start plus the margin).
    expect(sockets.counts()).toEqual({ w1: 4, w2: 4 });
    wakeups.stop();
  });

  it("a claiming worker still unknown after a re-read costs no further reads", async () => {
    const { sockets, connect, wakeups, notifyApart, reads } = setup({ cacheMs: 60_000 });
    await connect("known", [ad("x")]);
    await notifyApart(start("x"));
    expect(reads.reads).toBe(1);

    sockets.connected.add("never-advertises");
    for (let i = 0; i < 5; i++) await notifyApart(start("x"));
    // One re-read for the new miss, then the window holds.
    expect(reads.reads).toBe(2);

    sockets.connected.add("another");
    await notifyApart(start("x"));
    expect(reads.reads).toBe(3);
    wakeups.stop();
  });

  it("wakes a worker as soon as it identifies", async () => {
    const { sockets, wakeups } = setup();
    sockets.identify("w1");
    expect(sockets.counts()).toEqual({ w1: 1 });
    sockets.identify("agent-only", { claimsStarts: false });
    expect(sockets.counts()).toEqual({ w1: 1 });
    wakeups.stop();
    sockets.identify("w2");
    expect(sockets.counts()).toEqual({ w1: 1 });
  });

  it("does nothing while no worker is connected", async () => {
    const { sockets, wakeups, reads, clock } = setup();
    for (let i = 0; i < 10; i++) wakeups.notify(start("x"));
    await wakeups.idle();
    expect(reads.reads).toBe(0);
    expect(clock.pendingCount()).toBe(0);
    expect(sockets.sent).toEqual([]);
  });
});

describe("WorkerWakeups — how many", () => {
  it("wakes about as many workers as there are starts, round-robin across sends", async () => {
    const { sockets, connect, notifyApart, wakeups } = setup();
    const fleet = Array.from({ length: 10 }, (_, i) => `w${i}`);
    for (const id of fleet) await connect(id, [ad("x")]);

    const rounds: string[][] = [];
    for (let i = 0; i < 5; i++) {
      sockets.reset();
      const s = start("x");
      await notifyApart(s);
      wakeups.claimed([s]);
      rounds.push(Object.keys(sockets.counts()));
    }
    // Two per start (1 + margin), a different pair each time, covering the fleet.
    for (const r of rounds) expect(r).toHaveLength(2);
    expect(new Set(rounds.flat()).size).toBe(10);
    wakeups.stop();
  });

  it("an unclaimed start wakes every matching worker after followUpMs, once", async () => {
    const { sockets, connect, wakeups, clock } = setup({ followUpMs: 250 });
    for (const id of ["a", "b", "c", "d"]) await connect(id, [ad("x")]);
    await connect("other", [ad("y")]);

    wakeups.notify(start("x"));
    await wakeups.idle();
    expect(Object.keys(sockets.counts())).toHaveLength(2);
    sockets.reset();

    clock.advance(249);
    await wakeups.idle();
    expect(sockets.sent).toEqual([]);
    clock.advance(1);
    for (let i = 0; i < 10; i++) await Promise.resolve();
    await wakeups.idle();
    expect(sockets.counts()).toEqual({ a: 1, b: 1, c: 1, d: 1 });

    // Followed up once: nothing more, no timer left.
    sockets.reset();
    clock.advance(10_000);
    expect(sockets.sent).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
    wakeups.stop();
  });

  it("a claimed start needs no follow-up", async () => {
    const { sockets, connect, wakeups, clock } = setup({ followUpMs: 250 });
    for (const id of ["a", "b", "c", "d"]) await connect(id, [ad("x")]);
    const s = start("x");
    wakeups.notify(s);
    await wakeups.idle();
    wakeups.claimed([s]);
    sockets.reset();
    clock.advance(1_000);
    for (let i = 0; i < 10; i++) await Promise.resolve();
    expect(sockets.sent).toEqual([]);
    wakeups.stop();
  });
});

describe("WorkerWakeups — coalescing and bounds", () => {
  it("coalesces a burst: one send at once, then one trailing send per window", async () => {
    const { sockets, wakeups, connect, clock } = setup({ coalesceMs: 20 });
    await connect("w1", [ad("burst")]);
    await connect("w2", [ad("burst"), ad("other")]);

    const burst = Array.from({ length: 1_000 }, () => start("burst"));
    for (const s of burst) wakeups.notify(s);
    await wakeups.idle();
    // The first notice went out at once (one start: both workers, with the margin).
    expect(sockets.counts()).toEqual({ w1: 1, w2: 1 });

    clock.advance(19);
    expect(sockets.counts()).toEqual({ w1: 1, w2: 1 });
    clock.advance(1);
    await wakeups.idle();
    expect(sockets.counts()).toEqual({ w1: 2, w2: 2 });
    wakeups.claimed(burst);

    // Quiet since: the next notice goes out at once again.
    clock.advance(100);
    wakeups.notify(start("other"));
    await wakeups.idle();
    expect(sockets.counts()).toEqual({ w1: 2, w2: 3 });
    wakeups.stop();
  });

  it("past the pending cap, the next send wakes every connected worker", async () => {
    const { sockets, wakeups, connect, clock } = setup({ coalesceMs: 20 });
    for (const id of ["a", "b", "c", "d", "e"]) await connect(id, [ad(id)]);
    wakeups.notify(start("a"));
    await wakeups.idle();
    sockets.reset();
    for (let i = 0; i < 1_100; i++) wakeups.notify(start("a"));
    clock.advance(20);
    await wakeups.idle();
    expect(sockets.counts()).toEqual({ a: 1, b: 1, c: 1, d: 1, e: 1 });
    wakeups.stop();
  });

  it("reads the advertisements at most once per cache window", async () => {
    const { connect, wakeups, reads, notifyApart } = setup({ cacheMs: 5_000 });
    await connect("w", [ad("x")]);
    for (let i = 0; i < 4; i++) {
      const s = start("x");
      await notifyApart(s); // 1 s apart
      wakeups.claimed([s]);
    }
    expect(reads.reads).toBe(1);
    await notifyApart(start("x"));
    await notifyApart(start("x"));
    expect(reads.reads).toBe(2);
    wakeups.stop();
  });

  it("reports a failed advertisement read and keeps working", async () => {
    const { sockets, connect, wakeups, reads, errors, notifyApart } = setup();
    await connect("w", [ad("x")]);
    reads.fail = true;
    await notifyApart(start("x"));
    expect(errors).toHaveLength(1);
    expect(sockets.sent).toEqual([]);

    reads.fail = false;
    await notifyApart(start("x"));
    expect(sockets.counts()).toEqual({ w: 1 });
    wakeups.stop();
  });

  it("stop drops pending notices and leaves no timer", async () => {
    const { wakeups, connect, clock, sockets } = setup();
    await connect("w", [ad("x")]);
    wakeups.notify(start("x"));
    await wakeups.idle();
    wakeups.notify(start("x"));
    expect(clock.pendingCount()).toBeGreaterThan(0);
    wakeups.stop();
    expect(clock.pendingCount()).toBe(0);
    clock.advance(1_000);
    wakeups.notify(start("x"));
    await wakeups.idle();
    expect(sockets.counts()).toEqual({ w: 1 });
  });
});
