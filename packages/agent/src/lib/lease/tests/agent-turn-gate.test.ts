// ---------------------------------------------------------------------------
// AgentTurnGate — covers strict policy (acquire/run/release lifecycle,
// reject on contention, lease release on body errors) and the queued
// policy stub (NotImplementedError).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  AgentTurnGate,
  QueuedPolicyNotImplementedError,
  TurnInProgressError,
} from "../agent-turn-gate.ts";
import { InMemoryLeaseStore } from "../in-memory-lease-store.ts";

describe("AgentTurnGate — strict policy", () => {
  it("runs the body and returns its value when the lease is free", async () => {
    const gate = new AgentTurnGate({ leaseStore: new InMemoryLeaseStore() });
    const result = await gate.run({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-A",
      policy: "strict",
      run: async () => "ok",
    });
    expect(result).toBe("ok");
  });

  it("releases the lease after the body completes (next turn can acquire)", async () => {
    const store = new InMemoryLeaseStore();
    const gate = new AgentTurnGate({ leaseStore: store });
    await gate.run({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-A",
      policy: "strict",
      run: async () => "first",
    });
    const second = await gate.run({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-B",
      policy: "strict",
      run: async () => "second",
    });
    expect(second).toBe("second");
  });

  it("throws TurnInProgressError when contended", async () => {
    const store = new InMemoryLeaseStore();
    const gate = new AgentTurnGate({ leaseStore: store });
    // Pre-acquire the lease as worker-A so the gate's own acquire fails.
    await store.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-A",
      ttlMs: 60_000,
    });
    let caught: unknown = null;
    try {
      await gate.run({
        key: { namespaceId: "acme", threadId: "t-1" },
        ownerId: "worker-B",
        policy: "strict",
        run: async () => "should not reach",
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(TurnInProgressError);
    if (caught instanceof TurnInProgressError) {
      expect(caught.currentLease.ownerId).toBe("worker-A");
    }
  });

  it("releases the lease even when the body throws", async () => {
    const store = new InMemoryLeaseStore();
    const gate = new AgentTurnGate({ leaseStore: store });
    let caught: unknown = null;
    try {
      await gate.run({
        key: { namespaceId: "acme", threadId: "t-1" },
        ownerId: "worker-A",
        policy: "strict",
        run: async () => {
          throw new Error("body failed");
        },
      });
    } catch (err) {
      caught = err;
    }
    expect((caught as Error).message).toBe("body failed");
    // Lease should be released — worker-B can now acquire.
    const second = await gate.run({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-B",
      policy: "strict",
      run: async () => "second",
    });
    expect(second).toBe("second");
  });

  it("provides an extend() handle the body can use to push out the TTL", async () => {
    const store = new InMemoryLeaseStore();
    const gate = new AgentTurnGate({ leaseStore: store, defaultTtlMs: 10_000 });
    const seenExpires: number[] = [];
    await gate.run({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-A",
      policy: "strict",
      run: async (lease) => {
        seenExpires.push(lease.expiresAt);
        const extended = await lease.extend(20_000);
        seenExpires.push(extended.expiresAt);
        return "ok";
      },
    });
    expect(seenExpires).toHaveLength(2);
    expect(seenExpires[1]!).toBe(seenExpires[0]! + 20_000);
  });
});

describe("AgentTurnGate — queued policy", () => {
  it("throws QueuedPolicyNotImplementedError (Phase 3 work)", async () => {
    const gate = new AgentTurnGate({ leaseStore: new InMemoryLeaseStore() });
    let caught: unknown = null;
    try {
      await gate.run({
        key: { namespaceId: "acme", threadId: "t-1" },
        ownerId: "worker-A",
        policy: "queued",
        run: async () => "should not reach",
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(QueuedPolicyNotImplementedError);
  });
});
