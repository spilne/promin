// ---------------------------------------------------------------------------
// Portable `LeaseStore` conformance suite. Every implementation
// (in-memory, Postgres, future variants) must pass.
//
// Usage:
//   import { leaseStoreTestSuite } from "@promin/agent/testing";
//   leaseStoreTestSuite(() => new InMemoryLeaseStore());
//
// Tests use a manual clock (the suite passes `now` into a closure that
// each impl threads into its config) so we can expire leases without
// real wall-clock waits. Implementations that don't accept a clock
// override should still pass with real time, just slower.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { LeaseStore } from "./types.ts";

export interface LeaseStoreFactory {
  /**
   * Build a fresh, empty LeaseStore. The store should be wired to read
   * `getNow()` (a closure the suite mutates between sub-tests) so the
   * suite can simulate time passing without sleeping.
   */
  (getNow: () => number): LeaseStore | Promise<LeaseStore>;
}

export function leaseStoreTestSuite(factory: LeaseStoreFactory) {
  // Each test gets a fresh clock-controlled store via `make()`.
  async function make(): Promise<{
    store: LeaseStore;
    advance: (ms: number) => void;
    now: () => number;
  }> {
    let nowMs = 1_700_000_000_000; // arbitrary fixed start
    const getNow = () => nowMs;
    const advance = (ms: number) => {
      nowMs += ms;
    };
    const store = await factory(getNow);
    return { store, advance, now: getNow };
  }

  describe("LeaseStore conformance", () => {
    describe("acquire", () => {
      it("grants a lease on an unowned key", async () => {
        const { store, now } = await make();
        const r = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 60_000,
        });
        expect(r.acquired).toBe(true);
        if (r.acquired) {
          expect(r.lease.ownerId).toBe("worker-A");
          expect(r.lease.key.namespaceId).toBe("acme");
          expect(r.lease.key.threadId).toBe("t-1");
          expect(r.lease.acquiredAt).toBe(now());
          expect(r.lease.expiresAt).toBe(now() + 60_000);
          expect(r.lease.leaseId).toBeTruthy();
        }
      });

      it("rejects when another owner holds an unexpired lease", async () => {
        const { store } = await make();
        await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 60_000,
        });
        const r = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-B",
          ttlMs: 60_000,
        });
        expect(r.acquired).toBe(false);
        if (!r.acquired) {
          expect(r.currentLease.ownerId).toBe("worker-A");
        }
      });

      it("steals an expired lease and grants a fresh one", async () => {
        const { store, advance } = await make();
        const first = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 1_000,
        });
        expect(first.acquired).toBe(true);
        advance(1_500);
        const r = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-B",
          ttlMs: 60_000,
        });
        expect(r.acquired).toBe(true);
        if (r.acquired && first.acquired) {
          expect(r.lease.ownerId).toBe("worker-B");
          expect(r.lease.leaseId).not.toBe(first.lease.leaseId);
        }
      });

      it("rejects re-acquire by the SAME owner while their own lease is live", async () => {
        // Same-owner double acquire still rejects — caller should call
        // extend(), not acquire(), to renew. Avoids accidental re-entry.
        const { store } = await make();
        await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 60_000,
        });
        const r = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 60_000,
        });
        expect(r.acquired).toBe(false);
      });

      it("treats namespaceId+threadId tuple as the key (different threads do not collide)", async () => {
        const { store } = await make();
        const a = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 60_000,
        });
        const b = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-2" },
          ownerId: "worker-B",
          ttlMs: 60_000,
        });
        const c = await store.acquire({
          key: { namespaceId: "globex", threadId: "t-1" },
          ownerId: "worker-C",
          ttlMs: 60_000,
        });
        expect(a.acquired).toBe(true);
        expect(b.acquired).toBe(true);
        expect(c.acquired).toBe(true);
      });
    });

    describe("extend", () => {
      it("extends a live lease by additionalMs", async () => {
        const { store } = await make();
        const acq = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 60_000,
        });
        if (!acq.acquired) throw new Error("setup: acquire failed");
        const before = acq.lease.expiresAt;
        const r = await store.extend({ leaseId: acq.lease.leaseId, additionalMs: 30_000 });
        expect(r.extended).toBe(true);
        if (r.extended) {
          expect(r.lease.expiresAt).toBe(before + 30_000);
          expect(r.lease.leaseId).toBe(acq.lease.leaseId);
        }
      });

      it("refuses to extend an expired lease (caller must re-acquire)", async () => {
        const { store, advance } = await make();
        const acq = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 1_000,
        });
        if (!acq.acquired) throw new Error("setup: acquire failed");
        advance(2_000);
        const r = await store.extend({ leaseId: acq.lease.leaseId, additionalMs: 30_000 });
        expect(r.extended).toBe(false);
      });

      it("refuses to extend an unknown leaseId", async () => {
        const { store } = await make();
        const r = await store.extend({
          leaseId: "00000000-0000-0000-0000-000000000000",
          additionalMs: 30_000,
        });
        expect(r.extended).toBe(false);
        if (!r.extended) {
          expect(r.currentLease).toBeNull();
        }
      });

      it("refuses to extend after the lease has been stolen by another owner", async () => {
        const { store, advance } = await make();
        const a = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 1_000,
        });
        if (!a.acquired) throw new Error("setup: acquire failed");
        advance(2_000);
        const b = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-B",
          ttlMs: 60_000,
        });
        if (!b.acquired) throw new Error("setup: steal failed");
        const r = await store.extend({ leaseId: a.lease.leaseId, additionalMs: 30_000 });
        expect(r.extended).toBe(false);
      });
    });

    describe("release", () => {
      it("releases a held lease so a new acquire succeeds", async () => {
        const { store } = await make();
        const a = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 60_000,
        });
        if (!a.acquired) throw new Error("setup: acquire failed");
        await store.release({ leaseId: a.lease.leaseId });
        const b = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-B",
          ttlMs: 60_000,
        });
        expect(b.acquired).toBe(true);
      });

      it("is a no-op for an unknown leaseId", async () => {
        const { store } = await make();
        await store.release({ leaseId: "00000000-0000-0000-0000-000000000000" });
        // Survives without throwing.
      });

      it("does not release if a different owner now holds the lease", async () => {
        const { store, advance } = await make();
        const a = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 1_000,
        });
        if (!a.acquired) throw new Error("setup: acquire failed");
        advance(2_000);
        const b = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-B",
          ttlMs: 60_000,
        });
        if (!b.acquired) throw new Error("setup: steal failed");
        await store.release({ leaseId: a.lease.leaseId });
        const cur = await store.get({ namespaceId: "acme", threadId: "t-1" });
        expect(cur?.ownerId).toBe("worker-B");
      });
    });

    describe("get", () => {
      it("returns null when no lease exists", async () => {
        const { store } = await make();
        const r = await store.get({ namespaceId: "acme", threadId: "missing" });
        expect(r).toBeNull();
      });

      it("returns the current lease (even if expired)", async () => {
        const { store, advance } = await make();
        const a = await store.acquire({
          key: { namespaceId: "acme", threadId: "t-1" },
          ownerId: "worker-A",
          ttlMs: 1_000,
        });
        if (!a.acquired) throw new Error("setup");
        advance(2_000);
        const cur = await store.get({ namespaceId: "acme", threadId: "t-1" });
        expect(cur?.ownerId).toBe("worker-A");
      });
    });

    describe("contention races (same key, two acquires)", () => {
      it("exactly one acquire wins when both run concurrently", async () => {
        const { store } = await make();
        const [a, b] = await Promise.all([
          store.acquire({
            key: { namespaceId: "acme", threadId: "t-1" },
            ownerId: "worker-A",
            ttlMs: 60_000,
          }),
          store.acquire({
            key: { namespaceId: "acme", threadId: "t-1" },
            ownerId: "worker-B",
            ttlMs: 60_000,
          }),
        ]);
        const winners = [a.acquired, b.acquired].filter((x) => x);
        expect(winners.length).toBe(1);
      });
    });
  });
}
