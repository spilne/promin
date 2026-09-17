// ---------------------------------------------------------------------------
// InMemoryLeaseStore — runs the leaseStoreTestSuite + a few in-process
// specifics (clock injection, isolated stores).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryLeaseStore } from "../in-memory-lease-store.ts";
import { leaseStoreTestSuite } from "../lease-store-test-suite.ts";

leaseStoreTestSuite((getNow) => new InMemoryLeaseStore({ now: getNow }));

describe("InMemoryLeaseStore — in-process specifics", () => {
  it("two stores in the same process are independent", async () => {
    const a = new InMemoryLeaseStore();
    const b = new InMemoryLeaseStore();
    const ra = await a.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-A",
      ttlMs: 60_000,
    });
    const rb = await b.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-B",
      ttlMs: 60_000,
    });
    expect(ra.acquired).toBe(true);
    expect(rb.acquired).toBe(true);
  });
});
