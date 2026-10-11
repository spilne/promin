// ---------------------------------------------------------------------------
// Portable `PendingApprovalStore` conformance suite. Every implementation
// (in-memory, SQLite, Postgres) must pass.
//
// Usage:
//   import { pendingApprovalStoreTestSuite } from "@promin/agent/testing";
//   pendingApprovalStoreTestSuite(async () => {
//     const clock = FakeWallClock.create(1_700_000_000_000);
//     return {
//       store: new InMemoryPendingApprovalStore({ clock }),
//       advance: async (ms) => clock.advance(ms),
//     };
//   });
//
// `advance` moves the store's time forward. Stores on an injected clock
// advance it; a store on the database server's clock waits real time.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type {
  PendingApprovalStore,
  RecordPendingApprovalParams,
} from "./pending-approval-store.ts";

export interface PendingApprovalStoreHarness {
  readonly store: PendingApprovalStore;
  /** Move the store's time forward by at least `ms`. */
  advance(ms: number): Promise<void>;
}

export type PendingApprovalStoreFactory = () => Promise<PendingApprovalStoreHarness>;

function request(
  overrides: Partial<RecordPendingApprovalParams> = {},
): RecordPendingApprovalParams {
  return {
    namespaceId: "acme",
    threadId: "t",
    toolCallId: "call-1",
    workflowId: "wf-1",
    toolName: "deploy",
    input: { env: "prod" },
    ...overrides,
  };
}

export function pendingApprovalStoreTestSuite(factory: PendingApprovalStoreFactory): void {
  describe("PendingApprovalStore conformance", () => {
    describe("record / get", () => {
      it("records a pending approval and reads it back by its exact key", async () => {
        const { store } = await factory();
        const row = await store.record(
          request({ resourceId: "alice", agentId: "ops-bot", input: { env: "prod", n: 2 } }),
        );
        expect(row).toMatchObject({
          namespaceId: "acme",
          threadId: "t",
          toolCallId: "call-1",
          resourceId: "alice",
          agentId: "ops-bot",
          workflowId: "wf-1",
          toolName: "deploy",
          input: { env: "prod", n: 2 },
          status: "pending",
          expiresAt: null,
          decidedBy: null,
          decidedAt: null,
          reason: null,
          deliveredAt: null,
        });
        expect(row.requestedAt).toBeGreaterThan(0);
        expect(
          await store.get({ namespaceId: "acme", threadId: "t", toolCallId: "call-1" }),
        ).toEqual(row);
      });

      it("returns null for an unknown key", async () => {
        const { store } = await factory();
        expect(
          await store.get({ namespaceId: "acme", threadId: "t", toolCallId: "nope" }),
        ).toBeNull();
      });

      it("re-recording the same workflow's request keeps the first row", async () => {
        const { store, advance } = await factory();
        const first = await store.record(request());
        await advance(20);
        const again = await store.record(request({ input: { changed: true } }));
        expect(again).toEqual(first);
      });

      it("a request of the same key from another workflow replaces the row", async () => {
        const { store } = await factory();
        await store.record(request());
        await store.decide({
          namespaceId: "acme",
          threadId: "t",
          toolCallId: "call-1",
          approved: true,
        });
        const replaced = await store.record(request({ workflowId: "wf-2" }));
        expect(replaced.workflowId).toBe("wf-2");
        expect(replaced.status).toBe("pending");
      });

      it("never matches a thread id by prefix", async () => {
        const { store } = await factory();
        await store.record(request({ threadId: "t-1", workflowId: "wf-t1" }));
        expect(
          await store.get({ namespaceId: "acme", threadId: "t", toolCallId: "call-1" }),
        ).toBeNull();
        await store.record(request({ threadId: "t", workflowId: "wf-t" }));
        const t = await store.get({ namespaceId: "acme", threadId: "t", toolCallId: "call-1" });
        const t1 = await store.get({ namespaceId: "acme", threadId: "t-1", toolCallId: "call-1" });
        expect(t?.workflowId).toBe("wf-t");
        expect(t1?.workflowId).toBe("wf-t1");
        const listed = await store.list({ namespaceId: "acme", threadId: "t" });
        expect(listed.map((r) => r.workflowId)).toEqual(["wf-t"]);
      });

      it("keeps two namespaces with the same thread id apart", async () => {
        const { store } = await factory();
        await store.record(request({ namespaceId: "acme", workflowId: "wf-acme" }));
        await store.record(request({ namespaceId: "globex", workflowId: "wf-globex" }));
        const acme = await store.get({ namespaceId: "acme", threadId: "t", toolCallId: "call-1" });
        const globex = await store.get({
          namespaceId: "globex",
          threadId: "t",
          toolCallId: "call-1",
        });
        expect(acme?.workflowId).toBe("wf-acme");
        expect(globex?.workflowId).toBe("wf-globex");
        await store.decide({
          namespaceId: "acme",
          threadId: "t",
          toolCallId: "call-1",
          approved: true,
        });
        expect(
          (await store.get({ namespaceId: "globex", threadId: "t", toolCallId: "call-1" }))?.status,
        ).toBe("pending");
      });
    });

    describe("expiry", () => {
      it("stamps expiresAt from ttlMs", async () => {
        const { store } = await factory();
        const row = await store.record(request({ ttlMs: 60_000 }));
        expect(row.expiresAt).toBe(row.requestedAt + 60_000);
      });

      it("refuses to approve an expired approval but still rejects it", async () => {
        const { store, advance } = await factory();
        await store.record(request({ ttlMs: 30 }));
        await advance(60);
        const key = { namespaceId: "acme", threadId: "t", toolCallId: "call-1" };
        const approve = await store.decide({ ...key, approved: true });
        expect(approve.outcome).toBe("expired");
        expect((await store.get(key))?.status).toBe("pending");
        const reject = await store.decide({ ...key, approved: false, reason: "too late" });
        expect(reject.outcome).toBe("decided");
        expect((await store.get(key))?.status).toBe("expired");
      });

      it("onlyIfExpired decides an expired approval and leaves a live one alone", async () => {
        const { store, advance } = await factory();
        await store.record(request({ ttlMs: 30 }));
        await store.record(request({ toolCallId: "call-2", workflowId: "wf-2" }));
        const key = { namespaceId: "acme", threadId: "t", toolCallId: "call-1" };
        const early = await store.decide({ ...key, approved: false, onlyIfExpired: true });
        expect(early.outcome).toBe("not_expired");
        expect((await store.get(key))?.status).toBe("pending");
        await advance(60);
        const late = await store.decide({
          ...key,
          approved: false,
          decidedBy: "system",
          onlyIfExpired: true,
        });
        expect(late.outcome).toBe("decided");
        expect((await store.get(key))?.status).toBe("expired");
        const neverExpires = await store.decide({
          namespaceId: "acme",
          threadId: "t",
          toolCallId: "call-2",
          approved: false,
          onlyIfExpired: true,
        });
        expect(neverExpires.outcome).toBe("not_expired");
      });
    });

    describe("open rows and delivery", () => {
      it("a decided row stays open until markDelivered", async () => {
        const { store } = await factory();
        await store.record(request());
        await store.record(request({ toolCallId: "call-2", workflowId: "wf-2" }));
        const key = { namespaceId: "acme", threadId: "t", toolCallId: "call-1" };
        await store.decide({ ...key, approved: true, decidedBy: "bob" });
        const open = await store.list({ namespaceId: "acme", threadId: "t", open: true });
        expect(open.map((r) => [r.toolCallId, r.status])).toEqual([
          ["call-1", "approved"],
          ["call-2", "pending"],
        ]);
        await store.markDelivered(key);
        await store.markDelivered(key);
        const delivered = await store.get(key);
        expect(delivered?.deliveredAt).not.toBeNull();
        expect(
          (await store.list({ namespaceId: "acme", threadId: "t", open: true })).map(
            (r) => r.toolCallId,
          ),
        ).toEqual(["call-2"]);
        // A new request of the key from another workflow is open again.
        const again = await store.record(request({ workflowId: "wf-3" }));
        expect(again.deliveredAt).toBeNull();
      });
    });

    describe("decide", () => {
      it("records the decision, the approver and the time", async () => {
        const { store } = await factory();
        const recorded = await store.record(request());
        const key = { namespaceId: "acme", threadId: "t", toolCallId: "call-1" };
        const result = await store.decide({
          ...key,
          approved: false,
          decidedBy: "bob",
          reason: "not today",
        });
        expect(result.outcome).toBe("decided");
        const row = await store.get(key);
        expect(row).toMatchObject({
          status: "rejected",
          decidedBy: "bob",
          reason: "not today",
        });
        expect(row!.decidedAt).toBeGreaterThanOrEqual(recorded.requestedAt);
      });

      it("a second decision answers already_decided and changes nothing", async () => {
        const { store } = await factory();
        await store.record(request());
        const key = { namespaceId: "acme", threadId: "t", toolCallId: "call-1" };
        await store.decide({ ...key, approved: true, decidedBy: "alice" });
        const second = await store.decide({ ...key, approved: false, decidedBy: "mallory" });
        expect(second.outcome).toBe("already_decided");
        expect((await store.get(key))?.decidedBy).toBe("alice");
      });

      it("of concurrent decisions exactly one wins", async () => {
        const { store } = await factory();
        await store.record(request());
        const key = { namespaceId: "acme", threadId: "t", toolCallId: "call-1" };
        const results = await Promise.all(
          ["a", "b", "c", "d"].map((who) =>
            store.decide({ ...key, approved: true, decidedBy: who }),
          ),
        );
        expect(results.filter((r) => r.outcome === "decided")).toHaveLength(1);
      });

      it("answers not_found for an unknown key", async () => {
        const { store } = await factory();
        const r = await store.decide({
          namespaceId: "acme",
          threadId: "t",
          toolCallId: "missing",
          approved: true,
        });
        expect(r.outcome).toBe("not_found");
      });
    });

    describe("list", () => {
      it("lists one thread's pending approvals among many, oldest first", async () => {
        const { store, advance } = await factory();
        for (let i = 0; i < 120; i++) {
          await store.record(
            request({ threadId: `other-${i}`, toolCallId: `c-${i}`, workflowId: `wf-o-${i}` }),
          );
        }
        await store.record(request({ threadId: "mine", toolCallId: "a", workflowId: "wf-a" }));
        await advance(5);
        await store.record(request({ threadId: "mine", toolCallId: "b", workflowId: "wf-b" }));
        const mine = await store.list({ namespaceId: "acme", threadId: "mine" });
        expect(mine.map((r) => r.toolCallId)).toEqual(["a", "b"]);
        const all = await store.list({ namespaceId: "acme", limit: 500 });
        expect(all).toHaveLength(122);
      });

      it("filters by status and namespace", async () => {
        const { store } = await factory();
        await store.record(request({ toolCallId: "a" }));
        await store.record(request({ toolCallId: "b" }));
        await store.record(request({ namespaceId: "globex", toolCallId: "c" }));
        await store.decide({ namespaceId: "acme", threadId: "t", toolCallId: "a", approved: true });
        expect((await store.list({ namespaceId: "acme" })).map((r) => r.toolCallId)).toEqual(["b"]);
        expect(
          (await store.list({ namespaceId: "acme", status: "approved" })).map((r) => r.toolCallId),
        ).toEqual(["a"]);
        expect((await store.list({ namespaceId: "globex" })).map((r) => r.toolCallId)).toEqual([
          "c",
        ]);
      });

      it("honors limit", async () => {
        const { store } = await factory();
        for (let i = 0; i < 5; i++) {
          await store.record(request({ toolCallId: `c-${i}`, workflowId: `wf-${i}` }));
        }
        expect(await store.list({ namespaceId: "acme", limit: 3 })).toHaveLength(3);
      });
    });

    describe("deleteThread", () => {
      it("drops one thread's rows only", async () => {
        const { store } = await factory();
        await store.record(request({ threadId: "t" }));
        await store.record(request({ threadId: "t-1" }));
        await store.record(request({ namespaceId: "globex", threadId: "t" }));
        await store.deleteThread({ namespaceId: "acme", threadId: "t" });
        expect(
          await store.get({ namespaceId: "acme", threadId: "t", toolCallId: "call-1" }),
        ).toBeNull();
        expect(
          await store.get({ namespaceId: "acme", threadId: "t-1", toolCallId: "call-1" }),
        ).not.toBeNull();
        expect(
          await store.get({ namespaceId: "globex", threadId: "t", toolCallId: "call-1" }),
        ).not.toBeNull();
      });
    });
  });
}
