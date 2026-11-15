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
  DecidePendingApprovalParams,
  PendingApprovalBoundary,
  PendingApprovalKey,
  PendingApprovalStore,
  RecordPendingApprovalParams,
} from "./pending-approval-store.ts";
import { snapshotPendingApprovalBoundary } from "./pending-approval-store.ts";

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
    agentId: "ops-bot",
    resourceId: "alice",
    threadId: "t",
    toolCallId: "call-1",
    workflowId: "wf-1",
    toolName: "deploy",
    input: { env: "prod" },
    elevated: false,
    requestContext: {
      actor: { kind: "user", id: "alice" },
    },
    ...overrides,
  };
}

function key(overrides: Partial<PendingApprovalKey> = {}): PendingApprovalKey {
  return {
    namespaceId: "acme",
    agentId: "ops-bot",
    resourceId: "alice",
    threadId: "t",
    toolCallId: "call-1",
    ...overrides,
  };
}

function boundary(overrides: Partial<PendingApprovalBoundary> = {}): PendingApprovalBoundary {
  return Object.freeze({
    ...key(overrides),
    workflowId: "wf-1",
    elevated: false,
    requestContext: { actor: { kind: "user" as const, id: "alice" } },
    ...overrides,
  });
}

async function decideCurrent(
  store: PendingApprovalStore,
  approvalKey: PendingApprovalKey,
  decision: Omit<DecidePendingApprovalParams, "expected">,
) {
  const row = await store.get(approvalKey);
  if (!row) throw new Error("test approval missing");
  return store.decide({ expected: snapshotPendingApprovalBoundary(row), ...decision });
}

const aliceScope = { agentId: "ops-bot", resourceId: "alice" } as const;

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
          elevated: false,
          requestContext: { actor: { kind: "user", id: "alice" } },
          status: "pending",
          expiresAt: null,
          decidedBy: null,
          decidedAt: null,
          reason: null,
          deliveredAt: null,
        });
        expect(row.requestedAt).toBeGreaterThan(0);
        expect(await store.get(key())).toEqual(row);
      });

      it("persists the elevated approval boundary", async () => {
        const { store } = await factory();
        const row = await store.record(request({ elevated: true }));
        expect(row.elevated).toBe(true);
        expect(await store.get(key())).toEqual(row);
      });

      it("detaches the stored security boundary from request and result mutation", async () => {
        const { store } = await factory();
        const actor = { kind: "service-account" as const, id: "requester-sa" };
        const effective = { kind: "user" as const, id: "alice" };
        const input = { env: "prod" };
        const params = {
          ...request({
            elevated: true,
            requestContext: { actor, effective },
            input,
          }),
        };
        const recorded = await store.record(params);

        params.elevated = false;
        actor.id = "mutated-actor";
        effective.id = "mutated-user";
        input.env = "mutated";
        Reflect.set(recorded, "elevated", false);
        Reflect.set(recorded.requestContext!.actor, "id", "mutated-result");

        const read = (await store.get(key()))!;
        expect(read).not.toBe(recorded);
        expect(read).toMatchObject({
          elevated: true,
          input: { env: "prod" },
          requestContext: {
            actor: { kind: "service-account", id: "requester-sa" },
            effective: { kind: "user", id: "alice" },
          },
        });

        Reflect.set(read, "elevated", false);
        Reflect.set(read.requestContext!.actor, "id", "mutated-read");
        expect(await store.get(key())).toMatchObject({
          elevated: true,
          requestContext: { actor: { id: "requester-sa" } },
        });
      });

      it("returns null for an unknown key", async () => {
        const { store } = await factory();
        expect(await store.get(key({ toolCallId: "nope" }))).toBeNull();
      });

      it("re-recording the same workflow's request keeps the first row", async () => {
        const { store, advance } = await factory();
        const first = await store.record(request());
        await advance(20);
        const again = await store.record(request({ input: { changed: true } }));
        expect(again).toEqual(first);
      });

      it("retains an open row across workflows and replaces it only after delivery", async () => {
        const { store } = await factory();
        await store.record(request());
        await decideCurrent(store, key(), { approved: true });
        const retained = await store.record(request({ workflowId: "wf-2" }));
        expect(retained.workflowId).toBe("wf-1");
        expect(retained.status).toBe("approved");
        await store.markDelivered(key());
        const replaced = await store.record(request({ workflowId: "wf-2" }));
        expect(replaced.workflowId).toBe("wf-2");
        expect(replaced.status).toBe("pending");
      });

      it("never matches a thread id by prefix", async () => {
        const { store } = await factory();
        await store.record(request({ threadId: "t-1", workflowId: "wf-t1" }));
        expect(await store.get(key())).toBeNull();
        await store.record(request({ threadId: "t", workflowId: "wf-t" }));
        const t = await store.get(key());
        const t1 = await store.get(key({ threadId: "t-1" }));
        expect(t?.workflowId).toBe("wf-t");
        expect(t1?.workflowId).toBe("wf-t1");
        const listed = await store.list({ namespaceId: "acme", ...aliceScope, threadId: "t" });
        expect(listed.map((r) => r.workflowId)).toEqual(["wf-t"]);
      });

      it("keeps two namespaces with the same thread id apart", async () => {
        const { store } = await factory();
        await store.record(request({ namespaceId: "acme", workflowId: "wf-acme" }));
        await store.record(request({ namespaceId: "globex", workflowId: "wf-globex" }));
        const acme = await store.get(key());
        const globex = await store.get(key({ namespaceId: "globex" }));
        expect(acme?.workflowId).toBe("wf-acme");
        expect(globex?.workflowId).toBe("wf-globex");
        await decideCurrent(store, key(), { approved: true });
        expect((await store.get(key({ namespaceId: "globex" })))?.status).toBe("pending");
      });

      it("keeps identical thread/tool ids isolated by agent and resource", async () => {
        const { store } = await factory();
        await store.record(request({ workflowId: "wf-alice" }));
        await store.record(request({ resourceId: "bob", workflowId: "wf-bob-resource" }));
        await store.record(request({ agentId: "other-bot", workflowId: "wf-other-agent" }));

        expect((await store.get(key()))?.workflowId).toBe("wf-alice");
        const bobKey = key({ resourceId: "bob" });
        const otherAgentKey = key({ agentId: "other-bot" });
        expect((await store.get(bobKey))?.workflowId).toBe("wf-bob-resource");
        expect((await store.get(otherAgentKey))?.workflowId).toBe("wf-other-agent");
        expect(
          (await store.list({ namespaceId: "acme", ...aliceScope, threadId: "t" })).map(
            (row) => row.workflowId,
          ),
        ).toEqual(["wf-alice"]);
        await decideCurrent(store, key(), { approved: true });
        expect((await store.get(bobKey))?.status).toBe("pending");
        expect((await store.get(otherAgentKey))?.status).toBe("pending");
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
        const approvalKey = key();
        const approve = await decideCurrent(store, approvalKey, { approved: true });
        expect(approve.outcome).toBe("expired");
        expect((await store.get(approvalKey))?.status).toBe("pending");
        const reject = await decideCurrent(store, approvalKey, {
          approved: false,
          reason: "too late",
        });
        expect(reject.outcome).toBe("decided");
        expect((await store.get(approvalKey))?.status).toBe("expired");
      });

      it("onlyIfExpired decides an expired approval and leaves a live one alone", async () => {
        const { store, advance } = await factory();
        await store.record(request({ ttlMs: 30 }));
        await store.record(request({ toolCallId: "call-2", workflowId: "wf-2" }));
        const approvalKey = key();
        const early = await decideCurrent(store, approvalKey, {
          approved: false,
          onlyIfExpired: true,
        });
        expect(early.outcome).toBe("not_expired");
        expect((await store.get(approvalKey))?.status).toBe("pending");
        await advance(60);
        const late = await store.decide({
          expected: snapshotPendingApprovalBoundary((await store.get(approvalKey))!),
          approved: false,
          decidedBy: "system",
          onlyIfExpired: true,
        });
        expect(late.outcome).toBe("decided");
        expect((await store.get(approvalKey))?.status).toBe("expired");
        const neverExpires = await decideCurrent(store, key({ toolCallId: "call-2" }), {
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
        const approvalKey = key();
        await decideCurrent(store, approvalKey, { approved: true, decidedBy: "bob" });
        const open = await store.list({
          namespaceId: "acme",
          ...aliceScope,
          threadId: "t",
          open: true,
        });
        expect(open.map((r) => [r.toolCallId, r.status])).toEqual([
          ["call-1", "approved"],
          ["call-2", "pending"],
        ]);
        await store.markDelivered(approvalKey);
        await store.markDelivered(approvalKey);
        const delivered = await store.get(approvalKey);
        expect(delivered?.deliveredAt).not.toBeNull();
        expect(
          (
            await store.list({
              namespaceId: "acme",
              ...aliceScope,
              threadId: "t",
              open: true,
            })
          ).map((r) => r.toolCallId),
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
        const approvalKey = key();
        const result = await decideCurrent(store, approvalKey, {
          approved: false,
          decidedBy: "bob",
          decidedByPrincipal: { kind: "service-account", id: "bob" },
          reason: "not today",
        });
        expect(result.outcome).toBe("decided");
        const row = await store.get(approvalKey);
        expect(row).toMatchObject({
          status: "rejected",
          decidedBy: "bob",
          decidedByPrincipal: { kind: "service-account", id: "bob" },
          reason: "not today",
        });
        expect(row!.decidedAt).toBeGreaterThanOrEqual(recorded.requestedAt);
      });

      it("a second decision answers already_decided and changes nothing", async () => {
        const { store } = await factory();
        await store.record(request());
        const approvalKey = key();
        const expected = snapshotPendingApprovalBoundary((await store.get(approvalKey))!);
        await store.decide({ expected, approved: true, decidedBy: "alice" });
        const second = await store.decide({ expected, approved: false, decidedBy: "mallory" });
        expect(second.outcome).toBe("already_decided");
        expect((await store.get(approvalKey))?.decidedBy).toBe("alice");
      });

      it("of concurrent decisions exactly one wins", async () => {
        const { store } = await factory();
        await store.record(request());
        const approvalKey = key();
        const expected = snapshotPendingApprovalBoundary((await store.get(approvalKey))!);
        const results = await Promise.all(
          ["a", "b", "c", "d"].map((who) =>
            store.decide({ expected, approved: true, decidedBy: who }),
          ),
        );
        expect(results.filter((r) => r.outcome === "decided")).toHaveLength(1);
      });

      it("snapshots the expected boundary before asynchronous decision work", async () => {
        const { store } = await factory();
        const row = await store.record(request({ elevated: true }));
        const expected = {
          ...snapshotPendingApprovalBoundary(row),
          requestContext: structuredClone(row.requestContext),
        };
        const decision = store.decide({ expected, approved: false });
        Reflect.set(expected, "workflowId", "attacker-workflow");
        Reflect.set(expected, "elevated", false);
        Reflect.set(expected.requestContext!.actor, "id", "attacker");

        expect((await decision).outcome).toBe("decided");
        expect(await store.get(key())).toMatchObject({
          workflowId: "wf-1",
          elevated: true,
          status: "rejected",
          requestContext: { actor: { kind: "user", id: "alice" } },
        });
      });

      it("conflicts when the immutable approval boundary was replaced", async () => {
        const { store } = await factory();
        const approvalKey = key();
        const old = await store.record(request());
        const expected = snapshotPendingApprovalBoundary(old);
        await decideCurrent(store, approvalKey, { approved: false });
        await store.markDelivered(approvalKey);
        await store.record(
          request({ workflowId: "wf-2", elevated: true, input: { replacement: true } }),
        );

        const result = await store.decide({ expected, approved: true });
        expect(result.outcome).toBe("conflict");
        expect(await store.get(approvalKey)).toMatchObject({
          workflowId: "wf-2",
          elevated: true,
          status: "pending",
        });
      });

      it("answers not_found for an unknown key", async () => {
        const { store } = await factory();
        const r = await store.decide({
          expected: boundary({ toolCallId: "missing" }),
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
        const mine = await store.list({ namespaceId: "acme", ...aliceScope, threadId: "mine" });
        expect(mine.map((r) => r.toolCallId)).toEqual(["a", "b"]);
        const all = await store.list({ namespaceId: "acme", ...aliceScope, limit: 500 });
        expect(all).toHaveLength(122);
      });

      it("filters by status and namespace", async () => {
        const { store } = await factory();
        await store.record(request({ toolCallId: "a" }));
        await store.record(request({ toolCallId: "b" }));
        await store.record(request({ namespaceId: "globex", toolCallId: "c" }));
        await decideCurrent(store, key({ toolCallId: "a" }), { approved: true });
        expect(
          (await store.list({ namespaceId: "acme", ...aliceScope })).map((r) => r.toolCallId),
        ).toEqual(["b"]);
        expect(
          (await store.list({ namespaceId: "acme", ...aliceScope, status: "approved" })).map(
            (r) => r.toolCallId,
          ),
        ).toEqual(["a"]);
        expect(
          (await store.list({ namespaceId: "globex", ...aliceScope })).map((r) => r.toolCallId),
        ).toEqual(["c"]);
      });

      it("honors limit", async () => {
        const { store } = await factory();
        for (let i = 0; i < 5; i++) {
          await store.record(request({ toolCallId: `c-${i}`, workflowId: `wf-${i}` }));
        }
        expect(await store.list({ namespaceId: "acme", ...aliceScope, limit: 3 })).toHaveLength(3);
      });
    });

    describe("deleteThread", () => {
      it("drops one thread's rows only", async () => {
        const { store } = await factory();
        await store.record(request({ threadId: "t" }));
        await store.record(request({ threadId: "t-1" }));
        await store.record(request({ namespaceId: "globex", threadId: "t" }));
        await store.record(request({ resourceId: "bob", threadId: "t", workflowId: "wf-bob" }));
        await store.deleteThread({ namespaceId: "acme", ...aliceScope, threadId: "t" });
        expect(await store.get(key())).toBeNull();
        expect(await store.get(key({ threadId: "t-1" }))).not.toBeNull();
        expect(await store.get(key({ namespaceId: "globex" }))).not.toBeNull();
        expect(await store.get(key({ resourceId: "bob" }))).not.toBeNull();
      });
    });
  });
}
