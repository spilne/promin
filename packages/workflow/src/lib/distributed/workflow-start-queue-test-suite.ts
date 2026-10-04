// ---------------------------------------------------------------------------
// Portable WorkflowStartQueue conformance suite. Every implementation
// (in-memory, SQLite, Postgres) must pass.
//
// The factory receives a `FakeWallClock` and a reclaim window; the queue
// must use both so the suite can drive staleness deterministically.
//
// Usage:
//   import { workflowStartQueueTestSuite } from "@promin/workflow/testing";
//   workflowStartQueueTestSuite(({ clock, reclaimAfterMs }) =>
//     new InMemoryWorkflowStartQueue({ clock, reclaimAfterMs }),
//   );
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { WorkflowStartQueue } from "./workflow-start-queue.ts";
import { FakeWallClock } from "../shared/wall-clock.ts";

const ANY_VERSION: ReadonlyArray<string> = [];
const RECLAIM_AFTER_MS = 1_000;
const SUITE_START_TIME = "2026-01-01T00:00:00Z";

export interface WorkflowStartQueueSuiteFactoryParams {
  /** Time source the queue must use for every stamp and cutoff. */
  readonly clock: FakeWallClock;
  /** Reclaim window the queue must be configured with. */
  readonly reclaimAfterMs: number;
}

export function workflowStartQueueTestSuite(
  factory: (
    params: WorkflowStartQueueSuiteFactoryParams,
  ) => WorkflowStartQueue | Promise<WorkflowStartQueue>,
): void {
  async function makeWithClock(): Promise<{ q: WorkflowStartQueue; clock: FakeWallClock }> {
    const clock = FakeWallClock.create(SUITE_START_TIME);
    const q = await factory({ clock, reclaimAfterMs: RECLAIM_AFTER_MS });
    return { q, clock };
  }

  async function make(): Promise<WorkflowStartQueue> {
    return (await makeWithClock()).q;
  }

  describe("WorkflowStartQueue conformance", () => {
    it("starts empty", async () => {
      const q = await make();
      expect(await q.list()).toEqual([]);
    });

    it("enqueue then claim returns the record", async () => {
      const q = await make();
      const { id } = await q.enqueue({
        workflowId: "wf-1",
        workflowName: "hello",
        input: { name: "world" },
      });
      expect(typeof id).toBe("string");
      const claimed = await q.claim({
        workflowSpecs: [{ name: "hello", versions: ANY_VERSION }],
        workerId: "w1",
        limit: 10,
      });
      expect(claimed.length).toBe(1);
      expect(claimed[0]?.workflowId).toBe("wf-1");
      expect(claimed[0]?.workflowName).toBe("hello");
      expect(claimed[0]?.input).toEqual({ name: "world" });
      expect(claimed[0]?.claimedBy).toBe("w1");
      expect(typeof claimed[0]?.claimedAt).toBe("number");
    });

    it("claim respects the limit", async () => {
      const q = await make();
      for (let i = 0; i < 5; i++) {
        await q.enqueue({ workflowId: `wf-${i}`, workflowName: "hello", input: {} });
      }
      const claimed = await q.claim({
        workflowSpecs: [{ name: "hello", versions: ANY_VERSION }],
        limit: 2,
      });
      expect(claimed.length).toBe(2);
    });

    it("claim only returns starts the worker can run by name", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "a", workflowName: "alpha", input: {} });
      await q.enqueue({ workflowId: "b", workflowName: "beta", input: {} });
      const claimed = await q.claim({
        workflowSpecs: [{ name: "alpha", versions: ANY_VERSION }],
        limit: 10,
      });
      expect(claimed.length).toBe(1);
      expect(claimed[0]?.workflowName).toBe("alpha");
    });

    it("version filter: workers with a versions list only claim matching versions", async () => {
      const q = await make();
      await q.enqueue({
        workflowId: "v1",
        workflowName: "wf",
        input: {},
        version: "1",
      });
      await q.enqueue({
        workflowId: "v2",
        workflowName: "wf",
        input: {},
        version: "2",
      });
      const claimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ["2"] }],
        limit: 10,
      });
      expect(claimed.length).toBe(1);
      expect(claimed[0]?.version).toBe("2");
    });

    it("versionless start matches any worker advertising the name", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "x", workflowName: "wf", input: {} });
      const claimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ["3"] }],
        limit: 10,
      });
      // Versionless starts shouldn't be filtered out by a version-pinned spec
      expect(claimed.length).toBe(1);
    });

    it("claimed records do not appear in subsequent claim calls", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      const first = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 10,
      });
      expect(first.length).toBe(1);
      const second = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 10,
      });
      expect(second.length).toBe(0);
    });

    it("complete removes a claimed record", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      const claimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 10,
      });
      const claimedId = claimed[0]!.id;
      expect((await q.list()).length).toBe(1);
      expect(await q.complete({ id: claimedId, claimToken: claimed[0]!.claimToken! })).toBe(true);
      expect((await q.list()).length).toBe(0);
    });

    it("complete on unknown id returns false", async () => {
      const q = await make();
      expect(await q.complete({ id: "never-existed", claimToken: "nope" })).toBe(false);
    });

    it("claim stamps a claim token and the first heartbeat", async () => {
      const { q, clock } = await makeWithClock();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      await q.enqueue({ workflowId: "b", workflowName: "wf", input: {} });
      const claimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 10,
      });
      expect(claimed.length).toBe(2);
      for (const rec of claimed) {
        expect(typeof rec.claimToken).toBe("string");
        expect(rec.claimToken!.length).toBeGreaterThan(0);
        expect(rec.heartbeatAt).toBe(clock.currentTimeMs());
      }
      expect(claimed[0]!.claimToken).not.toBe(claimed[1]!.claimToken);
    });

    it("heartbeat returns true for the current claim and false for a wrong token", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      const [rec] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      expect(await q.heartbeat({ id: rec!.id, claimToken: rec!.claimToken! })).toBe(true);
      expect(await q.heartbeat({ id: rec!.id, claimToken: "stale" })).toBe(false);
      expect(await q.heartbeat({ id: "never-existed", claimToken: "x" })).toBe(false);
    });

    it("complete with a stale token returns false and leaves the record", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      const [rec] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      expect(await q.complete({ id: rec!.id, claimToken: "stale" })).toBe(false);
      const all = await q.list();
      expect(all.length).toBe(1);
      expect(all[0]?.id).toBe(rec!.id);
      // The real claimant can still finish it.
      expect(await q.heartbeat({ id: rec!.id, claimToken: rec!.claimToken! })).toBe(true);
    });

    it("a stale claim is reclaimed under a new token; the old token is fenced out", async () => {
      const { q, clock } = await makeWithClock();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      const [first] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w1",
        limit: 1,
      });

      clock.advance(RECLAIM_AFTER_MS + 1);
      const [second] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w2",
        limit: 1,
      });
      expect(second?.id).toBe(first!.id);
      expect(second?.claimedBy).toBe("w2");
      expect(second?.claimToken).not.toBe(first!.claimToken);

      const stale = { id: first!.id, claimToken: first!.claimToken! };
      const current = { id: second!.id, claimToken: second!.claimToken! };
      expect(await q.heartbeat(stale)).toBe(false);
      expect(await q.complete(stale)).toBe(false);
      expect((await q.list()).length).toBe(1);

      expect(await q.heartbeat(current)).toBe(true);
      expect(await q.complete(current)).toBe(true);
      expect((await q.list()).length).toBe(0);
    });

    it("a heartbeating claim is not reclaimed past the original window", async () => {
      const { q, clock } = await makeWithClock();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      const [rec] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w1",
        limit: 1,
      });
      const ref = { id: rec!.id, claimToken: rec!.claimToken! };

      // Heartbeat every half window, for three full windows.
      for (let i = 0; i < 6; i++) {
        clock.advance(RECLAIM_AFTER_MS / 2);
        expect(await q.heartbeat(ref)).toBe(true);
      }
      const stolen = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w2",
        limit: 1,
      });
      expect(stolen.length).toBe(0);

      // Once heartbeats stop, the window applies from the last one.
      clock.advance(RECLAIM_AFTER_MS + 1);
      const reclaimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w2",
        limit: 1,
      });
      expect(reclaimed.length).toBe(1);
      expect(await q.heartbeat(ref)).toBe(false);
    });

    it("metadata round-trips through enqueue/claim", async () => {
      const q = await make();
      await q.enqueue({
        workflowId: "a",
        workflowName: "wf",
        input: {},
        metadata: { source: "test", n: 42 },
      });
      const claimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      expect(claimed[0]?.metadata).toEqual({ source: "test", n: 42 });
    });

    it("list returns both pending and inflight records", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      await q.enqueue({ workflowId: "b", workflowName: "wf", input: {} });
      await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      const all = await q.list();
      expect(all.length).toBe(2);
    });
  });
}
