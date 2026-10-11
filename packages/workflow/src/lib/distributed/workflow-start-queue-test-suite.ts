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

export interface WorkflowStartQueueSuiteOptions {
  /**
   * The queue stamps and expires claims on a server clock (Postgres
   * `NOW()`), not the injected one: skip the tests that drive time with
   * the `FakeWallClock` (cover those against the real clock).
   */
  readonly serverClock?: boolean;
}

export function workflowStartQueueTestSuite(
  factory: (
    params: WorkflowStartQueueSuiteFactoryParams,
  ) => WorkflowStartQueue | Promise<WorkflowStartQueue>,
  options: WorkflowStartQueueSuiteOptions = {},
): void {
  const clockIt = options.serverClock ? it.skip : it;
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

    clockIt("claim stamps a claim token and the first heartbeat", async () => {
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

    clockIt(
      "a stale claim is reclaimed under a new token; the old token is fenced out",
      async () => {
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
      },
    );

    clockIt("a heartbeating claim is not reclaimed past the original window", async () => {
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

    it("enqueue is idempotent on workflowId while a record exists", async () => {
      const q = await make();
      const first = await q.enqueue({ workflowId: "a", workflowName: "wf", input: { n: 1 } });
      expect(first.created).toBe(true);
      const again = await q.enqueue({ workflowId: "a", workflowName: "wf", input: { n: 2 } });
      expect(again).toEqual({ id: first.id, created: false });
      expect((await q.list()).length).toBe(1);

      // Still one record while it is claimed.
      const [rec] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      expect(rec?.input).toEqual({ n: 1 });
      const whileClaimed = await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      expect(whileClaimed).toEqual({ id: first.id, created: false });
      expect((await q.list()).length).toBe(1);

      // Enqueued again while claimed: completing the claim puts the start
      // back to pending, so the run is driven once more.
      expect(await q.complete({ id: rec!.id, claimToken: rec!.claimToken! })).toBe(true);
      const [again2] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      expect(again2?.id).toBe(first.id);
      expect(again2?.claimToken).not.toBe(rec!.claimToken);
      expect(await q.complete({ id: again2!.id, claimToken: again2!.claimToken! })).toBe(true);
      expect(await q.list()).toHaveLength(0);

      // Once completed with no enqueue in between, the run can be queued afresh.
      const next = await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      expect(next.created).toBe(true);
      expect(next.id).not.toBe(first.id);
    });

    it("release gives the start back under a new claim; the old token is fenced out", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      const [first] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w1",
        limit: 1,
      });
      const ref = { id: first!.id, claimToken: first!.claimToken! };
      expect(await q.release({ id: ref.id, claimToken: "stale" })).toBe(false);
      expect(await q.release(ref)).toBe(true);
      expect(await q.release(ref)).toBe(false);
      expect(await q.heartbeat(ref)).toBe(false);
      expect(await q.complete(ref)).toBe(false);

      const [second] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w2",
        limit: 1,
      });
      expect(second?.id).toBe(first!.id);
      expect(second?.claimedBy).toBe("w2");
      expect(second?.claimToken).not.toBe(first!.claimToken);
    });

    clockIt("release with a delay hides the start until it is due", async () => {
      const { q, clock } = await makeWithClock();
      await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
      clock.advance(1);
      await q.enqueue({ workflowId: "b", workflowName: "wf", input: {} });
      const [first] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      expect(first?.workflowId).toBe("a");
      expect(await q.release({ id: first!.id, claimToken: first!.claimToken!, delayMs: 500 })).toBe(
        true,
      );
      const listed = (await q.list()).find((r) => r.id === first!.id);
      expect(listed?.claimToken).toBeUndefined();
      expect(listed?.availableAt).toBe(clock.currentTimeMs() + 500);

      // Only "b" is claimable during the delay.
      const during = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 10,
      });
      expect(during.map((r) => r.workflowId)).toEqual(["b"]);

      clock.advance(500);
      const after = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 10,
      });
      expect(after.map((r) => r.workflowId)).toEqual(["a"]);
      expect(after[0]?.availableAt).toBeUndefined();
    });

    it("claims oldest first", async () => {
      const { q, clock } = await makeWithClock();
      for (const id of ["a", "b", "c"]) {
        await q.enqueue({ workflowId: id, workflowName: "wf", input: {} });
        clock.advance(1);
      }
      const claimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 2,
      });
      expect(claimed.map((r) => r.workflowId)).toEqual(["a", "b"]);
    });

    it("a version-filtered claim skips starts it cannot run without starving them", async () => {
      const q = await make();
      for (let i = 0; i < 5; i++) {
        await q.enqueue({ workflowId: `v1-${i}`, workflowName: "wf", input: {}, version: "1" });
      }
      await q.enqueue({ workflowId: "v2", workflowName: "wf", input: {}, version: "2" });
      const v2 = await q.claim({ workflowSpecs: [{ name: "wf", versions: ["2"] }], limit: 1 });
      expect(v2.map((r) => r.workflowId)).toEqual(["v2"]);
      const v1 = await q.claim({ workflowSpecs: [{ name: "wf", versions: ["1"] }], limit: 10 });
      expect(v1.length).toBe(5);
    });

    it("an enqueue while claimed survives the claim's complete (resume, wake-up)", async () => {
      const q = await make();
      await q.enqueue({ workflowId: "r", workflowName: "wf", input: {} });
      const [rec] = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      // The worker finishes its drive; meanwhile a resume enqueues the run.
      await q.enqueue({ workflowId: "r", workflowName: "wf", input: {} });
      expect((await q.list())[0]?.requeued).toBe(true);
      expect(await q.complete({ id: rec!.id, claimToken: rec!.claimToken! })).toBe(true);
      const listed = await q.list();
      expect(listed).toHaveLength(1);
      expect(listed[0]?.claimToken).toBeUndefined();
      expect(listed[0]?.requeued).toBeFalsy();
      // A stale claimant can't complete the requeued start.
      expect(await q.complete({ id: rec!.id, claimToken: rec!.claimToken! })).toBe(false);
    });

    it("an enqueue makes a delayed pending start claimable at once, unless it keeps the delay", async () => {
      const q = await make();
      const specs = [{ name: "wf", versions: ANY_VERSION }];
      await q.enqueue({ workflowId: "d", workflowName: "wf", input: {} });
      const [rec] = await q.claim({ workflowSpecs: specs, limit: 1 });
      await q.release({ id: rec!.id, claimToken: rec!.claimToken!, delayMs: 3_600_000 });
      expect(await q.claim({ workflowSpecs: specs, limit: 1 })).toHaveLength(0);

      // A scheduled wake-up keeps the worker's delay.
      await q.enqueue({ workflowId: "d", workflowName: "wf", input: {}, keepAvailableAt: true });
      expect(await q.claim({ workflowSpecs: specs, limit: 1 })).toHaveLength(0);

      // A resume, rerun or signal makes it claimable now.
      expect(await q.enqueue({ workflowId: "d", workflowName: "wf", input: {} })).toEqual({
        id: rec!.id,
        created: false,
      });
      const [now] = await q.claim({ workflowSpecs: specs, limit: 1 });
      expect(now?.id).toBe(rec!.id);
    });

    it("a delayed release of a start enqueued again during its claim is claimable at once", async () => {
      const q = await make();
      const specs = [{ name: "wf", versions: ANY_VERSION }];
      await q.enqueue({ workflowId: "h", workflowName: "wf", input: {} });
      const [rec] = await q.claim({ workflowSpecs: specs, limit: 1 });
      // A resume enqueues the run while the worker still holds it; the worker
      // then releases with a delay chosen from the state it read before.
      await q.enqueue({ workflowId: "h", workflowName: "wf", input: {} });
      expect(
        await q.release({ id: rec!.id, claimToken: rec!.claimToken!, delayMs: 3_600_000 }),
      ).toBe(true);

      const [again] = await q.claim({ workflowSpecs: specs, limit: 1 });
      expect(again?.id).toBe(rec!.id);
      expect(again?.requeued).toBeFalsy();
    });

    it("a claim retried with the same claimId gets the same starts back", async () => {
      const q = await make();
      for (const id of ["a", "b", "c"]) {
        await q.enqueue({ workflowId: id, workflowName: "wf", input: {} });
      }
      const params = {
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        workerId: "w1",
        limit: 2,
        claimId: "claim-1",
      };
      const first = await q.claim(params);
      const retried = await q.claim(params);
      expect(retried.map((r) => [r.id, r.claimToken]).sort()).toEqual(
        first.map((r) => [r.id, r.claimToken]).sort(),
      );
      const other = await q.claim({ ...params, claimId: "claim-2" });
      const rest = ["a", "b", "c"].filter((id) => !first.some((r) => r.workflowId === id));
      expect(other.map((r) => r.workflowId)).toEqual(rest);
      await expect(q.claim({ ...params, claimId: "bad id%" })).rejects.toThrow(/claimId/);
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
