// ---------------------------------------------------------------------------
// Portable StepQueue test suite
//
// Usage:
//   import { stepQueueTestSuite } from "@promin/core/testing";
//   stepQueueTestSuite(() => new InMemoryStepQueue());
//
// Routing model: tasks declare `needs: string[]`; workers claim via
// `capabilities: string[]`. A task is claimable when `needs ⊆ capabilities`.
// Empty needs = unrestricted; empty capabilities = generalist (can only
// claim unrestricted tasks).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { StepQueue } from "./step-queue.ts";

export function stepQueueTestSuite(factory: () => StepQueue | Promise<StepQueue>) {
  let queue: StepQueue;

  async function getQueue(): Promise<StepQueue> {
    queue = await factory();
    return queue;
  }

  describe("StepQueue conformance", () => {
    // -------------------------------------------------------------------
    // enqueue
    // -------------------------------------------------------------------

    describe("enqueue", () => {
      it("returns a task ID", async () => {
        const q = await getQueue();
        const id = await q.enqueue({
          workflowId: "wf-1",
          stepName: "step-a",
          input: { x: 1 },
          prevResults: {},
        });
        expect(typeof id).toBe("string");
        expect(id.length).toBeGreaterThan(0);
      });

      it("is idempotent on (workflowId, stepName) while a prior task is pending", async () => {
        const q = await getQueue();
        const first = await q.enqueue({
          workflowId: "wf-idempo",
          stepName: "charge",
          input: { amount: 100 },
          prevResults: {},
        });
        const second = await q.enqueue({
          workflowId: "wf-idempo",
          stepName: "charge",
          input: { amount: 999 },
          prevResults: {},
        });
        expect(second).toBe(first);

        const m = await q.metrics({ since: new Date(Date.now() - 60_000) });
        expect(m.pending).toBe(1);
      });

      it("is idempotent while a prior task is claimed (running)", async () => {
        const q = await getQueue();
        const first = await q.enqueue({
          workflowId: "wf-idempo-running",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        await q.claim({ limit: 10 });

        const second = await q.enqueue({
          workflowId: "wf-idempo-running",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        expect(second).toBe(first);
      });

      it("allows a fresh enqueue after the prior task completes", async () => {
        const q = await getQueue();
        const first = await q.enqueue({
          workflowId: "wf-after-complete",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        const [claimed] = await q.claim({ limit: 1 });
        await q.complete({ taskId: claimed!.id, result: "ok", durationMs: 10 });

        const second = await q.enqueue({
          workflowId: "wf-after-complete",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        expect(second).not.toBe(first);

        const all = await q.claim({ limit: 10 });
        expect(all).toHaveLength(1);
        expect(all[0]!.id).toBe(second);
      });

      it("allows a fresh enqueue after the prior task fails", async () => {
        const q = await getQueue();
        const first = await q.enqueue({
          workflowId: "wf-after-fail",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        const [claimed] = await q.claim({ limit: 1 });
        await q.fail({ taskId: claimed!.id, error: "boom", durationMs: 5 });

        const second = await q.enqueue({
          workflowId: "wf-after-fail",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        expect(second).not.toBe(first);
      });

      it("dedupe is per (workflowId, stepName) — different steps don't collide", async () => {
        const q = await getQueue();
        const a = await q.enqueue({
          workflowId: "wf-scope",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        const b = await q.enqueue({
          workflowId: "wf-scope",
          stepName: "ship",
          input: {},
          prevResults: {},
        });
        const c = await q.enqueue({
          workflowId: "wf-other",
          stepName: "charge",
          input: {},
          prevResults: {},
        });
        expect(new Set([a, b, c]).size).toBe(3);
      });
    });

    // -------------------------------------------------------------------
    // claim — subset semantics
    // -------------------------------------------------------------------

    describe("claim", () => {
      it("returns pending tasks and marks them running", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        await q.enqueue({ workflowId: "wf-1", stepName: "s2", input: {}, prevResults: {} });

        const tasks = await q.claim({ limit: 10 });
        expect(tasks).toHaveLength(2);
        expect(tasks[0]!.status).toBe("running");
        expect(tasks[1]!.status).toBe("running");
      });

      it("respects limit", async () => {
        const q = await getQueue();
        for (let i = 0; i < 5; i++) {
          await q.enqueue({
            workflowId: "wf-1",
            stepName: `s${i}`,
            input: {},
            prevResults: {},
          });
        }
        const tasks = await q.claim({ limit: 2 });
        expect(tasks).toHaveLength(2);
      });

      it("returns empty when no pending tasks", async () => {
        const q = await getQueue();
        const tasks = await q.claim({ limit: 10 });
        expect(tasks).toHaveLength(0);
      });

      it("worker with capabilities ⊇ needs claims the task", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "transcode",
          input: {},
          prevResults: {},
          needs: ["gpu"],
        });
        const tasks = await q.claim({ capabilities: ["gpu"], limit: 10 });
        expect(tasks).toHaveLength(1);
        expect(tasks[0]!.needs).toEqual(["gpu"]);
      });

      it("worker missing a required capability leaves the task pending", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "transcode",
          input: {},
          prevResults: {},
          needs: ["gpu"],
        });
        const tasks = await q.claim({ capabilities: ["cpu"], limit: 10 });
        expect(tasks).toHaveLength(0);

        // A later worker with the right capability can still claim it.
        const gpuClaim = await q.claim({ capabilities: ["gpu"], limit: 10 });
        expect(gpuClaim).toHaveLength(1);
      });

      it("worker with superset of needs can still claim (extra caps don't block)", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "transcode",
          input: {},
          prevResults: {},
          needs: ["gpu"],
        });
        const tasks = await q.claim({
          capabilities: ["gpu", "h265", "large-ram"],
          limit: 10,
        });
        expect(tasks).toHaveLength(1);
      });

      it("tasks with no needs are claimable by any worker (including one with no caps)", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "decode",
          input: {},
          prevResults: {},
        });
        const tasks = await q.claim({ limit: 10 }); // no capabilities passed
        expect(tasks).toHaveLength(1);
      });

      it("worker with no capabilities cannot claim a task that has needs", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "transcode",
          input: {},
          prevResults: {},
          needs: ["gpu"],
        });
        const tasks = await q.claim({ limit: 10 }); // no capabilities passed
        expect(tasks).toHaveLength(0);
      });

      it("multi-need task requires ALL capabilities", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "composite",
          input: {},
          prevResults: {},
          needs: ["gpu", "nvme"],
        });
        // Only one of two → no claim.
        const halfMatch = await q.claim({ capabilities: ["gpu"], limit: 10 });
        expect(halfMatch).toHaveLength(0);
        // Both → claim succeeds.
        const fullMatch = await q.claim({ capabilities: ["gpu", "nvme"], limit: 10 });
        expect(fullMatch).toHaveLength(1);
      });
    });

    // -------------------------------------------------------------------
    // complete / fail
    // -------------------------------------------------------------------

    describe("complete / fail", () => {
      it("marks task as completed", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        const [task] = await q.claim({ limit: 1 });
        await q.complete({ taskId: task!.id, result: "done", durationMs: 100 });

        const more = await q.claim({ limit: 10 });
        expect(more).toHaveLength(0);
      });

      it("marks task as failed", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        const [task] = await q.claim({ limit: 1 });
        await q.fail({ taskId: task!.id, error: "boom", durationMs: 50 });

        const more = await q.claim({ limit: 10 });
        expect(more).toHaveLength(0);
      });
    });

    // -------------------------------------------------------------------
    // priority ordering
    // -------------------------------------------------------------------

    describe("priority ordering", () => {
      it("higher priority first", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "low",
          input: {},
          prevResults: {},
          priority: 1,
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "high",
          input: {},
          prevResults: {},
          priority: 10,
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "mid",
          input: {},
          prevResults: {},
          priority: 5,
        });

        const tasks = await q.claim({ limit: 3 });
        expect(tasks[0]!.stepName).toBe("high");
        expect(tasks[1]!.stepName).toBe("mid");
        expect(tasks[2]!.stepName).toBe("low");
      });

      it("FIFO within same priority", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "first", input: {}, prevResults: {} });
        await q.enqueue({ workflowId: "wf-1", stepName: "second", input: {}, prevResults: {} });

        const tasks = await q.claim({ limit: 2 });
        expect(tasks[0]!.stepName).toBe("first");
        expect(tasks[1]!.stepName).toBe("second");
      });
    });

    // -------------------------------------------------------------------
    // requeueStuck
    // -------------------------------------------------------------------

    describe("requeueStuck", () => {
      it("requeues stale tasks by staleTimeoutMs", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        await q.claim({ limit: 1 });

        await new Promise((r) => setTimeout(r, 10));
        const requeued = await q.requeueStuck({ staleTimeoutMs: 1 });
        expect(requeued).toBe(1);

        const tasks = await q.claim({ limit: 1 });
        expect(tasks).toHaveLength(1);
        expect(tasks[0]!.status).toBe("running");
      });

      it("does not requeue tasks within timeout", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        await q.claim({ limit: 1 });

        const requeued = await q.requeueStuck({ staleTimeoutMs: 600_000 });
        expect(requeued).toBe(0);
      });

      it("heartbeat prevents premature requeue", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        const [task] = await q.claim({ limit: 1 });

        // Wait so claimedAt is in the past, then heartbeat to reset last-activity
        await new Promise((r) => setTimeout(r, 20));
        await q.heartbeat({ taskId: task!.id });

        // staleTimeoutMs: 500 — heartbeat was < 500ms ago, so should not requeue
        const requeued = await q.requeueStuck({ staleTimeoutMs: 500 });
        expect(requeued).toBe(0);
      });

      it("does not requeue completed or failed tasks", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        await q.enqueue({ workflowId: "wf-1", stepName: "s2", input: {}, prevResults: {} });

        const tasks = await q.claim({ limit: 2 });
        await q.complete({ taskId: tasks[0]!.id, result: "ok", durationMs: 10 });
        await q.fail({ taskId: tasks[1]!.id, error: "err", durationMs: 10 });

        await new Promise((r) => setTimeout(r, 10));
        const requeued = await q.requeueStuck({ staleTimeoutMs: 0 });
        expect(requeued).toBe(0);
      });
    });

    // -------------------------------------------------------------------
    // metrics
    // -------------------------------------------------------------------

    describe("metrics", () => {
      it("returns counts scoped to the requested time window", async () => {
        const q = await getQueue();
        const windowStart = new Date(Date.now() - 60_000);
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        await q.enqueue({ workflowId: "wf-1", stepName: "s2", input: {}, prevResults: {} });
        await q.enqueue({ workflowId: "wf-1", stepName: "s3", input: {}, prevResults: {} });
        await q.claim({ limit: 1 });

        const m = await q.metrics({ since: windowStart });
        expect(m.pending).toBe(2);
        expect(m.running).toBe(1);
      });

      it("tracks completed and failed counts in the window", async () => {
        const q = await getQueue();
        const windowStart = new Date(Date.now() - 60_000);
        await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
        await q.enqueue({ workflowId: "wf-1", stepName: "s2", input: {}, prevResults: {} });

        const tasks = await q.claim({ limit: 2 });
        await q.complete({ taskId: tasks[0]!.id, result: "ok", durationMs: 10 });
        await q.fail({ taskId: tasks[1]!.id, error: "err", durationMs: 10 });

        const m = await q.metrics({ since: windowStart });
        expect(m.completed).toBe(1);
        expect(m.failed).toBe(1);
        expect(m.pending).toBe(0);
        expect(m.running).toBe(0);
      });

      it("reports latency stats derived from terminal tasks", async () => {
        const q = await getQueue();
        const windowStart = new Date(Date.now() - 60_000);

        // Enqueue three tasks with known wait/exec characteristics.
        const ids: string[] = [];
        for (let i = 0; i < 3; i++) {
          ids.push(
            await q.enqueue({
              workflowId: `wf-lat-${i}`,
              stepName: "s",
              input: {},
              prevResults: {},
            }),
          );
        }
        // Small delay so claimed_at - created_at > 0 in Postgres-resolution time.
        await new Promise((r) => setTimeout(r, 10));
        const claimed = await q.claim({ limit: 3 });
        expect(claimed).toHaveLength(3);

        // Durations 100, 200, 300 → avg 200, p95 ≈ 290.
        await q.complete({ taskId: claimed[0]!.id, result: "a", durationMs: 100 });
        await q.complete({ taskId: claimed[1]!.id, result: "b", durationMs: 200 });
        await q.complete({ taskId: claimed[2]!.id, result: "c", durationMs: 300 });

        const m = await q.metrics({ since: windowStart });
        expect(m.completed).toBe(3);
        expect(m.avgExecMs).toBeCloseTo(200, 0);
        // p95 on [100, 200, 300] with linear interpolation = 290.
        expect(m.p95ExecMs).toBeCloseTo(290, 0);
        // Wait time was small but non-negative; just assert it's finite + >= 0.
        expect(m.avgWaitMs).toBeGreaterThanOrEqual(0);
      });

      it("excludes events outside the window", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "wf-old", stepName: "s", input: {}, prevResults: {} });
        const claimed = await q.claim({ limit: 1 });
        await q.complete({ taskId: claimed[0]!.id, result: "ok", durationMs: 50 });

        // Window entirely in the future — nothing should match.
        const future = new Date(Date.now() + 60_000);
        const m = await q.metrics({ since: future });
        expect(m.pending).toBe(0);
        expect(m.running).toBe(0);
        expect(m.completed).toBe(0);
        expect(m.failed).toBe(0);
        expect(m.avgExecMs).toBe(0);
        expect(m.p95ExecMs).toBe(0);
      });
    });

    // -------------------------------------------------------------------
    // Versioned dispatch
    // -------------------------------------------------------------------

    describe("versioned dispatch", () => {
      it("persists version on enqueue and returns it on claim", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-v",
          stepName: "s",
          input: {},
          prevResults: {},
          version: "2",
        });

        const [task] = await q.claim({ limit: 1 });
        expect(task).toBeDefined();
        expect(task!.version).toBe("2");
      });

      it("claim filter skips tasks the worker can't handle", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "f-1",
          stepName: "s",
          input: {},
          prevResults: {},
          version: "1",
        });
        await q.enqueue({
          workflowId: "f-2",
          stepName: "s",
          input: {},
          prevResults: {},
          version: "2",
        });
        await q.enqueue({
          workflowId: "f-3",
          stepName: "s",
          input: {},
          prevResults: {},
          version: "3",
        });

        const claimed = await q.claim({
          limit: 10,
          filter: (t) => t.version === "1" || t.version === "2",
        });

        expect(claimed).toHaveLength(2);
        expect(claimed.map((t) => t.version).sort()).toEqual(["1", "2"]);
      });

      it("rejected tasks stay claimable by another worker on the next claim", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "r-1",
          stepName: "s",
          input: {},
          prevResults: {},
          version: "5",
        });

        const firstPass = await q.claim({
          limit: 10,
          filter: (t) => t.version === "1",
        });
        expect(firstPass).toHaveLength(0);

        const secondPass = await q.claim({
          limit: 10,
          filter: (t) => t.version === "5",
        });
        expect(secondPass).toHaveLength(1);
        expect(secondPass[0]!.version).toBe("5");
      });

      it("unversioned tasks have undefined version (backward compat)", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "u-1", stepName: "s", input: {}, prevResults: {} });

        const [task] = await q.claim({ limit: 1 });
        expect(task).toBeDefined();
        expect(task!.version).toBeUndefined();
      });
    });

    describe("metadata search attributes", () => {
      it("round-trips arbitrary metadata through enqueue → claim", async () => {
        const q = await getQueue();
        const meta = {
          userId: "user-42",
          tags: ["urgent", "experiment-A"],
          experiment: "ramp-7",
          customField: { nested: { value: 1 } },
        };
        await q.enqueue({
          workflowId: "m-1",
          stepName: "s",
          input: {},
          prevResults: {},
          metadata: meta,
        });

        const [task] = await q.claim({ limit: 1 });
        expect(task).toBeDefined();
        expect(task!.metadata).toEqual(meta);
      });

      it("tasks without metadata have undefined metadata (backward compat)", async () => {
        const q = await getQueue();
        await q.enqueue({ workflowId: "no-meta", stepName: "s", input: {}, prevResults: {} });

        const [task] = await q.claim({ limit: 1 });
        expect(task).toBeDefined();
        expect(task!.metadata).toBeUndefined();
      });

      it("metadata coexists with namespace + version + needs (no field collision)", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "all-fields",
          stepName: "s",
          input: { x: 1 },
          prevResults: {},
          namespace: "tenant-a",
          version: "2",
          needs: ["gpu"],
          priority: 7,
          metadata: { userId: "u-9" },
        });

        const [task] = await q.claim({ limit: 1, capabilities: ["gpu"] });
        expect(task).toBeDefined();
        expect(task!.version).toBe("2");
        expect(task!.priority).toBe(7);
        expect([...task!.needs]).toEqual(["gpu"]);
        expect(task!.metadata).toEqual({ userId: "u-9" });
      });
    });

    describe("concurrency keys", () => {
      it("caps concurrent running tasks per (scope, key)", async () => {
        const q = await getQueue();
        // Enqueue 4 tasks for tenant A (limit 2) and 3 for tenant B (limit 1).
        for (let i = 0; i < 4; i++) {
          await q.enqueue({
            workflowId: `wf-A-${i}`,
            stepName: "send",
            input: {},
            prevResults: {},
            concurrencyKey: "tenant-A",
            concurrencyScope: "send-email",
            concurrencyLimit: 2,
          });
        }
        for (let i = 0; i < 3; i++) {
          await q.enqueue({
            workflowId: `wf-B-${i}`,
            stepName: "send",
            input: {},
            prevResults: {},
            concurrencyKey: "tenant-B",
            concurrencyScope: "send-email",
            concurrencyLimit: 1,
          });
        }

        // Claim everything claimable. A: 2 max running, B: 1 max running.
        const claimed = await q.claim({ limit: 100 });
        const byKey = new Map<string, number>();
        for (const t of claimed) {
          const k = t.concurrencyKey ?? "(none)";
          byKey.set(k, (byKey.get(k) ?? 0) + 1);
        }
        expect(byKey.get("tenant-A")).toBe(2);
        expect(byKey.get("tenant-B")).toBe(1);
        // Total claimed = sum of caps.
        expect(claimed).toHaveLength(3);
      });

      it("releases capacity when a task completes — next claim picks up the queued one", async () => {
        const q = await getQueue();
        for (let i = 0; i < 3; i++) {
          await q.enqueue({
            workflowId: `wf-${i}`,
            stepName: "send",
            input: {},
            prevResults: {},
            concurrencyKey: "shared",
            concurrencyScope: "send",
            concurrencyLimit: 1,
          });
        }

        const first = await q.claim({ limit: 100 });
        expect(first).toHaveLength(1);

        // Completing the running task frees the (scope, key) slot.
        await q.complete({ taskId: first[0]!.id, result: { ok: true }, durationMs: 1 });

        const second = await q.claim({ limit: 100 });
        expect(second).toHaveLength(1);
        expect(second[0]!.id).not.toBe(first[0]!.id);
      });

      it("does not cap tasks without concurrency config (backward compat)", async () => {
        const q = await getQueue();
        for (let i = 0; i < 5; i++) {
          await q.enqueue({
            workflowId: `wf-${i}`,
            stepName: "s",
            input: {},
            prevResults: {},
          });
        }
        const claimed = await q.claim({ limit: 100 });
        expect(claimed).toHaveLength(5);
      });

      it("scopes are independent — same key in different scopes don't share the limit", async () => {
        const q = await getQueue();
        // Both tasks use key "X" but different scopes; each scope has limit 1.
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "send-email",
          input: {},
          prevResults: {},
          concurrencyKey: "X",
          concurrencyScope: "send-email",
          concurrencyLimit: 1,
        });
        await q.enqueue({
          workflowId: "wf-2",
          stepName: "send-sms",
          input: {},
          prevResults: {},
          concurrencyKey: "X",
          concurrencyScope: "send-sms",
          concurrencyLimit: 1,
        });

        const claimed = await q.claim({ limit: 100 });
        expect(claimed).toHaveLength(2);
      });

      it("a single claim() call doesn't itself violate the limit", async () => {
        const q = await getQueue();
        // 5 pending tasks, limit 2 — claim(limit=10) shouldn't return more
        // than 2 even though SKIP LOCKED would otherwise grab all 5.
        for (let i = 0; i < 5; i++) {
          await q.enqueue({
            workflowId: `wf-${i}`,
            stepName: "s",
            input: {},
            prevResults: {},
            concurrencyKey: "single",
            concurrencyScope: "s",
            concurrencyLimit: 2,
          });
        }
        const claimed = await q.claim({ limit: 10 });
        expect(claimed.length).toBeLessThanOrEqual(2);
      });
    });
  });
}
