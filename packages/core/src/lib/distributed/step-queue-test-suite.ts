// ---------------------------------------------------------------------------
// Portable StepQueue test suite
//
// Usage:
//   import { stepQueueTestSuite } from "@promin/core/testing";
//   stepQueueTestSuite(() => new InMemoryStepQueue());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { StepQueue } from "./step-queue.ts";

/**
 * Run the full StepQueue conformance suite against any implementation.
 * Verifies enqueue, claim, complete, fail, priority ordering, requeueStuck,
 * and metrics.
 *
 * @param factory — called before each test to get a fresh queue instance
 */
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
          queue: "default",
          input: { x: 1 },
          prevResults: {},
        });
        expect(typeof id).toBe("string");
        expect(id.length).toBeGreaterThan(0);
      });
    });

    // -------------------------------------------------------------------
    // claim
    // -------------------------------------------------------------------

    describe("claim", () => {
      it("returns pending tasks and marks them running", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s2",
          queue: "q1",
          input: {},
          prevResults: {},
        });

        const tasks = await q.claim({ queues: ["q1"], limit: 10 });
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
            queue: "q1",
            input: {},
            prevResults: {},
          });
        }
        const tasks = await q.claim({ queues: ["q1"], limit: 2 });
        expect(tasks).toHaveLength(2);
      });

      it("only returns tasks from requested queues", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "gpu",
          input: {},
          prevResults: {},
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s2",
          queue: "cpu",
          input: {},
          prevResults: {},
        });

        const tasks = await q.claim({ queues: ["gpu"], limit: 10 });
        expect(tasks).toHaveLength(1);
        expect(tasks[0]!.queue).toBe("gpu");
      });

      it("returns empty when no pending tasks", async () => {
        const q = await getQueue();
        const tasks = await q.claim({ queues: ["q1"], limit: 10 });
        expect(tasks).toHaveLength(0);
      });
    });

    // -------------------------------------------------------------------
    // complete / fail
    // -------------------------------------------------------------------

    describe("complete / fail", () => {
      it("marks task as completed", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        const [task] = await q.claim({ queues: ["q1"], limit: 1 });
        await q.complete({ taskId: task!.id, result: "done", durationMs: 100 });

        // Completed tasks should not be claimed again
        const more = await q.claim({ queues: ["q1"], limit: 10 });
        expect(more).toHaveLength(0);
      });

      it("marks task as failed", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        const [task] = await q.claim({ queues: ["q1"], limit: 1 });
        await q.fail({ taskId: task!.id, error: "boom", durationMs: 50 });

        // Failed tasks should not be claimed again
        const more = await q.claim({ queues: ["q1"], limit: 10 });
        expect(more).toHaveLength(0);
      });

      it("completed tasks are not claimed again", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        const [task] = await q.claim({ queues: ["q1"], limit: 1 });
        await q.complete({ taskId: task!.id, result: "done", durationMs: 100 });

        const more = await q.claim({ queues: ["q1"], limit: 10 });
        expect(more).toHaveLength(0);
      });

      it("failed tasks are not claimed again", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        const [task] = await q.claim({ queues: ["q1"], limit: 1 });
        await q.fail({ taskId: task!.id, error: "error", durationMs: 50 });

        const more = await q.claim({ queues: ["q1"], limit: 10 });
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
          queue: "q1",
          input: {},
          prevResults: {},
          priority: 1,
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "high",
          queue: "q1",
          input: {},
          prevResults: {},
          priority: 10,
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "mid",
          queue: "q1",
          input: {},
          prevResults: {},
          priority: 5,
        });

        const tasks = await q.claim({ queues: ["q1"], limit: 3 });
        expect(tasks[0]!.stepName).toBe("high");
        expect(tasks[1]!.stepName).toBe("mid");
        expect(tasks[2]!.stepName).toBe("low");
      });

      it("FIFO within same priority", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "first",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "second",
          queue: "q1",
          input: {},
          prevResults: {},
        });

        const tasks = await q.claim({ queues: ["q1"], limit: 2 });
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
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.claim({ queues: ["q1"], limit: 1 }); // now running

        // With a 1ms timeout after a 10ms wait, the task is stale
        await new Promise((r) => setTimeout(r, 10));
        const requeued = await q.requeueStuck({ staleTimeoutMs: 1 });
        expect(requeued).toBe(1);

        // Can be claimed again
        const tasks = await q.claim({ queues: ["q1"], limit: 1 });
        expect(tasks).toHaveLength(1);
        expect(tasks[0]!.status).toBe("running");
      });

      it("does not requeue tasks within timeout", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.claim({ queues: ["q1"], limit: 1 });

        // Very large timeout — task is not stale
        const requeued = await q.requeueStuck({ staleTimeoutMs: 600_000 });
        expect(requeued).toBe(0);
      });

      it("does not requeue completed or failed tasks", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s2",
          queue: "q1",
          input: {},
          prevResults: {},
        });

        const tasks = await q.claim({ queues: ["q1"], limit: 2 });
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
      it("returns counts per queue", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s2",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s3",
          queue: "q2",
          input: {},
          prevResults: {},
        });
        await q.claim({ queues: ["q1"], limit: 1 });

        const m = await q.metrics();
        expect(m["q1"]!.pending).toBe(1);
        expect(m["q1"]!.running).toBe(1);
        expect(m["q2"]!.pending).toBe(1);
      });

      it("tracks completed and failed counts", async () => {
        const q = await getQueue();
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s1",
          queue: "q1",
          input: {},
          prevResults: {},
        });
        await q.enqueue({
          workflowId: "wf-1",
          stepName: "s2",
          queue: "q1",
          input: {},
          prevResults: {},
        });

        const tasks = await q.claim({ queues: ["q1"], limit: 2 });
        await q.complete({ taskId: tasks[0]!.id, result: "ok", durationMs: 10 });
        await q.fail({ taskId: tasks[1]!.id, error: "err", durationMs: 10 });

        const m = await q.metrics();
        expect(m["q1"]!.completed).toBe(1);
        expect(m["q1"]!.failed).toBe(1);
        expect(m["q1"]!.pending).toBe(0);
        expect(m["q1"]!.running).toBe(0);
      });
    });
  });
}
