// ---------------------------------------------------------------------------
// Portable WorkflowStartQueue conformance suite. Every implementation
// (in-memory, SQLite, future Postgres) must pass.
//
// Usage:
//   import { workflowStartQueueTestSuite } from "@promin/workflow";
//   workflowStartQueueTestSuite(() => new InMemoryWorkflowStartQueue());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { WorkflowStartQueue } from "./workflow-start-queue.ts";

const ANY_VERSION: ReadonlyArray<string> = [];

export function workflowStartQueueTestSuite(
  factory: () => WorkflowStartQueue | Promise<WorkflowStartQueue>,
): void {
  async function make(): Promise<WorkflowStartQueue> {
    return factory();
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
      await q.complete(claimedId);
      expect((await q.list()).length).toBe(0);
    });

    it("complete on unknown id is a no-op", async () => {
      const q = await make();
      await q.complete("never-existed");
      // Should not throw.
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

    it("namespace round-trips through enqueue/claim", async () => {
      const q = await make();
      await q.enqueue({
        workflowId: "a",
        workflowName: "wf",
        namespace: "acme",
        input: {},
      });
      const claimed = await q.claim({
        workflowSpecs: [{ name: "wf", versions: ANY_VERSION }],
        limit: 1,
      });
      expect(claimed[0]?.namespace).toBe("acme");
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
