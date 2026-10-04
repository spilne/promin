// ---------------------------------------------------------------------------
// Portable workflow version-registry conformance suite
//
// Usage:
//   import { versionRegistryTestSuite } from "@promin/workflow/testing";
//   versionRegistryTestSuite(() => new MyRegistry());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "./workflow-builder.ts";
import type { Workflow } from "./workflow-types.ts";
import {
  InMemoryWorkflowVersionRegistry,
  type WorkflowVersionRegistry,
} from "./workflow-version-registry.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";

export interface VersionRegistryTestSuiteOptions {
  /**
   * Run the lifecycle section (`promote` / `rollback` / `findActive` /
   * `getStatus` / `listRecords`). Defaults to `true`; set `false` for
   * registries that only implement the core lookup methods.
   */
  hasLifecycle?: boolean;
}

function def(params: { name: string; version: string }): Workflow<unknown, unknown> {
  return workflow<unknown>({ name: params.name, version: params.version })
    .step(`${params.name}-step`, () => succeed(params.version))
    .build() as unknown as Workflow<unknown, unknown>;
}

/**
 * Run the version-registry conformance suite against any implementation of
 * `WorkflowVersionRegistry`. The factory is called once per test and must
 * return an empty registry.
 */
export function versionRegistryTestSuite(
  factory: () => WorkflowVersionRegistry | Promise<WorkflowVersionRegistry>,
  options: VersionRegistryTestSuiteOptions = {},
): void {
  const hasLifecycle = options.hasLifecycle ?? true;

  describe("WorkflowVersionRegistry conformance", () => {
    describe("lookup", () => {
      it("resolves by (name, version) and keeps names isolated", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "billing", version: "1" }));

        const billing = await reg.resolve({ name: "billing", version: "1" });
        expect(billing?.name).toBe("billing");
        expect(billing?.version).toBe("1");
        const orders = await reg.resolve({ name: "orders", version: "1" });
        expect(orders?.name).toBe("orders");
      });

      it("returns undefined for an unknown name or version", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        expect(await reg.resolve({ name: "ghost" })).toBeUndefined();
        expect(await reg.resolve({ name: "orders", version: "99" })).toBeUndefined();
        expect(await reg.latest("ghost")).toBeUndefined();
        expect(await reg.versions("ghost")).toEqual([]);
      });

      it("resolve without a version returns the latest registered", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "orders", version: "2" }));
        expect(await reg.latest("orders")).toBe("2");
        expect((await reg.resolve({ name: "orders" }))?.version).toBe("2");
      });

      it("latest stays correct past 100 registered versions", async () => {
        const reg = await factory();
        for (let i = 1; i <= 105; i++) {
          await reg.register(def({ name: "many", version: String(i) }));
        }
        expect(await reg.latest("many")).toBe("105");
        expect((await reg.resolve({ name: "many" }))?.version).toBe("105");
        expect(await reg.versions("many")).toHaveLength(105);
      });

      it("lists versions and names", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "orders", version: "2" }));
        await reg.register(def({ name: "billing", version: "7" }));
        expect([...(await reg.versions("orders"))].sort()).toEqual(["1", "2"]);
        expect([...(await reg.versions("billing"))]).toEqual(["7"]);
        expect([...(await reg.names())].sort()).toEqual(["billing", "orders"]);
      });

      it("re-registering the same (name, version) does not duplicate it", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "orders", version: "1" }));
        expect(await reg.versions("orders")).toEqual(["1"]);
      });

      it("rejects a definition without a version", async () => {
        const reg = await factory();
        const noVersion = workflow<unknown>({ name: "nov" })
          .step("s", () => succeed(1))
          .build() as unknown as Workflow<unknown, unknown>;
        let error: unknown;
        try {
          await reg.register(noVersion);
        } catch (e) {
          error = e;
        }
        expect(String(error)).toContain("must have a version");
      });
    });

    describe("deregister", () => {
      it("removes only the named workflow's version", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "billing", version: "1" }));

        await reg.deregister({ name: "orders", version: "1" });

        expect(await reg.resolve({ name: "orders", version: "1" })).toBeUndefined();
        expect((await reg.resolve({ name: "billing", version: "1" }))?.name).toBe("billing");
        expect(await reg.versions("billing")).toEqual(["1"]);
      });

      it("deregistering the latest falls back to the previous version", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "orders", version: "2" }));
        await reg.deregister({ name: "orders", version: "2" });
        expect(await reg.latest("orders")).toBe("1");
        expect((await reg.resolve({ name: "orders" }))?.version).toBe("1");
      });

      it("is a no-op for an unknown version", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.deregister({ name: "orders", version: "42" });
        expect(await reg.versions("orders")).toEqual(["1"]);
      });
    });

    if (hasLifecycle) {
      describe("lifecycle", () => {
        it("new registrations start inactive and nothing is active", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          expect((await reg.getStatus!({ name: "orders", version: "1" }))?.status).toBe("inactive");
          expect(await reg.findActive!("orders")).toBeNull();
          expect(await reg.getStatus!({ name: "orders", version: "9" })).toBeNull();
        });

        it("promote makes one version active and demotes the prior one", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.register(def({ name: "orders", version: "2" }));

          await reg.promote!({ name: "orders", version: "1" });
          expect((await reg.findActive!("orders"))?.version).toBe("1");

          const rec = await reg.promote!({ name: "orders", version: "2" });
          expect(rec.status).toBe("active");
          expect(rec.activeAt).toBeInstanceOf(Date);
          expect((await reg.findActive!("orders"))?.version).toBe("2");
          expect((await reg.getStatus!({ name: "orders", version: "1" }))?.status).toBe("inactive");
        });

        it("promote is scoped to the workflow name", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.register(def({ name: "billing", version: "1" }));
          await reg.promote!({ name: "orders", version: "1" });
          await reg.promote!({ name: "billing", version: "1" });
          expect((await reg.findActive!("orders"))?.name).toBe("orders");
          expect((await reg.getStatus!({ name: "orders", version: "1" }))?.status).toBe("active");
          expect((await reg.getStatus!({ name: "billing", version: "1" }))?.status).toBe("active");
        });

        it("re-registering keeps the lifecycle status", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.promote!({ name: "orders", version: "1" });
          await reg.register(def({ name: "orders", version: "1" }));
          expect((await reg.getStatus!({ name: "orders", version: "1" }))?.status).toBe("active");
        });

        it("rollback archives the active version and activates the target", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.register(def({ name: "orders", version: "2" }));
          await reg.promote!({ name: "orders", version: "2" });

          const { previous, active } = await reg.rollback!({ name: "orders", toVersion: "1" });
          expect(previous.version).toBe("2");
          expect(previous.status).toBe("archived");
          expect(previous.archivedAt).toBeInstanceOf(Date);
          expect(active.version).toBe("1");
          expect(active.status).toBe("active");
          expect((await reg.findActive!("orders"))?.version).toBe("1");
        });

        it("promote and rollback reject unknown versions", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await expect(
            Promise.resolve().then(() => reg.promote!({ name: "orders", version: "9" })),
          ).rejects.toThrow();
          await expect(
            Promise.resolve().then(() => reg.rollback!({ name: "orders", toVersion: "9" })),
          ).rejects.toThrow();
        });

        it("listRecords returns every version of one workflow only", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.register(def({ name: "orders", version: "2" }));
          await reg.register(def({ name: "billing", version: "1" }));
          const records = await reg.listRecords!("orders");
          expect(records.map((r) => r.version).sort()).toEqual(["1", "2"]);
          expect(records.every((r) => r.name === "orders")).toBe(true);
        });
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Drain conformance — `WorkflowVersionRegistry.countByVersion` over a
// `WorkflowStorage`. Run it against every storage backend: drain detection
// reads run counts per (name, version, status) from the store.
// ---------------------------------------------------------------------------

let drainSeq = 0;
function freshName(label: string): string {
  drainSeq += 1;
  return `drain-${label}-${drainSeq}-${crypto.randomUUID().slice(0, 8)}`;
}

async function createRun(params: {
  storage: WorkflowStorage;
  name: string;
  version?: string;
}): Promise<string> {
  const workflowId = `${params.name}-${crypto.randomUUID()}`;
  await params.storage.createWorkflow({
    workflowId,
    workflowName: params.name,
    input: {},
    version: params.version,
  });
  return workflowId;
}

/**
 * Run the version-drain conformance suite against a `WorkflowStorage`.
 * Each test uses fresh workflow names, so a shared database needs no
 * truncation between tests.
 */
export function versionDrainTestSuite(
  factory: () => WorkflowStorage | Promise<WorkflowStorage>,
): void {
  describe("WorkflowVersionRegistry drain conformance", () => {
    it("listWorkflows and countWorkflows filter by version", async () => {
      const storage = await factory();
      const name = freshName("filter");
      await createRun({ storage, name, version: "1" });
      await createRun({ storage, name, version: "1" });
      await createRun({ storage, name, version: "2" });
      await createRun({ storage, name });

      expect(await storage.listWorkflows({ name, version: "1" })).toHaveLength(2);
      expect((await storage.listWorkflows({ name, version: "2" })).map((w) => w.version)).toEqual([
        "2",
      ]);
      expect(await storage.listWorkflows({ name, version: "9" })).toEqual([]);
      if (storage.countWorkflows) {
        expect(await storage.countWorkflows({ name, version: "1" })).toBe(2);
        expect(await storage.countWorkflows({ name, version: "1", status: "pending" })).toBe(2);
        expect(await storage.countWorkflows({ name, version: "2" })).toBe(1);
        expect(await storage.countWorkflows({ name, version: "9" })).toBe(0);
        expect(await storage.countWorkflows({ name })).toBe(4);
      }
    });

    it("counts runs per version by status; terminal runs are not in flight", async () => {
      const storage = await factory();
      const name = freshName("counts");
      const registry = new InMemoryWorkflowVersionRegistry();
      registry.register(def({ name, version: "1" }));
      registry.register(def({ name, version: "2" }));

      const done = await createRun({ storage, name, version: "1" });
      await storage.completeWorkflow({ workflowId: done, result: "ok" });
      const failed = await createRun({ storage, name, version: "1" });
      await storage.failWorkflow({ workflowId: failed, error: "boom" });
      await createRun({ storage, name, version: "2" });
      if (storage.tripwireWorkflow) {
        const tripped = await createRun({ storage, name, version: "1" });
        await storage.tripwireWorkflow({ workflowId: tripped, reason: { reason: "halt" } });
      }

      const counts = await registry.countByVersion({ name, storage });
      expect(counts.get("1")).toEqual({
        running: 0,
        completed: 1,
        failed: 1,
        tripwire: storage.tripwireWorkflow ? 1 : 0,
      });
      expect(counts.get("2")).toEqual({ running: 1, completed: 0, failed: 0, tripwire: 0 });
    });

    it("a version whose only runs tripwired drains", async () => {
      const storage = await factory();
      if (!storage.tripwireWorkflow) return;
      const name = freshName("tripwire");
      const drained: string[] = [];
      const registry = new InMemoryWorkflowVersionRegistry({
        autoDeregister: true,
        onDrained: (_n, version) => void drained.push(version),
      });
      registry.register(def({ name, version: "1" }));
      registry.register(def({ name, version: "2" }));

      const tripped = await createRun({ storage, name, version: "1" });
      await storage.tripwireWorkflow({ workflowId: tripped, reason: { reason: "halt" } });
      await createRun({ storage, name, version: "2" });

      await registry.countByVersion({ name, storage });
      expect(drained).toEqual(["1"]);
      expect(await registry.versions(name)).toEqual(["2"]);
    });

    it("an in-flight run keeps its version registered", async () => {
      const storage = await factory();
      const name = freshName("inflight");
      const drained: string[] = [];
      const registry = new InMemoryWorkflowVersionRegistry({
        autoDeregister: true,
        onDrained: (_n, version) => void drained.push(version),
      });
      registry.register(def({ name, version: "1" }));
      registry.register(def({ name, version: "2" }));
      await createRun({ storage, name, version: "1" });

      await registry.countByVersion({ name, storage });
      expect(drained).toEqual(["2"]);
      expect([...(await registry.versions(name))].sort()).toEqual(["1", "2"]);
    });

    it("autoDeregister never removes the promoted active version", async () => {
      const storage = await factory();
      const name = freshName("active");
      const registry = new InMemoryWorkflowVersionRegistry({ autoDeregister: true });
      registry.register(def({ name, version: "1" }));
      registry.register(def({ name, version: "2" }));
      registry.register(def({ name, version: "3" }));
      await registry.promote({ name, version: "3" });
      // Roll back: 1 becomes active while 3 stays the latest registered.
      await registry.rollback({ name, toVersion: "1" });

      await registry.countByVersion({ name, storage });

      expect([...(await registry.versions(name))].sort()).toEqual(["1", "3"]);
      expect((await registry.findActive(name))?.version).toBe("1");
    });

    it("onDrained fires again after a drained version gets new runs and drains", async () => {
      const storage = await factory();
      const name = freshName("renotify");
      const drained: string[] = [];
      const registry = new InMemoryWorkflowVersionRegistry({
        onDrained: (_n, version) => void drained.push(version),
      });
      registry.register(def({ name, version: "1" }));

      await registry.countByVersion({ name, storage });
      await registry.countByVersion({ name, storage });
      expect(drained).toEqual(["1"]);

      const run = await createRun({ storage, name, version: "1" });
      await registry.countByVersion({ name, storage });
      expect(drained).toEqual(["1"]);

      await storage.completeWorkflow({ workflowId: run, result: "ok" });
      await registry.countByVersion({ name, storage });
      expect(drained).toEqual(["1", "1"]);
    });
  });
}
