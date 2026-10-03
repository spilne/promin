// ---------------------------------------------------------------------------
// Portable workflow version-registry conformance suite
//
// Usage:
//   import { versionRegistryTestSuite } from "@promin/workflow/testing";
//   versionRegistryTestSuite(() => new MyRegistry());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow, type Workflow } from "./durable-pipeline.ts";
import type { IWorkflowVersionRegistry } from "./workflow-version-registry.ts";

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
 * `IWorkflowVersionRegistry`. The factory is called once per test and must
 * return an empty registry.
 */
export function versionRegistryTestSuite(
  factory: () => IWorkflowVersionRegistry | Promise<IWorkflowVersionRegistry>,
  options: VersionRegistryTestSuiteOptions = {},
): void {
  const hasLifecycle = options.hasLifecycle ?? true;

  describe("WorkflowVersionRegistry conformance", () => {
    describe("lookup", () => {
      it("resolves by (name, version) and keeps names isolated", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "billing", version: "1" }));

        const billing = await reg.resolve("billing", "1");
        expect(billing?.name).toBe("billing");
        expect(billing?.version).toBe("1");
        const orders = await reg.resolve("orders", "1");
        expect(orders?.name).toBe("orders");
      });

      it("returns undefined for an unknown name or version", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        expect(await reg.resolve("ghost")).toBeUndefined();
        expect(await reg.resolve("orders", "99")).toBeUndefined();
        expect(await reg.latest("ghost")).toBeUndefined();
        expect(await reg.versions("ghost")).toEqual([]);
      });

      it("resolve without a version returns the latest registered", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "orders", version: "2" }));
        expect(await reg.latest("orders")).toBe("2");
        expect((await reg.resolve("orders"))?.version).toBe("2");
      });

      it("latest stays correct past 100 registered versions", async () => {
        const reg = await factory();
        for (let i = 1; i <= 105; i++) {
          await reg.register(def({ name: "many", version: String(i) }));
        }
        expect(await reg.latest("many")).toBe("105");
        expect((await reg.resolve("many"))?.version).toBe("105");
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

        await reg.deregister("orders", "1");

        expect(await reg.resolve("orders", "1")).toBeUndefined();
        expect((await reg.resolve("billing", "1"))?.name).toBe("billing");
        expect(await reg.versions("billing")).toEqual(["1"]);
      });

      it("deregistering the latest falls back to the previous version", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.register(def({ name: "orders", version: "2" }));
        await reg.deregister("orders", "2");
        expect(await reg.latest("orders")).toBe("1");
        expect((await reg.resolve("orders"))?.version).toBe("1");
      });

      it("is a no-op for an unknown version", async () => {
        const reg = await factory();
        await reg.register(def({ name: "orders", version: "1" }));
        await reg.deregister("orders", "42");
        expect(await reg.versions("orders")).toEqual(["1"]);
      });
    });

    if (hasLifecycle) {
      describe("lifecycle", () => {
        it("new registrations start inactive and nothing is active", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          expect((await reg.getStatus!("orders", "1"))?.status).toBe("inactive");
          expect(await reg.findActive!("orders")).toBeNull();
          expect(await reg.getStatus!("orders", "9")).toBeNull();
        });

        it("promote makes one version active and demotes the prior one", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.register(def({ name: "orders", version: "2" }));

          await reg.promote!("orders", "1");
          expect((await reg.findActive!("orders"))?.version).toBe("1");

          const rec = await reg.promote!("orders", "2");
          expect(rec.status).toBe("active");
          expect(rec.activeAt).toBeInstanceOf(Date);
          expect((await reg.findActive!("orders"))?.version).toBe("2");
          expect((await reg.getStatus!("orders", "1"))?.status).toBe("inactive");
        });

        it("promote is scoped to the workflow name", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.register(def({ name: "billing", version: "1" }));
          await reg.promote!("orders", "1");
          await reg.promote!("billing", "1");
          expect((await reg.findActive!("orders"))?.name).toBe("orders");
          expect((await reg.getStatus!("orders", "1"))?.status).toBe("active");
          expect((await reg.getStatus!("billing", "1"))?.status).toBe("active");
        });

        it("re-registering keeps the lifecycle status", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.promote!("orders", "1");
          await reg.register(def({ name: "orders", version: "1" }));
          expect((await reg.getStatus!("orders", "1"))?.status).toBe("active");
        });

        it("rollback archives the active version and activates the target", async () => {
          const reg = await factory();
          await reg.register(def({ name: "orders", version: "1" }));
          await reg.register(def({ name: "orders", version: "2" }));
          await reg.promote!("orders", "2");

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
          await expect(Promise.resolve().then(() => reg.promote!("orders", "9"))).rejects.toThrow();
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
