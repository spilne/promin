// ---------------------------------------------------------------------------
// Portable WorkflowAdvertisementRegistry conformance suite. Every
// implementation (in-memory, SQLite, future Postgres) must pass.
//
// Usage:
//   import { workflowAdvertisementRegistryTestSuite } from "@promin/workflow";
//   workflowAdvertisementRegistryTestSuite(() => new InMemoryWorkflowAdvertisementRegistry());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type {
  AdvertisedWorkflow,
  WorkflowAdvertisementRegistry,
} from "./workflow-advertisements.ts";

function adv(name: string, patch?: Partial<AdvertisedWorkflow>): AdvertisedWorkflow {
  return {
    name,
    steps: [{ name: "step", kind: "single", dependsOn: [] }],
    ...patch,
  };
}

export function workflowAdvertisementRegistryTestSuite(
  factory: () => WorkflowAdvertisementRegistry | Promise<WorkflowAdvertisementRegistry>,
): void {
  async function make(): Promise<WorkflowAdvertisementRegistry> {
    return factory();
  }

  describe("WorkflowAdvertisementRegistry conformance", () => {
    it("starts empty", async () => {
      const r = await make();
      expect(await r.list()).toEqual([]);
      expect(await r.distinct()).toEqual([]);
    });

    it("upsert stores the worker's advertisements; list returns them grouped", async () => {
      const r = await make();
      await r.upsert("worker-1", [adv("hello"), adv("world", { version: "2" })]);
      const entries = await r.list();
      expect(entries.length).toBe(1);
      expect(entries[0]?.workerId).toBe("worker-1");
      expect(entries[0]?.workflows.map((w) => w.name).sort()).toEqual(["hello", "world"]);
      expect(entries[0]?.advertisedAt).toBeInstanceOf(Date);
    });

    it("distinct dedupes on (name, version), latest advertisedAt wins", async () => {
      const r = await make();
      await r.upsert("worker-old", [adv("hello")]);
      await new Promise((res) => setTimeout(res, 5));
      await r.upsert("worker-new", [adv("hello", { sampleInput: { v: 2 } })]);
      const distinct = await r.distinct();
      const hello = distinct.find((w) => w.name === "hello");
      expect(hello?.sampleInput).toEqual({ v: 2 });
    });

    it("distinct keeps separate entries for different versions of the same name", async () => {
      const r = await make();
      await r.upsert("w1", [adv("hello", { version: "1" }), adv("hello", { version: "2" })]);
      const distinct = await r.distinct();
      const versions = distinct.filter((w) => w.name === "hello").map((w) => w.version);
      expect(versions.sort()).toEqual(["1", "2"]);
    });

    it("upsert replaces the worker's prior set atomically", async () => {
      const r = await make();
      await r.upsert("w1", [adv("a"), adv("b"), adv("c")]);
      await r.upsert("w1", [adv("b")]);
      const entry = (await r.list()).find((e) => e.workerId === "w1");
      expect(entry?.workflows.map((w) => w.name).sort()).toEqual(["b"]);
    });

    it("upsert from a second worker doesn't affect the first", async () => {
      const r = await make();
      await r.upsert("w1", [adv("a")]);
      await r.upsert("w2", [adv("b")]);
      const all = await r.list();
      expect(all.length).toBe(2);
      expect(all.find((e) => e.workerId === "w1")?.workflows[0]?.name).toBe("a");
      expect(all.find((e) => e.workerId === "w2")?.workflows[0]?.name).toBe("b");
    });

    it("remove drops only that worker's advertisements", async () => {
      const r = await make();
      await r.upsert("w1", [adv("a")]);
      await r.upsert("w2", [adv("b")]);
      await r.remove("w1");
      const entries = await r.list();
      expect(entries.map((e) => e.workerId)).toEqual(["w2"]);
    });

    it("remove of an unknown worker is a no-op", async () => {
      const r = await make();
      await r.upsert("w1", [adv("a")]);
      await r.remove("never-existed");
      expect((await r.list()).length).toBe(1);
    });

    it("round-trips steps, version, sampleInput intact", async () => {
      const r = await make();
      await r.upsert("w1", [
        {
          name: "video",
          version: "3",
          steps: [
            { name: "fetch", kind: "single", dependsOn: [] },
            { name: "transcode", kind: "single", dependsOn: ["fetch"], needs: ["gpu"] },
            { name: "upload", kind: "single", dependsOn: ["transcode"], priority: 5 },
          ],
          sampleInput: { url: "x" },
        },
      ]);
      const back = (await r.distinct()).find((w) => w.name === "video");
      expect(back?.version).toBe("3");
      expect(back?.steps).toEqual([
        { name: "fetch", kind: "single", dependsOn: [] },
        { name: "transcode", kind: "single", dependsOn: ["fetch"], needs: ["gpu"] },
        { name: "upload", kind: "single", dependsOn: ["transcode"], priority: 5 },
      ]);
      expect(back?.sampleInput).toEqual({ url: "x" });
    });
  });
}
