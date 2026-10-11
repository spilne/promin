// ---------------------------------------------------------------------------
// Portable WorkflowAdvertisementRegistry conformance suite. Every
// implementation (in-memory, SQLite, Postgres) must pass.
//
// The factory receives a `FakeWallClock` and a TTL; a registry that stamps
// `advertisedAt` from an injected clock should use both so the suite can
// order and expire advertisements deterministically. A registry that
// stamps from a server clock (Postgres `NOW()`) may ignore the clock —
// upserts run in sequence — and passes `{ serverClock: true }`, which
// skips the expiry tests (cover those against the real clock).
//
// Usage:
//   import { workflowAdvertisementRegistryTestSuite } from "@promin/workflow/testing";
//   workflowAdvertisementRegistryTestSuite(({ clock }) =>
//     new InMemoryWorkflowAdvertisementRegistry({ clock }),
//   );
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type {
  AdvertisedWorkflow,
  WorkflowAdvertisementRegistry,
} from "./workflow-advertisements.ts";
import { FakeWallClock } from "../shared/wall-clock.ts";

function adv(name: string, patch?: Partial<AdvertisedWorkflow>): AdvertisedWorkflow {
  return {
    name,
    steps: [{ name: "step", kind: "single", dependsOn: [] }],
    ...patch,
  };
}

export interface WorkflowAdvertisementRegistrySuiteFactoryParams {
  /** Time source for `advertisedAt` and expiry. */
  readonly clock: FakeWallClock;
  /** Advertisement lifetime the registry must be configured with. */
  readonly ttlMs: number;
}

export interface WorkflowAdvertisementRegistrySuiteOptions {
  /** The registry stamps and expires on a server clock: skip the clock-driven expiry tests. */
  readonly serverClock?: boolean;
}

/** TTL the suite configures registries with. */
const SUITE_TTL_MS = 60_000;

export function workflowAdvertisementRegistryTestSuite(
  factory: (
    params: WorkflowAdvertisementRegistrySuiteFactoryParams,
  ) => WorkflowAdvertisementRegistry | Promise<WorkflowAdvertisementRegistry>,
  options: WorkflowAdvertisementRegistrySuiteOptions = {},
): void {
  async function makeWithClock(): Promise<{
    r: WorkflowAdvertisementRegistry;
    clock: FakeWallClock;
  }> {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    return { r: await factory({ clock, ttlMs: SUITE_TTL_MS }), clock };
  }
  const expiryIt = options.serverClock ? it.skip : it;

  async function make(): Promise<WorkflowAdvertisementRegistry> {
    return (await makeWithClock()).r;
  }

  describe("WorkflowAdvertisementRegistry conformance", () => {
    it("starts empty", async () => {
      const r = await make();
      expect(await r.list()).toEqual([]);
      expect(await r.distinct()).toEqual([]);
    });

    it("upsert stores the worker's advertisements; list returns them grouped", async () => {
      const r = await make();
      await r.upsert({
        workerId: "worker-1",
        workflows: [adv("hello"), adv("world", { version: "2" })],
      });
      const entries = await r.list();
      expect(entries.length).toBe(1);
      expect(entries[0]?.workerId).toBe("worker-1");
      expect(entries[0]?.workflows.map((w) => w.name).sort()).toEqual(["hello", "world"]);
      expect(entries[0]?.advertisedAt).toBeInstanceOf(Date);
    });

    it("distinct dedupes on (name, version), latest advertisedAt wins", async () => {
      const { r, clock } = await makeWithClock();
      await r.upsert({ workerId: "worker-old", workflows: [adv("hello")] });
      clock.advance(1_000);
      await r.upsert({
        workerId: "worker-new",
        workflows: [adv("hello", { sampleInput: { v: 2 } })],
      });
      const distinct = await r.distinct();
      const hello = distinct.find((w) => w.name === "hello");
      expect(hello?.sampleInput).toEqual({ v: 2 });
    });

    it("distinct follows advertisement time, not first-insertion order", async () => {
      const { r, clock } = await makeWithClock();
      const stepsA = [{ name: "a", kind: "single", dependsOn: [] }];
      const stepsB = [{ name: "b", kind: "single", dependsOn: [] }];
      const helloSteps = async () =>
        (await r.distinct()).find((w) => w.name === "hello" && w.version === "1")?.steps;

      await r.upsert({
        workerId: "w1",
        workflows: [adv("hello", { version: "1", steps: stepsA })],
      });
      clock.advance(1_000);
      await r.upsert({
        workerId: "w2",
        workflows: [adv("hello", { version: "1", steps: stepsB })],
      });
      expect(await helloSteps()).toEqual(stepsB);

      // The first worker re-advertises: it is now the most recent.
      clock.advance(1_000);
      await r.upsert({
        workerId: "w1",
        workflows: [adv("hello", { version: "1", steps: stepsA })],
      });
      expect(await helloSteps()).toEqual(stepsA);

      // And the second worker updates again: it wins back.
      clock.advance(1_000);
      await r.upsert({
        workerId: "w2",
        workflows: [adv("hello", { version: "1", steps: stepsB })],
      });
      expect(await helloSteps()).toEqual(stepsB);
      expect((await r.distinct()).filter((w) => w.name === "hello")).toHaveLength(1);
    });

    it("distinct keeps separate entries for different versions of the same name", async () => {
      const r = await make();
      await r.upsert({
        workerId: "w1",
        workflows: [adv("hello", { version: "1" }), adv("hello", { version: "2" })],
      });
      const distinct = await r.distinct();
      const versions = distinct.filter((w) => w.name === "hello").map((w) => w.version);
      expect(versions.sort()).toEqual(["1", "2"]);
    });

    it("upsert replaces the worker's prior set atomically", async () => {
      const r = await make();
      await r.upsert({ workerId: "w1", workflows: [adv("a"), adv("b"), adv("c")] });
      await r.upsert({ workerId: "w1", workflows: [adv("b")] });
      const entry = (await r.list()).find((e) => e.workerId === "w1");
      expect(entry?.workflows.map((w) => w.name).sort()).toEqual(["b"]);
    });

    it("upsert from a second worker doesn't affect the first", async () => {
      const r = await make();
      await r.upsert({ workerId: "w1", workflows: [adv("a")] });
      await r.upsert({ workerId: "w2", workflows: [adv("b")] });
      const all = await r.list();
      expect(all.length).toBe(2);
      expect(all.find((e) => e.workerId === "w1")?.workflows[0]?.name).toBe("a");
      expect(all.find((e) => e.workerId === "w2")?.workflows[0]?.name).toBe("b");
    });

    it("remove drops only that worker's advertisements", async () => {
      const r = await make();
      await r.upsert({ workerId: "w1", workflows: [adv("a")] });
      await r.upsert({ workerId: "w2", workflows: [adv("b")] });
      await r.remove("w1");
      const entries = await r.list();
      expect(entries.map((e) => e.workerId)).toEqual(["w2"]);
    });

    it("remove of an unknown worker is a no-op", async () => {
      const r = await make();
      await r.upsert({ workerId: "w1", workflows: [adv("a")] });
      await r.remove("never-existed");
      expect((await r.list()).length).toBe(1);
    });

    expiryIt(
      "an advertisement not refreshed within the TTL expires; re-advertising revives it",
      async () => {
        const { r, clock } = await makeWithClock();
        await r.upsert({ workerId: "w-dead", workflows: [adv("a")] });
        clock.advance(SUITE_TTL_MS / 2);
        await r.upsert({ workerId: "w-live", workflows: [adv("b")] });
        clock.advance(SUITE_TTL_MS / 2);

        // w-dead's advertisement is exactly TTL old: expired.
        expect((await r.list()).map((e) => e.workerId)).toEqual(["w-live"]);
        expect((await r.distinct()).map((w) => w.name)).toEqual(["b"]);

        await r.upsert({ workerId: "w-dead", workflows: [adv("a")] });
        expect((await r.distinct()).map((w) => w.name)).toEqual(["a", "b"]);
      },
    );

    expiryIt("prune deletes expired advertisements and counts their workers", async () => {
      const { r, clock } = await makeWithClock();
      await r.upsert({ workerId: "w1", workflows: [adv("a"), adv("b")] });
      await r.upsert({ workerId: "w2", workflows: [adv("c")] });
      clock.advance(SUITE_TTL_MS / 2);
      await r.upsert({ workerId: "w3", workflows: [adv("d")] });
      expect(await r.prune()).toBe(0);

      clock.advance(SUITE_TTL_MS / 2);
      expect(await r.prune()).toBe(2);
      expect((await r.list()).map((e) => e.workerId)).toEqual(["w3"]);
      expect(await r.prune()).toBe(0);
    });

    it("round-trips steps, version, sampleInput intact", async () => {
      const r = await make();
      await r.upsert({
        workerId: "w1",
        workflows: [
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
        ],
      });
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
