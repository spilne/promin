import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { applyDiscoveredSchedules, ScheduleScanner } from "../schedule-scanner.ts";
import { WorkflowScanner } from "../workflow-scanner.ts";
import { InMemorySchedulerStorage } from "../../scheduler/in-memory-scheduler-storage.ts";
import type { DurableScheduleConfig } from "../../scheduler/types.ts";

const ids = async (storage: InMemorySchedulerStorage) =>
  (await storage.listSchedules({ limit: 10_000 })).map((s) => s.id).sort();

describe("applyDiscoveredSchedules — scope", () => {
  it("sync without a namespace only deletes global schedules", async () => {
    const storage = new InMemorySchedulerStorage();
    await storage.upsertSchedule({ id: "global-stale", intervalMs: 1_000 });
    await storage.upsertSchedule({ id: "tenant-a", intervalMs: 1_000, namespace: "a" });
    await storage.upsertSchedule({ id: "agent-made", intervalMs: 1_000, namespace: "agents" });

    const result = await applyDiscoveredSchedules({
      storage,
      schedules: [{ id: "global-new", intervalMs: 5_000 }],
      sync: true,
    });

    expect(result).toEqual({
      upserted: ["global-new"],
      added: ["global-new"],
      deleted: ["global-stale"],
      skipped: [],
    });
    expect(await ids(storage)).toEqual(["agent-made", "global-new", "tenant-a"]);
  });

  it("a namespace scopes upserts and deletes to that namespace", async () => {
    const storage = new InMemorySchedulerStorage();
    await storage.upsertSchedule({ id: "a-stale", intervalMs: 1_000, namespace: "a" });
    await storage.upsertSchedule({ id: "global", intervalMs: 1_000 });

    const result = await applyDiscoveredSchedules({
      storage,
      namespace: "a",
      sync: true,
      schedules: [
        { id: "a-new", intervalMs: 5_000, namespace: "a" },
        { id: "b-other", intervalMs: 5_000, namespace: "b" },
      ],
    });

    expect(result.upserted).toEqual(["a-new"]);
    expect(result.deleted).toEqual(["a-stale"]);
    expect(result.skipped).toEqual(["b-other"]);
    expect(await ids(storage)).toEqual(["a-new", "global"]);
  });

  it("pages through every stored schedule before deciding what to delete", async () => {
    const storage = new InMemorySchedulerStorage();
    const keep: DurableScheduleConfig[] = [];
    for (let i = 0; i < 1_200; i++) {
      const config = { id: `s-${String(i).padStart(4, "0")}`, intervalMs: 60_000 };
      await storage.upsertSchedule(config);
      if (i % 2 === 0) keep.push(config);
    }

    const result = await applyDiscoveredSchedules({ storage, schedules: keep, sync: true });

    expect(result.added).toEqual([]);
    expect(result.deleted).toHaveLength(600);
    expect(await storage.countSchedules()).toBe(600);
  });

  it("rejects invalid schedules before writing anything", async () => {
    const storage = new InMemorySchedulerStorage();
    await expect(
      applyDiscoveredSchedules({
        storage,
        schedules: [
          { id: "fine", intervalMs: 1_000 },
          { id: "two-triggers", intervalMs: 1_000, cron: "* * * * *" },
        ],
      }),
    ).rejects.toThrow(/two-triggers/);
    expect(await storage.countSchedules()).toBe(0);
  });
});

describe("scanners", () => {
  let root: string;

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), "promin-discovery-"));
    await mkdir(join(root, "nested"));
    await writeFile(
      join(root, "schedules.mjs"),
      [
        'export const nightly = { id: "nightly", cron: "0 0 * * *" };',
        'export const broken = { id: "broken", cron: "not a cron" };',
        'export const both = [{ id: "a", intervalMs: 1000 }, { id: "b", intervalMs: -5 }];',
      ].join("\n"),
    );
    const wf = (name: string, version?: string) =>
      JSON.stringify({ name, version, dag: { name, steps: [] }, _definition: { steps: [] } });
    await writeFile(
      join(root, "nested", "workflows.mjs"),
      [
        `export const orderV1 = ${wf("order", "1")};`,
        `export const orderV2 = ${wf("order", "2")};`,
        `export const plain = ${wf("plain")};`,
        "export default plain;",
      ].join("\n"),
    );
    await writeFile(join(root, "dup.mjs"), `export const again = ${wf("order", "2")};`);
  });

  afterAll(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it("ScheduleScanner leaves invalid configs out with a warning", async () => {
    const result = await ScheduleScanner.scanFolder({ root });

    expect(result.schedules.map((s) => s.id).sort()).toEqual(["a", "nightly"]);
    expect(result.warnings.filter((w) => w.startsWith("invalid schedule"))).toHaveLength(2);
  });

  it("WorkflowScanner keeps every version and warns only on a true duplicate", async () => {
    const seen: string[] = [];
    const result = await WorkflowScanner.scanFolder({
      root,
      onWorkflow: ({ workflow }) => seen.push(`${workflow.name}@${workflow.version ?? "-"}`),
    });

    const keys = result.definitions.map((d) => `${d.name}@${d.version ?? "-"}`).sort();
    expect(keys).toEqual(["order@1", "order@2", "plain@-"]);
    expect(Object.keys(result.workflows).sort()).toEqual(["order", "plain"]);
    expect(Object.keys(result.sources).sort()).toEqual(["order@1", "order@2", "plain"]);
    const duplicates = result.warnings.filter((w) => w.startsWith("duplicate workflow"));
    expect(duplicates).toHaveLength(1);
    expect(duplicates[0]).toContain('"order@2"');
    expect(seen.filter((s) => s === "plain@-")).toHaveLength(1);
  });
});
