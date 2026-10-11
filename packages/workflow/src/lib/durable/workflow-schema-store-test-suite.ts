// ---------------------------------------------------------------------------
// Portable `WorkflowSchemaStore` conformance suite. Every implementation
// (in-memory, SQLite, Postgres) must pass identically.
//
// Usage:
//   import { workflowSchemaStoreTestSuite } from "@promin/workflow/testing";
//   workflowSchemaStoreTestSuite(() => new InMemoryWorkflowSchemaStore());
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type {
  WorkflowSchemaDraft,
  WorkflowSchemaStore,
  WorkflowSchemaVersion,
} from "./workflow-schema-store.ts";
import type { WorkflowSchema } from "./workflow-schema.ts";

function schema(name: string, stepName = "a"): WorkflowSchema {
  return {
    version: 1,
    name,
    steps: [{ type: "step", name: stepName, dependsOn: [], activityRef: "text.upper" }],
  };
}

function draft(overrides: Partial<WorkflowSchemaDraft> & { name: string }): WorkflowSchemaDraft {
  return {
    schema: schema(overrides.name),
    revision: 1,
    createdAt: 1_000,
    updatedAt: 1_000,
    ...overrides,
  };
}

function version(
  overrides: Partial<WorkflowSchemaVersion> & { name: string; sequence: number },
): WorkflowSchemaVersion {
  return {
    version: `v${overrides.sequence}`,
    schema: schema(overrides.name),
    contentHash: `hash-${overrides.sequence}`,
    publishedAt: 2_000,
    ...overrides,
  };
}

export function workflowSchemaStoreTestSuite(
  factory: () => WorkflowSchemaStore | Promise<WorkflowSchemaStore>,
) {
  async function make(): Promise<WorkflowSchemaStore> {
    return factory();
  }

  describe("WorkflowSchemaStore conformance", () => {
    it("starts empty", async () => {
      const store = await make();
      expect(await store.list()).toEqual([]);
      expect(await store.get("missing")).toBeNull();
    });

    it("round-trips every draft and version field", async () => {
      const store = await make();
      const full: WorkflowSchema = {
        version: 1,
        name: "orders",
        inputSchema: {
          type: "object",
          properties: { id: { type: "string", default: "o-1" } },
          required: ["id"],
        },
        steps: [
          {
            type: "step",
            name: "fetch",
            dependsOn: [],
            activityRef: "http.get",
            config: { url: "https://example.com", nested: { retries: 2 } },
            options: { retry: { maxRetries: 3, baseDelayMs: 100 }, timeoutMs: 5_000 },
          },
          {
            type: "branch",
            name: "route",
            dependsOn: ["fetch"],
            conditionRef: "is.big",
            ifTrue: { activityRef: "text.upper" },
            ifFalse: { activityRef: "text.lower", config: { locale: "uk" } },
          },
          {
            type: "parallel",
            name: "fan",
            dependsOn: ["route"],
            branches: { a: { activityRef: "text.upper" }, b: { activityRef: "text.lower" } },
          },
          { type: "sleep", name: "wait", dependsOn: ["fan"], ms: 1_000 },
          { type: "approval", name: "ok", dependsOn: ["wait"], timeoutMs: 60_000 },
        ],
        ui: { fetch: { x: 10, y: 20, label: "Fetch — ✓ ünïcödé" } },
      };
      const d = draft({ name: "orders", schema: full, revision: 1, createdAt: 5, updatedAt: 6 });
      expect(await store.saveDraft({ draft: d, expectedRevision: null })).toBe(true);
      const v = version({ name: "orders", sequence: 1, schema: full, publishedAt: 7 });
      expect(await store.addVersion(v)).toBe(true);

      expect(await store.get("orders")).toEqual({ draft: d, versions: [v] });
      expect(await store.list()).toEqual([{ draft: d, versions: [v] }]);
    });

    it("creates a draft only when none exists", async () => {
      const store = await make();
      expect(await store.saveDraft({ draft: draft({ name: "w" }), expectedRevision: null })).toBe(
        true,
      );
      const again = draft({ name: "w", schema: schema("w", "other") });
      expect(await store.saveDraft({ draft: again, expectedRevision: null })).toBe(false);
      expect((await store.get("w"))!.draft.schema.steps[0]!.name).toBe("a");
    });

    it("updates a draft only from the revision it was read at", async () => {
      const store = await make();
      await store.saveDraft({ draft: draft({ name: "w" }), expectedRevision: null });
      const next = draft({ name: "w", schema: schema("w", "b"), revision: 2, updatedAt: 2_000 });
      expect(await store.saveDraft({ draft: next, expectedRevision: 1 })).toBe(true);

      // A second editor still holding revision 1 is refused.
      const stale = draft({ name: "w", schema: schema("w", "c"), revision: 2 });
      expect(await store.saveDraft({ draft: stale, expectedRevision: 1 })).toBe(false);
      const row = (await store.get("w"))!.draft;
      expect(row.revision).toBe(2);
      expect(row.schema.steps[0]!.name).toBe("b");
      expect(row.updatedAt).toBe(2_000);
    });

    it("refuses an update of a draft that does not exist", async () => {
      const store = await make();
      expect(await store.saveDraft({ draft: draft({ name: "w" }), expectedRevision: 3 })).toBe(
        false,
      );
      expect(await store.get("w")).toBeNull();
    });

    it("keeps versions ordered by sequence and refuses a duplicate tag or sequence", async () => {
      const store = await make();
      await store.saveDraft({ draft: draft({ name: "w" }), expectedRevision: null });
      expect(await store.addVersion(version({ name: "w", sequence: 2 }))).toBe(true);
      expect(await store.addVersion(version({ name: "w", sequence: 1 }))).toBe(true);
      expect(await store.addVersion(version({ name: "w", sequence: 3, version: "v1" }))).toBe(
        false,
      );
      expect(await store.addVersion(version({ name: "w", sequence: 2, version: "v9" }))).toBe(
        false,
      );
      const { versions } = (await store.get("w"))!;
      expect(versions.map((v) => v.version)).toEqual(["v1", "v2"]);
    });

    it("keeps the versions of different workflows apart", async () => {
      const store = await make();
      for (const name of ["b", "a"]) {
        await store.saveDraft({ draft: draft({ name }), expectedRevision: null });
        await store.addVersion(version({ name, sequence: 1 }));
      }
      const all = await store.list();
      expect(all.map((row) => row.draft.name)).toEqual(["a", "b"]);
      expect(all.map((row) => row.versions.length)).toEqual([1, 1]);
    });

    it("delete removes the draft and its versions; re-creating starts clean", async () => {
      const store = await make();
      await store.saveDraft({ draft: draft({ name: "w" }), expectedRevision: null });
      await store.addVersion(version({ name: "w", sequence: 1 }));
      await store.delete("w");
      expect(await store.get("w")).toBeNull();
      expect(await store.list()).toEqual([]);

      expect(await store.saveDraft({ draft: draft({ name: "w" }), expectedRevision: null })).toBe(
        true,
      );
      expect((await store.get("w"))!.versions).toEqual([]);
      expect(await store.addVersion(version({ name: "w", sequence: 1 }))).toBe(true);
    });

    it("deleting what is absent is a no-op", async () => {
      const store = await make();
      await store.delete("ghost");
      expect(await store.list()).toEqual([]);
    });

    it("hands out copies, so callers cannot mutate stored rows", async () => {
      const store = await make();
      const d = draft({ name: "w" });
      await store.saveDraft({ draft: d, expectedRevision: null });
      (d.schema.steps[0] as { name: string }).name = "mutated";
      const read = (await store.get("w"))!;
      (read.draft.schema.steps[0] as { name: string }).name = "mutated-too";
      expect((await store.get("w"))!.draft.schema.steps[0]!.name).toBe("a");
    });
  });
}
