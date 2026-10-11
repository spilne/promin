// `SqliteWorkflowSchemaStore` — runs the conformance suite plus SQLite-specific
// checks: rows persist across instances sharing one db, and the table prefix
// is validated before it reaches SQL.

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { workflowSchemaStoreTestSuite } from "@promin/workflow/testing";
import { SqliteWorkflowSchemaStore } from "../sqlite-workflow-schema-store.ts";

workflowSchemaStoreTestSuite(() =>
  SqliteWorkflowSchemaStore.make({ db: new Database(":memory:") }),
);

describe("SqliteWorkflowSchemaStore", () => {
  it("rows persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const writer = SqliteWorkflowSchemaStore.make({ db });
    const schema = {
      version: 1 as const,
      name: "orders",
      steps: [{ type: "step" as const, name: "a", dependsOn: [], activityRef: "text.upper" }],
    };
    await writer.saveDraft({
      draft: { name: "orders", schema, revision: 1, createdAt: 1, updatedAt: 1 },
      expectedRevision: null,
    });
    await writer.addVersion({
      name: "orders",
      version: "v1",
      sequence: 1,
      schema,
      contentHash: "h",
      publishedAt: 2,
    });

    const reader = SqliteWorkflowSchemaStore.make({ db });
    const row = await reader.get("orders");
    expect(row?.draft.revision).toBe(1);
    expect(row?.versions.map((v) => v.version)).toEqual(["v1"]);
  });

  it("rejects a table prefix that is not a plain identifier", () => {
    expect(() =>
      SqliteWorkflowSchemaStore.make({ db: new Database(":memory:"), tablePrefix: "x; DROP" }),
    ).toThrow(/invalid tablePrefix/);
  });
});
