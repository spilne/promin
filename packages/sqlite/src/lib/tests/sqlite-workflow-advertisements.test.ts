// ---------------------------------------------------------------------------
// SqliteWorkflowAdvertisementRegistry — conformance against the shared
// WorkflowAdvertisementRegistry suite, plus persistence-specific
// behaviour (survives a fresh registry on the same db).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { workflowAdvertisementRegistryTestSuite } from "@promin/workflow";
import { SqliteWorkflowAdvertisementRegistry } from "../sqlite-workflow-advertisements.ts";

let counter = 0;
function freshRegistry(): SqliteWorkflowAdvertisementRegistry {
  // Unique table per factory invocation so the conformance suite's
  // independent describes start from an empty store.
  const db = new Database(":memory:");
  return SqliteWorkflowAdvertisementRegistry.make({
    db,
    tableName: `promin_workflow_advertisements_${++counter}`,
  });
}

workflowAdvertisementRegistryTestSuite(freshRegistry);

describe("SqliteWorkflowAdvertisementRegistry — persistence", () => {
  it("survives a fresh registry instance over the same db", async () => {
    const db = new Database(":memory:");
    const a = SqliteWorkflowAdvertisementRegistry.make({ db });
    await a.upsert("w1", [
      { name: "hello", version: "1", steps: [{ name: "greet", kind: "single", dependsOn: [] }] },
    ]);

    // Brand-new instance pointed at the same db — should rebind to the
    // existing table and read what the prior instance wrote. This is the
    // operational shape that mattered: bouncing the demo doesn't lose
    // the catalog.
    const b = SqliteWorkflowAdvertisementRegistry.make({ db });
    const distinct = await b.distinct();
    expect(distinct.length).toBe(1);
    expect(distinct[0]?.name).toBe("hello");
    expect(distinct[0]?.version).toBe("1");
  });
});
