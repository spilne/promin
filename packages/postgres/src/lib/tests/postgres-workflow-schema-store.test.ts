// PostgresWorkflowSchemaStore — runs the conformance suite against a real
// Postgres container, plus concurrent-writer checks across store instances.

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { workflowSchemaStoreTestSuite } from "@promin/workflow/testing";
import type { WorkflowSchema } from "@promin/workflow";
import { migrate } from "../migrate.ts";
import { PostgresWorkflowSchemaStore } from "../postgres-workflow-schema-store.ts";
import { PostgresTestContainer } from "../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE wf_workflow_schema_draft, wf_workflow_schema_version`;
});

workflowSchemaStoreTestSuite(() => new PostgresWorkflowSchemaStore({ db: pg.db }));

const schema = (step: string): WorkflowSchema => ({
  version: 1,
  name: "orders",
  steps: [{ type: "step", name: step, dependsOn: [], activityRef: "text.upper" }],
});

describe("PostgresWorkflowSchemaStore — Postgres-specific", () => {
  it("of two concurrent saves from the same revision exactly one wins", async () => {
    const a = new PostgresWorkflowSchemaStore({ db: pg.db });
    const b = new PostgresWorkflowSchemaStore({ db: pg.db });
    await a.saveDraft({
      draft: { name: "orders", schema: schema("first"), revision: 1, createdAt: 1, updatedAt: 1 },
      expectedRevision: null,
    });
    const results = await Promise.all(
      [a, b].map((store, i) =>
        store.saveDraft({
          draft: {
            name: "orders",
            schema: schema(`edit-${i}`),
            revision: 2,
            createdAt: 1,
            updatedAt: 2,
          },
          expectedRevision: 1,
        }),
      ),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
    expect((await b.get("orders"))!.draft.revision).toBe(2);
  });

  it("of two concurrent publishes of the same sequence exactly one wins", async () => {
    const a = new PostgresWorkflowSchemaStore({ db: pg.db });
    const b = new PostgresWorkflowSchemaStore({ db: pg.db });
    await a.saveDraft({
      draft: { name: "orders", schema: schema("s"), revision: 1, createdAt: 1, updatedAt: 1 },
      expectedRevision: null,
    });
    const results = await Promise.all(
      [a, b].map((store) =>
        store.addVersion({
          name: "orders",
          version: "v1",
          sequence: 1,
          schema: schema("s"),
          contentHash: "h",
          publishedAt: 2,
        }),
      ),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
    expect((await a.get("orders"))!.versions).toHaveLength(1);
  });
});
