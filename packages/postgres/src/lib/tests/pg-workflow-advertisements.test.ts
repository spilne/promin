// ---------------------------------------------------------------------------
// PgWorkflowAdvertisementRegistry — conformance against the shared
// WorkflowAdvertisementRegistry suite, plus Pg-specific behaviour
// (multi-replica catalog sharing).
// ---------------------------------------------------------------------------

import { beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { workflowAdvertisementRegistryTestSuite } from "@promin/workflow/testing";
import { PgWorkflowAdvertisementRegistry } from "../pg-workflow-advertisements.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PgWorkflowAdvertisementRegistry conformance", (pg) => {
  // Create the schema once; truncate between tests so each `it()` sees
  // an empty registry (the conformance suite's factory expects that
  // and the in-memory ref impl gets it for free).
  beforeAll(async () => {
    await new PgWorkflowAdvertisementRegistry({ db: pg.db }).ensureTable();
  });
  beforeEach(async () => {
    await pg.db.execute(sql`TRUNCATE TABLE wf_workflow_advertisements`);
  });

  const factory = async () => new PgWorkflowAdvertisementRegistry({ db: pg.db });

  workflowAdvertisementRegistryTestSuite(factory);

  describe("PgWorkflowAdvertisementRegistry — Pg-specific", () => {
    it("two registry instances on the same DB share state — multi-replica catalog", async () => {
      // The whole point of moving from in-memory to Pg: two Zorya
      // processes pointed at the same DB see one consolidated catalog.
      const a = new PgWorkflowAdvertisementRegistry({ db: pg.db });
      await a.ensureTable();
      await a.upsert("worker-a", [
        { name: "alpha", version: "1", steps: [{ name: "s", kind: "single", dependsOn: [] }] },
      ]);

      const b = new PgWorkflowAdvertisementRegistry({ db: pg.db });
      await b.upsert("worker-b", [
        { name: "beta", version: "1", steps: [{ name: "s", kind: "single", dependsOn: [] }] },
      ]);

      // Either instance sees BOTH worker advertisements.
      const fromA = await a.distinct();
      const fromB = await b.distinct();
      expect(fromA.map((w) => w.name).sort()).toEqual(["alpha", "beta"]);
      expect(fromB.map((w) => w.name).sort()).toEqual(["alpha", "beta"]);
    });

    it("upsert is atomic — a mid-replace error leaves prior advertisement intact", async () => {
      const reg = new PgWorkflowAdvertisementRegistry({ db: pg.db });
      await reg.ensureTable();
      await reg.upsert("worker-1", [
        { name: "stable", version: "1", steps: [{ name: "s", kind: "single", dependsOn: [] }] },
      ]);
      // Second upsert with bad payload — the steps field is required
      // notNull jsonb; pass undefined as the steps array element to
      // force a constraint failure inside the transaction. Note: this
      // smells like a force-fault, but it's the contract we want to
      // pin: any throw inside the txn must roll the delete back.
      await expect(
        reg.upsert("worker-1", [
          // biome-ignore lint/suspicious/noExplicitAny: deliberate fault
          { name: "broken", version: "1", steps: undefined as any },
        ]),
      ).rejects.toThrow();
      // Original row still there.
      const distinct = await reg.distinct();
      expect(distinct.map((w) => w.name)).toEqual(["stable"]);
    });
  });
});
