import { it, expect } from "bun:test";
import { sql } from "drizzle-orm";
import { ensureTable, execRaw } from "@spilne/perfect-postgres";
import { postgresDescribe } from "../test-utils.ts";
import { stepQueue } from "../schema.ts";

// ---------------------------------------------------------------------------
// The step-queue schema bootstraps through ensureTable (what
// PgStepQueue.ensureTable() uses for quick setups without migrations).
// ---------------------------------------------------------------------------

postgresDescribe("stepQueue schema — ensureTable bootstrap", (pg) => {
  it("creates wf_step_queue with its core columns", async () => {
    await ensureTable(pg.db, stepQueue);

    const rows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'wf_step_queue'
        ORDER BY ordinal_position
      `),
    );

    const colNames = rows.map((r) => r["column_name"]);
    expect(colNames).toContain("id");
    expect(colNames).toContain("workflow_id");
    expect(colNames).toContain("step_name");
    expect(colNames).toContain("priority");
    expect(colNames).toContain("status");
  });

  it("is idempotent", async () => {
    await ensureTable(pg.db, stepQueue);
    await ensureTable(pg.db, stepQueue);
  });
});
