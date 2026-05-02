// ---------------------------------------------------------------------------
// `PgWorkflowAdvertisementRegistry` — `WorkflowAdvertisementRegistry`
// over Postgres for multi-replica deployments.
//
// Workers advertise their workflow definitions on connect (`upsert`) and
// the dashboard reads `distinct()` to populate the Workflows page. The
// in-memory impl loses everything on restart; the Postgres impl persists
// across boots AND across replicas — two Zorya servers pointed at the
// same DB see one consolidated catalog.
//
// Lifecycle:
//   - upsert(workerId, workflows) replaces the worker's row set atomically
//     (a worker that re-advertises with a smaller set drops the workflows
//     it no longer hosts)
//   - remove(workerId) deletes everything for that worker (graceful shutdown)
//   - readers (`list`, `distinct`) return the union across all workers
//
// Schema lives in `schema.ts` (`workflowAdvertisements`); call
// `ensureTable()` for quick bootstrap or include it in your migration
// pipeline.
// ---------------------------------------------------------------------------

import { eq, sql } from "drizzle-orm";
import type {
  AdvertisedWorkflow,
  AdvertisementEntry,
  WorkflowAdvertisementRegistry,
} from "@promin/workflow";
import type { DrizzleDb } from "./drizzle-db.ts";
import { workflowAdvertisements } from "./schema.ts";
import { ensureTable as ensureTableFromSchema } from "./schema-utils.ts";

export interface PgWorkflowAdvertisementRegistryConfig {
  db: DrizzleDb;
}

export class PgWorkflowAdvertisementRegistry implements WorkflowAdvertisementRegistry {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = workflowAdvertisements;

  private readonly db: DrizzleDb;

  constructor(config: PgWorkflowAdvertisementRegistryConfig) {
    this.db = config.db;
  }

  /**
   * Idempotent CREATE TABLE for quick bootstrap (demos, tests). Production
   * deployments should run schema via Drizzle migrations instead.
   */
  async ensureTable(): Promise<void> {
    await ensureTableFromSchema(this.db, workflowAdvertisements);
  }

  async upsert(workerId: string, workflows: AdvertisedWorkflow[]): Promise<void> {
    // Replace all rows for this worker atomically — a worker that
    // re-advertises with a smaller set should drop the workflows it no
    // longer hosts. Single transaction so a mid-replace failure leaves
    // the previous advertisement intact rather than partially gone.
    await this.db.transaction(async (tx) => {
      await tx.delete(workflowAdvertisements).where(eq(workflowAdvertisements.workerId, workerId));
      if (workflows.length === 0) return;
      const now = new Date();
      const rows = workflows.map((wf) => ({
        workerId,
        workflowName: wf.name,
        version: wf.version ?? null,
        steps: wf.steps as unknown,
        sampleInput: wf.sampleInput as unknown,
        advertisedAt: now,
      }));
      await tx.insert(workflowAdvertisements).values(rows);
    });
  }

  async remove(workerId: string): Promise<void> {
    await this.db
      .delete(workflowAdvertisements)
      .where(eq(workflowAdvertisements.workerId, workerId));
  }

  async list(): Promise<AdvertisementEntry[]> {
    const rows = await this.db
      .select()
      .from(workflowAdvertisements)
      .orderBy(workflowAdvertisements.workerId, workflowAdvertisements.workflowName);
    const byWorker = new Map<string, AdvertisementEntry>();
    for (const r of rows) {
      let entry = byWorker.get(r.workerId);
      if (!entry) {
        entry = { workerId: r.workerId, workflows: [], advertisedAt: r.advertisedAt };
        byWorker.set(r.workerId, entry);
      }
      entry.workflows.push(rowToAdvertisedWorkflow(r));
      // Latest advertised_at across this worker's rows.
      if (r.advertisedAt.getTime() > entry.advertisedAt.getTime()) {
        entry.advertisedAt = r.advertisedAt;
      }
    }
    return [...byWorker.values()];
  }

  async distinct(): Promise<AdvertisedWorkflow[]> {
    // Dedupe on (workflow_name, version), keeping whichever row was
    // advertised most recently. Mirrors InMemoryWorkflowAdvertisementRegistry's
    // behaviour and the Sqlite impl. We pull every row ordered so the
    // first row we see per key is the freshest, then JS-dedupe.
    const rows = await this.db
      .select()
      .from(workflowAdvertisements)
      .orderBy(
        workflowAdvertisements.workflowName,
        // COALESCE so NULL versions sort with empty-string versions
        // deterministically. Postgres' default NULL-last would interleave
        // weirdly with mixed-version advertisements.
        sql`COALESCE(${workflowAdvertisements.version}, '')`,
        sql`${workflowAdvertisements.advertisedAt} DESC`,
      );
    const byKey = new Map<string, AdvertisedWorkflow>();
    for (const r of rows) {
      const key = `${r.workflowName}@${r.version ?? ""}`;
      if (!byKey.has(key)) byKey.set(key, rowToAdvertisedWorkflow(r));
    }
    return [...byKey.values()];
  }
}

function rowToAdvertisedWorkflow(r: {
  workflowName: string;
  version: string | null;
  steps: unknown;
  sampleInput: unknown;
}): AdvertisedWorkflow {
  const out: AdvertisedWorkflow = {
    name: r.workflowName,
    steps: r.steps as AdvertisedWorkflow["steps"],
  };
  if (r.version !== null) out.version = r.version;
  if (r.sampleInput !== null && r.sampleInput !== undefined) out.sampleInput = r.sampleInput;
  return out;
}
