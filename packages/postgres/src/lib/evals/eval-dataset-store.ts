// ---------------------------------------------------------------------------
// PostgresEvalDatasetStore — Postgres-backed `EvalDatasetStore`.
//
// One row per dataset; the case list rides in the `cases` jsonb column.
// `save` upserts, so re-saving a dataset replaces its cases wholesale.
// ---------------------------------------------------------------------------

import { asc, eq } from "drizzle-orm";
import type { EvalCase, EvalDatasetStore } from "@promin/evals";
import type { DrizzleDb } from "../drizzle-db.ts";
import { evalDataset } from "../schema.ts";

export interface PostgresEvalDatasetStoreConfig {
  readonly db: DrizzleDb;
}

export class PostgresEvalDatasetStore implements EvalDatasetStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresEvalDatasetStoreConfig) {
    this.db = config.db;
  }

  async save(datasetId: string, cases: ReadonlyArray<EvalCase>): Promise<void> {
    const stored = [...cases];
    await this.db
      .insert(evalDataset)
      .values({ datasetId, cases: stored })
      .onConflictDoUpdate({ target: evalDataset.datasetId, set: { cases: stored } });
  }

  async get(datasetId: string): Promise<EvalCase[] | null> {
    const [row] = await this.db
      .select()
      .from(evalDataset)
      .where(eq(evalDataset.datasetId, datasetId));
    return row ? (row.cases as EvalCase[]) : null;
  }

  async list(): Promise<string[]> {
    const rows = await this.db
      .select({ datasetId: evalDataset.datasetId })
      .from(evalDataset)
      .orderBy(asc(evalDataset.datasetId));
    return rows.map((row) => row.datasetId);
  }

  async delete(datasetId: string): Promise<void> {
    await this.db.delete(evalDataset).where(eq(evalDataset.datasetId, datasetId));
  }
}
