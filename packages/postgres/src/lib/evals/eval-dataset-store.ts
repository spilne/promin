// ---------------------------------------------------------------------------
// PostgresEvalDatasetStore — Postgres-backed `EvalDatasetStore`.
//
// One row per dataset; the case list rides in the `cases` jsonb column.
// `save` upserts, so re-saving a dataset replaces its cases wholesale.
// ---------------------------------------------------------------------------

import { and, asc, eq, sql } from "drizzle-orm";
import type { EvalCase, EvalDatasetStore } from "@promin/evals";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { evalDataset } from "../schema.ts";

export interface PostgresEvalDatasetStoreConfig {
  readonly db: DrizzleDb;
}

export class PostgresEvalDatasetStore implements EvalDatasetStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresEvalDatasetStoreConfig) {
    this.db = config.db;
  }

  async save(params: {
    readonly datasetId: string;
    readonly cases: ReadonlyArray<EvalCase>;
  }): Promise<void> {
    const { datasetId } = params;
    const stored = [...params.cases];
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

  async appendCase(datasetId: string, evalCase: EvalCase): Promise<boolean> {
    // Each statement is atomic (the UPDATE holds the row lock while it
    // checks the id), so concurrent writers cannot lose or duplicate a case.
    const inserted = await this.db
      .insert(evalDataset)
      .values({ datasetId, cases: [evalCase] })
      .onConflictDoNothing({ target: evalDataset.datasetId })
      .returning({ datasetId: evalDataset.datasetId });
    if (inserted.length > 0) return true;
    const appended = await this.db
      .update(evalDataset)
      .set({ cases: sql`${evalDataset.cases} || ${JSON.stringify([evalCase])}::jsonb` })
      .where(
        and(
          eq(evalDataset.datasetId, datasetId),
          sql`NOT EXISTS (SELECT 1 FROM jsonb_array_elements(${evalDataset.cases}) AS item WHERE item->>'id' = ${evalCase.id})`,
        ),
      )
      .returning({ datasetId: evalDataset.datasetId });
    return appended.length > 0;
  }

  async replaceCase(datasetId: string, evalCase: EvalCase): Promise<boolean> {
    const replaced = await this.db
      .update(evalDataset)
      .set({
        cases: sql`(SELECT jsonb_agg(CASE WHEN item->>'id' = ${evalCase.id} THEN ${JSON.stringify(evalCase)}::jsonb ELSE item END ORDER BY pos)
          FROM jsonb_array_elements(${evalDataset.cases}) WITH ORDINALITY AS t(item, pos))`,
      })
      .where(
        and(
          eq(evalDataset.datasetId, datasetId),
          sql`EXISTS (SELECT 1 FROM jsonb_array_elements(${evalDataset.cases}) AS item WHERE item->>'id' = ${evalCase.id})`,
        ),
      )
      .returning({ datasetId: evalDataset.datasetId });
    return replaced.length > 0;
  }

  async deleteCase(datasetId: string, caseId: string): Promise<boolean> {
    const removed = await this.db
      .update(evalDataset)
      .set({
        cases: sql`COALESCE((SELECT jsonb_agg(item ORDER BY pos)
          FROM jsonb_array_elements(${evalDataset.cases}) WITH ORDINALITY AS t(item, pos)
          WHERE item->>'id' <> ${caseId}), '[]'::jsonb)`,
      })
      .where(
        and(
          eq(evalDataset.datasetId, datasetId),
          sql`EXISTS (SELECT 1 FROM jsonb_array_elements(${evalDataset.cases}) AS item WHERE item->>'id' = ${caseId})`,
        ),
      )
      .returning({ datasetId: evalDataset.datasetId });
    return removed.length > 0;
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
