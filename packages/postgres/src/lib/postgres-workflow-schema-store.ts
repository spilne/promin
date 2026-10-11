// ---------------------------------------------------------------------------
// PostgresWorkflowSchemaStore — `WorkflowSchemaStore` over the
// `wf_workflow_schema_draft` and `wf_workflow_schema_version` tables.
// Mirrors `SqliteWorkflowSchemaStore`.
//
// `saveDraft` is one conditional statement (an insert that does nothing on
// conflict, or an update guarded by the expected revision), so two editors
// saving the same draft from different replicas cannot both win.
// ---------------------------------------------------------------------------

import { and, asc, eq } from "drizzle-orm";
import type {
  StoredWorkflowSchema,
  WorkflowSchema,
  WorkflowSchemaDraft,
  WorkflowSchemaStore,
  WorkflowSchemaVersion,
} from "@promin/workflow";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { workflowSchemaDraft, workflowSchemaVersion } from "./schema.ts";

export interface PostgresWorkflowSchemaStoreConfig {
  readonly db: DrizzleDb;
}

type DraftRow = typeof workflowSchemaDraft.$inferSelect;
type VersionRow = typeof workflowSchemaVersion.$inferSelect;

export class PostgresWorkflowSchemaStore implements WorkflowSchemaStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresWorkflowSchemaStoreConfig) {
    this.db = config.db;
  }

  async list(): Promise<ReadonlyArray<StoredWorkflowSchema>> {
    const drafts = await this.db
      .select()
      .from(workflowSchemaDraft)
      .orderBy(asc(workflowSchemaDraft.name));
    const versions = await this.db
      .select()
      .from(workflowSchemaVersion)
      .orderBy(asc(workflowSchemaVersion.name), asc(workflowSchemaVersion.sequence));
    const byName = new Map<string, WorkflowSchemaVersion[]>();
    for (const row of versions) {
      const list = byName.get(row.name) ?? [];
      list.push(toVersion(row));
      byName.set(row.name, list);
    }
    return drafts.map((row) => ({ draft: toDraft(row), versions: byName.get(row.name) ?? [] }));
  }

  async get(name: string): Promise<StoredWorkflowSchema | null> {
    const [draft] = await this.db
      .select()
      .from(workflowSchemaDraft)
      .where(eq(workflowSchemaDraft.name, name));
    if (!draft) return null;
    const versions = await this.db
      .select()
      .from(workflowSchemaVersion)
      .where(eq(workflowSchemaVersion.name, name))
      .orderBy(asc(workflowSchemaVersion.sequence));
    return { draft: toDraft(draft), versions: versions.map(toVersion) };
  }

  async saveDraft(params: {
    draft: WorkflowSchemaDraft;
    expectedRevision: number | null;
  }): Promise<boolean> {
    const { draft, expectedRevision } = params;
    const values = {
      schema: draft.schema as unknown as Record<string, unknown>,
      revision: draft.revision,
      createdAt: draft.createdAt,
      updatedAt: draft.updatedAt,
    };
    if (expectedRevision === null) {
      const inserted = await this.db
        .insert(workflowSchemaDraft)
        .values({ name: draft.name, ...values })
        .onConflictDoNothing()
        .returning({ name: workflowSchemaDraft.name });
      return inserted.length === 1;
    }
    const updated = await this.db
      .update(workflowSchemaDraft)
      .set(values)
      .where(
        and(
          eq(workflowSchemaDraft.name, draft.name),
          eq(workflowSchemaDraft.revision, expectedRevision),
        ),
      )
      .returning({ name: workflowSchemaDraft.name });
    return updated.length === 1;
  }

  async addVersion(version: WorkflowSchemaVersion): Promise<boolean> {
    // Conflicts on either the (name, version) key or the (name, sequence)
    // unique index leave the row out.
    const inserted = await this.db
      .insert(workflowSchemaVersion)
      .values({
        name: version.name,
        version: version.version,
        sequence: version.sequence,
        schema: version.schema as unknown as Record<string, unknown>,
        contentHash: version.contentHash,
        publishedAt: version.publishedAt,
      })
      .onConflictDoNothing()
      .returning({ name: workflowSchemaVersion.name });
    return inserted.length === 1;
  }

  async delete(name: string): Promise<void> {
    await this.db.transaction(async (tx) => {
      await tx.delete(workflowSchemaVersion).where(eq(workflowSchemaVersion.name, name));
      await tx.delete(workflowSchemaDraft).where(eq(workflowSchemaDraft.name, name));
    });
  }
}

function toDraft(row: DraftRow): WorkflowSchemaDraft {
  return {
    name: row.name,
    schema: row.schema as unknown as WorkflowSchema,
    revision: row.revision,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function toVersion(row: VersionRow): WorkflowSchemaVersion {
  return {
    name: row.name,
    version: row.version,
    sequence: row.sequence,
    schema: row.schema as unknown as WorkflowSchema,
    contentHash: row.contentHash,
    publishedAt: row.publishedAt,
  };
}
