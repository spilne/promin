// ---------------------------------------------------------------------------
// WorkflowSchemaStore — durable rows behind workflows authored as JSON
// (`WorkflowSchema`) in a visual editor.
//
// Each authored workflow has one mutable draft — the working copy an editor
// saves — and any number of immutable published versions, snapshots of the
// draft taken when it was published. A version's compiled `Workflow` is
// registered in a `WorkflowVersionRegistry` under the version's tag; the
// store keeps the source so a host can recompile every version at boot.
//
// Concurrent editors are fenced by the draft's `revision`: `saveDraft` is a
// compare-and-set on it, so a stale editor's save is refused rather than
// silently overwriting a newer one.
//
// Implementations: `InMemoryWorkflowSchemaStore` (here),
// `SqliteWorkflowSchemaStore` (@promin/sqlite) and
// `PostgresWorkflowSchemaStore` (@promin/postgres). All pass
// `workflowSchemaStoreTestSuite` (`@promin/workflow/testing`).
// ---------------------------------------------------------------------------

import type { WorkflowSchema } from "./workflow-schema.ts";

/** The working copy of an authored workflow. */
export interface WorkflowSchemaDraft {
  /** The workflow name, equal to `schema.name`. */
  readonly name: string;
  readonly schema: WorkflowSchema;
  /** 1 on create, bumped by every save: the compare-and-set token. */
  readonly revision: number;
  /** Epoch ms. */
  readonly createdAt: number;
  /** Epoch ms. */
  readonly updatedAt: number;
}

/** A published snapshot of a draft. Never changes once added. */
export interface WorkflowSchemaVersion {
  readonly name: string;
  /** The version tag the compiled workflow is registered under. */
  readonly version: string;
  /** 1 for the first published version, then 2, 3, … Orders the versions. */
  readonly sequence: number;
  readonly schema: WorkflowSchema;
  /** Hash of the schema's executable content; equal hashes run the same. */
  readonly contentHash: string;
  /** Epoch ms. */
  readonly publishedAt: number;
}

export interface StoredWorkflowSchema {
  readonly draft: WorkflowSchemaDraft;
  /** Ordered by `sequence`, oldest first. */
  readonly versions: ReadonlyArray<WorkflowSchemaVersion>;
}

export interface WorkflowSchemaStore {
  /** Every authored workflow with its versions, ordered by name. */
  list(): Promise<ReadonlyArray<StoredWorkflowSchema>>;
  /** One workflow with its versions, or `null`. */
  get(name: string): Promise<StoredWorkflowSchema | null>;
  /**
   * Write `draft` when the stored revision is `expectedRevision` — `null`
   * means "no draft of this name exists yet" (create). Returns `false`, and
   * writes nothing, when the stored state differs.
   */
  saveDraft(params: {
    draft: WorkflowSchemaDraft;
    expectedRevision: number | null;
  }): Promise<boolean>;
  /**
   * Add a published version. Returns `false`, and writes nothing, when the
   * workflow already has a version with this tag or sequence.
   */
  addVersion(version: WorkflowSchemaVersion): Promise<boolean>;
  /** Remove a workflow's draft and every version. No-op when absent. */
  delete(name: string): Promise<void>;
}

/** Reference implementation for tests and hosts without durable storage. */
export class InMemoryWorkflowSchemaStore implements WorkflowSchemaStore {
  private readonly drafts = new Map<string, WorkflowSchemaDraft>();
  private readonly versions = new Map<string, WorkflowSchemaVersion[]>();

  async list(): Promise<ReadonlyArray<StoredWorkflowSchema>> {
    return [...this.drafts.keys()].sort().map((name) => this.snapshot(name)!);
  }

  async get(name: string): Promise<StoredWorkflowSchema | null> {
    return this.snapshot(name);
  }

  async saveDraft(params: {
    draft: WorkflowSchemaDraft;
    expectedRevision: number | null;
  }): Promise<boolean> {
    const current = this.drafts.get(params.draft.name);
    if ((current?.revision ?? null) !== params.expectedRevision) return false;
    this.drafts.set(params.draft.name, structuredClone(params.draft));
    return true;
  }

  async addVersion(version: WorkflowSchemaVersion): Promise<boolean> {
    const list = this.versions.get(version.name) ?? [];
    if (list.some((v) => v.version === version.version || v.sequence === version.sequence)) {
      return false;
    }
    list.push(structuredClone(version));
    list.sort((a, b) => a.sequence - b.sequence);
    this.versions.set(version.name, list);
    return true;
  }

  async delete(name: string): Promise<void> {
    this.drafts.delete(name);
    this.versions.delete(name);
  }

  private snapshot(name: string): StoredWorkflowSchema | null {
    const draft = this.drafts.get(name);
    if (!draft) return null;
    return {
      draft: structuredClone(draft),
      versions: (this.versions.get(name) ?? []).map((v) => structuredClone(v)),
    };
  }
}
