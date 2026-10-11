// ---------------------------------------------------------------------------
// ZoryaWorkflowBuilder — workflows authored in the dashboard's visual
// builder as JSON (`WorkflowSchema`) and run like any in-process workflow.
//
// Each builder workflow has one draft (the editor's working copy) and
// immutable published versions `1`, `2`, … kept in a `WorkflowSchemaStore`.
// Every save is validated with the workflow package's schema validator and
// compiled with `compileWorkflow` against the host's `ActivityCatalog`, so a
// stored draft always compiles. Publishing compiles the draft as the next
// version and hands it to the existing version machinery:
//
//   - it is registered in the `LocalWorkflows` layer's version registry
//     (the registry the Versions panel manages and runs resolve through),
//   - the layer `define`s it as the workflow's in-process definition, so the
//     normal trigger path (`POST /api/workflows/:name/runs`, schedules, the
//     Workflows page) starts it, and
//   - it is promoted to active unless the caller opts out.
//
// Runs started on an older version keep resolving that version through the
// registry, so they finish on the code they started with. `start()`
// recompiles every stored version and registers it again (registration is
// idempotent), because compiled workflows hold functions and live only in
// this process.
//
// Concurrency: the draft's `revision` is a compare-and-set token — a save or
// publish that names a stale revision fails with a conflict instead of
// overwriting another editor's work. Writes to one workflow also run one at
// a time in this process. Replicas share the store but each defines the
// workflows it loaded at start or published itself; publish from one replica
// (or restart the others) in clustered deployments.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";
import {
  InMemoryWorkflowSchemaStore,
  SystemWallClock,
  WorkflowCompilationError,
  compileWorkflow,
  hasCapability,
  validateWorkflowSchemaSafe,
  type ActivityCatalog,
  type ActivityDescriptor,
  type StoredWorkflowSchema,
  type VersionStatus,
  type WallClock,
  type Workflow,
  type WorkflowSchema,
  type WorkflowSchemaStore,
  type WorkflowSchemaVersion,
  type WorkflowStatus,
  type WorkflowVersionRegistry,
} from "@promin/workflow";
import { sampleWorkflowInput } from "../../../shared/workflow-input.ts";
import type { LocalWorkflows } from "../workflows/local-workflows.ts";
import {
  BuilderWorkflowConflictError,
  BuilderWorkflowInUseError,
  BuilderWorkflowNameTakenError,
  BuilderWorkflowNotFoundError,
  InvalidBuilderWorkflowError,
  type WorkflowIssue,
} from "./errors.ts";

/** Builder workflow names: they appear in URLs and run lists. */
export const BUILDER_WORKFLOW_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/** Workflow type recorded on runs of builder workflows. */
export const BUILDER_WORKFLOW_TYPE = "builder";

/** Default cap on the steps of one workflow. */
export const DEFAULT_MAX_BUILDER_STEPS = 200;

const MAX_STEP_NAME_LENGTH = 128;
const ACTIVE_RUN_STATUSES: readonly WorkflowStatus[] = [
  "pending",
  "running",
  "suspended",
  "compensating",
];

export interface ZoryaWorkflowBuilderConfig {
  /**
   * The in-process layer published workflows run on. It needs a version
   * registry (`LocalWorkflowsConfig.versionRegistry`), and the server must
   * be given a workflows chain that contains it.
   */
  readonly workflows: LocalWorkflows;
  /** The activities steps may reference. Its metadata feeds the builder's palette. */
  readonly activities: ActivityCatalog;
  /** Drafts and published versions. Default: in memory (lost on restart). */
  readonly store?: WorkflowSchemaStore;
  /** Most steps one workflow may have. Default: 200. */
  readonly maxSteps?: number;
  /** Time source for draft and version timestamps. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** One published version, with its status in the version registry. */
export interface BuilderWorkflowVersion {
  readonly version: string;
  readonly sequence: number;
  readonly contentHash: string;
  /** Epoch ms. */
  readonly publishedAt: number;
  /** Registry status; absent when the registry does not track lifecycle. */
  readonly status?: VersionStatus;
}

/** A builder workflow as the dashboard edits it. */
export interface BuilderWorkflow {
  readonly name: string;
  /** The draft. */
  readonly schema: WorkflowSchema;
  /** Send it back with the next save or publish (see the conflict rules). */
  readonly revision: number;
  /** Epoch ms. */
  readonly createdAt: number;
  /** Epoch ms. */
  readonly updatedAt: number;
  /** Oldest first. */
  readonly versions: ReadonlyArray<BuilderWorkflowVersion>;
  /** The promoted version new runs start with, if any. */
  readonly activeVersion?: string;
  /** True when the draft runs differently from the latest version, or nothing is published. */
  readonly unpublishedChanges: boolean;
  /** Why a published version could not be loaded at start (e.g. an activity was removed). */
  readonly loadError?: string;
}

/** A row of the builder's workflow list. */
export interface BuilderWorkflowSummary {
  readonly name: string;
  readonly revision: number;
  readonly updatedAt: number;
  readonly stepCount: number;
  readonly latestVersion?: string;
  readonly activeVersion?: string;
  readonly unpublishedChanges: boolean;
  readonly loadError?: string;
}

export interface PublishResult {
  readonly workflow: BuilderWorkflow;
  /** The version that now carries the draft's content. */
  readonly version: string;
  /** False when the latest version already had this content (nothing new was published). */
  readonly created: boolean;
}

export class ZoryaWorkflowBuilder {
  readonly store: WorkflowSchemaStore;
  readonly activities: ActivityCatalog;
  readonly workflows: LocalWorkflows;
  private readonly registry: WorkflowVersionRegistry;
  private readonly maxSteps: number;
  private readonly clock: WallClock;
  /** Names whose in-process definition this service set. */
  private readonly defined = new Set<string>();
  /** Schema of each workflow's newest loaded version, for sample inputs. */
  private readonly heads = new Map<string, WorkflowSchema>();
  /** Load failures from `start()`, by workflow name. */
  private readonly loadErrors = new Map<string, string>();
  /** Tail of each workflow's write chain: writes to one name run in order. */
  private readonly writes = new Map<string, Promise<unknown>>();

  constructor(config: ZoryaWorkflowBuilderConfig) {
    const registry = config.workflows.versionRegistry;
    if (!registry) {
      throw new Error(
        "ZoryaWorkflowBuilder: the LocalWorkflows layer needs a versionRegistry — builder workflows are published as registry versions",
      );
    }
    this.workflows = config.workflows;
    this.registry = registry;
    this.activities = config.activities;
    this.store = config.store ?? new InMemoryWorkflowSchemaStore();
    this.maxSteps = config.maxSteps ?? DEFAULT_MAX_BUILDER_STEPS;
    this.clock = config.clock ?? SystemWallClock;
  }

  /**
   * Compile every stored version, register it and define each workflow's
   * newest version. A version that no longer compiles is skipped and
   * reported as the workflow's `loadError`; when no version of a workflow
   * is promoted yet, the newest one is.
   */
  async start(): Promise<void> {
    for (const stored of await this.store.list()) {
      await this.load(stored);
    }
  }

  async stop(): Promise<void> {}

  /** Metadata of every activity steps may use (the builder's palette). */
  describeActivities(): ActivityDescriptor[] {
    return this.activities.describe();
  }

  async list(): Promise<BuilderWorkflowSummary[]> {
    const rows = await this.store.list();
    return Promise.all(
      rows.map(async (stored) => {
        const latest = stored.versions.at(-1);
        const active = await this.activeVersion(stored.draft.name);
        const loadError = this.loadErrors.get(stored.draft.name);
        return {
          name: stored.draft.name,
          revision: stored.draft.revision,
          updatedAt: stored.draft.updatedAt,
          stepCount: stored.draft.schema.steps.length,
          ...(latest && { latestVersion: latest.version }),
          ...(active !== undefined && { activeVersion: active }),
          unpublishedChanges: hasUnpublishedChanges(stored),
          ...(loadError !== undefined && { loadError }),
        };
      }),
    );
  }

  async get(name: string): Promise<BuilderWorkflow | undefined> {
    const stored = await this.store.get(name);
    return stored ? this.toBuilderWorkflow(stored) : undefined;
  }

  /**
   * Check a schema without saving it: the issues a save would refuse it
   * for (empty when it would be accepted).
   */
  validate(raw: unknown): WorkflowIssue[] {
    try {
      this.check(raw);
      return [];
    } catch (err) {
      if (err instanceof InvalidBuilderWorkflowError) return [...err.issues];
      throw err;
    }
  }

  /** Create a workflow from its first draft. The name is `schema.name`. */
  async create(params: { schema: unknown }): Promise<BuilderWorkflow> {
    const schema = this.check(params.schema);
    return this.serialize(schema.name, async () => {
      if (await this.store.get(schema.name)) {
        throw new BuilderWorkflowNameTakenError({ name: schema.name, reason: "exists" });
      }
      if (await this.isReserved(schema.name)) {
        throw new BuilderWorkflowNameTakenError({ name: schema.name, reason: "reserved" });
      }
      const now = this.clock.currentTimeMs();
      const draft = { name: schema.name, schema, revision: 1, createdAt: now, updatedAt: now };
      if (!(await this.store.saveDraft({ draft, expectedRevision: null }))) {
        throw new BuilderWorkflowNameTakenError({ name: schema.name, reason: "exists" });
      }
      return this.toBuilderWorkflow({ draft, versions: [] });
    });
  }

  /**
   * Replace the draft of `name`. `revision` is the one the caller read;
   * a different stored revision is a conflict. `schema.name` must be
   * `name`: a workflow's name is its identity, so a renamed draft is saved
   * as a new workflow with `create`.
   */
  async saveDraft(params: {
    name: string;
    schema: unknown;
    revision: number;
  }): Promise<BuilderWorkflow> {
    const { name, revision } = params;
    const schema = this.check(params.schema);
    if (schema.name !== name) {
      throw new InvalidBuilderWorkflowError([
        {
          path: "name",
          message: `must be "${name}"; save a renamed workflow as a new one`,
        },
      ]);
    }
    return this.serialize(name, async () => {
      const stored = await this.require(name);
      if (stored.draft.revision !== revision) {
        throw new BuilderWorkflowConflictError({ name, currentRevision: stored.draft.revision });
      }
      const draft = {
        ...stored.draft,
        schema,
        revision: revision + 1,
        updatedAt: this.clock.currentTimeMs(),
      };
      if (!(await this.store.saveDraft({ draft, expectedRevision: revision }))) {
        const now = await this.store.get(name);
        throw new BuilderWorkflowConflictError({
          name,
          currentRevision: now?.draft.revision ?? null,
        });
      }
      return this.toBuilderWorkflow({ draft, versions: stored.versions });
    });
  }

  /**
   * Publish the draft as the next version (`"1"`, `"2"`, …), register it, define it
   * as the workflow's in-process definition and, unless `promote` is
   * false, promote it so new runs start with it. When the latest version
   * already runs the same content nothing new is published. With
   * `revision`, a draft that changed since is a conflict.
   */
  async publish(params: {
    name: string;
    revision?: number;
    promote?: boolean;
  }): Promise<PublishResult> {
    const { name } = params;
    return this.serialize(name, async () => {
      const stored = await this.require(name);
      if (params.revision !== undefined && stored.draft.revision !== params.revision) {
        throw new BuilderWorkflowConflictError({ name, currentRevision: stored.draft.revision });
      }
      const schema = this.check(stored.draft.schema);
      const contentHash = hashWorkflowContent(schema);
      const latest = stored.versions.at(-1);
      let versions = stored.versions;
      let version: WorkflowSchemaVersion;
      let created = false;
      if (latest && latest.contentHash === contentHash) {
        version = latest;
      } else {
        const sequence = (latest?.sequence ?? 0) + 1;
        version = {
          name,
          version: String(sequence),
          sequence,
          schema,
          contentHash,
          publishedAt: this.clock.currentTimeMs(),
        };
        const compiled = this.compile({ schema, version: version.version });
        if (!(await this.store.addVersion(version))) {
          throw new BuilderWorkflowConflictError({
            name,
            currentRevision: stored.draft.revision,
            message: `Builder workflow "${name}" was published by someone else at the same time; reload it and publish again.`,
          });
        }
        await this.registry.register(compiled);
        this.workflows.define(compiled);
        this.defined.add(name);
        this.heads.set(name, schema);
        this.loadErrors.delete(name);
        versions = [...stored.versions, version];
        created = true;
      }
      if (params.promote !== false && this.registry.promote) {
        if ((await this.activeVersion(name)) !== version.version) {
          await this.registry.promote({ name, version: version.version });
        }
      }
      return {
        workflow: await this.toBuilderWorkflow({ draft: stored.draft, versions }),
        version: version.version,
        created,
      };
    });
  }

  /**
   * Delete a workflow, its draft and every version, and stop serving it.
   * Refused with `BuilderWorkflowInUseError` while any run of it has not
   * ended: those runs would have no definition left to finish on.
   */
  async remove(name: string): Promise<void> {
    return this.serialize(name, async () => {
      const stored = await this.require(name);
      // Stop new starts first, so no run can slip in between the count and
      // the delete; put the definition back if the delete is refused.
      const definition = this.defined.has(name) ? this.workflows.definitions[name] : undefined;
      if (definition) this.workflows.undefine(name);
      const activeRuns = await this.countActiveRuns(name);
      if (activeRuns > 0) {
        if (definition) this.workflows.define(definition);
        throw new BuilderWorkflowInUseError({ name, activeRuns });
      }
      await this.store.delete(name);
      for (const v of stored.versions) {
        await this.registry.deregister({ name, version: v.version });
      }
      this.defined.delete(name);
      this.heads.delete(name);
      this.loadErrors.delete(name);
    });
  }

  /**
   * A sample input for the trigger form of a published builder workflow,
   * from its newest version (see `sampleWorkflowInput`). `undefined` for
   * other workflows.
   */
  sampleInput(name: string): unknown {
    const schema = this.heads.get(name);
    if (!schema) return undefined;
    return sampleWorkflowInput({
      schema,
      activity: (ref) => this.activities.descriptor(ref),
    });
  }

  // -------------------------------------------------------------------------

  private async load(stored: StoredWorkflowSchema): Promise<void> {
    const { name } = stored.draft;
    if (stored.versions.length === 0) return;
    if (!this.defined.has(name) && Object.hasOwn(this.workflows.definitions, name)) {
      this.loadErrors.set(
        name,
        `An in-process workflow named "${name}" is defined in code; the builder versions are not served.`,
      );
      return;
    }
    let newest: { compiled: Workflow<unknown, unknown>; schema: WorkflowSchema } | undefined;
    const failures: string[] = [];
    for (const v of stored.versions) {
      let compiled: Workflow<unknown, unknown>;
      try {
        compiled = this.compile({ schema: v.schema, version: v.version });
      } catch (err) {
        failures.push(`version ${v.version}: ${errorMessage(err)}`);
        continue;
      }
      await this.registry.register(compiled);
      newest = { compiled, schema: v.schema };
    }
    if (failures.length > 0) this.loadErrors.set(name, failures.join("; "));
    else this.loadErrors.delete(name);
    if (!newest) return;
    this.workflows.define(newest.compiled);
    this.defined.add(name);
    this.heads.set(name, newest.schema);
    if (this.registry.findActive && this.registry.promote) {
      if (!(await this.registry.findActive(name)) && newest.compiled.version !== undefined) {
        await this.registry.promote({ name, version: newest.compiled.version });
      }
    }
  }

  /**
   * Validate `raw` as a `WorkflowSchema`, check the builder's limits and
   * compile it against the catalog. Returns the parsed schema or throws
   * `InvalidBuilderWorkflowError` with every issue found.
   */
  private check(raw: unknown): WorkflowSchema {
    const parsed = validateWorkflowSchemaSafe(raw);
    if (!parsed.success) {
      throw new InvalidBuilderWorkflowError(
        parsed.error.issues.map((i) => ({ path: i.path.join("."), message: i.message })),
      );
    }
    const schema = parsed.data as WorkflowSchema;
    const issues: WorkflowIssue[] = [];
    if (!BUILDER_WORKFLOW_NAME_PATTERN.test(schema.name)) {
      issues.push({
        path: "name",
        message: "must be 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit",
      });
    }
    if (schema.steps.length > this.maxSteps) {
      issues.push({ path: "steps", message: `at most ${this.maxSteps} steps` });
    }
    schema.steps.forEach((step, i) => {
      if (step.name.length > MAX_STEP_NAME_LENGTH) {
        issues.push({
          path: `steps.${i}.name`,
          message: `at most ${MAX_STEP_NAME_LENGTH} characters`,
        });
      }
    });
    if (issues.length > 0) throw new InvalidBuilderWorkflowError(issues);
    this.compile({ schema });
    return schema;
  }

  /** Compile against the catalog; compilation problems become `InvalidBuilderWorkflowError`. */
  private compile(params: {
    schema: WorkflowSchema;
    version?: string;
  }): Workflow<unknown, unknown> {
    try {
      return compileWorkflow({
        schema: params.schema,
        registry: this.activities,
        type: BUILDER_WORKFLOW_TYPE,
        ...(params.version !== undefined && { version: params.version }),
      });
    } catch (err) {
      if (err instanceof WorkflowCompilationError) {
        throw new InvalidBuilderWorkflowError(err.issues.map((message) => ({ path: "", message })));
      }
      // An activity factory that rejects its config throws here.
      throw new InvalidBuilderWorkflowError([{ path: "", message: errorMessage(err) }]);
    }
  }

  /** True when a workflow of this name runs from code or is registered by the host. */
  private async isReserved(name: string): Promise<boolean> {
    if (this.defined.has(name)) return false;
    if (Object.hasOwn(this.workflows.definitions, name)) return true;
    return (await this.registry.versions(name)).length > 0;
  }

  private async require(name: string): Promise<StoredWorkflowSchema> {
    const stored = await this.store.get(name);
    if (!stored) throw new BuilderWorkflowNotFoundError(name);
    return stored;
  }

  private async activeVersion(name: string): Promise<string | undefined> {
    return (await this.registry.findActive?.(name))?.version;
  }

  private async toBuilderWorkflow(stored: StoredWorkflowSchema): Promise<BuilderWorkflow> {
    const { draft } = stored;
    const records = (await this.registry.listRecords?.(draft.name)) ?? [];
    const statusOf = new Map(records.map((r) => [r.version, r.status]));
    const active = records.find((r) => r.status === "active")?.version;
    const loadError = this.loadErrors.get(draft.name);
    return {
      name: draft.name,
      schema: draft.schema,
      revision: draft.revision,
      createdAt: draft.createdAt,
      updatedAt: draft.updatedAt,
      versions: stored.versions.map((v) => {
        const status = statusOf.get(v.version);
        return {
          version: v.version,
          sequence: v.sequence,
          contentHash: v.contentHash,
          publishedAt: v.publishedAt,
          ...(status !== undefined && { status }),
        };
      }),
      ...(active !== undefined && { activeVersion: active }),
      unpublishedChanges: hasUnpublishedChanges(stored),
      ...(loadError !== undefined && { loadError }),
    };
  }

  private async countActiveRuns(name: string): Promise<number> {
    const storage = this.workflows.storage;
    let total = 0;
    for (const status of ACTIVE_RUN_STATUSES) {
      if (hasCapability(storage, "countWorkflows")) {
        total += await storage.countWorkflows({ name, status });
      } else {
        total += (await storage.listWorkflows({ name, status, limit: 1 })).length;
      }
      if (total > 0) return total;
    }
    return total;
  }

  /** Run `fn` after every earlier write to `name` has settled. */
  private serialize<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.writes.get(name) ?? Promise.resolve();
    const next = prev.then(fn, fn);
    const tail = next.catch(() => {});
    this.writes.set(name, tail);
    void tail.then(() => {
      if (this.writes.get(name) === tail) this.writes.delete(name);
    });
    return next;
  }
}

/**
 * Hash of what a workflow runs: its steps and input schema, with object
 * keys sorted. Canvas positions (`ui`) do not count, so moving nodes never
 * makes a new version.
 */
export function hashWorkflowContent(schema: WorkflowSchema): string {
  const content = {
    name: schema.name,
    inputSchema: schema.inputSchema ?? null,
    steps: schema.steps,
  };
  return createHash("sha256").update(stableStringify(content)).digest("hex");
}

function hasUnpublishedChanges(stored: StoredWorkflowSchema): boolean {
  const latest = stored.versions.at(-1);
  return !latest || latest.contentHash !== hashWorkflowContent(stored.draft.schema);
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(",")}}`;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
