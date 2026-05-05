// ---------------------------------------------------------------------------
// WorkflowVersionRegistry — maps (workflowName, version) to Workflow
// ---------------------------------------------------------------------------

import type { Workflow } from "./durable-pipeline.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";

// ---------------------------------------------------------------------------
// Async registry interface — implemented by both the local in-memory class
// and remote backends (Postgres, HTTP). Coordinator + runner accept either.
// ---------------------------------------------------------------------------

/**
 * Lifecycle status of a registered version. Three states:
 *   `inactive` — default; registered but not the chosen version. New
 *                workflows that resolve via `latest()` still get this if
 *                it's the most-recently-registered.
 *   `active`   — explicitly promoted. `findActive(name)` returns it.
 *                At most one row per name carries this status (DB-enforced
 *                in Postgres via partial unique index; in-memory enforced
 *                by `promote` swapping atomically).
 *   `archived` — drained / rolled-back. Out of rotation.
 */
export type VersionStatus = "inactive" | "active" | "archived";

export interface VersionRecord {
  readonly name: string;
  readonly version: string;
  readonly status: VersionStatus;
  readonly contentHash: string | null;
  readonly registeredAt: Date;
  readonly activeAt: Date | null;
  readonly archivedAt: Date | null;
}

/**
 * Async interface for resolving workflow definitions by name/version.
 * All backends (in-memory, Postgres, HTTP) implement this interface so
 * coordinator and runner code is backend-agnostic.
 *
 * The local `WorkflowVersionRegistry` class also implements this interface
 * (its sync methods are exposed via trivially-async wrappers) so existing
 * code continues to work without changes.
 *
 * Lifecycle methods (`promote`, `rollback`, `findActive`, `getStatus`,
 * `listRecords`) are optional — backends without persistent status throw
 * a clear error when invoked. Callers who only use `register` + `resolve`
 * + `latest` see no change.
 */
export interface IWorkflowVersionRegistry {
  /** Register a workflow definition (persists for remote backends). */
  register(definition: Workflow<unknown, unknown>): Promise<void> | void;
  /** Resolve by name + optional version. `undefined` when not found. */
  resolve(
    name: string,
    version?: string,
  ): Promise<Workflow<unknown, unknown> | undefined> | Workflow<unknown, unknown> | undefined;
  /** All registered version strings for a workflow name. */
  versions(name: string): Promise<readonly string[]> | readonly string[];
  /** Latest registered version string, or undefined. */
  latest(name: string): Promise<string | undefined> | string | undefined;
  /** All registered workflow names. */
  names(): Promise<readonly string[]> | readonly string[];
  /** Remove a specific (name, version) from the registry. */
  deregister(name: string, version: string): Promise<void> | void;

  /**
   * Lifecycle methods — explicit promote/rollback/inspect. Not all
   * backends implement these; callers can feature-detect via instanceof
   * or a try/catch. The dashboard + auto-mint trigger path use
   * `findActive` to route new starts to the chosen version instead of
   * always picking `latest`.
   */
  /** Resolve "the active version of workflow X". Null when no version has been promoted. */
  findActive?(name: string): Promise<VersionRecord | null> | VersionRecord | null;
  /** Inspect status + timestamps for one (name, version). */
  getStatus?(name: string, version: string): Promise<VersionRecord | null> | VersionRecord | null;
  /**
   * Promote a version to `active`. Atomically demotes the prior active
   * (if any) for the same name to `inactive` (NOT `archived` — we don't
   * presume the demoted version is rolling-back; see `rollback` for that).
   */
  promote?(name: string, version: string): Promise<VersionRecord> | VersionRecord;
  /**
   * Roll back the current active to `archived` and promote a target to
   * `active`. The archive distinguishes "demoted by promote" (still in
   * rotation, just not chosen) from "explicitly rolled back" (out of
   * rotation, drain expected).
   */
  rollback?(params: {
    readonly name: string;
    readonly toVersion: string;
  }):
    | Promise<{ readonly previous: VersionRecord; readonly active: VersionRecord }>
    | { readonly previous: VersionRecord; readonly active: VersionRecord };
  /** List all version records for one workflow, ordered by registration desc. */
  listRecords?(name: string): Promise<ReadonlyArray<VersionRecord>> | ReadonlyArray<VersionRecord>;
}

/**
 * Registry mapping (workflowName, version) to pure `Workflow` definitions.
 *
 * Pure lookup — no storage, no `.run()`. Hand the registry to a
 * `WorkflowRunner`, and the runner drives execution against its own
 * storage, resolving the right version per workflowId.
 *
 * @example
 * ```ts
 * const registry = createWorkflowVersionRegistry();
 * registry.register(orderV1);
 * registry.register(orderV2);
 *
 * const runner = createWorkflowRunner({ storage, registry });
 * await runner.run({ name: "order", workflowId: "order-new", input });
 * ```
 */
export interface WorkflowVersionRegistryConfig {
  /**
   * Automatically deregister versions whose in-flight count hits zero.
   * Polled when `countByVersion()` is called; also triggered via
   * `checkDrained()`. Default: false (manual deregistration).
   */
  autoDeregister?: boolean;
  /**
   * Callback fired when a version finishes draining (running + suspended
   * count reaches zero). Fires regardless of `autoDeregister`; useful for
   * logging/alerting even when you want to keep the version registered.
   */
  onDrained?: (name: string, version: string) => void | Promise<void>;
}

/** Internal record holding lifecycle metadata alongside the definition. */
interface VersionEntry {
  definition: Workflow<unknown, unknown>;
  status: VersionStatus;
  contentHash: string | null;
  registeredAt: Date;
  activeAt: Date | null;
  archivedAt: Date | null;
}

export class WorkflowVersionRegistry {
  // Map: workflowName -> Map<version, entry>
  private definitions = new Map<string, Map<string, VersionEntry>>();
  // Map: workflowName -> latest version string
  private latestVersions = new Map<string, string>();
  // Versions we've already fired onDrained for — prevents double-firing.
  private drainedNotified = new Set<string>();
  private readonly autoDeregister: boolean;
  private readonly onDrained?: (name: string, version: string) => void | Promise<void>;

  constructor(config?: WorkflowVersionRegistryConfig) {
    this.autoDeregister = config?.autoDeregister ?? false;
    this.onDrained = config?.onDrained;
  }

  /** Convert an internal VersionEntry into the public VersionRecord shape. */
  private toRecord(name: string, version: string, entry: VersionEntry): VersionRecord {
    return {
      name,
      version,
      status: entry.status,
      contentHash: entry.contentHash,
      registeredAt: new Date(entry.registeredAt.getTime()),
      activeAt: entry.activeAt ? new Date(entry.activeAt.getTime()) : null,
      archivedAt: entry.archivedAt ? new Date(entry.archivedAt.getTime()) : null,
    };
  }

  /**
   * Fluent builder for a per-workflow registry. Reads more naturally than
   * the raw constructor when you only care about one workflow name:
   *
   * ```typescript
   * const orders = WorkflowVersionRegistry.for("orders")
   *   .register(v1)
   *   .register(v2)
   *   .register(v3);
   * ```
   */
  static for(name: string, config?: WorkflowVersionRegistryConfig): ScopedWorkflowVersionRegistry {
    const underlying = new WorkflowVersionRegistry(config);
    return new ScopedWorkflowVersionRegistry(underlying, name);
  }

  /**
   * Register a versioned workflow definition. The definition must have a
   * `version` (set via `workflow({ version: "2" })`). Initial status is
   * `inactive` — promote it explicitly via `promote()` to make it the
   * `findActive()` target.
   */
  register(definition: Workflow<unknown, unknown>, options?: { contentHash?: string }): void {
    const { name, version } = definition;

    if (!version) {
      throw new Error(`Workflow "${name}" must have a version to register in the registry`);
    }

    if (!this.definitions.has(name)) {
      this.definitions.set(name, new Map());
    }
    const versions = this.definitions.get(name)!;
    const existing = versions.get(version);
    if (existing) {
      // Re-registration of the same (name, version) — refresh the
      // definition pointer + contentHash but preserve lifecycle status.
      existing.definition = definition;
      if (options?.contentHash !== undefined) existing.contentHash = options.contentHash;
    } else {
      versions.set(version, {
        definition,
        status: "inactive",
        contentHash: options?.contentHash ?? null,
        registeredAt: new Date(),
        activeAt: null,
        archivedAt: null,
      });
    }

    // Track latest (by registration order — last registered is latest).
    this.latestVersions.set(name, version);
  }

  /** Resolve a definition by name + version. Returns undefined if not found. */
  resolve(name: string, version?: string): Workflow<unknown, unknown> | undefined {
    const versions = this.definitions.get(name);
    if (!versions) return undefined;
    if (version) return versions.get(version)?.definition;
    // No version specified -> return latest
    const latest = this.latestVersions.get(name);
    return latest ? versions.get(latest)?.definition : undefined;
  }

  /** Get the latest registered version string for a workflow name. */
  latest(name: string): string | undefined {
    return this.latestVersions.get(name);
  }

  /** List all registered versions for a workflow name. */
  versions(name: string): string[] {
    const versions = this.definitions.get(name);
    return versions ? [...versions.keys()] : [];
  }

  /** List all registered workflow names. */
  names(): string[] {
    return [...this.definitions.keys()];
  }

  // ---------------------------------------------------------------------------
  // Lifecycle — promote / rollback / inspect.
  //
  // `findActive` returns the explicitly-promoted version. When nothing has
  // been promoted, it returns null and the auto-mint trigger path falls
  // back to `latest()` (preserving today's behaviour).
  // ---------------------------------------------------------------------------

  /** Resolve the explicitly-promoted version of a workflow. Null when none. */
  findActive(name: string): VersionRecord | null {
    const versions = this.definitions.get(name);
    if (!versions) return null;
    for (const [version, entry] of versions) {
      if (entry.status === "active") return this.toRecord(name, version, entry);
    }
    return null;
  }

  /** Inspect status + timestamps for one (name, version). Null when not registered. */
  getStatus(name: string, version: string): VersionRecord | null {
    const entry = this.definitions.get(name)?.get(version);
    return entry ? this.toRecord(name, version, entry) : null;
  }

  /**
   * Promote a version to `active`. Atomically demotes the prior active
   * (if any) for the same name back to `inactive` (NOT `archived` — the
   * demoted version is still in rotation, just not chosen).
   *
   * Idempotent: promoting an already-active version is a no-op.
   */
  promote(name: string, version: string): VersionRecord {
    const versions = this.definitions.get(name);
    if (!versions) {
      throw new Error(`promote: workflow "${name}" has no registered versions`);
    }
    const target = versions.get(version);
    if (!target) {
      throw new Error(`promote: ${name}@${version} not registered`);
    }
    if (target.status === "active") return this.toRecord(name, version, target);

    const now = new Date();
    // Demote the current active to inactive.
    for (const [v, entry] of versions) {
      if (v !== version && entry.status === "active") {
        entry.status = "inactive";
      }
    }
    target.status = "active";
    target.activeAt = now;
    target.archivedAt = null;
    return this.toRecord(name, version, target);
  }

  /**
   * Roll back the current active to `archived` and promote `toVersion` to
   * `active`. The archive distinguishes "demoted by promote" (still in
   * rotation, just not chosen) from "explicitly rolled back" (drain
   * expected, out of rotation).
   */
  rollback(params: { name: string; toVersion: string }): {
    previous: VersionRecord;
    active: VersionRecord;
  } {
    const versions = this.definitions.get(params.name);
    if (!versions) {
      throw new Error(`rollback: workflow "${params.name}" has no registered versions`);
    }
    const target = versions.get(params.toVersion);
    if (!target) {
      throw new Error(`rollback: ${params.name}@${params.toVersion} not registered`);
    }
    let previousEntry: VersionEntry | null = null;
    let previousVersion = "";
    for (const [v, entry] of versions) {
      if (entry.status === "active" && v !== params.toVersion) {
        previousEntry = entry;
        previousVersion = v;
      }
    }
    if (!previousEntry) {
      throw new Error(`rollback: no active version for "${params.name}" to roll back`);
    }
    const now = new Date();
    previousEntry.status = "archived";
    previousEntry.archivedAt = now;
    target.status = "active";
    target.activeAt = now;
    target.archivedAt = null;
    return {
      previous: this.toRecord(params.name, previousVersion, previousEntry),
      active: this.toRecord(params.name, params.toVersion, target),
    };
  }

  /** List every (name, version) record for one workflow, registration-desc. */
  listRecords(name: string): ReadonlyArray<VersionRecord> {
    const versions = this.definitions.get(name);
    if (!versions) return [];
    const records: VersionRecord[] = [];
    for (const [version, entry] of versions) {
      records.push(this.toRecord(name, version, entry));
    }
    records.sort((a, b) => b.registeredAt.getTime() - a.registeredAt.getTime());
    return records;
  }

  /**
   * Count in-flight (pending/running/suspended) workflows per version
   * using the supplied storage. Useful for monitoring drain progress
   * before deregistering old versions.
   */
  async countByVersion(params: {
    name: string;
    storage: WorkflowStorage;
  }): Promise<Map<string, { running: number; completed: number; failed: number }>> {
    const result = new Map<string, { running: number; completed: number; failed: number }>();

    // Query storage once for all workflows with this name
    const workflows = await params.storage.listWorkflows({
      name: params.name,
    });

    // Initialize counters for all registered versions
    for (const version of this.versions(params.name)) {
      result.set(version, { running: 0, completed: 0, failed: 0 });
    }

    // Count by version (filter in JS since storage has no version filter)
    for (const wf of workflows) {
      const version = wf.version;
      if (!version) continue;

      let counts = result.get(version);
      if (!counts) {
        counts = { running: 0, completed: 0, failed: 0 };
        result.set(version, counts);
      }

      if (wf.status === "completed") counts.completed++;
      else if (wf.status === "failed") counts.failed++;
      else counts.running++; // pending, running, suspended, compensating
    }

    // Drain detection: if a registered version has zero in-flight (no
    // running counter) AND we haven't already notified, fire onDrained and
    // optionally deregister.
    for (const version of this.versions(params.name)) {
      const counts = result.get(version) ?? { running: 0, completed: 0, failed: 0 };
      if (counts.running === 0) {
        const key = `${params.name}::${version}`;
        if (!this.drainedNotified.has(key)) {
          this.drainedNotified.add(key);
          if (this.onDrained) await this.onDrained(params.name, version);
          // Don't deregister the latest version — new workflows need it.
          if (this.autoDeregister && this.latestVersions.get(params.name) !== version) {
            this.deregister(params.name, version);
          }
        }
      }
    }

    return result;
  }

  /**
   * Manually deregister a specific (name, version). Removes it from the
   * registry so it can't be resolved. Doesn't touch stored workflows.
   */
  deregister(name: string, version: string): void {
    this.definitions.get(name)?.delete(version);
    // If we deregistered the latest, pick a new latest (last remaining).
    if (this.latestVersions.get(name) === version) {
      const remaining = this.definitions.get(name);
      if (remaining && remaining.size > 0) {
        const keys = [...remaining.keys()];
        this.latestVersions.set(name, keys[keys.length - 1]!);
      } else {
        this.latestVersions.delete(name);
      }
    }
  }
}

/**
 * Thin wrapper over `WorkflowVersionRegistry` scoped to a single workflow
 * name. Returned by `WorkflowVersionRegistry.for(name)` for a fluent API
 * when you only manage one workflow's versions.
 */
export class ScopedWorkflowVersionRegistry {
  constructor(
    private readonly registry: WorkflowVersionRegistry,
    private readonly name: string,
  ) {}

  /**
   * Register a versioned definition. Returns `this` for chaining.
   * The definition's `name` must match the scoped registry's name.
   */
  register(definition: Workflow<unknown, unknown>): this {
    if (definition.name !== this.name) {
      throw new Error(
        `ScopedWorkflowVersionRegistry("${this.name}"): definition has name "${definition.name}" — ` +
          `use the unscoped registry for cross-name registrations.`,
      );
    }
    this.registry.register(definition);
    return this;
  }

  /** Resolve a definition by version (or latest if omitted). */
  resolve(version?: string): Workflow<unknown, unknown> | undefined {
    return this.registry.resolve(this.name, version);
  }

  /** Latest registered version string, or undefined. */
  latest(): string | undefined {
    return this.registry.latest(this.name);
  }

  /** All registered versions for this workflow. */
  versions(): string[] {
    return this.registry.versions(this.name);
  }

  /** Count in-flight workflows per version; see base registry. */
  countByVersion(params: {
    storage: WorkflowStorage;
  }): Promise<Map<string, { running: number; completed: number; failed: number }>> {
    return this.registry.countByVersion({ ...params, name: this.name });
  }

  /** Deregister a specific version. */
  deregister(version: string): void {
    this.registry.deregister(this.name, version);
  }

  /** Access the underlying unscoped registry (escape hatch). */
  get unscoped(): WorkflowVersionRegistry {
    return this.registry;
  }
}

/**
 * Convenience factory. Prefer this over `new WorkflowVersionRegistry(...)`
 * in new code — mirrors how every other promin building block is built.
 */
export function createWorkflowVersionRegistry(
  config?: WorkflowVersionRegistryConfig,
): WorkflowVersionRegistry {
  return new WorkflowVersionRegistry(config);
}
