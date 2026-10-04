// ---------------------------------------------------------------------------
// WorkflowVersionRegistry — maps (workflowName, version) to Workflow
// ---------------------------------------------------------------------------

import type { Workflow } from "./workflow-types.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
import { WORKFLOW_STATUSES, type WorkflowStatus } from "./workflow-state.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { hasCapability } from "./storage/capabilities.ts";

// ---------------------------------------------------------------------------
// WorkflowVersionRegistry — the async registry interface, implemented by the
// in-memory class below and by remote backends (Postgres, HTTP).
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
 * Resolves workflow definitions by name and version. Every backend
 * (in-memory, Postgres, HTTP) implements it, so coordinator and runner code
 * is backend-agnostic. Every method is async.
 *
 * Lifecycle methods (`promote`, `rollback`, `findActive`, `getStatus`,
 * `listRecords`) are optional — backends without persistent status leave
 * them out. Callers who only use `register` + `resolve` + `latest` see no
 * difference.
 */
export interface WorkflowVersionRegistry {
  /** Register a workflow definition (persists for remote backends). */
  register(definition: Workflow<unknown, unknown>): Promise<void>;
  /** Resolve by name + optional version (latest when absent). `undefined` when not found. */
  resolve(params: {
    name: string;
    version?: string;
  }): Promise<Workflow<unknown, unknown> | undefined>;
  /** All registered version strings for a workflow name. */
  versions(name: string): Promise<readonly string[]>;
  /** Latest registered version string, or undefined. */
  latest(name: string): Promise<string | undefined>;
  /** All registered workflow names. */
  names(): Promise<readonly string[]>;
  /** Remove a specific (name, version) from the registry. */
  deregister(params: { name: string; version: string }): Promise<void>;

  /**
   * Lifecycle methods — explicit promote/rollback/inspect. Not all
   * backends implement these. The dashboard + auto-mint trigger path use
   * `findActive` to route new starts to the chosen version instead of
   * always picking `latest`.
   */
  /** Resolve "the active version of workflow X". Null when no version has been promoted. */
  findActive?(name: string): Promise<VersionRecord | null>;
  /** Inspect status + timestamps for one (name, version). */
  getStatus?(params: { name: string; version: string }): Promise<VersionRecord | null>;
  /**
   * Promote a version to `active`. Atomically demotes the prior active
   * (if any) for the same name to `inactive` (NOT `archived` — we don't
   * presume the demoted version is rolling-back; see `rollback` for that).
   */
  promote?(params: { name: string; version: string }): Promise<VersionRecord>;
  /**
   * Roll back the current active to `archived` and promote a target to
   * `active`. The archive distinguishes "demoted by promote" (still in
   * rotation, just not chosen) from "explicitly rolled back" (out of
   * rotation, drain expected).
   */
  rollback?(params: {
    readonly name: string;
    readonly toVersion: string;
  }): Promise<{ readonly previous: VersionRecord; readonly active: VersionRecord }>;
  /** List all version records for one workflow, ordered by registration desc. */
  listRecords?(name: string): Promise<ReadonlyArray<VersionRecord>>;
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
 * const registry = new InMemoryWorkflowVersionRegistry();
 * await registry.register(orderV1);
 * await registry.register(orderV2);
 *
 * const runner = createWorkflowRunner({ storage, registry });
 * await runner.run({ name: "order", workflowId: "order-new", input });
 * ```
 */
export interface WorkflowVersionRegistryConfig {
  /**
   * Automatically deregister versions whose in-flight count hits zero.
   * Checked when `countByVersion()` is called. The latest registered and
   * the promoted active version are never removed. Default: false (manual
   * deregistration).
   */
  autoDeregister?: boolean;
  /**
   * Callback fired when a version finishes draining (no pending, running,
   * suspended or compensating runs left). Fires once per drain, again if
   * runs reappear and drain later. Fires regardless of `autoDeregister`;
   * useful for logging/alerting even when you want to keep the version
   * registered.
   */
  onDrained?: (name: string, version: string) => void | Promise<void>;
  /**
   * Time source for `registeredAt` / `activeAt` / `archivedAt` (and so the
   * registration-desc order of `listRecords`). Default: `SystemWallClock`.
   */
  clock?: WallClock;
}

/** Per-version run counts returned by `countByVersion`. */
export interface VersionRunCounts {
  /** Non-terminal runs: pending, running, suspended or compensating. */
  running: number;
  completed: number;
  failed: number;
  tripwire: number;
}

function addRunCount(params: {
  counts: VersionRunCounts;
  status: WorkflowStatus;
  n: number;
}): void {
  const { counts, status, n } = params;
  if (status === "completed") counts.completed += n;
  else if (status === "failed") counts.failed += n;
  else if (status === "tripwire") counts.tripwire += n;
  else counts.running += n;
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

/** In-process `WorkflowVersionRegistry`: definitions and lifecycle in plain Maps. */
export class InMemoryWorkflowVersionRegistry implements WorkflowVersionRegistry {
  // Map: workflowName -> Map<version, entry>
  private definitions = new Map<string, Map<string, VersionEntry>>();
  // Map: workflowName -> latest version string
  private latestVersions = new Map<string, string>();
  // Versions we've already fired onDrained for — prevents double-firing.
  private drainedNotified = new Set<string>();
  private readonly autoDeregister: boolean;
  private readonly onDrained?: (name: string, version: string) => void | Promise<void>;
  private readonly clock: WallClock;

  constructor(config?: WorkflowVersionRegistryConfig) {
    this.autoDeregister = config?.autoDeregister ?? false;
    this.onDrained = config?.onDrained;
    this.clock = config?.clock ?? SystemWallClock;
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
   * const orders = InMemoryWorkflowVersionRegistry.for("orders")
   *   .register(v1)
   *   .register(v2)
   *   .register(v3);
   * ```
   */
  static for(name: string, config?: WorkflowVersionRegistryConfig): ScopedWorkflowVersionRegistry {
    const underlying = new InMemoryWorkflowVersionRegistry(config);
    return new ScopedWorkflowVersionRegistry({ registry: underlying, name });
  }

  /**
   * Register a versioned workflow definition. The definition must have a
   * `version` (set via `workflow({ version: "2" })`). Initial status is
   * `inactive` — promote it explicitly via `promote()` to make it the
   * `findActive()` target.
   */
  async register(
    definition: Workflow<unknown, unknown>,
    options?: { contentHash?: string },
  ): Promise<void> {
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
        registeredAt: this.clock.now(),
        activeAt: null,
        archivedAt: null,
      });
    }

    // Track latest (by registration order — last registered is latest).
    this.latestVersions.set(name, version);
  }

  /** Resolve a definition by name + version. Returns undefined if not found. */
  async resolve(params: {
    name: string;
    version?: string;
  }): Promise<Workflow<unknown, unknown> | undefined> {
    const { name, version } = params;
    const versions = this.definitions.get(name);
    if (!versions) return undefined;
    if (version) return versions.get(version)?.definition;
    // No version specified -> return latest
    const latest = this.latestVersions.get(name);
    return latest ? versions.get(latest)?.definition : undefined;
  }

  /** Get the latest registered version string for a workflow name. */
  async latest(name: string): Promise<string | undefined> {
    return this.latestVersions.get(name);
  }

  /** List all registered versions for a workflow name. */
  async versions(name: string): Promise<string[]> {
    return this.versionsOf(name);
  }

  private versionsOf(name: string): string[] {
    const versions = this.definitions.get(name);
    return versions ? [...versions.keys()] : [];
  }

  /** List all registered workflow names. */
  async names(): Promise<string[]> {
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
  async findActive(name: string): Promise<VersionRecord | null> {
    const versions = this.definitions.get(name);
    if (!versions) return null;
    for (const [version, entry] of versions) {
      if (entry.status === "active") return this.toRecord(name, version, entry);
    }
    return null;
  }

  /** Inspect status + timestamps for one (name, version). Null when not registered. */
  async getStatus(params: { name: string; version: string }): Promise<VersionRecord | null> {
    const { name, version } = params;
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
  async promote(params: { name: string; version: string }): Promise<VersionRecord> {
    const { name, version } = params;
    const versions = this.definitions.get(name);
    if (!versions) {
      throw new Error(`promote: workflow "${name}" has no registered versions`);
    }
    const target = versions.get(version);
    if (!target) {
      throw new Error(`promote: ${name}@${version} not registered`);
    }
    if (target.status === "active") return this.toRecord(name, version, target);

    const now = this.clock.now();
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
  async rollback(params: {
    name: string;
    toVersion: string;
  }): Promise<{ previous: VersionRecord; active: VersionRecord }> {
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
    const now = this.clock.now();
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
  async listRecords(name: string): Promise<ReadonlyArray<VersionRecord>> {
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
   * Count runs per registered version using the supplied storage. `running`
   * is every non-terminal run (pending, running, suspended, compensating);
   * terminal runs (`completed`, `failed`, `tripwire`) never hold a version
   * open. Useful for monitoring drain progress before deregistering old
   * versions.
   *
   * Uses `storage.countWorkflows({ name, version, status })` when the
   * backend has it, so no rows are loaded; otherwise lists the lean
   * summaries for the name once.
   *
   * Drain detection: a registered version with no in-flight runs fires
   * `onDrained` once (re-armed when runs appear again) and, with
   * `autoDeregister`, is deregistered unless it is the latest or the
   * promoted active version.
   */
  async countByVersion(params: {
    name: string;
    storage: WorkflowStorage;
  }): Promise<Map<string, VersionRunCounts>> {
    const { name, storage } = params;
    const registered = this.versionsOf(name);
    const result = new Map<string, VersionRunCounts>();
    for (const version of registered) {
      result.set(version, { running: 0, completed: 0, failed: 0, tripwire: 0 });
    }

    if (hasCapability(storage, "countWorkflows")) {
      const count = storage.countWorkflows.bind(storage);
      await Promise.all(
        registered.flatMap((version) =>
          WORKFLOW_STATUSES.map(async (status) => {
            const n = await count({ name, version, status });
            if (n > 0) addRunCount({ counts: result.get(version)!, status, n });
          }),
        ),
      );
    } else {
      const list = hasCapability(storage, "summaries")
        ? storage.listWorkflowSummaries.bind(storage)
        : storage.listWorkflows.bind(storage);
      for (const wf of await list({ name })) {
        const counts = wf.version ? result.get(wf.version) : undefined;
        if (counts) addRunCount({ counts, status: wf.status, n: 1 });
      }
    }

    for (const version of registered) {
      // A hook awaited below may have deregistered it meanwhile.
      if (!this.definitions.get(name)?.has(version)) continue;
      const key = `${name}::${version}`;
      if (result.get(version)!.running > 0) {
        // In flight again — the next drain notifies afresh.
        this.drainedNotified.delete(key);
        continue;
      }
      if (this.drainedNotified.has(key)) continue;
      this.drainedNotified.add(key);
      if (this.onDrained) await this.onDrained(name, version);
      if (this.autoDeregister && !this.isRoutable(name, version)) {
        this.deregisterNow(name, version);
      }
    }

    return result;
  }

  /**
   * True for the versions new runs can be routed to: the latest registered
   * and the promoted active one. Auto-deregistration never removes them —
   * dropping the active version after a rollback would silently route new
   * runs back to the latest.
   */
  private isRoutable(name: string, version: string): boolean {
    if (this.latestVersions.get(name) === version) return true;
    return this.definitions.get(name)?.get(version)?.status === "active";
  }

  /**
   * Manually deregister a specific (name, version). Removes it from the
   * registry so it can't be resolved. Doesn't touch stored workflows.
   */
  async deregister(params: { name: string; version: string }): Promise<void> {
    const { name, version } = params;
    this.deregisterNow(name, version);
  }

  private deregisterNow(name: string, version: string): void {
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
 * Thin wrapper over `InMemoryWorkflowVersionRegistry` scoped to a single
 * workflow name. Returned by `InMemoryWorkflowVersionRegistry.for(name)` for
 * a fluent API when you only manage one workflow's versions.
 */
export class ScopedWorkflowVersionRegistry {
  private readonly registry: InMemoryWorkflowVersionRegistry;
  private readonly name: string;

  constructor(params: { registry: InMemoryWorkflowVersionRegistry; name: string }) {
    this.registry = params.registry;
    this.name = params.name;
  }

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
    if (!definition.version) {
      throw new Error(
        `Workflow "${definition.name}" must have a version to register in the registry`,
      );
    }
    // Validated above, so the in-memory register can't reject; it records
    // the definition before it returns.
    void this.registry.register(definition);
    return this;
  }

  /** Resolve a definition by version (or latest if omitted). */
  resolve(version?: string): Promise<Workflow<unknown, unknown> | undefined> {
    return this.registry.resolve({ name: this.name, version });
  }

  /** Latest registered version string, or undefined. */
  latest(): Promise<string | undefined> {
    return this.registry.latest(this.name);
  }

  /** All registered versions for this workflow. */
  versions(): Promise<string[]> {
    return this.registry.versions(this.name);
  }

  /** Count in-flight workflows per version; see base registry. */
  countByVersion(params: { storage: WorkflowStorage }): Promise<Map<string, VersionRunCounts>> {
    return this.registry.countByVersion({ ...params, name: this.name });
  }

  /** Deregister a specific version. */
  deregister(version: string): Promise<void> {
    return this.registry.deregister({ name: this.name, version });
  }

  /** Access the underlying unscoped registry (escape hatch). */
  get unscoped(): InMemoryWorkflowVersionRegistry {
    return this.registry;
  }
}
