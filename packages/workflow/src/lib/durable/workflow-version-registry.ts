// ---------------------------------------------------------------------------
// WorkflowVersionRegistry — maps (workflowName, version) to Workflow
// ---------------------------------------------------------------------------

import type { Workflow, RunnableWorkflow } from "./durable-pipeline.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";

/**
 * Registry mapping (workflowName, version) to pure `Workflow` definitions.
 *
 * Enables running multiple versions simultaneously:
 * - New workflows use the latest registered version
 * - Existing workflows resume with their original version's definition
 *
 * The registry carries a single `storage` — it binds each resolved
 * `Workflow` to that storage on demand, returning a `RunnableWorkflow`.
 * Callers register pure definitions (no storage on the def itself) so
 * the same definition module can be shared across processes that may
 * have different storage wiring.
 *
 * @example
 * ```ts
 * const registry = new WorkflowVersionRegistry({ storage });
 * registry.register(orderV1);  // version "1"
 * registry.register(orderV2);  // version "2"
 *
 * // New workflow -> uses v2, bound to registry.storage
 * await registry.run({ workflowId: "order-new", input, name: "order" });
 *
 * // Existing v1 workflow -> resumes with v1 definition
 * await registry.run({ workflowId: "order-old", input, name: "order" });
 * ```
 */
/** Optional config for WorkflowVersionRegistry. */
export interface WorkflowVersionRegistryConfig {
  /**
   * Storage backend used to bind resolved definitions. Required for
   * `run()`, `resolveRunnable()`, and `countByVersion()`. Optional only
   * for registries that are used purely as a definition catalog (e.g.
   * the coordinator's DAG lookup — it reads name/version/dag from the
   * pure Workflow and never calls .run() through the registry).
   */
  storage?: WorkflowStorage;
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

export class WorkflowVersionRegistry {
  // Map: workflowName -> Map<version, definition>
  private definitions = new Map<string, Map<string, Workflow<unknown, unknown>>>();
  // Map: workflowName -> latest version string
  private latestVersions = new Map<string, string>();
  // Versions we've already fired onDrained for — prevents double-firing.
  private drainedNotified = new Set<string>();
  private readonly autoDeregister: boolean;
  private readonly onDrained?: (name: string, version: string) => void | Promise<void>;
  private readonly storage?: WorkflowStorage;

  constructor(config?: WorkflowVersionRegistryConfig) {
    this.autoDeregister = config?.autoDeregister ?? false;
    this.onDrained = config?.onDrained;
    this.storage = config?.storage;
  }

  private _requireStorage(op: string): WorkflowStorage {
    if (!this.storage) {
      throw new Error(
        `WorkflowVersionRegistry.${op}() requires \`storage\` on the registry config. ` +
          `Pass \`new WorkflowVersionRegistry({ storage })\` or bind definitions manually.`,
      );
    }
    return this.storage;
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
   *
   * Returns a thin wrapper that keeps method calls scoped to the given
   * workflow name but delegates storage to the underlying registry.
   */
  static for(name: string, config?: WorkflowVersionRegistryConfig): ScopedWorkflowVersionRegistry {
    const underlying = new WorkflowVersionRegistry(config);
    return new ScopedWorkflowVersionRegistry(underlying, name);
  }

  /**
   * Register a versioned workflow definition.
   * The definition must have a version (set via `workflow({ version: "2" })` or `.version("2")`).
   * Accepts pure `Workflow` (no storage) — the registry binds to its own
   * storage when resolving. `RunnableWorkflow` also works (it extends
   * `Workflow`); the registry ignores the bound storage in favor of its own.
   */
  register(definition: Workflow<unknown, unknown>): void {
    const { name, version } = definition;

    if (!version) {
      throw new Error(`Workflow "${name}" must have a version to register in the registry`);
    }

    if (!this.definitions.has(name)) {
      this.definitions.set(name, new Map());
    }
    this.definitions.get(name)!.set(version, definition);

    // Track latest (by registration order — last registered is latest)
    this.latestVersions.set(name, version);
  }

  /** Resolve a definition by name + version. Returns undefined if not found. */
  resolve(name: string, version?: string): Workflow<unknown, unknown> | undefined {
    const versions = this.definitions.get(name);
    if (!versions) return undefined;
    if (version) return versions.get(version);
    // No version specified -> return latest
    const latest = this.latestVersions.get(name);
    return latest ? versions.get(latest) : undefined;
  }

  /**
   * Resolve and bind to the registry's storage, returning a runnable.
   * Throws if storage wasn't configured on the registry or the name/
   * version isn't registered.
   */
  resolveRunnable(name: string, version?: string): RunnableWorkflow<unknown, unknown> | undefined {
    const def = this.resolve(name, version);
    if (!def) return undefined;
    return def.bind(this._requireStorage("resolveRunnable"));
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

  /**
   * Run a workflow using the registry for version resolution.
   * - If workflowId exists in storage -> resume with stored version's definition
   * - If workflowId doesn't exist -> create with latest version
   */
  async run<Output>(params: {
    workflowId: string;
    input: unknown;
    name: string;
    force?: boolean;
  }): Promise<Output> {
    const { workflowId, input, name, force } = params;
    const storage = this._requireStorage("run");

    // Check if workflow already exists in storage
    const latestDef = this.resolve(name);
    if (!latestDef) {
      throw new Error(`No workflow "${name}" registered in the registry`);
    }

    const existing = await storage.loadWorkflow(workflowId);

    if (existing) {
      // Resume with stored version
      const storedVersion = existing.version;
      const def = this.resolve(name, storedVersion);
      if (!def) {
        throw new Error(
          `Workflow "${name}" version "${storedVersion}" not found in registry. ` +
            `Available versions: ${this.versions(name).join(", ")}. ` +
            `Keep old definitions registered until in-flight workflows drain.`,
        );
      }
      return def.bind(storage).run({ workflowId, input, force }) as Promise<Output>;
    }

    // New workflow — use latest version
    return latestDef.bind(storage).run({ workflowId, input, force }) as Promise<Output>;
  }

  /**
   * Count in-flight (pending/running/suspended) workflows per version.
   * Useful for monitoring drain progress before deregistering old versions.
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

  /** Run a workflow; see `WorkflowVersionRegistry.run`. */
  run<Output>(params: { workflowId: string; input: unknown; force?: boolean }): Promise<Output> {
    return this.registry.run<Output>({ ...params, name: this.name });
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
 * Convenience factory for constructing a `WorkflowVersionRegistry`. Mirrors
 * how every other promin building block (runner, scheduler, coordinator,
 * cache) is constructed — `createThing(config)` — so the registry can move
 * from concrete class to interface without churning every call site.
 *
 * Prefer this over `new WorkflowVersionRegistry(...)` in new code.
 */
export function createWorkflowVersionRegistry(
  config?: WorkflowVersionRegistryConfig,
): WorkflowVersionRegistry {
  return new WorkflowVersionRegistry(config);
}
