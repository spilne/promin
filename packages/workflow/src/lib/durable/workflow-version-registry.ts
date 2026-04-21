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
 * Async interface for resolving workflow definitions by name/version.
 * All backends (in-memory, Postgres, HTTP) implement this interface so
 * coordinator and runner code is backend-agnostic.
 *
 * The local `WorkflowVersionRegistry` class also implements this interface
 * (its sync methods are exposed via trivially-async wrappers) so existing
 * code continues to work without changes.
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

export class WorkflowVersionRegistry {
  // Map: workflowName -> Map<version, definition>
  private definitions = new Map<string, Map<string, Workflow<unknown, unknown>>>();
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
   * `version` (set via `workflow({ version: "2" })`).
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
