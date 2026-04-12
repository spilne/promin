// ---------------------------------------------------------------------------
// WorkflowVersionRegistry — maps (workflowName, version) to WorkflowDefinition
// ---------------------------------------------------------------------------

import type { WorkflowDefinition } from "./durable-pipeline.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";

/**
 * Registry mapping (workflowName, version) to WorkflowDefinition.
 *
 * Enables running multiple versions simultaneously:
 * - New workflows use the latest registered version
 * - Existing workflows resume with their original version's definition
 *
 * @example
 * ```ts
 * const registry = new WorkflowVersionRegistry();
 * registry.register(orderV1);  // version "1"
 * registry.register(orderV2);  // version "2"
 *
 * // New workflow -> uses v2
 * await registry.run({ workflowId: "order-new", input, name: "order" });
 *
 * // Existing v1 workflow -> resumes with v1 definition
 * await registry.run({ workflowId: "order-old", input, name: "order" });
 * ```
 */
export class WorkflowVersionRegistry {
  // Map: workflowName -> Map<version, definition>
  private definitions = new Map<string, Map<string, WorkflowDefinition<unknown, unknown>>>();
  // Map: workflowName -> latest version string
  private latestVersions = new Map<string, string>();

  /**
   * Register a versioned workflow definition.
   * The definition must have a version (set via `workflow({ version: "2" })` or `.version("2")`).
   */
  register(definition: WorkflowDefinition<unknown, unknown>): void {
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
  resolve(name: string, version?: string): WorkflowDefinition<unknown, unknown> | undefined {
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

    // Check if workflow already exists in storage
    const latestDef = this.resolve(name);
    if (!latestDef) {
      throw new Error(`No workflow "${name}" registered in the registry`);
    }

    const storage = latestDef.storage as WorkflowStorage;
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
      return def.run({ workflowId, input, force }) as Promise<Output>;
    }

    // New workflow — use latest version
    return latestDef.run({ workflowId, input, force }) as Promise<Output>;
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

    return result;
  }
}
