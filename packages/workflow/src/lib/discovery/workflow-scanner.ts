// ---------------------------------------------------------------------------
// WorkflowScanner — auto-discover Workflow exports from a directory tree.
//
// Lives in @promin/workflow (rather than @promin/zorya) so workers, agents,
// and any other consumer can construct one without depending on the
// dashboard. ZoryaServer's `scanWorkflowsFolder` helper is a thin wrapper
// around this class, and split-mode workers can share the same configured
// instance.
//
// Detection is structural: an export counts as a Workflow if it has a
// string `name`, an object `dag`, and an object `_definition`. Any
// authoring style — `defineWorkflow(...)`, factory functions, builder
// pattern — works as long as the resulting object matches that shape.
//
// Definitions are told apart by `name@version`, so several versions of one
// workflow exported side by side are all kept.
// ---------------------------------------------------------------------------

import type { Workflow } from "../durable/workflow-types.ts";
import { scanModules } from "./scan-modules.ts";

export interface WorkflowScannerOptions {
  /** File extensions to consider. Default: .ts, .tsx, .js, .mjs. */
  extensions?: ReadonlyArray<string>;
  /** Recursion depth. Default: 10. */
  maxDepth?: number;
  /** Predicate to filter candidate files before import. */
  filter?: (absPath: string) => boolean;
  /** Called for each discovered workflow definition. */
  onWorkflow?: (params: {
    readonly name: string;
    readonly workflow: Workflow<unknown, unknown>;
    readonly sourcePath: string;
  }) => void;
}

export interface WorkflowScanResult {
  /**
   * One definition per workflow name — the last one discovered when a name
   * is exported in several versions. `definitions` has all of them.
   */
  workflows: Record<string, Workflow<unknown, unknown>>;
  /** Every distinct definition, one per `name@version` (or `name` when unversioned). */
  definitions: Workflow<unknown, unknown>[];
  /** Source path per `name@version` (or `name`) key. */
  sources: Record<string, string>;
  warnings: string[];
}

/** The key a scan tells definitions apart by: `name@version`, or `name`. */
export function workflowDefinitionKey(workflow: { name: string; version?: string }): string {
  return workflow.version === undefined ? workflow.name : `${workflow.name}@${workflow.version}`;
}

export class WorkflowScanner {
  private readonly options: WorkflowScannerOptions;

  constructor(options: WorkflowScannerOptions = {}) {
    this.options = options;
  }

  /** One-shot helper for callers that don't want to hold an instance. */
  static async scanFolder(
    params: { readonly root: string } & WorkflowScannerOptions,
  ): Promise<WorkflowScanResult> {
    const { root, ...options } = params;
    return await new WorkflowScanner(options).scan(root);
  }

  async scan(root: string): Promise<WorkflowScanResult> {
    const byKey = new Map<string, Workflow<unknown, unknown>>();
    const workflows: Record<string, Workflow<unknown, unknown>> = {};
    const sources: Record<string, string> = {};
    const warnings: string[] = [];

    await scanModules({
      root,
      extensions: this.options.extensions,
      maxDepth: this.options.maxDepth,
      filter: this.options.filter,
      warnings,
      visit: ({ exports, path }) => {
        for (const value of Object.values(exports)) {
          if (!isWorkflow(value)) continue;
          const key = workflowDefinitionKey(value);
          const existing = byKey.get(key);
          if (existing === value) continue;
          if (existing) {
            warnings.push(`duplicate workflow "${key}": ${sources[key]} vs ${path} — last wins`);
          }
          byKey.set(key, value);
          workflows[value.name] = value;
          sources[key] = path;
          this.options.onWorkflow?.({ name: value.name, workflow: value, sourcePath: path });
        }
      },
    });

    return { workflows, definitions: [...byKey.values()], sources, warnings };
  }
}

function isWorkflow(v: unknown): v is Workflow<unknown, unknown> {
  if (v === null || typeof v !== "object") return false;
  const o = v as { name?: unknown; dag?: unknown; _definition?: unknown };
  return (
    typeof o.name === "string" &&
    typeof o.dag === "object" &&
    o.dag !== null &&
    typeof o._definition === "object" &&
    o._definition !== null
  );
}
