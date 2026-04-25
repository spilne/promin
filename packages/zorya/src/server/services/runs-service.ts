// ---------------------------------------------------------------------------
// RunsService — business logic for the /api/runs surface.
//
// Sits between route handlers (which only deal with HTTP parsing + responses)
// and the storage / advertisement / trigger primitives. Returns plain DTOs so
// it's testable without spinning up a server, and reusable from a future CLI
// / RPC layer.
// ---------------------------------------------------------------------------

import type { Workflow, WorkflowState, WorkflowStorage } from "@promin/workflow";
import { runToDto, runToSummaryDto } from "../serialize.ts";
import type { RunDto, RunListQuery, RunListResponse, StepDto } from "../api-types.ts";
import type { WorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";

/** Minimal step-shape — same fields on Workflow.dag.steps and AdvertisedWorkflow.steps. */
interface StepDefLike {
  readonly name: string;
  readonly kind: string;
  readonly dependsOn: readonly string[];
}

export interface RunsServiceDeps {
  storage: WorkflowStorage;
  /** Static workflow definitions (in-process workflows). */
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /** Worker-advertised step defs — used when `workflows` doesn't have the def. */
  advertisements?: WorkflowAdvertisementRegistry;
}

export class RunsService {
  constructor(private readonly deps: RunsServiceDeps) {}

  /**
   * List runs with the given filters. Storage doesn't filter on `version`
   * yet, so when that filter is set we over-fetch and trim in JS to keep
   * pagination roughly correct.
   */
  async list(query: RunListQuery): Promise<RunListResponse> {
    const { version, limit = 50, offset = 0, ...rest } = query;
    const fetchLimit = version ? Math.min(limit * 5, 500) : limit;
    const rows = await this.deps.storage.listWorkflows({
      ...rest,
      limit: fetchLimit,
      offset,
    });
    const filtered = version ? rows.filter((r) => r.version === version) : rows;
    return { runs: filtered.slice(0, limit).map(runToSummaryDto) };
  }

  /**
   * Load one run, augmented with not-yet-executed steps from the workflow
   * definition (or worker advertisement) so the dashboard can render the
   * planned timeline / graph. Returns null when the run doesn't exist.
   */
  async get(id: string): Promise<RunDto | null> {
    const state = await this.deps.storage.loadWorkflow(id);
    if (!state) return null;
    const dto = runToDto(state);
    const stepDefs = await this.resolveStepDefs(state.workflowName);
    if (stepDefs.length > 0) {
      dto.steps = mergePlannedSteps(dto, stepDefs);
    }
    return dto;
  }

  /** Cancel a running or suspended workflow. */
  async cancel(id: string): Promise<void> {
    await this.deps.storage.cancelWorkflow(id);
  }

  /**
   * Distinct names / types / namespaces from the most recent ~1000 runs.
   * Used to populate the dashboard's filter dropdowns.
   */
  async listNames(): Promise<{
    names: string[];
    types?: string[];
    namespaces?: string[];
  }> {
    const rows = await this.deps.storage.listWorkflows({ limit: 1000 });
    const names = Array.from(new Set(rows.map((r) => r.workflowName))).sort();
    const types = Array.from(
      new Set(rows.map((r) => r.workflowType).filter((t): t is string => !!t)),
    ).sort();
    const namespaces = Array.from(
      new Set(rows.map((r) => r.namespace).filter((n): n is string => !!n)),
    ).sort();
    return {
      names,
      types: types.length > 0 ? types : undefined,
      namespaces: namespaces.length > 0 ? namespaces : undefined,
    };
  }

  /**
   * Resolve a workflow's step definitions from the in-process registry,
   * falling back to worker advertisements. Empty array if nothing matches.
   */
  private async resolveStepDefs(name: string): Promise<readonly StepDefLike[]> {
    const def = this.deps.workflows?.[name];
    if (def) return def.dag.steps;
    if (this.deps.advertisements) {
      const distinct = await this.deps.advertisements.distinct();
      const adv = distinct.find((a) => a.name === name);
      if (adv) return adv.steps;
    }
    return [];
  }
}

/**
 * Merge the workflow definition's static step list with executed-step DTOs.
 * Executed steps inherit `dependsOn` from the def when storage didn't
 * persist it (most backends only set dependsOn at insert time and the
 * saveStepResult / saveStepFailure paths default it to []), so the run-detail
 * Graph view shows edges instead of disconnected nodes.
 */
function mergePlannedSteps(dto: RunDto, stepDefs: readonly StepDefLike[]): StepDto[] {
  const executedByName = new Map(dto.steps.map((s) => [s.stepName, s]));
  const out: StepDto[] = [];
  const seen = new Set<string>();

  for (const defStep of stepDefs) {
    const executed = executedByName.get(defStep.name);
    if (executed) {
      if (executed.dependsOn.length === 0 && defStep.dependsOn.length > 0) {
        executed.dependsOn = [...defStep.dependsOn];
      }
      out.push(executed);
    } else {
      out.push({
        stepName: defStep.name,
        run: dto.run,
        status: "pending",
        stepType: (defStep.kind as StepDto["stepType"]) ?? "single",
        dependsOn: [...defStep.dependsOn],
        attempt: 0,
        isPlanned: true,
      });
    }
    seen.add(defStep.name);
  }

  for (const s of dto.steps) {
    if (!seen.has(s.stepName)) out.push(s);
  }
  return out;
}

// Exposed for tests and for embedders that want to compose serialization
// without going through the service.
export { mergePlannedSteps };
export type { StepDefLike };
// Re-export for consumers writing services that need WorkflowState.
export type { WorkflowState };
