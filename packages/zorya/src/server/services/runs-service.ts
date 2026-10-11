// ---------------------------------------------------------------------------
// RunsService — business logic for the /api/runs surface.
//
// Sits between route handlers (which only deal with HTTP parsing + responses)
// and the storage / advertisement / trigger primitives. Returns plain DTOs so
// it's testable without spinning up a server, and reusable from a future CLI
// / RPC layer.
// ---------------------------------------------------------------------------

import type { Workflow, WorkflowState, WorkflowStatus, WorkflowStorage } from "@promin/workflow";
import {
  hasCapability,
  isCancelledRun,
  isTerminalWorkflowStatus,
  wakeParentOfEndedRun,
} from "@promin/workflow";
import { findDefinition } from "./workflows/zorya-workflows.ts";
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

/**
 * Outcome of an operator action on a run: `ok` when this call ended it
 * (with the status it ended in), `not_found` for an unknown id, `ended`
 * when the run had already ended (the action changed nothing).
 */
export type RunActionResult =
  | { readonly outcome: "ok"; readonly status: WorkflowStatus }
  | { readonly outcome: "not_found" }
  | { readonly outcome: "ended"; readonly status: WorkflowStatus };

export class RunsService {
  constructor(private readonly deps: RunsServiceDeps) {}

  /**
   * One page of runs matching `query`, most recently started first unless
   * `orderBy` says otherwise. Storage applies every filter (`version`
   * included), so `limit`/`offset` page over exactly the matching runs.
   * `total` is set when the storage can count.
   *
   * Uses the lean summary list when the storage has it — the list view
   * doesn't need `steps`/`input`/`result`.
   */
  async list(query: RunListQuery): Promise<RunListResponse> {
    const { storage } = this.deps;
    const { limit = 50, offset = 0, orderBy = "startedAt", orderDir = "desc", ...filter } = query;
    const listParams = { ...filter, limit, offset, orderBy, orderDir };
    const [rows, total] = await Promise.all([
      hasCapability(storage, "summaries")
        ? storage.listWorkflowSummaries(listParams)
        : storage.listWorkflows(listParams),
      hasCapability(storage, "countWorkflows") ? storage.countWorkflows(filter) : undefined,
    ]);
    return {
      runs: rows.map(runToSummaryDto),
      ...(total !== undefined && { total }),
    };
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

  /** Cancel a pending, running or suspended workflow. */
  async cancel(id: string): Promise<RunActionResult> {
    return this.endRun({
      workflowId: id,
      expect: (state) => isCancelledRun(state),
      write: () => this.deps.storage.cancelWorkflow({ workflowId: id }),
    });
  }

  /** Operator override: end an in-flight run as completed (result `null`). */
  async markSuccess(id: string): Promise<RunActionResult> {
    return this.endRun({
      workflowId: id,
      expect: (state) => state.status === "completed",
      write: () => this.deps.storage.completeWorkflow({ workflowId: id, result: null }),
    });
  }

  /** Operator override: end an in-flight run as failed with `reason`. */
  async markFailed(params: { id: string; reason: string }): Promise<RunActionResult> {
    const { id, reason } = params;
    return this.endRun({
      workflowId: id,
      expect: (state) => state.status === "failed" && state.error === reason,
      write: () => this.deps.storage.failWorkflow({ workflowId: id, error: reason }),
    });
  }

  /**
   * Apply a terminal write to an in-flight run, then wake a parent parked
   * on it (a child that ends by operator action wakes its parent exactly
   * like one that ends by itself). Terminal writes are conditional in
   * storage, so a run that ended first — by itself or by a concurrent
   * action — keeps its outcome and the call reports `ended`.
   */
  private async endRun(params: {
    workflowId: string;
    expect: (state: WorkflowState) => boolean;
    write: () => Promise<void>;
  }): Promise<RunActionResult> {
    const { storage } = this.deps;
    const before = await storage.loadWorkflow(params.workflowId);
    if (!before) return { outcome: "not_found" };
    if (isTerminalWorkflowStatus(before.status)) {
      return { outcome: "ended", status: before.status };
    }
    await params.write();
    const after = await storage.loadWorkflow(params.workflowId);
    if (!after) return { outcome: "not_found" };
    // Lost to a run that ended first: its own end already woke the parent.
    if (!params.expect(after)) return { outcome: "ended", status: after.status };
    await wakeParentOfEndedRun({ storage, state: after });
    return { outcome: "ok", status: after.status };
  }

  /**
   * Distinct names / types / namespaces ever observed. Powers the
   * dashboard's filter dropdowns. Backed by storage-side `SELECT DISTINCT`
   * (or equivalent) so values that haven't run recently still appear.
   *
   * `namespace`, when provided, scopes the names + types to that namespace
   * (the global namespace dropdown stays unscoped).
   */
  async listNames(params?: { namespace?: string }): Promise<{
    names: string[];
    types?: string[];
    namespaces?: string[];
  }> {
    const ns = params?.namespace;
    const [names, types, namespaces] = await Promise.all([
      this.deps.storage.distinctWorkflowNames({ namespace: ns }),
      this.deps.storage.distinctWorkflowTypes({ namespace: ns }),
      this.deps.storage.distinctNamespaces(),
    ]);
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
    const def = findDefinition(this.deps.workflows, name);
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
