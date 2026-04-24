// ---------------------------------------------------------------------------
// Workflow definitions — expose registered workflows as DTOs so the UI can
// render a "Workflows" directory page and synthesise trigger forms from
// sample inputs.
// ---------------------------------------------------------------------------

import type { Workflow } from "@promin/workflow";
import { json, jsonError } from "../router.ts";

export interface WorkflowStepDefDto {
  name: string;
  kind: string;
  dependsOn: readonly string[];
}

export interface WorkflowDefDto {
  name: string;
  type?: string;
  version?: string;
  steps: WorkflowStepDefDto[];
  /** Example input shape, used to auto-build trigger forms. Optional. */
  sampleInput?: unknown;
}

export interface WorkflowDefsResponse {
  workflows: WorkflowDefDto[];
}

export interface WorkflowDefsDeps {
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /**
   * Returns a sample input for a given workflow name. Used by the UI to
   * pre-fill trigger forms with plausible defaults so users don't have to
   * start from raw JSON. Returning `undefined` means "no sample".
   */
  sampleInput?: (workflowName: string) => unknown;
}

function toDto(name: string, wf: Workflow<unknown, unknown>, sample?: unknown): WorkflowDefDto {
  return {
    name,
    type: wf._definition.type,
    version: wf.version,
    steps: wf.dag.steps.map((s) => ({ name: s.name, kind: s.kind, dependsOn: s.dependsOn })),
    sampleInput: sample,
  };
}

export function listWorkflowDefs(deps: WorkflowDefsDeps) {
  return async (): Promise<Response> => {
    const entries = Object.entries(deps.workflows ?? {});
    const dtos: WorkflowDefDto[] = entries.map(([name, wf]) =>
      toDto(name, wf, deps.sampleInput?.(name)),
    );
    dtos.sort((a, b) => a.name.localeCompare(b.name));
    const response: WorkflowDefsResponse = { workflows: dtos };
    return json(200, response);
  };
}

export function getWorkflowDef(deps: WorkflowDefsDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const wf = deps.workflows?.[name];
    if (!wf) return jsonError(404, "not_found");
    return json(200, toDto(name, wf, deps.sampleInput?.(name)));
  };
}
