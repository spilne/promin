// ---------------------------------------------------------------------------
// Workflow definitions — expose registered workflows as DTOs so the UI can
// render a "Workflows" directory page and synthesise trigger forms from
// sample inputs.
// ---------------------------------------------------------------------------

import type { Workflow } from "@promin/workflow";
import { json, jsonError } from "../router.ts";
import type { WorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";

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
  /**
   * Remote advertisements from connected workers. Merged into the response
   * alongside any statically-configured workflows. When both a static and
   * an advertised entry exist for the same name, the static one wins.
   */
  advertisements?: WorkflowAdvertisementRegistry;
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
    const byName = new Map<string, WorkflowDefDto>();
    for (const [name, wf] of Object.entries(deps.workflows ?? {})) {
      byName.set(name, toDto(name, wf, deps.sampleInput?.(name)));
    }
    // Merge in remote advertisements — static wins on conflict (embedded
    // workflow definitions are authoritative over worker hearsay).
    if (deps.advertisements) {
      const distinct = await deps.advertisements.distinct();
      for (const adv of distinct) {
        if (byName.has(adv.name)) continue;
        byName.set(adv.name, {
          name: adv.name,
          version: adv.version,
          steps: adv.steps.map((s) => ({
            name: s.name,
            kind: s.kind,
            dependsOn: [...s.dependsOn],
          })),
          sampleInput: adv.sampleInput,
        });
      }
    }
    const dtos = [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
    const response: WorkflowDefsResponse = { workflows: dtos };
    return json(200, response);
  };
}

export function getWorkflowDef(deps: WorkflowDefsDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const wf = deps.workflows?.[name];
    if (wf) return json(200, toDto(name, wf, deps.sampleInput?.(name)));

    // Fall back to remote advertisements.
    if (deps.advertisements) {
      const distinct = await deps.advertisements.distinct();
      const adv = distinct.find((a) => a.name === name);
      if (adv) {
        return json(200, {
          name: adv.name,
          version: adv.version,
          steps: adv.steps.map((s) => ({
            name: s.name,
            kind: s.kind,
            dependsOn: [...s.dependsOn],
          })),
          sampleInput: adv.sampleInput,
        });
      }
    }
    return jsonError(404, "not_found");
  };
}
