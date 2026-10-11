// ---------------------------------------------------------------------------
// Workflow input inference for builder-authored workflows, shared by the
// server (sample input on the Workflows page's trigger form) and the
// dashboard's builder (the typed Input node and the test-run input).
//
// A workflow's input type is its declared `inputSchema`; without one it is
// what its first root step reads — the `inputSchema` of that step's activity
// in the catalog. Pure: type imports only, no DOM and no Node built-ins.
// ---------------------------------------------------------------------------

import type { JsonSchema, StepSchema, WorkflowSchema } from "@promin/workflow";

/** Looks an activity's declared input up by its `activityRef`. */
export type ActivityInputLookup = (
  activityRef: string,
) => { readonly inputSchema?: JsonSchema } | undefined;

/** Where a workflow's input type comes from. */
export type WorkflowInputSource =
  | { readonly kind: "workflow" }
  | { readonly kind: "step"; readonly step: string; readonly activityRef: string };

export interface WorkflowInputType {
  readonly schema: JsonSchema;
  readonly source: WorkflowInputSource;
}

/**
 * The workflow's input type: the declared `inputSchema`, else the input of
 * the first root step whose activity declares one, else `undefined`.
 */
export function workflowInputType(params: {
  readonly schema: WorkflowSchema;
  readonly activity: ActivityInputLookup;
}): WorkflowInputType | undefined {
  const { schema, activity } = params;
  if (schema.inputSchema) return { schema: schema.inputSchema, source: { kind: "workflow" } };
  for (const step of schema.steps) {
    if (!isRootStep(step)) continue;
    for (const ref of inputActivityRefs(step)) {
      const inputSchema = activity(ref)?.inputSchema;
      if (inputSchema) {
        return { schema: inputSchema, source: { kind: "step", step: step.name, activityRef: ref } };
      }
    }
  }
  return undefined;
}

/** A plausible input for the workflow (see `workflowInputType`), or `undefined`. */
export function sampleWorkflowInput(params: {
  readonly schema: WorkflowSchema;
  readonly activity: ActivityInputLookup;
}): unknown {
  const type = workflowInputType(params);
  return type ? sampleFromJsonSchema(type.schema) : undefined;
}

/**
 * A value that matches `schema`: its `default`, else its first `enum`
 * value, else a placeholder of its type (objects recurse into their
 * properties). `undefined` for no schema or an untyped one.
 */
export function sampleFromJsonSchema(schema: JsonSchema | undefined, depth = 0): unknown {
  if (!schema || depth > 8) return undefined;
  if (schema.default !== undefined) return structuredClone(schema.default);
  if (schema.enum && schema.enum.length > 0) return structuredClone(schema.enum[0]);
  switch (schema.type) {
    case "string":
      return "hello workflow";
    case "number":
    case "integer":
      return 1;
    case "boolean":
      return true;
    case "null":
      return null;
    case "array": {
      const item = sampleFromJsonSchema(schema.items, depth + 1);
      return item === undefined ? [] : [item];
    }
    case "object": {
      const out: Record<string, unknown> = {};
      for (const [key, property] of Object.entries(schema.properties ?? {})) {
        const value = sampleFromJsonSchema(property, depth + 1);
        if (value !== undefined) out[key] = value;
      }
      return out;
    }
    default:
      return undefined;
  }
}

/** Steps with no dependencies: they read the workflow input as `prev`. */
export function isRootStep(step: StepSchema): boolean {
  return step.type !== "map" && step.dependsOn.length === 0;
}

/** The activities that read a root step's input, in the order they matter. */
function inputActivityRefs(step: StepSchema): string[] {
  switch (step.type) {
    case "step":
      return [step.activityRef];
    case "branch":
      return [step.conditionRef, step.ifTrue.activityRef, step.ifFalse.activityRef];
    case "parallel":
      return Object.values(step.branches).map((branch) => branch.activityRef);
    default:
      return [];
  }
}
