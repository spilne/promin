// ---------------------------------------------------------------------------
// workflow-builder-input — what the builder shows about a workflow's input
// and an activity's config: the typed Input node's label and schema
// template, the test-run sample, and the form fields of a config schema.
// Inference itself is shared with the server (`shared/workflow-input.ts`),
// so the trigger form on the Workflows page samples the same way.
// ---------------------------------------------------------------------------

import type { ActivityDescriptor, JsonSchema, WorkflowSchema } from "@promin/workflow";
import { sampleWorkflowInput, workflowInputType } from "../../shared/workflow-input.ts";
import { schemaTypeLabel } from "./workflow-builder-validation.ts";

/** The Input node's template when nothing in the workflow suggests a type. */
export const DEFAULT_INPUT_SCHEMA_TEMPLATE: JsonSchema = {
  type: "object",
  properties: { text: { type: "string" } },
  required: ["text"],
};

type Activities = ReadonlyMap<string, ActivityDescriptor>;

/** One line for the Input node: the input's type and where it comes from. */
export function describeWorkflowInput(params: {
  readonly schema: WorkflowSchema;
  readonly activities: Activities;
}): string {
  const type = workflowInputType({
    schema: params.schema,
    activity: (ref) => params.activities.get(ref),
  });
  if (!type) return "any value";
  const label = schemaTypeLabel(type.schema);
  return type.source.kind === "workflow" ? label : `${label} · read by ${type.source.step}`;
}

/**
 * The schema the Input node's editor starts from: what the first root
 * step's activity reads, or a generic object with a `text` field.
 */
export function inputSchemaTemplate(params: {
  readonly schema: WorkflowSchema;
  readonly activities: Activities;
}): JsonSchema {
  const { inputSchema: _declared, ...undeclared } = params.schema;
  const inferred = workflowInputType({
    schema: undeclared,
    activity: (ref) => params.activities.get(ref),
  });
  return inferred?.schema ?? DEFAULT_INPUT_SCHEMA_TEMPLATE;
}

/** A test-run input for the draft: sampled from its input type, else `{}`. */
export function suggestedTestInput(params: {
  readonly schema: WorkflowSchema;
  readonly activities: Activities;
}): unknown {
  return (
    sampleWorkflowInput({
      schema: params.schema,
      activity: (ref) => params.activities.get(ref),
    }) ?? {}
  );
}

/** A config property the inspector renders as a form control. */
export interface ConfigField {
  readonly name: string;
  readonly schema: JsonSchema;
  readonly required: boolean;
  readonly control: "select" | "toggle" | "number" | "text";
}

/**
 * Form fields for an object config schema's scalar properties. Other
 * properties (objects, arrays) are edited in the config JSON.
 */
export function configFields(schema: JsonSchema | undefined): ConfigField[] {
  if (!schema || schema.type !== "object" || !schema.properties) return [];
  const required = new Set(schema.required ?? []);
  const out: ConfigField[] = [];
  for (const [name, property] of Object.entries(schema.properties)) {
    const control =
      property.enum && property.enum.length > 0
        ? "select"
        : property.type === "boolean"
          ? "toggle"
          : property.type === "number" || property.type === "integer"
            ? "number"
            : property.type === "string"
              ? "text"
              : undefined;
    if (control) out.push({ name, schema: property, required: required.has(name), control });
  }
  return out;
}

/** Parse JSON typed by the user; `{ error }` names what is wrong. */
export function parseJsonInput(
  text: string,
): { readonly ok: true; readonly value: unknown } | { readonly ok: false; readonly error: string } {
  if (!text.trim()) return { ok: true, value: undefined };
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  }
}
