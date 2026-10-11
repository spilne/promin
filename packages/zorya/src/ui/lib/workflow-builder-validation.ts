// ---------------------------------------------------------------------------
// workflow-builder-validation — the builder's live checks of a draft, run on
// every edit so problems show on their nodes before a save.
//
// Errors mirror what the server refuses on save (it validates with the
// workflow package's schema validator and compiles against the activity
// catalog): a missing or invalid name, duplicate step names, unknown
// activities, missing dependencies, cycles, empty fields. Warnings flag
// wiring that compiles but likely misbehaves — a workflow input whose type
// differs from what its root step reads, or a branch condition that does not
// return a boolean. The server stays the authority; this is for feedback.
// ---------------------------------------------------------------------------

import type { ActivityDescriptor, JsonSchema, WorkflowSchema } from "@promin/workflow";
import { isRootStep } from "../../shared/workflow-input.ts";
import {
  cycleSteps,
  isPredicate,
  stepActivityRefs,
  stepDependencies,
} from "./workflow-builder-graph.ts";

/** Same rule as the server's builder workflow names. */
export const WORKFLOW_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export interface DraftIssue {
  /** The step the issue is about; absent for workflow-level issues. */
  readonly step?: string;
  /** Set for the Input node (the workflow's input schema). */
  readonly input?: true;
  readonly severity: "error" | "warning";
  readonly message: string;
}

export function validateDraft(params: {
  readonly schema: WorkflowSchema;
  readonly activities: ReadonlyMap<string, ActivityDescriptor>;
}): DraftIssue[] {
  const { schema, activities } = params;
  const issues: DraftIssue[] = [];
  const error = (message: string, step?: string) =>
    issues.push({ severity: "error", message, ...(step !== undefined && { step }) });
  const warn = (message: string, step?: string) =>
    issues.push({ severity: "warning", message, ...(step !== undefined && { step }) });

  if (!WORKFLOW_NAME_PATTERN.test(schema.name)) {
    error(
      "Workflow name: 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit.",
    );
  }
  if (schema.steps.length === 0) error("Add at least one step.");

  const names = new Set<string>();
  for (const step of schema.steps) {
    if (!step.name.trim()) error("A step has an empty name.");
    else if (names.has(step.name)) error(`Two steps are named "${step.name}".`, step.name);
    names.add(step.name);
  }

  for (const step of schema.steps) {
    for (const { role, ref } of stepActivityRefs(step)) {
      const what = role === "activity" ? "activity" : `${role} activity`;
      if (!ref) error(`Pick the ${what}.`, step.name);
      else if (!activities.has(ref)) error(`Unknown ${what} "${ref}".`, step.name);
    }
    for (const dep of stepDependencies(step)) {
      if (!names.has(dep)) error(`Depends on "${dep}", which does not exist.`, step.name);
    }
    switch (step.type) {
      case "map":
        if (!step.arrayFrom) error("Connect the step whose array output it maps over.", step.name);
        break;
      case "signal":
        if (!step.signalName.trim()) error("Name the signal it waits for.", step.name);
        break;
      case "parallel":
        if (Object.keys(step.branches).length === 0) error("Add at least one branch.", step.name);
        break;
      case "branch": {
        const condition = activities.get(step.conditionRef);
        if (condition && !isPredicate(condition)) {
          warn(`Condition "${step.conditionRef}" does not declare a boolean output.`, step.name);
        }
        break;
      }
      default:
        break;
    }
  }

  const cycle = cycleSteps(schema);
  if (cycle.length > 0) {
    for (const name of cycle) error("Part of a dependency cycle.", name);
  }

  if (schema.inputSchema) {
    for (const step of schema.steps) {
      if (!isRootStep(step)) continue;
      const ref = stepActivityRefs(step)[0]?.ref;
      const expected = ref ? activities.get(ref)?.inputSchema : undefined;
      if (expected && !typesCompatible({ actual: schema.inputSchema, expected })) {
        issues.push({
          severity: "warning",
          input: true,
          step: step.name,
          message: `The workflow input is ${schemaTypeLabel(schema.inputSchema)} but "${step.name}" reads ${schemaTypeLabel(expected)}.`,
        });
      }
    }
  }
  return issues;
}

/** True when nothing in `issues` blocks a save. */
export function canSave(issues: ReadonlyArray<DraftIssue>): boolean {
  return issues.every((i) => i.severity !== "error");
}

/** Loose JSON Schema type check: untyped schemas match anything. */
export function typesCompatible(params: { actual: JsonSchema; expected: JsonSchema }): boolean {
  const { actual, expected } = params;
  if (!actual.type || !expected.type) return true;
  if (actual.type === expected.type) return true;
  return actual.type === "integer" && expected.type === "number";
}

/** A short label for a schema's type, e.g. `object { text, urgent }`. */
export function schemaTypeLabel(schema: JsonSchema | undefined): string {
  if (!schema) return "any value";
  if (schema.enum && schema.enum.length > 0) {
    return `one of ${schema.enum.map((v) => JSON.stringify(v)).join(", ")}`;
  }
  if (schema.type === "object" && schema.properties) {
    const keys = Object.keys(schema.properties);
    return keys.length > 0 ? `object { ${keys.join(", ")} }` : "object";
  }
  if (schema.type === "array") {
    return schema.items?.type ? `array of ${String(schema.items.type)}` : "array";
  }
  return typeof schema.type === "string" ? schema.type : "any value";
}
