// ---------------------------------------------------------------------------
// WorkflowSchema Zod validator — meta-schema for validating JSON from UI/API
// ---------------------------------------------------------------------------

import { z } from "zod";

// ---------------------------------------------------------------------------
// JSON Schema (loose — we validate structure, not full JSON Schema spec)
// ---------------------------------------------------------------------------

const JsonSchemaZ: z.ZodType<Record<string, unknown>> = z.record(z.unknown());

// ---------------------------------------------------------------------------
// Step options
// ---------------------------------------------------------------------------

const StepSchemaOptionsZ = z.object({
  retry: z
    .object({
      maxRetries: z.number().int().min(0).optional(),
      baseDelayMs: z.number().int().min(0).optional(),
    })
    .optional(),
  timeoutMs: z.number().int().min(0).optional(),
});

const MapStepSchemaOptionsZ = StepSchemaOptionsZ.extend({
  concurrency: z.number().int().min(1).optional(),
});

// ---------------------------------------------------------------------------
// Step schemas
// ---------------------------------------------------------------------------

const SingleStepSchemaZ = z.object({
  type: z.literal("step"),
  name: z.string().min(1),
  dependsOn: z.array(z.string()),
  activityRef: z.string().min(1),
  config: z.record(z.unknown()).optional(),
  options: StepSchemaOptionsZ.optional(),
  outputSchema: JsonSchemaZ.optional(),
});

const MapStepSchemaZ = z.object({
  type: z.literal("map"),
  name: z.string().min(1),
  arrayFrom: z.string().min(1),
  activityRef: z.string().min(1),
  config: z.record(z.unknown()).optional(),
  options: MapStepSchemaOptionsZ.optional(),
  outputSchema: JsonSchemaZ.optional(),
});

const StepSchemaZ = z.discriminatedUnion("type", [SingleStepSchemaZ, MapStepSchemaZ]);

// ---------------------------------------------------------------------------
// Node UI metadata
// ---------------------------------------------------------------------------

const NodeUiMetaZ = z.object({
  x: z.number(),
  y: z.number(),
  width: z.number().optional(),
  color: z.string().optional(),
  label: z.string().optional(),
});

// ---------------------------------------------------------------------------
// WorkflowSchema — the top-level schema
// ---------------------------------------------------------------------------

/**
 * Zod schema for validating `WorkflowSchema` JSON from untrusted sources (UI, API).
 *
 * Validates structure, types, and constraints (non-empty step names, at least one step).
 * Does **not** validate DAG integrity (cycles, missing deps) — that's done by `compileWorkflow()`.
 *
 * @example
 * ```ts
 * // Validate incoming JSON from an API request
 * const parsed = WorkflowSchemaZ.parse(req.body);
 *
 * // Or use safeParse for error handling
 * const result = WorkflowSchemaZ.safeParse(req.body);
 * if (!result.success) {
 *   return res.status(400).json({ errors: result.error.issues });
 * }
 * ```
 */
export const WorkflowSchemaZ = z.object({
  version: z.literal(1),
  name: z.string().min(1),
  inputSchema: JsonSchemaZ.optional(),
  steps: z.array(StepSchemaZ).min(1, "Workflow must have at least one step"),
  ui: z.record(NodeUiMetaZ).optional(),
});

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

export type WorkflowSchemaValidationError = z.ZodError;

/**
 * Validate a `WorkflowSchema` JSON object. Throws `ZodError` on failure.
 *
 * Use this at API boundaries to validate untrusted input before passing
 * it to `compileWorkflow()`.
 *
 * @example
 * ```ts
 * import { validateWorkflowSchema } from "@ts-backend/core";
 *
 * // In an API route handler
 * try {
 *   const schema = validateWorkflowSchema(req.body);
 *   const definition = compileWorkflow({ schema, storage, registry });
 *   await definition.run({ workflowId: "wf-1", input: req.query });
 * } catch (e) {
 *   if (e instanceof ZodError) {
 *     return res.status(400).json({ errors: e.issues });
 *   }
 *   throw e;
 * }
 * ```
 */
export function validateWorkflowSchema(input: unknown) {
  return WorkflowSchemaZ.parse(input);
}

/**
 * Validate a `WorkflowSchema` JSON object without throwing.
 *
 * Returns `{ success: true, data }` on success or `{ success: false, error }` on failure.
 *
 * @example
 * ```ts
 * import { validateWorkflowSchemaSafe } from "@ts-backend/core";
 *
 * const result = validateWorkflowSchemaSafe(jsonFromUi);
 * if (!result.success) {
 *   console.error("Invalid schema:", result.error.issues);
 *   return;
 * }
 * const definition = compileWorkflow({ schema: result.data, storage, registry });
 * ```
 */
export function validateWorkflowSchemaSafe(input: unknown) {
  return WorkflowSchemaZ.safeParse(input);
}
