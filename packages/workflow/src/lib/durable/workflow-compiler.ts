// ---------------------------------------------------------------------------
// Workflow Compiler — WorkflowSchema JSON → WorkflowDefinition
//
// Validates the DAG, resolves activity references from the registry,
// builds the WorkflowBuilder chain programmatically, and returns a
// frozen WorkflowDefinition ready for .run() or trigger().
//
// The compiler is pure — no side effects. Execution is separate.
// ---------------------------------------------------------------------------

import { workflow } from "./durable-pipeline.ts";
import type { Workflow } from "./durable-pipeline.ts";
import type {
  WorkflowSchema,
  StepSchema,
  SingleStepSchema,
  MapStepSchema,
} from "./workflow-schema.ts";
import type { ActivityRegistry, ActivityContext } from "./activity-registry.ts";
import { validateWorkflowSchema } from "./workflow-schema-validator.ts";
import { topologicalSort } from "./workflow-dag.ts";
import type { RetryPolicy } from "@promin/core";

// ---------------------------------------------------------------------------
// Compiler errors
// ---------------------------------------------------------------------------

/**
 * Thrown when `compileWorkflow()` fails validation.
 *
 * Contains all issues found during compilation (missing activity refs,
 * broken dependencies, duplicate step names, DAG cycles).
 *
 * @example
 * ```ts
 * try {
 *   compileWorkflow({ schema, storage, registry });
 * } catch (e) {
 *   if (e instanceof WorkflowCompilationError) {
 *     console.error("Compilation issues:", e.issues);
 *     // e.issues: [
 *     //   'Step "fetch" references unknown activity "http.get"',
 *     //   'Step "process" depends on "missing" which does not exist',
 *     // ]
 *   }
 * }
 * ```
 */
export class WorkflowCompilationError extends Error {
  readonly issues: string[];
  constructor(issues: string[]) {
    super(`Workflow compilation failed:\n${issues.map((i) => `  - ${i}`).join("\n")}`);
    this.name = "WorkflowCompilationError";
    this.issues = issues;
  }
}

// ---------------------------------------------------------------------------
// compileWorkflow
// ---------------------------------------------------------------------------

/**
 * Compile a JSON `WorkflowSchema` into an executable `WorkflowDefinition`.
 *
 * The compiler:
 * 1. Validates the schema structure (Zod)
 * 2. Validates DAG integrity (cycles, missing deps, undefined activity refs)
 * 3. Topologically sorts steps
 * 4. Builds the `WorkflowBuilder` chain programmatically
 * 5. Returns a frozen `WorkflowDefinition` ready for `.run()` or `trigger()`
 *
 * The compiler is pure — no side effects. Execution is separate.
 *
 * @example
 * ```ts
 * import { compileWorkflow, MapActivityRegistry, Pipeline } from "@promin/core";
 * import { PostgresWorkflowStorage, migrate } from "@promin/postgres";
 *
 * // 1. Set up storage + registry
 * const storage = new PostgresWorkflowStorage({ db });
 * const registry = new MapActivityRegistry({
 *   "http.get": (config) => () =>
 *     httpClient.get({ url: config?.url as string }),
 *   "transform.uppercase": () => (ctx) =>
 *     Pipeline.succeed(String(ctx.prev).toUpperCase()),
 * });
 *
 * // 2. Compile schema from UI/API
 * const definition = compileWorkflow({
 *   schema: {
 *     version: 1,
 *     name: "fetch-and-transform",
 *     steps: [
 *       { type: "step", name: "fetch", dependsOn: [],
 *         activityRef: "http.get", config: { url: "https://api.example.com" } },
 *       { type: "step", name: "transform", dependsOn: ["fetch"],
 *         activityRef: "transform.uppercase" },
 *     ],
 *   },
 *   registry,
 * });
 *
 * // 3. Execute
 * const runner = createWorkflowRunner({ storage });
 * const result = await runner.run({ workflow: definition, workflowId: "wf-1", input: {} });
 *
 * // 4. Or wire to a stream trigger
 * eventStream.through(trigger({
 *   workflow: definition,
 *   runner,
 *   storage,
 *   toInput: (event) => event.payload,
 *   toWorkflowId: (event) => `wf-${event.id}`,
 * }));
 * ```
 *
 * @throws {WorkflowCompilationError} When the schema has validation issues
 * @throws {ZodError} When the schema structure is invalid
 */
export function compileWorkflow<Input = unknown>(params: {
  schema: WorkflowSchema;
  registry: ActivityRegistry;
}): Workflow<Input, unknown> {
  const { schema, registry } = params;

  // 1. Validate schema structure
  validateWorkflowSchema(schema);

  // 2. Validate DAG integrity + activity refs
  const issues = validateDag(schema, registry);
  if (issues.length > 0) {
    throw new WorkflowCompilationError(issues);
  }

  // 3. Topological sort
  const stepsByName = new Map(schema.steps.map((s) => [s.name, s]));
  const dagNodes = schema.steps.map((s) => ({
    name: s.name,
    dependsOn: stepDependsOn(s),
  }));
  const sorted = topologicalSort({
    nodes: dagNodes,
    workflowId: `compile:${schema.name}`,
  });

  // 4. Build WorkflowBuilder chain
  // Use `any` for the builder — generics can't be tracked across a dynamic loop
  let builder: any = workflow<Input>({ name: schema.name });

  for (const stepName of sorted) {
    const step = stepsByName.get(stepName)!;

    if (step.type === "step") {
      builder = compileSingleStep(builder, step, registry);
    } else if (step.type === "map") {
      builder = compileMapStep(builder, step, registry);
    }
  }

  // 5. Freeze into a pure `Workflow`. The caller drives it via a
  // `createWorkflowRunner({ storage }).run({ workflow: compiled, ... })`
  // (or wires it into a `trigger`/`webhookTrigger` with their own runner).
  return builder.build() as Workflow<Input, unknown>;
}

// ---------------------------------------------------------------------------
// Step compilers
// ---------------------------------------------------------------------------

/** Compile a SingleStepSchema into a .step() call on the builder. */
function compileSingleStep(builder: any, step: SingleStepSchema, registry: ActivityRegistry): any {
  const activityFn = registry.resolve(step.activityRef, step.config);
  const options = compileStepOptions(step.options);

  if (step.dependsOn.length === 0) {
    // Root step — linear mode (receives workflow input as prev)
    return builder.step(step.name, (ctx: any) => activityFn(toActivityContext(ctx)), options);
  }

  // DAG step — dependsOn mode
  return builder.step(
    step.name,
    { dependsOn: step.dependsOn },
    (ctx: any) => activityFn(toActivityContext(ctx)),
    options,
  );
}

/** Compile a MapStepSchema into a .mapOver() call on the builder. */
function compileMapStep(builder: any, step: MapStepSchema, registry: ActivityRegistry): any {
  const activityFn = registry.resolve(step.activityRef, step.config);
  const options = compileStepOptions(step.options);

  return builder.mapOver(
    step.name,
    {
      array: step.arrayFrom,
      concurrency: step.options?.concurrency,
    },
    (element: unknown, ctx: any) =>
      activityFn({
        input: ctx.input,
        prev: element,
        deps: {},
        workflowId: ctx.workflowId,
        attempt: ctx.attempt,
      }),
    options,
  ) as any;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Convert a WorkflowBuilder step context into an ActivityContext.
 *
 * Normalizes the difference between linear contexts (has `prev`) and
 * DAG contexts (has `deps`) so activities get a consistent interface.
 */
function toActivityContext(ctx: any): ActivityContext {
  // Linear steps have ctx.prev, DAG steps have ctx.deps
  const deps: Record<string, unknown> = ctx.deps ?? {};
  const depValues = Object.values(deps);
  // For DAG steps with a single dep, use that value as prev (most natural for activities)
  const prev = ctx.prev !== undefined ? ctx.prev : depValues.length === 1 ? depValues[0] : deps;
  return {
    input: ctx.input,
    prev,
    deps,
    workflowId: ctx.workflowId,
    attempt: ctx.attempt,
  };
}

/** Convert serializable step options to runtime StepOptions. */
function compileStepOptions(options?: {
  retry?: { maxRetries?: number; baseDelayMs?: number };
  timeoutMs?: number;
}) {
  if (!options) return undefined;
  return {
    timeoutMs: options.timeoutMs,
    retry: options.retry
      ? ({
          maxRetries: options.retry.maxRetries ?? 3,
          baseDelayMs: options.retry.baseDelayMs ?? 1000,
        } satisfies RetryPolicy<any>)
      : undefined,
  };
}

/** Extract dependency list from any step type. */
function stepDependsOn(step: StepSchema): string[] {
  if (step.type === "step") return step.dependsOn;
  if (step.type === "map") return [step.arrayFrom];
  return [];
}

// ---------------------------------------------------------------------------
// DAG validation
// ---------------------------------------------------------------------------

/**
 * Validate DAG integrity: duplicate names, missing deps, unknown activity refs.
 * Returns a list of human-readable issue strings (empty = valid).
 */
function validateDag(schema: WorkflowSchema, registry: ActivityRegistry): string[] {
  const issues: string[] = [];
  const stepNames = new Set(schema.steps.map((s) => s.name));

  // Check for duplicate step names
  const seen = new Set<string>();
  for (const step of schema.steps) {
    if (seen.has(step.name)) {
      issues.push(`Duplicate step name: "${step.name}"`);
    }
    seen.add(step.name);
  }

  for (const step of schema.steps) {
    // Check activity refs exist in registry
    if (!registry.has(step.activityRef)) {
      issues.push(
        `Step "${step.name}" references unknown activity "${step.activityRef}". Available: ${registry.list().join(", ")}`,
      );
    }

    // Check dependencies reference existing steps
    const deps = stepDependsOn(step);
    for (const dep of deps) {
      if (!stepNames.has(dep)) {
        issues.push(`Step "${step.name}" depends on "${dep}" which does not exist`);
      }
    }

    // Check map steps reference existing array source
    if (step.type === "map" && !stepNames.has(step.arrayFrom)) {
      issues.push(
        `Map step "${step.name}" references arrayFrom "${step.arrayFrom}" which does not exist`,
      );
    }
  }

  return issues;
}
