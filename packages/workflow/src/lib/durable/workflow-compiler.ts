// ---------------------------------------------------------------------------
// Workflow Compiler — WorkflowSchema JSON → WorkflowDefinition
//
// Validates the DAG, resolves activity references from the registry,
// builds the WorkflowBuilder chain programmatically, and returns a
// frozen WorkflowDefinition ready for .run() or trigger().
//
// The compiler is pure — no side effects. Execution is separate.
// ---------------------------------------------------------------------------

import { workflow } from "./workflow-builder.ts";
import type { Workflow } from "./workflow-types.ts";
import type {
  WorkflowSchema,
  StepSchema,
  SingleStepSchema,
  MapStepSchema,
  SleepStepSchema,
  SignalStepSchema,
  ApprovalStepSchema,
  BranchStepSchema,
  ParallelStepSchema,
} from "./workflow-schema.ts";
import type { ActivityRegistry, ActivityContext } from "./activity-registry.ts";
import { validateWorkflowSchema } from "./workflow-schema-validator.ts";
import { topologicalSort } from "./workflow-dag.ts";
import type { RetryPolicy } from "../shared/retry-policy.ts";

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
 * import { succeed } from "@spilne/perfect-core";
 * import { compileWorkflow, MapActivityRegistry } from "@promin/workflow";
 * import { PostgresWorkflowStorage, migrate } from "@promin/postgres";
 *
 * // 1. Set up storage + registry
 * const storage = new PostgresWorkflowStorage({ db });
 * const registry = new MapActivityRegistry({
 *   "http.get": (config) => () =>
 *     httpClient.get({ url: config?.url as string }),
 *   "transform.uppercase": () => (ctx) =>
 *     succeed(String(ctx.prev).toUpperCase()),
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
  /**
   * Version tag of the compiled workflow (`workflow({ version })`), needed to
   * register it in a `WorkflowVersionRegistry`. Unset by default.
   */
  version?: string;
  /** Workflow type recorded on its runs (`workflow({ type })`). */
  type?: string;
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
  let builder: any = workflow<Input>({
    name: schema.name,
    ...(params.version !== undefined && { version: params.version }),
    ...(params.type !== undefined && { type: params.type }),
  });

  for (const stepName of sorted) {
    const step = stepsByName.get(stepName)!;

    if (step.type === "step") {
      builder = compileSingleStep(builder, step, registry);
    } else if (step.type === "map") {
      builder = compileMapStep(builder, step, registry);
    } else if (step.type === "sleep") {
      builder = compileSleepStep(builder, step);
    } else if (step.type === "signal") {
      builder = compileSignalStep(builder, step);
    } else if (step.type === "approval") {
      builder = compileApprovalStep(builder, step);
    } else if (step.type === "branch") {
      builder = compileBranchStep(builder, step, registry);
    } else {
      builder = compileParallelStep(builder, step, registry);
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
  const activityFn = registry.resolve({ ref: step.activityRef, config: step.config });
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
  const activityFn = registry.resolve({ ref: step.activityRef, config: step.config });
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

/** Compile a SleepStepSchema into a DAG-form .sleep() call. */
function compileSleepStep(builder: any, step: SleepStepSchema): any {
  return builder.sleep(step.name, step.ms, { dependsOn: step.dependsOn });
}

/** Compile a SignalStepSchema into a .waitForSignal() call. */
function compileSignalStep(builder: any, step: SignalStepSchema): any {
  return builder.waitForSignal(step.name, {
    dependsOn: step.dependsOn,
    signalName: step.signalName,
    timeoutMs: step.timeoutMs,
  });
}

/** Compile an ApprovalStepSchema into an .approval() call. */
function compileApprovalStep(builder: any, step: ApprovalStepSchema): any {
  return builder.approval(step.name, {
    dependsOn: step.dependsOn,
    signalName: step.signalName,
    timeoutMs: step.timeoutMs,
  });
}

/**
 * Compile a BranchStepSchema into one DAG step that runs the condition
 * activity, then `ifTrue` or `ifFalse` with the same context.
 */
function compileBranchStep(builder: any, step: BranchStepSchema, registry: ActivityRegistry): any {
  const condition = registry.resolve({ ref: step.conditionRef });
  const ifTrue = registry.resolve({ ref: step.ifTrue.activityRef, config: step.ifTrue.config });
  const ifFalse = registry.resolve({ ref: step.ifFalse.activityRef, config: step.ifFalse.config });
  return builder.step(
    step.name,
    { dependsOn: step.dependsOn },
    (ctx: any) => {
      const activityCtx = toActivityContext(ctx);
      return condition(activityCtx).flatMap((passes) =>
        (passes === true ? ifTrue : ifFalse)(activityCtx),
      );
    },
    compileStepOptions(step.options),
  );
}

/** Compile a ParallelStepSchema into a DAG-form .parallel() call. */
function compileParallelStep(
  builder: any,
  step: ParallelStepSchema,
  registry: ActivityRegistry,
): any {
  const branches: Record<string, (ctx: any) => unknown> = {};
  for (const [label, branch] of Object.entries(step.branches)) {
    const activityFn = registry.resolve({ ref: branch.activityRef, config: branch.config });
    branches[label] = (ctx: any) => activityFn(toActivityContext(ctx));
  }
  return builder.parallel(
    step.name,
    { dependsOn: step.dependsOn },
    branches,
    compileStepOptions(step.options),
  );
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
  // For DAG steps with a single dep, use that value as prev (most natural
  // for activities); a DAG step with no deps gets the workflow input.
  const prev =
    ctx.prev !== undefined
      ? ctx.prev
      : depValues.length === 1
        ? depValues[0]
        : depValues.length === 0
          ? ctx.input
          : deps;
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
  if (step.type === "map") return [step.arrayFrom];
  return step.dependsOn;
}

/** The activity references a step uses, labelled for error messages. */
function stepActivityRefs(step: StepSchema): Array<{ label: string; ref: string }> {
  switch (step.type) {
    case "step":
    case "map":
      return [{ label: `Step "${step.name}"`, ref: step.activityRef }];
    case "branch":
      return [
        { label: `Branch step "${step.name}" condition`, ref: step.conditionRef },
        { label: `Branch step "${step.name}" ifTrue`, ref: step.ifTrue.activityRef },
        { label: `Branch step "${step.name}" ifFalse`, ref: step.ifFalse.activityRef },
      ];
    case "parallel":
      return Object.entries(step.branches).map(([branch, arm]) => ({
        label: `Parallel step "${step.name}" branch "${branch}"`,
        ref: arm.activityRef,
      }));
    default:
      return [];
  }
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
    for (const { label, ref } of stepActivityRefs(step)) {
      if (!registry.has(ref)) {
        issues.push(
          `${label} references unknown activity "${ref}". Available: ${registry.list().join(", ")}`,
        );
      }
    }
    if (step.type === "parallel" && Object.keys(step.branches).length === 0) {
      issues.push(`Parallel step "${step.name}" requires at least one branch`);
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

  const cycle = cycleSteps(schema);
  if (cycle.length > 0) {
    issues.push(`Dependency cycle between steps: ${cycle.map((name) => `"${name}"`).join(", ")}`);
  }

  return issues;
}

/**
 * Steps that sit on or downstream of a dependency cycle (empty when the
 * graph is acyclic). Dependencies on missing steps are ignored here; the
 * caller reports them separately.
 */
function cycleSteps(schema: WorkflowSchema): string[] {
  const names = new Set(schema.steps.map((s) => s.name));
  const remaining = new Map(
    schema.steps.map((s) => [s.name, stepDependsOn(s).filter((d) => names.has(d)).length]),
  );
  const dependents = new Map<string, string[]>();
  for (const step of schema.steps) {
    for (const dep of stepDependsOn(step)) {
      if (!names.has(dep)) continue;
      dependents.set(dep, [...(dependents.get(dep) ?? []), step.name]);
    }
  }
  const ready = [...remaining].filter(([, n]) => n === 0).map(([name]) => name);
  while (ready.length > 0) {
    const name = ready.pop()!;
    remaining.delete(name);
    for (const next of dependents.get(name) ?? []) {
      const n = remaining.get(next)! - 1;
      remaining.set(next, n);
      if (n === 0) ready.push(next);
    }
  }
  return [...remaining.keys()].sort();
}
