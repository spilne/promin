// ---------------------------------------------------------------------------
// Bound workflows — a `Workflow` paired with the runner that drives it, so
// call sites pass only the run's id and input, typed by the workflow.
// ---------------------------------------------------------------------------

import type { TaggedError } from "../shared/tagged-error.ts";
import {
  createWorkflowRunner,
  type WorkflowRunner,
  type WorkflowRunnerConfig,
  type WorkflowRunParamsFor,
  type WorkflowRunSafeResult,
} from "./workflow-runner.ts";
import type { Workflow, WorkflowHandle } from "./workflow-types.ts";

/**
 * What a bound workflow's `run` / `runSafe` / `start` take: everything
 * `WorkflowRunner.run` takes except the workflow, with `input` typed by it.
 */
export type BoundWorkflowRunParams<Input> = Omit<
  WorkflowRunParamsFor<Workflow<Input, unknown>>,
  "workflow" | "input"
> & { readonly input: Input };

/** A `Workflow` bound to a `WorkflowRunner` (see `bindWorkflow`). */
export interface BoundWorkflow<Input, Output, E extends TaggedError = never> {
  readonly workflow: Workflow<Input, Output, E>;
  readonly runner: WorkflowRunner;
  /** `runner.run({ workflow, ...params })`. */
  run(params: BoundWorkflowRunParams<Input>): Promise<Output>;
  /** `runner.runSafe({ workflow, ...params })`. */
  runSafe(params: BoundWorkflowRunParams<Input>): Promise<WorkflowRunSafeResult<Output, E>>;
  /** `runner.start({ workflow, ...params })`. */
  start(params: BoundWorkflowRunParams<Input>): Promise<WorkflowHandle<Output, E>>;
  /** `runner.resume({ workflow, ...params })`. */
  resume(params: { readonly workflowId: string; readonly fromStep: string }): Promise<Output>;
  /** `runner.handle(workflowId)`, typed by the workflow's output. */
  handle(workflowId: string): WorkflowHandle<Output>;
}

/**
 * Bind `workflow` to `runner`, so callers run it without repeating the
 * definition and get its input and output types on every call.
 *
 * ```typescript
 * const onboard = bindWorkflow({ runner, workflow: onboardUser });
 * const user = await onboard.run({ workflowId: "onboard-1", input: { name: "Ada" } });
 * ```
 */
export function bindWorkflow<Input, Output, E extends TaggedError = never>(params: {
  readonly runner: WorkflowRunner;
  readonly workflow: Workflow<Input, Output, E>;
}): BoundWorkflow<Input, Output, E> {
  const { runner, workflow } = params;
  return {
    workflow,
    runner,
    run: (run) => runner.run<Output, E>({ ...run, workflow }),
    runSafe: (run) => runner.runSafe<Output, E>({ ...run, workflow }),
    start: (run) => runner.start<Input, Output, E>({ ...run, workflow }),
    resume: (resume) => runner.resume<Input, Output>({ ...resume, workflow }),
    handle: (workflowId) => runner.handle<Output>(workflowId),
  };
}

/** A runner plus `bindWorkflow` against it (see `createWorkflowApp`). */
export interface WorkflowApp {
  readonly runner: WorkflowRunner;
  /** Bind `workflow` to this app's runner. */
  workflow<Input, Output, E extends TaggedError = never>(
    workflow: Workflow<Input, Output, E>,
  ): BoundWorkflow<Input, Output, E>;
}

/**
 * Create a `WorkflowApp`: a runner (built from `config`, or the one given
 * as `{ runner }`) and a `workflow()` that binds definitions to it.
 *
 * ```typescript
 * const app = createWorkflowApp({ storage });
 * const onboard = app.workflow(onboardUser);
 * await onboard.run({ workflowId: "onboard-1", input: { name: "Ada" } });
 * ```
 */
export function createWorkflowApp(
  config: WorkflowRunnerConfig | { readonly runner: WorkflowRunner },
): WorkflowApp {
  const runner = "runner" in config ? config.runner : createWorkflowRunner(config);
  return {
    runner,
    workflow: (workflow) => bindWorkflow({ runner, workflow }),
  };
}
