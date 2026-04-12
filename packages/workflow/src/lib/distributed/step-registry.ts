// ---------------------------------------------------------------------------
// StepRegistry — maps step names to Pipeline implementations on workers
//
// Workers register the steps they can execute. The coordinator dispatches
// steps by name; workers resolve them from their local registry.
// ---------------------------------------------------------------------------

import type { Pipeline, TaggedError } from "@promin/core";
import type { RetryPolicy } from "@promin/core";

export interface StepContext {
  readonly input: unknown;
  readonly prev: unknown;
  readonly deps: Record<string, unknown>;
  readonly workflowId: string;
  readonly stepName: string;
  readonly attempt: number;
}

export type StepHandler = (ctx: StepContext) => Pipeline<unknown, TaggedError> | Promise<unknown>;

export type StepFailureStrategy = "fail" | "skip" | { fallback: (error: unknown) => unknown };

export interface WorkerStepOptions {
  /** Retry policy for this step. */
  retry?: RetryPolicy<TaggedError>;
  /** What to do when the step fails (after retries). Default: "fail". */
  onFailure?: StepFailureStrategy;
  /** Compensation function — undo side effects during saga rollback. */
  compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Pipeline<void, any> | Promise<void>;
}

export interface StepRegistration {
  handler: StepHandler;
  options?: WorkerStepOptions;
}

export interface StepRegistry {
  register(stepName: string, handler: StepHandler, options?: WorkerStepOptions): void;
  resolve(stepName: string): StepRegistration | undefined;
  has(stepName: string): boolean;
  list(): string[];
}

export class MapStepRegistry implements StepRegistry {
  private readonly steps = new Map<string, StepRegistration>();

  register(stepName: string, handler: StepHandler, options?: WorkerStepOptions): void {
    this.steps.set(stepName, { handler, options });
  }

  resolve(stepName: string): StepRegistration | undefined {
    return this.steps.get(stepName);
  }

  has(stepName: string): boolean {
    return this.steps.has(stepName);
  }

  list(): string[] {
    return [...this.steps.keys()];
  }
}
