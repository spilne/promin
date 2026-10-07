// ---------------------------------------------------------------------------
// StepRegistry — maps step names to step handlers on workers
//
// Workers register the steps they can execute. The coordinator dispatches
// steps by name; workers resolve them from their local registry.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../shared/tagged-error.ts";
import type { RetryPolicy } from "../shared/retry-policy.ts";

export interface StepContext {
  readonly input: unknown;
  readonly prev: unknown;
  readonly deps: Record<string, unknown>;
  readonly workflowId: string;
  readonly stepName: string;
  readonly attempt: number;
}

/**
 * A worker step body. Return an `Eff` (typed failures are what step retry
 * sees) or a Promise.
 */
export type StepHandler = (
  ctx: StepContext,
) => Eff<unknown, Throws<TaggedError>> | Promise<unknown>;

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
  }) => Eff<unknown, Throws<unknown>> | Promise<void>;
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
