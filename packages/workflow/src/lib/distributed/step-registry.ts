// ---------------------------------------------------------------------------
// StepRegistry — maps step names to step handlers on workers
//
// Workers register the steps they can execute. The coordinator dispatches
// steps by name; workers resolve them from their local registry.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../shared/tagged-error.ts";

/**
 * What a worker step handler receives: the values the inline runner hands
 * the same step's body.
 */
export interface WorkerStepContext {
  /** The workflow input. */
  readonly input: unknown;
  /**
   * The inline `prev`: the result of the step's first declared dependency,
   * or the workflow input for a step with no dependencies.
   */
  readonly prev: unknown;
  /**
   * The inline `deps`: results of the step's declared dependencies only,
   * by step name (`{}` for a step with no dependencies).
   */
  readonly deps: Readonly<Record<string, unknown>>;
  readonly workflowId: string;
  readonly stepName: string;
  /** The runner's attempt number for this step (the inline `ctx.attempt`). */
  readonly attempt: number;
  /**
   * Aborted when this attempt is over for this worker: the step's
   * `timeoutMs` elapsed (`StepTimeoutError`), its claim was lost (a
   * heartbeat found the task reclaimed by another worker after this one
   * stalled), or the worker is stopping and gave the task back. Anything the
   * handler does after that is discarded, so long handlers should pass the
   * signal on (fetch, child processes) or check it.
   */
  readonly signal: AbortSignal;
  /**
   * Progress details to resume from: the last details a previous delivery of
   * this task passed to `heartbeat(details)` before its worker crashed or lost
   * its lease, or what this attempt has passed itself. Undefined when none
   * was ever sent (a first delivery). JSON-compatible values only.
   */
  readonly lastHeartbeatDetails: unknown;
  /**
   * Record progress `details` with the task's lease. The latest value is sent
   * with the worker's next lease heartbeat (every `heartbeatIntervalMs`), so
   * calling this often is cheap and a crash loses at most that interval of
   * progress. A later delivery of the task (after a lost lease or a crash)
   * reads it as `lastHeartbeatDetails`; a retry of the step after a failure
   * is a new task and starts without it. With a short-lived step the details
   * may never be sent.
   */
  heartbeat(details: unknown): void;
}

/**
 * A worker step body. Return an `Eff` or a Promise. Any failure — an `Eff`
 * failure, a throw or a rejection — fails the attempt; the coordinator then
 * applies the definition's `retry` and `onFailure` to it.
 */
export type StepHandler = (
  ctx: WorkerStepContext,
) => Eff<unknown, Throws<TaggedError>> | Promise<unknown>;

/**
 * A registered step. Retry, timeout and `onFailure` are not registration
 * options: they come from the step's definition (`StepOptions`) and behave
 * as they do inline — the coordinator retries and applies `onFailure`
 * across attempts, the worker enforces `timeoutMs` on each attempt.
 * Worker-local behaviour inside one attempt belongs in `WorkerMiddleware`.
 */
export interface StepRegistration {
  handler: StepHandler;
}

/** What `StepRegistry.register` takes: the step name and its handler. */
export interface RegisterStepParams {
  readonly stepName: string;
  readonly handler: StepHandler;
}

export interface StepRegistry {
  register(params: RegisterStepParams): void;
  resolve(stepName: string): StepRegistration | undefined;
  has(stepName: string): boolean;
  list(): string[];
}

export class MapStepRegistry implements StepRegistry {
  private readonly steps = new Map<string, StepRegistration>();

  register(params: RegisterStepParams): void {
    this.steps.set(params.stepName, { handler: params.handler });
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
