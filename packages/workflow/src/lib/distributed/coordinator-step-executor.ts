// ---------------------------------------------------------------------------
// CoordinatorStepExecutor — the StepExecutor the distributed runner hands its
// inner runner.
//
// The runner sends every ready step to its executor, whatever the step's
// kind. Only ordinary steps belong on the step queue: a sleep, signal-wait or
// subworkflow step enqueued as a task is named after the step, no worker
// registers it, and it sits pending forever. So this executor runs `sleep` /
// `signal` / `child` steps in-process, through `InProcessStepExecutor` with
// the runner's fenced runtime, and sends every other step to the queue.
// Sleep and signal steps only write their suspension to storage; a `child`
// step drives its child run through the runtime's `runChild`, so the child
// inherits this runner's executor and clock and its own ordinary steps go
// to the queue like any other.
//
// It also gives the queue executor a cheaper view of storage and the queue:
// - step waits on one run share their storage reads (one `loadWorkflow` per
//   run per poll instead of one per in-flight step);
// - the per-step stale sweep is dropped: the coordinator's leader-fenced
//   sweep is the one place that requeues stuck tasks, rather than one
//   unfenced global sweep per in-flight step.
// ---------------------------------------------------------------------------

import type { StepKind, Workflow } from "../durable/durable-pipeline.ts";
import {
  InProcessStepExecutor,
  type StepExecutionRequest,
  type StepExecutionResult,
  type StepExecutor,
} from "../durable/workflow-runner.ts";
import type { WorkflowState } from "../durable/workflow-state.ts";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import type { StepQueue, StepQueueRequeueResult } from "./step-queue.ts";

/** Step kinds that run on the coordinator instead of a worker. */
const IN_PROCESS_KINDS: ReadonlySet<StepKind> = new Set<StepKind>(["sleep", "signal", "child"]);

/** Expired shared reads are swept once the cache holds this many runs. */
const MAX_SHARED_READS = 256;

export class CoordinatorStepExecutor implements StepExecutor {
  private readonly queueExecutor: StepExecutor;
  private readonly storage: WorkflowStorage;
  private readonly clock: WallClock;
  private readonly workflow?: Workflow<unknown, unknown>;
  private inProcess?: InProcessStepExecutor;

  constructor(params: {
    /** Executor for ordinary steps (the step-queue executor). */
    readonly queueExecutor: StepExecutor;
    readonly storage: WorkflowStorage;
    readonly clock: WallClock;
    /** The definition this executor is bound to (set by `forWorkflow`). */
    readonly workflow?: Workflow<unknown, unknown>;
  }) {
    this.queueExecutor = params.queueExecutor;
    this.storage = params.storage;
    this.clock = params.clock;
    this.workflow = params.workflow;
  }

  forWorkflow(workflow: Workflow<unknown, unknown>): StepExecutor {
    if (workflow === this.workflow) return this;
    return new CoordinatorStepExecutor({
      queueExecutor: this.queueExecutor,
      storage: this.storage,
      clock: this.clock,
      workflow,
    });
  }

  executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    const workflow = this.workflow;
    const stepDef = workflow?._definition.steps.find((s) => s.name === req.stepName);
    if (workflow && stepDef && IN_PROCESS_KINDS.has(stepDef.kind)) {
      this.inProcess ??= new InProcessStepExecutor(workflow, {
        storage: this.storage,
        clock: this.clock,
      });
      return this.inProcess.executeStep(req);
    }
    return this.queueExecutor.executeStep(req);
  }
}

/**
 * A view of `storage` whose `loadWorkflow` is shared: concurrent calls for
 * one run share one read, and a read younger than `maxAgeMs` is reused.
 * Everything else passes through to `storage`.
 */
export function sharedReadStorage(params: {
  readonly storage: WorkflowStorage;
  readonly clock: WallClock;
  readonly maxAgeMs: number;
}): WorkflowStorage {
  const { storage, clock, maxAgeMs } = params;
  const reads = new Map<string, { at: number; state: Promise<WorkflowState | null> }>();
  const loadWorkflow = (workflowId: string): Promise<WorkflowState | null> => {
    const now = clock.currentTimeMs();
    const cached = reads.get(workflowId);
    if (cached && now - cached.at < maxAgeMs) return cached.state;
    if (reads.size >= MAX_SHARED_READS) {
      for (const [id, entry] of reads) if (now - entry.at >= maxAgeMs) reads.delete(id);
    }
    const state = storage.loadWorkflow(workflowId);
    const entry = { at: now, state };
    reads.set(workflowId, entry);
    // Never reuse a failed read.
    state.catch(() => {
      if (reads.get(workflowId) === entry) reads.delete(workflowId);
    });
    return state;
  };
  return passThrough(storage, { loadWorkflow });
}

/**
 * A view of `stepQueue` whose `requeueStuck` does nothing, for executors
 * whose per-step sweep would duplicate the coordinator's.
 */
export function withoutRequeue(stepQueue: StepQueue): StepQueue {
  const noSweep: StepQueueRequeueResult = { requeued: 0, deadLettered: 0 };
  return passThrough(stepQueue, { requeueStuck: async () => noSweep });
}

/** `target` with `overrides` in place; other members are bound to `target`. */
function passThrough<T extends object>(target: T, overrides: Partial<T>): T {
  return new Proxy(target, {
    get(obj, prop) {
      if (Object.hasOwn(overrides, prop)) return overrides[prop as keyof T];
      const value: unknown = Reflect.get(obj, prop, obj);
      return typeof value === "function" ? value.bind(obj) : value;
    },
  });
}
