// ---------------------------------------------------------------------------
// OpenTelemetry adapter for `WorkflowTracer`.
//
// One root span per orchestration of a run (`workflow.run`), with child spans
// for each step attempt, wait, and compensation (and a span per compensate
// attempt under it). A child workflow's root span is a child of its parent's.
// A run resumed after a sleep or signal wait is a new root span in the same
// trace when its `traceparent` is known (see `runStart.traceparent`).
//
// Only structural types are used, so `@opentelemetry/api` is not a
// dependency: pass its `Tracer` as `tracer` and its namespace as `api`.
// Attributes are an allowlist of ids, names, tags, counts and durations;
// step inputs, results and error messages never reach a span. Identifiers
// (workflow, namespace, parent, executor, worker, task ids) are replaced by a
// stable SHA-256 prefix unless `includeIdentifiers` is set.
// ---------------------------------------------------------------------------

import { sha256Hex } from "../shared/sha256.ts";
import type {
  WorkflowTraceCompensationEnd,
  WorkflowTraceCompensationStart,
  WorkflowTraceCompensationStep,
  WorkflowTraceLockAcquired,
  WorkflowTraceLockLost,
  WorkflowTraceRecoveryReclaim,
  WorkflowTraceRunEnd,
  WorkflowTraceRunStart,
  WorkflowTraceStepAttemptEnd,
  WorkflowTraceStepAttemptStart,
  WorkflowTraceStepThrottleEnd,
  WorkflowTraceStepThrottled,
  WorkflowTraceWaitEnd,
  WorkflowTraceWaitStart,
  WorkflowTraceWorkerTask,
  WorkflowTraceWorkerTaskEnd,
  WorkflowTracer,
  WorkflowTracerErrorEvent,
} from "../durable/workflow-tracer.ts";

export type OtelAttributeValue = string | number | boolean;
type Attributes = Record<string, OtelAttributeValue>;

/** `SpanStatusCode.ERROR` of `@opentelemetry/api`. */
const SPAN_STATUS_ERROR = 2;
/** `SpanKind.CONSUMER` of `@opentelemetry/api`. */
const SPAN_KIND_CONSUMER = 4;

/** The small span surface this adapter uses. An `@opentelemetry/api` `Span` satisfies it. */
export interface OtelSpanLike {
  end(endTime?: Date): void;
  setAttributes?(attributes: Attributes): unknown;
  setAttribute?(key: string, value: OtelAttributeValue): unknown;
  /** Code 2 is `SpanStatusCode.ERROR`. */
  setStatus?(status: { code: number; message?: string }): unknown;
  addEvent?(name: string, attributes?: Attributes, startTime?: Date): unknown;
  spanContext?(): {
    readonly traceId: string;
    readonly spanId: string;
    readonly traceFlags: number;
  };
}

/** An `@opentelemetry/api` `Tracer` satisfies this interface. */
export interface OtelTracerLike {
  startSpan(
    name: string,
    options?: {
      readonly startTime?: Date;
      readonly attributes?: Attributes;
      readonly kind?: number;
    },
    context?: unknown,
  ): OtelSpanLike;
}

/** The part of the `@opentelemetry/api` namespace the adapter needs (`import * as api`). */
export interface OtelApiLike {
  readonly trace: { setSpan(context: unknown, span: OtelSpanLike): unknown };
  readonly context: { active(): unknown };
  readonly propagation?: {
    extract(context: unknown, carrier: Record<string, string>): unknown;
  };
  readonly ROOT_CONTEXT?: unknown;
}

export interface OpenTelemetryWorkflowTracerOptions {
  readonly tracer: OtelTracerLike;
  /** `import * as api from "@opentelemetry/api"`: builds child contexts and extracts `traceparent`. */
  readonly api: OtelApiLike;
  /** Return undefined to drop an attribute before it reaches the tracer. */
  readonly filterAttribute?: (
    key: string,
    value: OtelAttributeValue,
  ) => OtelAttributeValue | undefined;
  /** A span operation threw; it never reaches the run. Default: `console.error`. */
  readonly onError?: (event: WorkflowTracerErrorEvent) => void;
  /**
   * Export raw ids (`workflow.id`, `workflow.namespace`, ...). By default they
   * are replaced by a stable SHA-256 prefix so spans still group without
   * naming anyone.
   */
  readonly includeIdentifiers?: boolean;
  /** Mixed into the identifier hash so hashes cannot be correlated across deployments. */
  readonly identifierSalt?: string;
  /**
   * Parent context for a run's root span, an `@opentelemetry/api` Context.
   * Return `undefined` to fall back to: the parent run's span (child
   * workflow), the context extracted from the run's `traceparent`, then the
   * active context.
   */
  readonly parentContext?: (event: WorkflowTraceRunStart) => unknown;
  /** Hand the engine a `traceparent` to carry across step queues. Default: true. */
  readonly injectTraceparent?: boolean;
}

interface SpanEntry {
  readonly span: OtelSpanLike;
  readonly context: unknown;
}

interface RunEntry extends SpanEntry {
  readonly steps: Map<string, SpanEntry>;
  compensation?: SpanEntry;
}

const MAX_PENDING_LOCKS = 256;

/**
 * Build a `WorkflowTracer` that reports to OpenTelemetry. Pass it as
 * `tracer` to `createWorkflowRunner`, `createDistributedWorkflowRunner` and
 * `createWorker`.
 */
export function createOpenTelemetryWorkflowTracer(
  options: OpenTelemetryWorkflowTracerOptions,
): WorkflowTracer {
  const { tracer, api } = options;
  const runs = new Map<string, RunEntry>();
  const workerTasks = new Map<string, SpanEntry>();
  const pendingLocks = new Map<string, WorkflowTraceLockAcquired>();

  const id = (value: string): string =>
    options.includeIdentifiers === true
      ? value
      : sha256Hex(options.identifierSalt ? `${options.identifierSalt}\0${value}` : value).slice(
          0,
          16,
        );

  /** Allowlisted attributes: undefined values and filtered keys are dropped. */
  const attrs = (input: Record<string, OtelAttributeValue | undefined>): Attributes => {
    const out: Attributes = {};
    for (const [key, raw] of Object.entries(input)) {
      if (raw === undefined) continue;
      const value = options.filterAttribute ? options.filterAttribute(key, raw) : raw;
      if (value !== undefined) out[key] = value;
    }
    return out;
  };

  const setAttrs = (span: OtelSpanLike, attributes: Attributes): void => {
    if (span.setAttributes) span.setAttributes(attributes);
    else if (span.setAttribute) {
      for (const [key, value] of Object.entries(attributes)) span.setAttribute(key, value);
    }
  };

  const fail = (span: OtelSpanLike, tag: string | undefined): void => {
    // The tag, never the message: messages can carry payload data.
    span.setStatus?.({ code: SPAN_STATUS_ERROR, ...(tag !== undefined && { message: tag }) });
  };

  const start = (params: {
    name: string;
    startTime: Date;
    attributes: Attributes;
    parent: unknown;
    kind?: number;
  }): SpanEntry => {
    const span = tracer.startSpan(
      params.name,
      {
        startTime: params.startTime,
        attributes: params.attributes,
        ...(params.kind !== undefined && { kind: params.kind }),
      },
      params.parent,
    );
    return { span, context: api.trace.setSpan(params.parent ?? api.context.active(), span) };
  };

  const identity = (e: {
    workflowId: string;
    workflowName: string;
    namespace?: string;
    run?: number;
  }): Record<string, OtelAttributeValue | undefined> => ({
    "workflow.id": id(e.workflowId),
    "workflow.name": e.workflowName,
    "workflow.namespace": e.namespace !== undefined ? id(e.namespace) : undefined,
    "workflow.run": e.run,
  });

  const extract = (traceparent: string | undefined): unknown => {
    if (traceparent === undefined || api.propagation === undefined) return undefined;
    return api.propagation.extract(api.ROOT_CONTEXT ?? api.context.active(), { traceparent });
  };

  const closeRun = (entry: RunEntry): void => {
    for (const step of entry.steps.values()) step.span.end();
    entry.steps.clear();
    entry.compensation?.span.end();
  };

  const formatTraceparent = (span: OtelSpanLike): string | undefined => {
    const c = span.spanContext?.();
    if (c === undefined || c.traceId.length !== 32 || c.spanId.length !== 16) return undefined;
    return `00-${c.traceId}-${c.spanId}-${(c.traceFlags & 0xff).toString(16).padStart(2, "0")}`;
  };

  const stepKey = (stepName: string, attempt: number): string => `${stepName}\0${attempt}`;

  const stepAttrs = (e: WorkflowTraceStepAttemptStart): Record<string, OtelAttributeValue> => ({
    "workflow.step.name": e.stepName,
    "workflow.step.attempt": e.attempt,
  });

  const result: WorkflowTracer = {
    onTracerError: options.onError,

    ...(options.injectTraceparent !== false && {
      traceparent(ref: {
        workflowId: string;
        stepName?: string;
        attempt?: number;
      }): string | undefined {
        const run = runs.get(ref.workflowId);
        if (run === undefined) return undefined;
        const step =
          ref.stepName !== undefined && ref.attempt !== undefined
            ? run.steps.get(stepKey(ref.stepName, ref.attempt))
            : undefined;
        return formatTraceparent((step ?? run).span);
      },
    }),

    runStart(e: WorkflowTraceRunStart): void {
      const previous = runs.get(e.workflowId);
      if (previous !== undefined) {
        closeRun(previous);
        previous.span.end();
      }
      const parentRun = e.parentWorkflowId !== undefined ? runs.get(e.parentWorkflowId) : undefined;
      const parent = options.parentContext?.(e) ?? parentRun?.context ?? extract(e.traceparent);
      const entry = start({
        name: `workflow.run ${e.workflowName}`,
        startTime: e.startedAt,
        parent,
        attributes: attrs({
          ...identity(e),
          "workflow.version": e.version,
          "workflow.parent.id":
            e.parentWorkflowId !== undefined ? id(e.parentWorkflowId) : undefined,
          "workflow.resumed": e.resumed,
          "workflow.executor.id": e.executorId !== undefined ? id(e.executorId) : undefined,
        }),
      });
      runs.set(e.workflowId, { ...entry, steps: new Map() });
      const lock = pendingLocks.get(e.workflowId);
      if (lock !== undefined) {
        pendingLocks.delete(e.workflowId);
        entry.span.addEvent?.(
          "workflow.lock.acquired",
          attrs({ "workflow.lock.fenced": lock.fenced }),
          lock.at,
        );
      }
    },

    runEnd(e: WorkflowTraceRunEnd): void {
      const run = runs.get(e.workflowId);
      if (run === undefined) return;
      runs.delete(e.workflowId);
      closeRun(run);
      setAttrs(
        run.span,
        attrs({
          "workflow.outcome": e.outcome,
          "workflow.duration_ms": e.durationMs,
          "workflow.failure.tag": e.failure?.tag,
          "workflow.failure.step": e.failure?.stepName,
        }),
      );
      if (e.outcome === "failed") fail(run.span, e.failure?.tag);
      run.span.end(e.endedAt);
    },

    stepAttemptStart(e: WorkflowTraceStepAttemptStart): void {
      const run = runs.get(e.workflowId);
      if (run === undefined) return;
      run.steps.set(
        stepKey(e.stepName, e.attempt),
        start({
          name: `workflow.step ${e.stepName}`,
          startTime: e.startedAt,
          parent: run.context,
          attributes: attrs({ ...identity(e), ...stepAttrs(e) }),
        }),
      );
    },

    stepAttemptEnd(e: WorkflowTraceStepAttemptEnd): void {
      const run = runs.get(e.workflowId);
      if (run === undefined) return;
      const key = stepKey(e.stepName, e.attempt);
      // An attempt that ran elsewhere (a worker's retry) has no start event.
      const entry =
        run.steps.get(key) ??
        start({
          name: `workflow.step ${e.stepName}`,
          startTime: e.startedAt,
          parent: run.context,
          attributes: attrs({ ...identity(e), ...stepAttrs(e) }),
        });
      run.steps.delete(key);
      setAttrs(
        entry.span,
        attrs({
          "workflow.step.outcome": e.outcome,
          "workflow.step.will_retry": e.willRetry,
          "workflow.step.error.tag": e.error?.tag,
          "workflow.duration_ms": e.durationMs,
          "workflow.executor.id": e.executorId !== undefined ? id(e.executorId) : undefined,
        }),
      );
      if (e.outcome === "failed") fail(entry.span, e.error?.tag);
      entry.span.end(e.endedAt);
    },

    waitStart(e: WorkflowTraceWaitStart): void {
      runs
        .get(e.workflowId)
        ?.span.addEvent?.(
          "workflow.wait.start",
          attrs({ "workflow.step.name": e.stepName, "workflow.wait.kind": e.kind }),
          e.at,
        );
    },

    waitEnd(e: WorkflowTraceWaitEnd): void {
      const run = runs.get(e.workflowId);
      if (run === undefined) return;
      const { span } = start({
        name: `workflow.wait ${e.kind}`,
        startTime: e.since ?? e.endedAt,
        parent: run.context,
        attributes: attrs({
          ...identity(e),
          "workflow.step.name": e.stepName,
          "workflow.wait.kind": e.kind,
          "workflow.wait.waited_ms": e.waitedMs,
        }),
      });
      span.end(e.endedAt);
    },

    stepThrottled(e: WorkflowTraceStepThrottled): void {
      runs.get(e.workflowId)?.span.addEvent?.(
        "workflow.step.throttled",
        attrs({
          "workflow.step.name": e.stepName,
          "workflow.step.group": e.groupName,
          "workflow.step.throttle.reason": e.reason,
        }),
        e.at,
      );
    },

    stepThrottleEnd(e: WorkflowTraceStepThrottleEnd): void {
      const run = runs.get(e.workflowId);
      if (run === undefined) return;
      const { span } = start({
        name: "workflow.step.throttle",
        startTime: e.since,
        parent: run.context,
        attributes: attrs({
          ...identity(e),
          "workflow.step.name": e.stepName,
          "workflow.step.group": e.groupName,
          "workflow.step.attempt": e.attempt,
          "workflow.wait.waited_ms": e.waitedMs,
        }),
      });
      span.end(e.endedAt);
    },

    lockAcquired(e: WorkflowTraceLockAcquired): void {
      pendingLocks.set(e.workflowId, e);
      if (pendingLocks.size > MAX_PENDING_LOCKS) {
        // A lock whose run never started (rejected before `runStart`).
        pendingLocks.delete(pendingLocks.keys().next().value!);
      }
    },

    lockLost(e: WorkflowTraceLockLost): void {
      runs
        .get(e.workflowId)
        ?.span.addEvent?.(
          "workflow.lock.lost",
          attrs({ "workflow.lock.error.tag": e.error.tag }),
          e.at,
        );
    },

    compensationStart(e: WorkflowTraceCompensationStart): void {
      const run = runs.get(e.workflowId);
      if (run === undefined) return;
      run.compensation?.span.end();
      run.compensation = start({
        name: "workflow.compensation",
        startTime: e.at,
        parent: run.context,
        attributes: attrs({
          ...identity(e),
          "workflow.compensation.cancelled": e.cancelled,
          "workflow.compensation.resumed": e.resumed,
        }),
      });
    },

    compensationStep(e: WorkflowTraceCompensationStep): void {
      const run = runs.get(e.workflowId);
      if (run === undefined) return;
      const { span } = start({
        name: `workflow.compensation.step ${e.stepName}`,
        startTime: e.startedAt,
        parent: (run.compensation ?? run).context,
        attributes: attrs({
          ...identity(e),
          "workflow.step.name": e.stepName,
          "workflow.compensation.attempt": e.attempt,
          "workflow.step.outcome": e.outcome,
          "workflow.step.error.tag": e.error?.tag,
          "workflow.duration_ms": e.durationMs,
        }),
      });
      if (e.outcome === "failed") fail(span, e.error?.tag);
      span.end(e.endedAt);
    },

    compensationEnd(e: WorkflowTraceCompensationEnd): void {
      const run = runs.get(e.workflowId);
      const compensation = run?.compensation;
      if (run === undefined || compensation === undefined) return;
      delete run.compensation;
      setAttrs(
        compensation.span,
        attrs({
          "workflow.compensation.compensated": e.compensated,
          "workflow.compensation.failed": e.failed,
          "workflow.duration_ms": e.durationMs,
        }),
      );
      if (e.failed > 0) fail(compensation.span, "CompensationFailed");
      compensation.span.end(e.endedAt);
    },

    recoveryReclaim(e: WorkflowTraceRecoveryReclaim): void {
      const { span } = start({
        name: "workflow.recovery.reclaim",
        startTime: e.at,
        parent: undefined,
        attributes: attrs({
          "workflow.id": id(e.workflowId),
          "workflow.name": e.workflowName,
          "workflow.version": e.version,
        }),
      });
      span.end(e.at);
    },

    workerTaskStart(e: WorkflowTraceWorkerTask): void {
      workerTasks.set(
        e.taskId,
        start({
          name: `workflow.worker.task ${e.stepName}`,
          startTime: e.startedAt,
          parent: extract(e.traceparent),
          kind: SPAN_KIND_CONSUMER,
          attributes: workerAttrs(e),
        }),
      );
    },

    workerTaskEnd(e: WorkflowTraceWorkerTaskEnd): void {
      const entry =
        workerTasks.get(e.taskId) ??
        start({
          name: `workflow.worker.task ${e.stepName}`,
          startTime: e.startedAt,
          parent: extract(e.traceparent),
          kind: SPAN_KIND_CONSUMER,
          attributes: workerAttrs(e),
        });
      workerTasks.delete(e.taskId);
      setAttrs(
        entry.span,
        attrs({
          "workflow.step.outcome": e.outcome,
          "workflow.step.error.tag": e.error?.tag,
          "workflow.duration_ms": e.durationMs,
        }),
      );
      if (e.outcome === "failed") fail(entry.span, e.error?.tag);
      entry.span.end(e.endedAt);
    },
  };

  function workerAttrs(e: WorkflowTraceWorkerTask): Attributes {
    return attrs({
      "workflow.id": id(e.workflowId),
      "workflow.run": e.run,
      "workflow.step.name": e.stepName,
      "workflow.step.attempt": e.attempt,
      "workflow.worker.id": id(e.workerId),
      "workflow.task.id": id(e.taskId),
    });
  }

  return result;
}
