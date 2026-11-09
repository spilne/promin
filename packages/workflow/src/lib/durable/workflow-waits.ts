// ---------------------------------------------------------------------------
// Workflow waits — "what is this run waiting on?", derived from stored step
// rows. Pure: no storage reads beyond the `WorkflowState` the caller holds.
// ---------------------------------------------------------------------------

import { APPROVAL_NAME_PREFIX } from "../signals/define-signal.ts";
import { childOfEndedSignal } from "./child-wake.ts";
import { eventOfWakeSignal } from "./wait-for-event.ts";
import type { StepState, StoredStepProgress, WorkflowState } from "./workflow-state.ts";

/**
 * One thing a live run is parked on, or busy with.
 *
 * - `sleep`: a sleeping step; `until` is its wake time.
 * - `signal`: a step waiting for signal `signalName`; `until` is its timeout, when it has one.
 * - `approval`: a signal wait on an `approve:<id>` signal (`ctx.approval`).
 * - `event`: a step in `ctx.waitForEvent`; `eventName` / `eventKey` address the event and
 *   `until` is its timeout, when it has one.
 * - `child`: a step parked on child workflow `childWorkflowId`; `until` is the child's
 *   earliest wake time, when it has one (the parent is re-driven then).
 * - `step-queue`: a queued step its rate-limit / concurrency group (`groupName`) is
 *   holding back; `until` is when the rate window frees a slot (absent while it
 *   waits on a running task, `reason: "concurrency"`). Not stored on a step
 *   row: `getStatus` reads it from the step executor (`StepExecutor.queuedWaits`).
 * - `activity`: a step body is executing right now, attempt `attempt`, since `since`;
 *   `holder` is who runs it (a worker id, or the runner's `executorId`) when known.
 *   Read from the step's progress record (`StorageCapability` `stepProgress`).
 * - `retry-backoff`: attempt `attempt` of the step failed and the next one is due at
 *   `until`. Read from the step's progress record.
 *
 * `since` is when the step started (the run's last update when the step row has no start;
 * when the attempt began for `activity`, when it failed for `retry-backoff`).
 * `activity` and `retry-backoff` are advisory and need a storage with the `stepProgress`
 * capability; without one they are not listed. A step task queued for a worker that has
 * not claimed it is `step-queue` only when a group holds it back.
 */
export interface WorkflowWait {
  readonly kind:
    | "sleep"
    | "signal"
    | "approval"
    | "event"
    | "child"
    | "step-queue"
    | "activity"
    | "retry-backoff";
  readonly stepName: string;
  readonly since: Date;
  readonly until?: Date;
  readonly signalName?: string;
  readonly eventName?: string;
  readonly eventKey?: string;
  readonly childWorkflowId?: string;
  /** `step-queue` waits: the group holding the step back. */
  readonly groupName?: string;
  /** `step-queue` waits: out of concurrent slots, or out of starts for the window. */
  readonly reason?: "concurrency" | "rate";
  /** `activity` waits: who runs the attempt, when known. */
  readonly holder?: string;
  readonly attempt?: number;
}

const LIVE_STATUSES = new Set(["pending", "running", "suspended"]);

/**
 * The waits of a live run (`pending`, `running`, `suspended`), ordered by step
 * name. Empty for a run with nothing parked and for every ended run.
 */
export function deriveWaitingOn(state: WorkflowState): WorkflowWait[] {
  if (!LIVE_STATUSES.has(state.status)) return [];
  const waits: WorkflowWait[] = [];
  const names = new Set([...Object.keys(state.steps), ...Object.keys(state.stepProgress ?? {})]);
  for (const name of names) {
    const step = state.steps[name];
    const progress = liveProgress({ state, step, progress: state.stepProgress?.[name] });
    if (progress !== undefined) {
      waits.push(progressWait({ stepName: name, progress }));
      continue;
    }
    if (step === undefined) continue;
    const since = step.startedAt ?? state.updatedAt;
    const common = { stepName: step.stepName, since, attempt: step.attempt };
    if (step.status === "sleeping") {
      waits.push({
        kind: "sleep",
        ...common,
        ...(step.wakeAt && { until: step.wakeAt }),
      });
    } else if (step.status === "waiting_for_signal") {
      const signalName = step.signalName;
      const until = step.signalTimeoutAt && { until: step.signalTimeoutAt };
      const childWorkflowId = signalName === undefined ? undefined : childOfEndedSignal(signalName);
      const event = signalName === undefined ? undefined : eventOfWakeSignal(signalName);
      if (event !== undefined) {
        waits.push({
          kind: "event",
          ...common,
          eventName: event.name,
          ...(event.key !== undefined && { eventKey: event.key }),
          ...until,
        });
      } else if (childWorkflowId !== undefined) {
        waits.push({ kind: "child", ...common, childWorkflowId, ...until });
      } else if (signalName?.startsWith(APPROVAL_NAME_PREFIX)) {
        waits.push({ kind: "approval", ...common, signalName, ...until });
      } else {
        waits.push({
          kind: "signal",
          ...common,
          ...(signalName !== undefined && { signalName }),
          ...until,
        });
      }
    }
  }
  return sortWaits(waits);
}

/**
 * The progress record of a step when it still describes what the step is
 * doing: written for this run, by an attempt that is not over. Records go
 * stale without being deleted, so each is checked against the step row:
 * a settled step (completed, skipped, compensated) has none, a failed one
 * only when the record is newer than the failure (a workflow retry runs it
 * again), a parked one only when the record is newer than the park (the
 * step re-executes after its wake). A suspended run executes nothing.
 */
function liveProgress(params: {
  state: WorkflowState;
  step: StepState | undefined;
  progress: StoredStepProgress | undefined;
}): StoredStepProgress | undefined {
  const { state, step, progress } = params;
  if (progress === undefined || progress.run !== state.run || state.status === "suspended") {
    return undefined;
  }
  if (step === undefined || step.status === "pending" || step.status === "running") return progress;
  const after =
    step.status === "failed"
      ? step.completedAt
      : step.status === "sleeping" || step.status === "waiting_for_signal"
        ? step.startedAt
        : undefined;
  return after !== undefined && progress.at.getTime() > after.getTime() ? progress : undefined;
}

function progressWait(params: { stepName: string; progress: StoredStepProgress }): WorkflowWait {
  const { stepName, progress } = params;
  const common = { stepName, since: progress.at, attempt: progress.attempt };
  if (progress.phase === "backoff") {
    return {
      kind: "retry-backoff",
      ...common,
      ...(progress.nextAttemptAt !== undefined && { until: progress.nextAttemptAt }),
    };
  }
  return {
    kind: "activity",
    ...common,
    ...(progress.holder !== undefined && { holder: progress.holder }),
  };
}

function sortWaits(waits: WorkflowWait[]): WorkflowWait[] {
  return waits.sort((a, b) => (a.stepName < b.stepName ? -1 : a.stepName > b.stepName ? 1 : 0));
}

/**
 * `stored` waits plus `extra` ones from outside the step rows (a queued step
 * its group holds back), ordered by step name. A step already parked in
 * storage is not also reported as queued; a step the group holds back
 * replaces its `activity` / `retry-backoff` record, which is the older fact
 * (the next attempt is queued). Live runs only: `stored` empty for an ended
 * run still yields `extra`, so callers pass extras only for a live run.
 */
export function mergeWaits(params: {
  stored: readonly WorkflowWait[];
  extra: readonly WorkflowWait[];
}): WorkflowWait[] {
  const isProgress = (w: WorkflowWait) => w.kind === "activity" || w.kind === "retry-backoff";
  const parked = new Set(params.stored.filter((w) => !isProgress(w)).map((w) => w.stepName));
  const extra = params.extra.filter((w) => !parked.has(w.stepName));
  const queued = new Set(extra.map((w) => w.stepName));
  return sortWaits([
    ...params.stored.filter((w) => !queued.has(w.stepName) || !isProgress(w)),
    ...extra,
  ]);
}

/** `1500000` → `"25m"`; the two largest non-zero units at most, `"now"` for <= 0. */
function formatSpan(ms: number): string {
  if (ms <= 0) return "now";
  const s = Math.floor(ms / 1000);
  const units: Array<[string, number]> = [
    ["d", 86_400],
    ["h", 3_600],
    ["m", 60],
    ["s", 1],
  ];
  const parts: string[] = [];
  let rest = s;
  for (const [label, size] of units) {
    const n = Math.floor(rest / size);
    rest -= n * size;
    if (n > 0 && parts.length < 2) parts.push(`${n}${label}`);
  }
  return parts.length > 0 ? parts.join("") : "<1s";
}

function describeWait(params: { wait: WorkflowWait; now: Date }): string {
  const { wait, now } = params;
  const left = wait.until && formatSpan(wait.until.getTime() - now.getTime());
  const detail = (verb: string) =>
    `step ${wait.stepName}${left !== undefined ? `, ${verb} ${left === "now" ? "now" : `in ${left}`}` : ""}`;
  switch (wait.kind) {
    case "sleep":
      return `sleeping (${detail("wakes")})`;
    case "signal":
      return `waiting for signal ${wait.signalName ?? "(unnamed)"} (${detail("times out")})`;
    case "event":
      return `waiting for event ${wait.eventName}${
        wait.eventKey !== undefined ? ` (key ${wait.eventKey})` : ""
      } (${detail("times out")})`;
    case "approval":
      return `waiting for approval ${wait.signalName ?? ""} (${detail("times out")})`;
    case "child":
      return `waiting for child workflow ${wait.childWorkflowId} (${detail("wakes")})`;
    case "activity": {
      const ran = now.getTime() - wait.since.getTime();
      const ranFor = ran > 0 ? formatSpan(ran) : "<1s";
      return `running step ${wait.stepName} (attempt ${wait.attempt ?? 1}, for ${ranFor}${wait.holder !== undefined ? `, on ${wait.holder}` : ""})`;
    }
    case "retry-backoff":
      return `retrying step ${wait.stepName} after failed attempt ${wait.attempt ?? 1}${
        left !== undefined ? ` (next attempt ${left === "now" ? "now" : `in ${left}`})` : ""
      }`;
    case "step-queue":
      return `queued behind ${wait.reason === "rate" ? "rate limit" : "concurrency limit"} of group ${wait.groupName} (${detail("next slot")})`;
  }
}

/**
 * Human summary of `waits`, relative to `now`, e.g.
 * `waiting for signal manager-approved (step approve, times out in 2h)`.
 * Several waits are joined with `"; "`. `undefined` when there are none.
 */
export function summarizeWaits(params: {
  waits: readonly WorkflowWait[];
  now: Date;
}): string | undefined {
  if (params.waits.length === 0) return undefined;
  return params.waits.map((wait) => describeWait({ wait, now: params.now })).join("; ");
}
