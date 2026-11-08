// ---------------------------------------------------------------------------
// Workflow waits — "what is this run waiting on?", derived from stored step
// rows. Pure: no storage reads beyond the `WorkflowState` the caller holds.
// ---------------------------------------------------------------------------

import { APPROVAL_NAME_PREFIX } from "../signals/define-signal.ts";
import { childOfEndedSignal } from "./child-wake.ts";
import type { WorkflowState } from "./workflow-state.ts";

/**
 * One thing a live run is parked on.
 *
 * - `sleep`: a sleeping step; `until` is its wake time.
 * - `signal`: a step waiting for signal `signalName`; `until` is its timeout, when it has one.
 * - `approval`: a signal wait on an `approve:<id>` signal (`ctx.approval`).
 * - `child`: a step parked on child workflow `childWorkflowId`; `until` is the child's
 *   earliest wake time, when it has one (the parent is re-driven then).
 *
 * `since` is when the step started (the run's last update when the step row has no start).
 * Waits that are not persisted on a step row (an activity in flight, a retry backoff,
 * a step task queued for a worker) are not derivable from storage and are not listed.
 */
export interface WorkflowWait {
  readonly kind: "sleep" | "signal" | "approval" | "child";
  readonly stepName: string;
  readonly since: Date;
  readonly until?: Date;
  readonly signalName?: string;
  readonly childWorkflowId?: string;
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
  for (const step of Object.values(state.steps)) {
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
      if (childWorkflowId !== undefined) {
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
  return waits.sort((a, b) => (a.stepName < b.stepName ? -1 : a.stepName > b.stepName ? 1 : 0));
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
    case "approval":
      return `waiting for approval ${wait.signalName ?? ""} (${detail("times out")})`;
    case "child":
      return `waiting for child workflow ${wait.childWorkflowId} (${detail("wakes")})`;
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
