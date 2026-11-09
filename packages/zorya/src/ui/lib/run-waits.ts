// ---------------------------------------------------------------------------
// Run waits — the chips run detail shows for a run's `waitingOn` entries.
// ---------------------------------------------------------------------------

import type { RunWaitDto } from "../../server/api-types.ts";

export interface WaitChip {
  readonly label: string;
  /** daisyUI badge colour: parked waits are neutral, work in flight is accent, a backoff warns. */
  readonly tone: "neutral" | "info" | "warning";
}

const KIND_LABEL: Record<RunWaitDto["kind"], string> = {
  sleep: "sleeping",
  signal: "signal",
  approval: "approval",
  event: "event",
  child: "child run",
  "step-queue": "queued",
  activity: "running",
  "retry-backoff": "retry backoff",
};

/** One chip per wait: its kind, the step it belongs to, and the attempt for work the step does. */
export function waitChip(wait: RunWaitDto): WaitChip {
  const attempt =
    (wait.kind === "activity" || wait.kind === "retry-backoff") && wait.attempt !== undefined
      ? ` #${wait.attempt}`
      : "";
  const holder = wait.kind === "activity" && wait.holder !== undefined ? ` on ${wait.holder}` : "";
  return {
    label: `${KIND_LABEL[wait.kind]}: ${wait.stepName}${attempt}${holder}`,
    tone: wait.kind === "activity" ? "info" : wait.kind === "retry-backoff" ? "warning" : "neutral",
  };
}
