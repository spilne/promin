// ---------------------------------------------------------------------------
// RetentionPolicy — how long ended runs (and the step-queue rows behind them)
// are kept. Declared per workflow definition (`workflow({ retention })`) and
// as the sweeper's default; `RetentionSweeper` carries it out.
// ---------------------------------------------------------------------------

/** An age in ms, or `{ olderThan }` (the same age, spelled out). */
export type RetentionAge = number | { readonly olderThan: number };

/**
 * How long a workflow's ended runs are kept. Absent fields are inherited
 * from the sweeper's default policy (a definition's policy wins field by
 * field); `null` keeps that class forever even when the default would
 * delete it. No age at all anywhere means nothing is deleted.
 *
 * What counts as what:
 * - `completed`: runs that completed, and runs a `.tripwire()` stopped.
 * - `failed`: runs that failed (not by a cancel).
 * - `cancelled`: cancelled runs whose cancel is settled. A cancelled run
 *   still owing its rollback (`owesCancelRollback`) is never deleted.
 */
export interface RetentionPolicy {
  readonly completed?: RetentionAge | null;
  readonly failed?: RetentionAge | null;
  readonly cancelled?: RetentionAge | null;
  /**
   * Terminal step-queue tasks (completed or failed, dead-lettered ones
   * included). The queue is shared by every workflow, so this is read from
   * the sweeper's default policy only; on a definition it is ignored.
   */
  readonly stepQueue?: RetentionAge | null;
  /**
   * Never delete the newest N ended runs of a workflow, however old.
   * Default: 0.
   */
  readonly keepAtLeast?: number;
}

/** The age classes a run falls into. */
export type RetentionRunClass = "completed" | "failed" | "cancelled";

/** The age in ms, or `undefined` when the field is absent or `null` (keep forever). */
export function retentionAgeMs(age: RetentionAge | null | undefined): number | undefined {
  if (age === undefined || age === null) return undefined;
  return typeof age === "number" ? age : age.olderThan;
}

/** Throw when a policy cannot work. `where` names it in the message. */
export function validateRetentionPolicy(params: {
  readonly policy: RetentionPolicy;
  readonly where: string;
}): void {
  const { policy, where } = params;
  for (const field of ["completed", "failed", "cancelled", "stepQueue"] as const) {
    const ms = retentionAgeMs(policy[field]);
    if (ms !== undefined && (!Number.isFinite(ms) || ms <= 0)) {
      throw new Error(`${where}: retention.${field} must be a positive finite number of ms`);
    }
  }
  const keep = policy.keepAtLeast;
  if (keep !== undefined && (!Number.isInteger(keep) || keep < 0)) {
    throw new Error(`${where}: retention.keepAtLeast must be a non-negative integer`);
  }
}

/** A definition's policy over the default one, field by field (`null` and values win over absent). */
export function mergeRetentionPolicy(params: {
  readonly defaults: RetentionPolicy | undefined;
  readonly own: RetentionPolicy | undefined;
}): RetentionPolicy {
  const { defaults, own } = params;
  // The queue is not a definition's to configure.
  const { stepQueue: _ignored, ...definitionFields } = own ?? {};
  return {
    ...defaults,
    ...Object.fromEntries(Object.entries(definitionFields).filter(([, v]) => v !== undefined)),
  };
}
