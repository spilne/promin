// ---------------------------------------------------------------------------
// `listWorkflows` ordering for backends that sort in process.
// ---------------------------------------------------------------------------

import type { WorkflowOrderBy } from "./query-store.ts";

/** The fields `listWorkflows` can sort on, timestamps as epoch ms. */
export interface WorkflowSortFields {
  readonly workflowName: string;
  readonly status: string;
  readonly createdAtMs: number;
  readonly startedAtMs?: number;
  readonly completedAtMs?: number;
}

/**
 * The value a row sorts by for `orderBy`. `duration` is
 * `completedAt - createdAt`; `undefined` means "no value" (a run not started
 * or not finished yet).
 */
export function workflowSortKey(params: {
  readonly fields: WorkflowSortFields;
  readonly orderBy: WorkflowOrderBy;
}): number | string | undefined {
  const { fields, orderBy } = params;
  switch (orderBy) {
    case "createdAt":
      return fields.createdAtMs;
    case "startedAt":
      return fields.startedAtMs;
    case "completedAt":
      return fields.completedAtMs;
    case "duration":
      return fields.completedAtMs !== undefined
        ? fields.completedAtMs - fields.createdAtMs
        : undefined;
    case "status":
      return fields.status;
    case "name":
      return fields.workflowName;
  }
}

/**
 * Sort `rows` in place by `orderBy` / `orderDir`, the way `listWorkflows`
 * orders a page: rows without a value sort last in either direction, and
 * ties keep their input order. Each row's key is read once.
 */
export function sortWorkflowRows<T>(params: {
  readonly rows: T[];
  readonly orderBy: WorkflowOrderBy;
  readonly orderDir: "asc" | "desc";
  readonly fields: (row: T) => WorkflowSortFields;
}): T[] {
  const { rows, orderBy, orderDir } = params;
  const sign = orderDir === "asc" ? 1 : -1;
  const keyed = rows.map((row) => ({
    row,
    key: workflowSortKey({ fields: params.fields(row), orderBy }),
  }));
  keyed.sort((a, b) => {
    const av = a.key;
    const bv = b.key;
    if (av === undefined && bv === undefined) return 0;
    if (av === undefined) return 1;
    if (bv === undefined) return -1;
    if (av < bv) return -1 * sign;
    if (av > bv) return 1 * sign;
    return 0;
  });
  for (let i = 0; i < keyed.length; i++) rows[i] = keyed[i]!.row;
  return rows;
}
