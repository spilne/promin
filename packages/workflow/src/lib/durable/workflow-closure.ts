// ---------------------------------------------------------------------------
// workflowClosure — every definition a set of workflows can run.
//
// A host that drives runs by name and version (a worker claiming starts, a
// scanner resuming due runs) has to resolve more than the workflows it was
// given: the older versions they still serve (`previousVersions`, for runs
// started before a deploy) and the child workflows their `.subworkflow()`
// steps start, whose runs are resumed on their own after a sleep.
// ---------------------------------------------------------------------------

import type { Workflow } from "./workflow-types.ts";

/**
 * `workflows`, their `previousVersions` and, transitively, the children of
 * their `.subworkflow()` steps. Each definition object appears once, in
 * discovery order (the given workflows first).
 */
export function workflowClosure(
  workflows: Iterable<Workflow<unknown, unknown>>,
): Workflow<unknown, unknown>[] {
  const out: Workflow<unknown, unknown>[] = [];
  const seen = new Set<Workflow<unknown, unknown>>();
  const queue = [...workflows];
  while (queue.length > 0) {
    const wf = queue.shift()!;
    if (seen.has(wf)) continue;
    seen.add(wf);
    out.push(wf);
    for (const prev of wf._definition.previousVersions ?? []) queue.push(prev);
    for (const step of wf._definition.steps) {
      if (step.child) queue.push(step.child);
    }
  }
  return out;
}
