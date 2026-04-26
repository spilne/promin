// ---------------------------------------------------------------------------
// Process-local registry for `ctx.setQueryHandler` handlers.
//
// Query handlers are closures over in-memory workflow state (variables
// captured by the journaled body's generator). They are inherently
// per-process — the worker hosting the live execution is the only place
// the closures exist. Storing them in a module-level Map keeps the
// journaled-step → runner → worker plumbing flat: the body writes via
// `ctx.setQueryHandler`, the worker control socket reads via
// `lookupQueryHandler` to answer cross-process query requests routed
// from the server.
//
// The registry is naturally GC'd: handlers are removed when a journaled
// body completes (the runner clears them) or when the workflow row
// terminates. Replay re-runs the body, which re-registers handlers.
// ---------------------------------------------------------------------------

type QueryHandler = (args?: unknown) => unknown | Promise<unknown>;

const registry = new Map<string, Map<string, QueryHandler>>();

function key(workflowId: string): string {
  return workflowId;
}

/** Internal: called by JournaledContext.setQueryHandler. */
export function registerQueryHandler(
  workflowId: string,
  name: string,
  handler: QueryHandler,
): void {
  let perWorkflow = registry.get(key(workflowId));
  if (!perWorkflow) {
    perWorkflow = new Map();
    registry.set(key(workflowId), perWorkflow);
  }
  perWorkflow.set(name, handler);
}

/**
 * Look up + invoke a registered handler. Throws when no handler matches
 * the (workflowId, name) — caller surfaces this as
 * `WorkflowNotRunningError` or `NoHandlerForQueryError` based on
 * whether the workflow is hosted on this worker at all.
 */
export async function invokeQueryHandler(
  workflowId: string,
  name: string,
  args?: unknown,
): Promise<unknown> {
  const perWorkflow = registry.get(key(workflowId));
  if (!perWorkflow) {
    throw new Error(
      `No query handlers registered for workflow "${workflowId}" — either it isn't running on this process or no handler set has been declared yet.`,
    );
  }
  const handler = perWorkflow.get(name);
  if (!handler) {
    const known = [...perWorkflow.keys()].join(", ") || "(none)";
    throw new Error(`No query handler "${name}" on workflow "${workflowId}". Known: ${known}.`);
  }
  return await handler(args);
}

/** Whether any query handlers are registered for a workflow on this process. */
export function hasQueryHandlers(workflowId: string): boolean {
  return registry.has(key(workflowId));
}

/** List registered handler names — used by the worker control surface. */
export function listQueryHandlers(workflowId: string): string[] {
  return [...(registry.get(key(workflowId))?.keys() ?? [])];
}

/**
 * Clear all handlers for a workflow. Called by the runner on workflow
 * completion (success / failure / tripwire / continue-as-new) so the
 * Map doesn't leak across long-running processes.
 */
export function clearQueryHandlers(workflowId: string): void {
  registry.delete(key(workflowId));
}
