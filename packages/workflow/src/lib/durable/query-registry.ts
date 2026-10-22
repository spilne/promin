// ---------------------------------------------------------------------------
// Process-local registry for `ctx.setQueryHandler` handlers.
//
// Query handlers are closures over in-memory workflow state (variables
// captured by the journaled body's generator). They are inherently
// per-process — the worker hosting the live execution is the only place
// the closures exist. Storing them in a module-level registry keeps the
// journaled-step → runner → worker plumbing flat: the body writes via
// `ctx.setQueryHandler`, the worker control socket reads via
// `invokeQueryHandler` to answer cross-process query requests routed
// from the server.
//
// Entries are scoped to the execution that hosts them. The runner opens a
// scope when it takes a run's lock (`openQueryScope`) and closes it when
// the run stops here:
//   - a run that ends drops its handlers;
//   - a run that suspends keeps them (a later resume on this process
//     re-registers them on replay) until `suspendedTtlMs` passes, so runs
//     resumed on another node do not pile up here;
//   - only the scope that owns an entry can drop it, so an invocation that
//     never got the lock (a duplicate run) cannot clear a live run's
//     handlers.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

type QueryHandler = (args?: unknown) => unknown | Promise<unknown>;

/** Default time a suspended run's handlers stay registered: 1 hour. */
export const DEFAULT_SUSPENDED_QUERY_TTL_MS = 60 * 60 * 1000;

/** Minimum gap between two sweeps of expired entries. */
const SWEEP_INTERVAL_MS = 1_000;

interface Entry {
  readonly handlers: Map<string, QueryHandler>;
  /** The scope hosting the run, while it runs here. */
  owner?: object;
  /** When a suspended run's handlers are dropped (ms, registry clock). */
  evictAtMs?: number;
}

/** An execution's hold on its workflow's query handlers (see `openQueryScope`). */
export interface QueryScope {
  /** Drop the handlers registered so far, keeping the scope (a new run under the same lock). */
  reset(): void;
  /**
   * The execution stops here. With `suspended`, the handlers stay for
   * `suspendedTtlMs`; otherwise they are dropped. A no-op once another
   * scope owns the entry.
   */
  close(params?: { readonly suspended?: boolean }): void;
}

const registry = new Map<string, Entry>();
let clock: WallClock = SystemWallClock;
let suspendedTtlMs = DEFAULT_SUSPENDED_QUERY_TTL_MS;
let lastSweepMs = Number.NEGATIVE_INFINITY;

/**
 * Configure the process-wide registry: its time source (tests pass a
 * `FakeWallClock`) and how long a suspended run's handlers stay
 * registered. Omitted fields keep their current value.
 */
export function configureQueryRegistry(params: {
  readonly clock?: WallClock;
  readonly suspendedTtlMs?: number;
}): void {
  if (params.clock !== undefined) clock = params.clock;
  if (params.suspendedTtlMs !== undefined) suspendedTtlMs = params.suspendedTtlMs;
}

/** The live entry for `workflowId`, dropping it first when it has expired. */
function liveEntry(workflowId: string): Entry | undefined {
  const entry = registry.get(workflowId);
  if (entry?.evictAtMs !== undefined && entry.evictAtMs <= clock.currentTimeMs()) {
    registry.delete(workflowId);
    return undefined;
  }
  return entry;
}

/** Drop every expired entry, at most once per `SWEEP_INTERVAL_MS`. */
function sweep(): void {
  const now = clock.currentTimeMs();
  if (now - lastSweepMs < SWEEP_INTERVAL_MS) return;
  lastSweepMs = now;
  for (const [workflowId, entry] of registry) {
    if (entry.evictAtMs !== undefined && entry.evictAtMs <= now) registry.delete(workflowId);
  }
}

/**
 * Internal: called by the runner once it holds `workflowId`'s lock. The
 * new scope owns the workflow's entry; handlers a suspended execution left
 * stay until the replay registers them again.
 */
export function openQueryScope(workflowId: string): QueryScope {
  sweep();
  const owner = {};
  const entry = liveEntry(workflowId) ?? { handlers: new Map<string, QueryHandler>() };
  entry.owner = owner;
  delete entry.evictAtMs;
  registry.set(workflowId, entry);

  const owned = (): Entry | undefined => {
    const current = registry.get(workflowId);
    return current?.owner === owner ? current : undefined;
  };
  return {
    reset: () => owned()?.handlers.clear(),
    close: (params) => {
      const current = owned();
      if (!current) return;
      if (params?.suspended === true) {
        delete current.owner;
        current.evictAtMs = clock.currentTimeMs() + suspendedTtlMs;
      } else {
        registry.delete(workflowId);
      }
    },
  };
}

/** Internal: called by JournaledContext.setQueryHandler. */
export function registerQueryHandler(
  workflowId: string,
  name: string,
  handler: QueryHandler,
): void {
  let entry = liveEntry(workflowId);
  if (!entry) {
    entry = { handlers: new Map() };
    registry.set(workflowId, entry);
  }
  entry.handlers.set(name, handler);
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
  const entry = liveEntry(workflowId);
  if (!entry) {
    throw new Error(
      `No query handlers registered for workflow "${workflowId}" — either it isn't running on this process or no handler set has been declared yet.`,
    );
  }
  const handler = entry.handlers.get(name);
  if (!handler) {
    const known = [...entry.handlers.keys()].join(", ") || "(none)";
    throw new Error(`No query handler "${name}" on workflow "${workflowId}". Known: ${known}.`);
  }
  return await handler(args);
}

/** Whether any query handlers are registered for a workflow on this process. */
export function hasQueryHandlers(workflowId: string): boolean {
  return (liveEntry(workflowId)?.handlers.size ?? 0) > 0;
}

/** List registered handler names — used by the worker control surface. */
export function listQueryHandlers(workflowId: string): string[] {
  return [...(liveEntry(workflowId)?.handlers.keys() ?? [])];
}

/**
 * Clear all handlers for a workflow, whoever registered them. The runner
 * itself goes through its scope; this is for hosts that track runs their
 * own way.
 */
export function clearQueryHandlers(workflowId: string): void {
  registry.delete(workflowId);
}
