// ---------------------------------------------------------------------------
// Lifecycle hooks — the single place the runner invokes `WorkflowHooks`.
// ---------------------------------------------------------------------------

import type { WorkflowHooks } from "../durable-pipeline.ts";

type HookName = keyof WorkflowHooks;
type HookEvent<K extends HookName> = Parameters<NonNullable<WorkflowHooks[K]>>[0];

/**
 * Invoke the named lifecycle hook, if one is configured. The hook's return
 * value is passed straight through, so a rejecting or throwing hook
 * propagates to the caller exactly as a direct call would.
 */
export function fireHook<K extends HookName>(params: {
  hooks: WorkflowHooks | undefined;
  name: K;
  event: HookEvent<K>;
}): void | Promise<void> {
  const hook = params.hooks?.[params.name] as
    | ((event: HookEvent<K>) => void | Promise<void>)
    | undefined;
  return hook?.call(params.hooks, params.event);
}
