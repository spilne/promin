// ---------------------------------------------------------------------------
// Lifecycle hooks — the single place the runner invokes `WorkflowHooks`.
// ---------------------------------------------------------------------------

import type { WorkflowHooks } from "../durable-pipeline.ts";

type HookName = Exclude<keyof WorkflowHooks, "onHookError">;
type HookEvent<K extends HookName> = Parameters<NonNullable<WorkflowHooks[K]>>[0];

/**
 * Invoke the named lifecycle hook, if one is configured, and wait for it.
 * Hooks are observers: a hook that throws or rejects is reported to
 * `hooks.onHookError` (default `console.error`) and never changes the run's
 * outcome, so this never rejects.
 */
export async function fireHook<K extends HookName>(params: {
  hooks: WorkflowHooks | undefined;
  name: K;
  event: HookEvent<K>;
}): Promise<void> {
  const { hooks, name, event } = params;
  const hook = hooks?.[name] as ((event: HookEvent<K>) => void | Promise<void>) | undefined;
  if (hook === undefined) return;
  try {
    await hook.call(hooks, event);
  } catch (error) {
    reportHookError({ hooks, hook: name, workflowId: event.workflowId, error });
  }
}

function reportHookError(params: {
  hooks: WorkflowHooks | undefined;
  hook: HookName;
  workflowId: string;
  error: unknown;
}): void {
  const { hooks, hook, workflowId, error } = params;
  try {
    if (hooks?.onHookError) {
      hooks.onHookError({ workflowId, hook, error });
      return;
    }
    console.error(`[workflow] ${hook} hook failed for "${workflowId}":`, error);
  } catch {
    // A failing error reporter has nowhere left to report to.
  }
}
