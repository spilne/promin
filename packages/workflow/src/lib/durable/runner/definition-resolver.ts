// ---------------------------------------------------------------------------
// Definition resolver — picks the workflow definition that drives a run:
// by name + version through the registry for `run({ name })`, and the
// matching `previousVersions` entry when a stored run must drain under the
// version it was created with.
// ---------------------------------------------------------------------------

import type { Workflow } from "../durable-pipeline.ts";
import { WorkflowVersionMismatchError } from "../durable-pipeline-error.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { WorkflowVersionRegistry } from "../workflow-version-registry.ts";
import {
  orchestrationContextFor,
  runtimeOf,
  type WorkflowOrchestrationContext,
} from "./orchestration-context.ts";

/**
 * Resolve the definition for `run({ name, version? })`. Implements
 * version-drain-resume: if the workflow row already exists under a
 * different version, the matching older definition continues the run.
 */
export async function resolveRunDefinition(params: {
  registry: WorkflowVersionRegistry | undefined;
  storage: WorkflowStorage;
  name: string;
  version: string | undefined;
  workflowId: string;
}): Promise<Workflow<unknown, unknown>> {
  const { registry, storage, workflowId } = params;
  if (!registry) {
    throw new Error(
      `WorkflowRunner.run({ name }) requires a registry on the runner config. ` +
        `Pass \`createWorkflowRunner({ storage, registry })\`.`,
    );
  }

  // Resolution order when the caller doesn't supply a version:
  //   1. `findActive(name)` — explicit `promote()` target if any.
  //   2. `latest(name)` (the registry's existing fallback inside
  //      `resolve(name, undefined)`) — last-registered version.
  //
  // The first call lets `promote/rollback` actually shift dispatch
  // without breaking pre-promote workflows: when nothing's been
  // promoted, `findActive` returns null and we drop through to the
  // legacy path. When the caller passes an explicit version, neither
  // hook fires — the explicit version always wins.
  let resolvedVersion = params.version;
  if (!resolvedVersion && typeof registry.findActive === "function") {
    const active = await registry.findActive(params.name);
    if (active) resolvedVersion = active.version;
  }
  const latestDef = await registry.resolve(params.name, resolvedVersion);
  if (!latestDef) {
    const allNames = await registry.names();
    throw new Error(
      `No workflow "${params.name}"${resolvedVersion ? ` version "${resolvedVersion}"` : ""} in registry. ` +
        `Registered: ${(allNames as string[]).join(", ") || "(none)"}.`,
    );
  }

  const existing = await storage.loadWorkflow(workflowId);
  if (existing && existing.version && existing.version !== latestDef.version) {
    const storedDef = await registry.resolve(params.name, existing.version);
    if (!storedDef) {
      const allVersions = await registry.versions(params.name);
      throw new Error(
        `Workflow "${params.name}" version "${existing.version}" not found in registry. ` +
          `Available versions: ${(allVersions as string[]).join(", ")}. ` +
          `Keep old definitions registered until in-flight workflows drain.`,
      );
    }
    return storedDef;
  }

  return latestDef;
}

/**
 * Drain pre-check — if the stored workflow was created under a different
 * version and this definition has `onVersionMismatch: "drain"`, return an
 * orchestration context for the matching previousVersion definition, which
 * should drive the whole run. Returns `undefined` when no drain applies.
 * Stored version is immutable per workflow, so this is race-safe.
 */
export async function resolveDrainContext(params: {
  ctx: WorkflowOrchestrationContext;
  workflowId: string;
}): Promise<WorkflowOrchestrationContext | undefined> {
  const { ctx, workflowId } = params;
  if (ctx.onVersionMismatch === "drain" && ctx.version) {
    const existing = await ctx.storage.loadWorkflow(workflowId);
    if (existing && existing.version !== ctx.version) {
      const previousDef = ctx.previousVersions?.find((d) => d.version === existing.version);
      if (!previousDef) {
        throw new WorkflowVersionMismatchError({
          workflowId,
          expected: ctx.version,
          actual: existing.version ?? "(none)",
          message:
            `Workflow "${workflowId}" was created with version "${existing.version ?? "(none)"}" ` +
            `but current code is version "${ctx.version}". ` +
            `onVersionMismatch is "drain" but no matching previousVersion was registered.`,
        });
      }
      // Delegate drain to the previous version through its own
      // orchestration context on this run's runtime — same storage (one
      // source of truth for the stored state), clock, step executor and
      // executor id — keeping the hooks of the run it takes over.
      return orchestrationContextFor({
        workflow: previousDef,
        runtime: runtimeOf(ctx),
        ...(ctx.hooks !== undefined && { hooks: ctx.hooks }),
      });
    }
  }
  return undefined;
}
