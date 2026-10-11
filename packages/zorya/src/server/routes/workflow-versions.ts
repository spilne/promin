// ---------------------------------------------------------------------------
// Workflow versions — read/write surface over WorkflowVersionRegistry's
// lifecycle methods. Routes live under `/api/workflows/:name/versions/...`
// because version status is a property of a workflow, not its own
// resource.
//
// Routes:
//   GET  /api/workflows/:name/versions                — list version records.
//   GET  /api/workflows/:name/versions/active         — current active version.
//   POST /api/workflows/:name/versions/:version/promote
//                                                     — promote to active.
//   POST /api/workflows/:name/rollback                — rollback (body: { toVersion }).
//
// The registry is the one the workflows layer dispatches through, so a
// promote here changes which version new runs start under. Errors are
// decided before the registry call, from its own records, so every
// backend answers the same: 404 `version_not_registered` for an unknown
// version, 409 `no_active_version` for a rollback with nothing promoted.
// Without a registry the routes answer 501 `versioning_not_configured`.
// ---------------------------------------------------------------------------

import type { Workflow, WorkflowVersionRegistry, VersionRecord } from "@promin/workflow";
import { z } from "zod";
import { json, jsonError } from "../router.ts";
import { NameSchema, RollbackRequestSchema } from "../api-types.ts";
import { parseBody, parseParams } from "../request-validation.ts";
import { WorkflowParamsSchema } from "./runs.ts";

export interface WorkflowVersionsRoutesDeps {
  /** The workflows layer's registry; absent when versioning isn't configured. */
  readonly registry?: WorkflowVersionRegistry;
  /** Runs before each route touches the registry (see `createVersionSeeder`). */
  readonly prepare?: () => Promise<void>;
}

/**
 * Register every versioned definition the server runs (and the previous
 * versions it still serves) that the registry doesn't list yet. Runs once;
 * a failure is retried on the next call.
 */
export function createVersionSeeder(params: {
  registry: WorkflowVersionRegistry;
  definitions: ReadonlyArray<Workflow<unknown, unknown>>;
}): () => Promise<void> {
  const { registry, definitions } = params;
  let seeded: Promise<void> | undefined;
  const seed = async (): Promise<void> => {
    const all = definitions.flatMap((d) => [d, ...(d._definition.previousVersions ?? [])]);
    for (const def of all) {
      if (def.version === undefined) continue;
      const known = await registry.versions(def.name);
      if (!known.includes(def.version)) await registry.register(def);
    }
  };
  return () => {
    seeded ??= seed().catch((err: unknown) => {
      seeded = undefined;
      throw err;
    });
    return seeded;
  };
}

export interface WorkflowVersionDto {
  readonly name: string;
  readonly version: string;
  readonly status: "inactive" | "active" | "archived";
  readonly contentHash: string | null;
  readonly registeredAt: string;
  readonly activeAt: string | null;
  readonly archivedAt: string | null;
}

export type { RollbackRequest } from "../api-types.ts";

const PromoteParamsSchema = z.object({ name: NameSchema, version: NameSchema });

function toDto(record: VersionRecord): WorkflowVersionDto {
  return {
    name: record.name,
    version: record.version,
    status: record.status,
    contentHash: record.contentHash,
    registeredAt: record.registeredAt.toISOString(),
    activeAt: record.activeAt ? record.activeAt.toISOString() : null,
    archivedAt: record.archivedAt ? record.archivedAt.toISOString() : null,
  };
}

interface Lifecycle {
  readonly findActive: NonNullable<WorkflowVersionRegistry["findActive"]>;
  readonly listRecords: NonNullable<WorkflowVersionRegistry["listRecords"]>;
  readonly promote: NonNullable<WorkflowVersionRegistry["promote"]>;
  readonly rollback: NonNullable<WorkflowVersionRegistry["rollback"]>;
}

/** The registry's lifecycle methods, or the 501 explaining why there are none. */
async function lifecycleOf(deps: WorkflowVersionsRoutesDeps): Promise<Lifecycle | Response> {
  const r = deps.registry;
  if (!r) {
    return jsonError(
      501,
      "versioning_not_configured",
      "The workflows layer has no version registry.",
    );
  }
  if (!r.findActive || !r.listRecords || !r.promote || !r.rollback) {
    return jsonError(
      501,
      "registry_lacks_lifecycle",
      "The configured version registry does not implement promote/rollback.",
    );
  }
  await deps.prepare?.();
  return {
    findActive: r.findActive.bind(r),
    listRecords: r.listRecords.bind(r),
    promote: r.promote.bind(r),
    rollback: r.rollback.bind(r),
  };
}

export function listWorkflowVersions(deps: WorkflowVersionsRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { name } = parseParams({ params, schema: WorkflowParamsSchema });
    // Unversioned deployments list nothing; the dashboard hides the panel.
    if (!deps.registry) return json(200, { versions: [] });
    const lifecycle = await lifecycleOf(deps);
    if (lifecycle instanceof Response) return lifecycle;
    const records = await lifecycle.listRecords(name);
    return json(200, { versions: records.map(toDto) });
  };
}

export function getActiveWorkflowVersion(deps: WorkflowVersionsRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { name } = parseParams({ params, schema: WorkflowParamsSchema });
    const lifecycle = await lifecycleOf(deps);
    if (lifecycle instanceof Response) return lifecycle;
    const record = await lifecycle.findActive(name);
    if (!record) return jsonError(404, "no_active_version");
    return json(200, toDto(record));
  };
}

export function promoteWorkflowVersion(deps: WorkflowVersionsRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { name, version } = parseParams({ params, schema: PromoteParamsSchema });
    const lifecycle = await lifecycleOf(deps);
    if (lifecycle instanceof Response) return lifecycle;
    const records = await lifecycle.listRecords(name);
    if (!records.some((r) => r.version === version)) {
      return versionNotRegistered({ name, version });
    }
    const record = await lifecycle.promote({ name, version });
    return json(200, { version: toDto(record) });
  };
}

export function rollbackWorkflow(deps: WorkflowVersionsRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { name } = parseParams({ params, schema: WorkflowParamsSchema });
    const { toVersion } = await parseBody({ req, schema: RollbackRequestSchema });
    const lifecycle = await lifecycleOf(deps);
    if (lifecycle instanceof Response) return lifecycle;
    const records = await lifecycle.listRecords(name);
    if (!records.some((r) => r.version === toVersion)) {
      return versionNotRegistered({ name, version: toVersion });
    }
    const active = records.find((r) => r.status === "active");
    if (!active || active.version === toVersion) {
      return jsonError(
        409,
        "no_active_version",
        `No other active version of "${name}" to roll back from`,
      );
    }
    const result = await lifecycle.rollback({ name, toVersion });
    return json(200, {
      previous: toDto(result.previous),
      active: toDto(result.active),
    });
  };
}

function versionNotRegistered(params: { name: string; version: string }): Response {
  return jsonError(
    404,
    "version_not_registered",
    `${params.name}@${params.version} is not registered`,
  );
}
