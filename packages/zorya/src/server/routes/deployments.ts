// ---------------------------------------------------------------------------
// Deployments — thin read/write surface over WorkflowVersionRegistry's
// lifecycle methods (`promote`, `rollback`, `findActive`, `listRecords`).
//
// "Deployment" in our model = a registered workflow version with a
// lifecycle status (`inactive | active | archived`). It's not a separate
// entity from the version registry — promote/rollback drive the
// `findActive(name)` pointer the auto-mint trigger path will route on.
//
// Routes (Phase 1 — coordinator routing on `findActive` is a follow-up):
//   GET  /api/deployments?name=...        — list version records for a workflow.
//   GET  /api/deployments/:name/active    — current active version.
//   POST /api/deployments/:name/promote   — promote (body: { version }).
//   POST /api/deployments/:name/rollback  — rollback (body: { toVersion }).
// ---------------------------------------------------------------------------

import type { IWorkflowVersionRegistry, VersionRecord } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";

export interface DeploymentRoutesDeps {
  readonly registry: IWorkflowVersionRegistry;
}

export interface DeploymentDto {
  readonly name: string;
  readonly version: string;
  readonly status: "inactive" | "active" | "archived";
  readonly contentHash: string | null;
  readonly registeredAt: string;
  readonly activeAt: string | null;
  readonly archivedAt: string | null;
}

function toDto(record: VersionRecord): DeploymentDto {
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

function requireLifecycle(deps: DeploymentRoutesDeps): {
  readonly findActive: NonNullable<IWorkflowVersionRegistry["findActive"]>;
  readonly listRecords: NonNullable<IWorkflowVersionRegistry["listRecords"]>;
  readonly promote: NonNullable<IWorkflowVersionRegistry["promote"]>;
  readonly rollback: NonNullable<IWorkflowVersionRegistry["rollback"]>;
} | null {
  const r = deps.registry;
  if (!r.findActive || !r.listRecords || !r.promote || !r.rollback) return null;
  return {
    findActive: r.findActive.bind(r),
    listRecords: r.listRecords.bind(r),
    promote: r.promote.bind(r),
    rollback: r.rollback.bind(r),
  };
}

/**
 * List every version record for one workflow. Most-recent first.
 * Returns 501 when the configured registry doesn't support lifecycle.
 */
export function listDeployments(deps: DeploymentRoutesDeps) {
  return async (req: Request): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) {
      return jsonError(
        501,
        "registry_lacks_lifecycle",
        "Configured registry does not implement promote/rollback.",
      );
    }
    const url = new URL(req.url);
    const name = url.searchParams.get("name");
    if (!name) return jsonError(400, "missing_name");
    const records = await lifecycle.listRecords(name);
    return json(200, { deployments: records.map(toDto) });
  };
}

export function getActiveDeployment(deps: DeploymentRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) {
      return jsonError(501, "registry_lacks_lifecycle");
    }
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const record = await lifecycle.findActive(name);
    if (!record) return jsonError(404, "no_active_deployment");
    return json(200, toDto(record));
  };
}

export interface PromoteRequest {
  readonly version: string;
}

export function promoteDeployment(deps: DeploymentRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) {
      return jsonError(501, "registry_lacks_lifecycle");
    }
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const body = await readJson<PromoteRequest>(req);
    if (!body || !body.version) return jsonError(400, "missing_version");
    try {
      const record = await lifecycle.promote(name, body.version);
      return json(200, { deployment: toDto(record) });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (message.includes("not registered")) {
        return jsonError(404, "version_not_registered", message);
      }
      return jsonError(500, "promote_failed", message);
    }
  };
}

export interface RollbackRequest {
  readonly toVersion: string;
}

export function rollbackDeployment(deps: DeploymentRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) {
      return jsonError(501, "registry_lacks_lifecycle");
    }
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const body = await readJson<RollbackRequest>(req);
    if (!body || !body.toVersion) return jsonError(400, "missing_to_version");
    try {
      const result = await lifecycle.rollback({ name, toVersion: body.toVersion });
      return json(200, {
        previous: toDto(result.previous),
        active: toDto(result.active),
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return jsonError(500, "rollback_failed", message);
    }
  };
}
