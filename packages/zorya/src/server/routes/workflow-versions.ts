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
//   DELETE /api/workflows/:name/versions/:version     — deregister/unpublish.
// ---------------------------------------------------------------------------

import type { IWorkflowVersionRegistry, VersionRecord } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";

export interface WorkflowVersionsRoutesDeps {
  readonly registry: IWorkflowVersionRegistry;
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

function requireLifecycle(deps: WorkflowVersionsRoutesDeps): {
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

export function listWorkflowVersions(deps: WorkflowVersionsRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) {
      return jsonError(
        501,
        "registry_lacks_lifecycle",
        "Configured registry does not implement promote/rollback.",
      );
    }
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const records = await lifecycle.listRecords(name);
    return json(200, { versions: records.map(toDto) });
  };
}

export function getActiveWorkflowVersion(deps: WorkflowVersionsRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) return jsonError(501, "registry_lacks_lifecycle");
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const record = await lifecycle.findActive(name);
    if (!record) return jsonError(404, "no_active_version");
    return json(200, toDto(record));
  };
}

export function promoteWorkflowVersion(deps: WorkflowVersionsRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) return jsonError(501, "registry_lacks_lifecycle");
    const name = params.name;
    const version = params.version;
    if (!name) return jsonError(400, "missing_name");
    if (!version) return jsonError(400, "missing_version");
    try {
      const record = await lifecycle.promote(name, version);
      return json(200, { version: toDto(record) });
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

export function rollbackWorkflow(deps: WorkflowVersionsRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const lifecycle = requireLifecycle(deps);
    if (!lifecycle) return jsonError(501, "registry_lacks_lifecycle");
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const body = await readJson<RollbackRequest>(req);
    if (!body || !body.toVersion) {
      return jsonError(400, "missing_to_version");
    }
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

export function deleteWorkflowVersion(deps: WorkflowVersionsRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const name = params.name;
    const version = params.version;
    if (!name) return jsonError(400, "missing_name");
    if (!version) return jsonError(400, "missing_version");
    const active = await deps.registry.findActive?.(name);
    const force = new URL(req.url).searchParams.get("force") === "true";
    if (active?.version === version && !force) {
      return jsonError(
        409,
        "active_version",
        "Refusing to delete the active workflow version without force=true.",
      );
    }
    const before = await deps.registry.resolve(name, version);
    if (!before) return jsonError(404, "version_not_registered");
    await deps.registry.deregister(name, version);
    return json(200, { ok: true });
  };
}
