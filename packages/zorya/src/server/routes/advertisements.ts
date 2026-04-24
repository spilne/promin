// ---------------------------------------------------------------------------
// Workflow advertisement routes.
//
// POST   /api/advertisements             — upsert for a worker (body: { workerId, workflows })
// DELETE /api/advertisements/:workerId   — remove
// GET    /api/advertisements             — list (debugging)
// ---------------------------------------------------------------------------

import { json, jsonError, readJson } from "../router.ts";
import type {
  AdvertisedWorkflow,
  WorkflowAdvertisementRegistry,
} from "../workflow-advertisements.ts";

interface UpsertBody {
  workerId?: string;
  workflows?: AdvertisedWorkflow[];
}

export function upsertAdvertisements(registry: WorkflowAdvertisementRegistry) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<UpsertBody>(req);
    if (!body || !body.workerId || !Array.isArray(body.workflows)) {
      return jsonError(400, "invalid_body", "Expected { workerId, workflows }");
    }
    await registry.upsert(body.workerId, body.workflows);
    return json(200, { ok: true });
  };
}

export function removeAdvertisements(registry: WorkflowAdvertisementRegistry) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.workerId;
    if (!id) return jsonError(400, "missing_workerId");
    await registry.remove(id);
    return json(200, { ok: true });
  };
}

export function listAdvertisements(registry: WorkflowAdvertisementRegistry) {
  return async (): Promise<Response> => {
    const entries = await registry.list();
    return json(200, { entries });
  };
}
