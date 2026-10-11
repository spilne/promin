// ---------------------------------------------------------------------------
// Workflow advertisement routes. Workers advertise the workflows (and
// versions) they host on start and then periodically; an advertisement not
// refreshed within the registry's TTL expires.
//
// POST   /api/advertisements             — upsert for a worker (body: { workerId, workflows })
// DELETE /api/advertisements/:workerId   — remove
// GET    /api/advertisements             — list live entries (debugging)
// ---------------------------------------------------------------------------

import { json } from "../router.ts";
import { parseBody, parseParams } from "../request-validation.ts";
import { UpsertAdvertisementsRequestSchema, WorkerParamsSchema } from "../api-types.ts";
import type { WorkflowAdvertisementRegistry } from "@promin/workflow/distributed";

export function upsertAdvertisements(registry: WorkflowAdvertisementRegistry) {
  return async (req: Request): Promise<Response> => {
    const { workerId, workflows } = await parseBody({
      req,
      schema: UpsertAdvertisementsRequestSchema,
    });
    await registry.upsert({
      workerId,
      workflows: workflows.map((wf) => ({
        name: wf.name,
        steps: wf.steps.map((s) => ({
          name: s.name,
          kind: s.kind,
          dependsOn: s.dependsOn,
          ...(s.needs !== undefined && { needs: s.needs }),
          ...(s.priority !== undefined && { priority: s.priority }),
        })),
        ...(wf.version !== undefined && { version: wf.version }),
        ...(wf.sampleInput !== undefined && { sampleInput: wf.sampleInput }),
      })),
    });
    return json(200, { ok: true });
  };
}

export function removeAdvertisements(registry: WorkflowAdvertisementRegistry) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { workerId } = parseParams({ params, schema: WorkerParamsSchema });
    await registry.remove(workerId);
    return json(200, { ok: true });
  };
}

export function listAdvertisements(registry: WorkflowAdvertisementRegistry) {
  return async (): Promise<Response> => {
    const entries = await registry.list();
    return json(200, { entries });
  };
}
