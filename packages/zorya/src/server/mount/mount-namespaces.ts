// ---------------------------------------------------------------------------
// Namespaces API. Without a registry only `GET /api/namespaces` answers,
// with `configured: false`, so the dashboard can tell "free-form
// namespaces" from "no namespaces registered".
// ---------------------------------------------------------------------------

import {
  NamespaceExistsError,
  isValidNamespaceId,
  type NamespaceRegistry,
  type WorkflowStorage,
} from "@promin/workflow";
import type { SchedulerStorage } from "@promin/workflow/scheduler";
import type { Router } from "../router.ts";
import {
  createNamespace,
  deleteNamespace,
  getNamespace,
  listNamespaces,
  listNamespacesUnconfigured,
  updateNamespace,
  type NamespaceRoutesDeps,
} from "../routes/namespaces.ts";

export function mountNamespaces(params: {
  readonly router: Router;
  readonly registry: NamespaceRegistry | undefined;
  readonly storage: WorkflowStorage;
  readonly schedulerStorage: SchedulerStorage | undefined;
}): void {
  const { router, registry } = params;
  if (!registry) {
    router.get("/api/namespaces", listNamespacesUnconfigured());
    return;
  }
  const deps: NamespaceRoutesDeps = {
    registry,
    storage: params.storage,
    ...(params.schedulerStorage !== undefined && { schedulerStorage: params.schedulerStorage }),
  };
  router
    .get("/api/namespaces", listNamespaces(deps))
    .post("/api/namespaces", createNamespace(deps))
    .get("/api/namespaces/:id", getNamespace(deps))
    .patch("/api/namespaces/:id", updateNamespace(deps))
    .delete("/api/namespaces/:id", deleteNamespace(deps));
}

/**
 * Register, as active, every namespace runs in `storage` already use that
 * the registry lacks — so introducing a registry to an existing deployment
 * keeps its tenants usable. Ids that are not well-formed are skipped; a
 * concurrent adoption by another instance is not an error. Returns the
 * adopted ids. The server runs it once at start.
 */
export async function adoptStoredNamespaces(params: {
  readonly registry: NamespaceRegistry;
  readonly storage: WorkflowStorage;
}): Promise<string[]> {
  const adopted: string[] = [];
  for (const id of await params.storage.distinctNamespaces()) {
    if (!isValidNamespaceId(id) || (await params.registry.get(id))) continue;
    try {
      await params.registry.create({ id });
      adopted.push(id);
    } catch (err) {
      if (!(err instanceof NamespaceExistsError)) throw err;
    }
  }
  return adopted;
}
