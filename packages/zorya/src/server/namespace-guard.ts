// ---------------------------------------------------------------------------
// Namespace guard — with a namespace registry configured
// (`ZoryaServerConfig.namespaces`), routes that put new work or state into a
// namespace check it first: triggering a run, creating a schedule, invoking
// an agent, writing namespace memory, secrets or agent instances, and
// accepting a webhook delivery. Reads and filters are not checked: a query
// for an unknown namespace simply matches nothing, and archived namespaces
// stay readable. Requests without a namespace (the global namespace of runs
// and schedules) are always allowed. Without a registry nothing is checked.
// ---------------------------------------------------------------------------

import type { SecretScope } from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import { HttpError } from "./router.ts";

/** The namespace a secret scope writes into; undefined for the global scope. */
export function secretScopeNamespace(scope: SecretScope): string | undefined {
  return scope.kind === "global" ? undefined : scope.namespaceId;
}

/**
 * Throw `HttpError` 404 `namespace_not_found` unless `namespace` is
 * registered, 409 `namespace_archived` when it is archived. A no-op without
 * a registry or without a namespace.
 */
export async function requireActiveNamespace(params: {
  readonly registry: NamespaceRegistry | undefined;
  readonly namespace: string | undefined;
}): Promise<void> {
  const { registry, namespace } = params;
  if (!registry || namespace === undefined) return;
  const record = await registry.get(namespace);
  if (!record) {
    throw new HttpError({
      status: 404,
      code: "namespace_not_found",
      message: `Namespace "${namespace}" is not registered`,
    });
  }
  if (record.status !== "active") {
    throw new HttpError({
      status: 409,
      code: "namespace_archived",
      message: `Namespace "${namespace}" is archived`,
    });
  }
}
