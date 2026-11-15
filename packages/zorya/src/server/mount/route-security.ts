import type { ProtectedRouteAccess, RouteTargetRequest } from "../router.ts";
import { assertUniqueQueryParameters } from "../request-validation.ts";
import {
  ORG_WIDE_NAMESPACE,
  type SecurityAction,
  type SecurityResourceKind,
  type SecurityResourceRef,
} from "../security/index.ts";

type Target<K extends SecurityResourceKind> = (
  request: RouteTargetRequest,
) => SecurityResourceRef<K> | Promise<SecurityResourceRef<K>>;

/** Small typed adapter used while mount groups migrate to declared route security. */
export function protectedAccess<K extends SecurityResourceKind>(params: {
  readonly resource: K;
  readonly action: SecurityAction<K>;
  readonly target?: Target<K>;
}): ProtectedRouteAccess {
  const target = params.target;
  return {
    kind: "protected",
    permission: { resource: params.resource, action: params.action },
    resolveTarget: async (request) => ({
      resource: target
        ? await target(request)
        : { kind: params.resource, namespaceId: ORG_WIDE_NAMESPACE },
    }),
  } as ProtectedRouteAccess;
}

/** Deployment-wide target, optionally naming a path resource. */
export function orgTarget<K extends SecurityResourceKind>(params: {
  readonly kind: K;
  readonly idParam?: string;
}): Target<K> {
  return ({ params: route }) => ({
    kind: params.kind,
    namespaceId: ORG_WIDE_NAMESPACE,
    ...(params.idParam !== undefined && route[params.idParam] !== undefined
      ? { id: route[params.idParam] }
      : {}),
  });
}

/** Namespace query target; missing/invalid input stays org-scoped until the handler returns 400. */
export function queryNamespaceTarget<K extends SecurityResourceKind>(params: {
  readonly kind: K;
  readonly idParam?: string;
  readonly query?: string;
}): Target<K> {
  return ({ request, params: route }) => {
    const query = params.query ?? "namespaceId";
    const url = new URL(request.url);
    assertUniqueQueryParameters({ url, names: new Set([query]) });
    const namespaceId = url.searchParams.get(query) || ORG_WIDE_NAMESPACE;
    return {
      kind: params.kind,
      namespaceId,
      ...(params.idParam !== undefined && route[params.idParam] !== undefined
        ? { id: route[params.idParam] }
        : {}),
    };
  };
}

/** Read a namespace from a cloned JSON request without consuming the handler's body. */
export function bodyNamespaceTarget<K extends SecurityResourceKind>(params: {
  readonly kind: K;
  readonly idParam?: string;
}): Target<K> {
  return async ({ request, params: route }) => {
    let namespaceId = ORG_WIDE_NAMESPACE;
    try {
      const body = (await request.clone().json()) as Record<string, unknown>;
      if (typeof body.namespaceId === "string" && body.namespaceId.length > 0) {
        namespaceId = body.namespaceId;
      }
    } catch {
      // Handler-owned validation produces the existing 4xx response.
    }
    return {
      kind: params.kind,
      namespaceId,
      ...(params.idParam !== undefined && route[params.idParam] !== undefined
        ? { id: route[params.idParam] }
        : {}),
    };
  };
}
