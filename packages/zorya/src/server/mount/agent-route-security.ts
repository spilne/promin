import type { PendingApprovalStore } from "@promin/agent";
import { HttpError, type DynamicRouteTarget, type RouteTargetRequest } from "../router.ts";
import { assertUniqueQueryParameters } from "../request-validation.ts";
import {
  requestPrincipal,
  setRequestSecurityContext,
  type SecurityResourceKind,
  type SecurityResourceRef,
  type ZoryaSecurity,
} from "../security/index.ts";
import { ORG_WIDE_NAMESPACE } from "../security/security.ts";
import { deriveScope, type ScopeFields } from "../routes/agents/agent-scope.ts";

interface ScopeInput {
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly ownerId?: unknown;
}

/**
 * Derive the same principal-bound scope the agent handler will use and
 * attach the actor/effective context before the central route guard runs.
 */
async function secureScope(params: {
  readonly request: Request;
  readonly security?: ZoryaSecurity;
  readonly requested: ScopeInput;
}): Promise<ScopeFields> {
  const actor = requestPrincipal(params.request);
  const scope = deriveScope({ principal: actor, requested: params.requested });
  if ("error" in scope) {
    throw new HttpError({
      status: scope.status,
      code: scope.error,
      ...(scope.message !== undefined && { message: scope.message }),
    });
  }
  if (!actor || !params.security) return scope;

  const representedId = scope.resourceId ?? scope.ownerId;
  const context = await params.security.resolveIdentity({
    actor,
    ...(actor.canActFor && representedId !== undefined
      ? { asserted: { kind: "user" as const, id: representedId } }
      : {}),
  });
  if (!context) throw new HttpError({ status: 403, code: "unknown_user" });
  setRequestSecurityContext({ req: params.request, context });
  return scope;
}

function target<K extends SecurityResourceKind>(params: {
  readonly kind: K;
  readonly scope: ScopeFields;
  readonly route: Readonly<Record<string, string>>;
  readonly idParam?: string;
  readonly bodyId?: string;
  readonly identityId?: boolean;
}): SecurityResourceRef<K> {
  const routeId = params.idParam === undefined ? undefined : params.route[params.idParam];
  const id = params.identityId
    ? (params.scope.resourceId ?? params.scope.ownerId)
    : (params.bodyId ?? routeId);
  return {
    kind: params.kind,
    namespaceId: params.scope.namespaceId,
    ...(id !== undefined && { id }),
  };
}

/** Scope-bearing JSON body target. The clone leaves the handler's body untouched. */
export function agentBodyTarget<K extends SecurityResourceKind>(params: {
  readonly kind: K;
  readonly security?: ZoryaSecurity;
  readonly idParam?: string;
  readonly idField?: string;
  readonly identityId?: boolean;
}) {
  return async ({
    request,
    params: route,
  }: RouteTargetRequest): Promise<SecurityResourceRef<K>> => {
    let requested: ScopeInput = {};
    let rawBody: Readonly<Record<string, unknown>> = {};
    try {
      const raw = (await request.clone().json()) as unknown;
      if (raw && typeof raw === "object" && !Array.isArray(raw)) {
        rawBody = raw as Readonly<Record<string, unknown>>;
        requested = rawBody;
      }
    } catch {
      // secureScope produces the same missing-scope 400 as the handler.
    }
    const scope = await secureScope({ request, security: params.security, requested });
    const bodyId = params.idField === undefined ? undefined : rawBody[params.idField];
    return target({
      kind: params.kind,
      scope,
      route,
      ...(params.idParam !== undefined && { idParam: params.idParam }),
      ...(typeof bodyId === "string" && { bodyId }),
      ...(params.identityId !== undefined && { identityId: params.identityId }),
    });
  };
}

/** Scope-bearing query target. Duplicate identity keys are always ambiguous. */
export function agentQueryTarget<K extends SecurityResourceKind>(params: {
  readonly kind: K;
  readonly security?: ZoryaSecurity;
  readonly idParam?: string;
  readonly identityId?: boolean;
}) {
  return async ({
    request,
    params: route,
  }: RouteTargetRequest): Promise<SecurityResourceRef<K>> => {
    const url = new URL(request.url);
    assertUniqueQueryParameters({
      url,
      names: new Set(["namespaceId", "resourceId", "ownerId"]),
    });
    const scope = await secureScope({
      request,
      security: params.security,
      requested: {
        namespaceId: url.searchParams.get("namespaceId") ?? undefined,
        resourceId: url.searchParams.get("resourceId") ?? undefined,
        ownerId: url.searchParams.get("ownerId") ?? undefined,
      },
    });
    return target({
      kind: params.kind,
      scope,
      route,
      ...(params.idParam !== undefined && { idParam: params.idParam }),
      ...(params.identityId !== undefined && { identityId: params.identityId }),
    });
  };
}

/** Query target for namespace-scoped non-thread surfaces. */
export function namespaceQueryTarget<K extends SecurityResourceKind>(params: {
  readonly kind: K;
  readonly idParam?: string;
  readonly identityQuery?: string;
}) {
  return ({ request, params: route }: RouteTargetRequest): SecurityResourceRef<K> => {
    const url = new URL(request.url);
    const identityQuery = params.identityQuery;
    const names = new Set(["namespaceId"]);
    if (identityQuery !== undefined) names.add(identityQuery);
    assertUniqueQueryParameters({ url, names });
    const namespaceId = url.searchParams.get("namespaceId") || ORG_WIDE_NAMESPACE;
    const routeId = params.idParam === undefined ? undefined : route[params.idParam];
    const identity = identityQuery === undefined ? undefined : url.searchParams.get(identityQuery);
    const id = routeId ?? identity ?? undefined;
    return { kind: params.kind, namespaceId, ...(id !== undefined && { id }) };
  };
}

/** Resolve the stored approval before the guard selects ordinary or elevated permission. */
export function approvalBodyTarget(params: { readonly approvals?: PendingApprovalStore }) {
  return async ({
    request,
    params: route,
  }: RouteTargetRequest): Promise<DynamicRouteTarget<"approval">> => {
    let body: Readonly<Record<string, unknown>> = {};
    try {
      const raw = (await request.clone().json()) as unknown;
      if (raw && typeof raw === "object" && !Array.isArray(raw)) {
        body = raw as Readonly<Record<string, unknown>>;
      }
    } catch {
      // Handler validation owns malformed-body responses.
    }
    const namespaceId = typeof body.namespaceId === "string" ? body.namespaceId : "org";
    const toolCallId = typeof body.toolCallId === "string" ? body.toolCallId : "unknown";
    const threadId = route.threadId;
    const found =
      params.approvals && threadId && namespaceId !== "org" && toolCallId !== "unknown"
        ? await params.approvals.get({ namespaceId, threadId, toolCallId })
        : null;
    const approval = found && (found.agentId === null || found.agentId === route.id) ? found : null;
    // Unknown legacy rows cannot be treated as ordinary. They take the
    // elevated path, and the handler rejects them if requester identity is absent.
    const elevated = approval?.elevated !== false;
    return {
      permission: {
        resource: "approval",
        action: elevated ? "decide-elevated" : "decide",
      },
      resource: {
        kind: "approval",
        namespaceId: approval?.namespaceId ?? namespaceId,
        id: elevated
          ? (approval?.toolCallId ?? toolCallId)
          : (approval?.requestContext?.actor.id ?? "legacy-requester"),
      },
      value: approval,
    };
  };
}
