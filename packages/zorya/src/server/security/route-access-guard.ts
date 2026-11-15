import type { WallClock } from "@promin/workflow";
import { jsonError, type RouteAccessGuard } from "../router.ts";
import type { Logger } from "../server-config.ts";
import { requestSecurityContext } from "./request-principal.ts";
import {
  AUTHORIZATION_REASONS,
  type AuthorizationDecision,
  type AuthorizationRequest,
  type Principal,
  type PrincipalKind,
  type PrincipalRef,
  type SecurityAuditEvent,
  type SecurityContext,
  type SecurityPermission,
  type SecurityResourceRef,
  type ZoryaSecurity,
} from "./security.ts";

export interface SecurityRouteAccessGuardConfig {
  readonly security: ZoryaSecurity;
  readonly clock: Pick<WallClock, "now">;
  readonly logger: Pick<Logger, "error">;
}

/** Core protected-route admission shared by every declared HTTP surface. */
export function createSecurityRouteAccessGuard(
  config: SecurityRouteAccessGuardConfig,
): RouteAccessGuard {
  return async ({ request, access }) => {
    if (access.kind !== "protected") return undefined;

    const requestContext = requestSecurityContext(request);
    if (!requestContext) return forbidden();

    // Use one immutable snapshot for both policy and audit. A configured
    // adapter must not be able to change identity between those two steps.
    const context = snapshotSecurityContext(requestContext);
    const permission = snapshotSecurityPermission(access.permission);
    const resource = snapshotSecurityResource(access.target.resource);
    const metadata = Object.freeze({
      method: request.method,
      path: new URL(request.url).pathname,
    });

    const decision = snapshotAuthorizationDecision(
      await config.security.authorize({
        context,
        permission,
        resource,
      } as AuthorizationRequest),
    );
    const event: SecurityAuditEvent = Object.freeze({
      event: "authorization",
      occurredAt: config.clock.now().toISOString(),
      context,
      namespaceId: resource.namespaceId,
      resource,
      permission,
      decision,
      metadata,
    });

    if (decision.allowed) {
      try {
        await config.security.recordAudit(event);
      } catch (error) {
        reportAuditFailure(config.logger, "allowed", error);
        return jsonError(503, "security_audit_unavailable");
      }
      return undefined;
    }

    try {
      await config.security.recordAudit(event);
    } catch (error) {
      // Authorization already denied. Audit failure must never turn denial
      // into a retryable success path, but operators still need the signal.
      reportAuditFailure(config.logger, "denied", error);
    }
    return forbidden();
  };
}

const PRINCIPAL_KINDS: ReadonlySet<string> = new Set<PrincipalKind>([
  "operator",
  "worker",
  "user",
  "service-account",
]);
const REASONS: ReadonlySet<string> = new Set(AUTHORIZATION_REASONS);

export function snapshotAuthorizationDecision(
  source: AuthorizationDecision,
): AuthorizationDecision {
  if (!source || typeof source !== "object") throw new TypeError("Invalid authorization decision");
  const record = source as unknown as Record<string, unknown>;
  const allowed = record.allowed;
  const reason = record.reason;
  if (typeof allowed !== "boolean" || typeof reason !== "string" || !REASONS.has(reason)) {
    throw new TypeError("Invalid authorization decision");
  }
  return Object.freeze({ allowed, reason }) as AuthorizationDecision;
}

export function snapshotSecurityContext(source: SecurityContext): SecurityContext {
  if (!source || typeof source !== "object") throw new TypeError("Invalid security context");
  const record = source as unknown as Record<string, unknown>;
  const actor = snapshotPrincipal(record.actor as Principal);
  const effective = snapshotPrincipalRef(record.effective as PrincipalRef);
  return Object.freeze({ actor, effective });
}

function snapshotPrincipal(source: Principal): Principal {
  if (!source || typeof source !== "object") throw new TypeError("Invalid security principal");
  const record = source as unknown as Record<string, unknown>;
  const kind = record.kind;
  const id = record.id;
  const email = record.email;
  const accessRoles = record.accessRoles;
  const canActFor = record.canActFor;
  if (
    typeof kind !== "string" ||
    !PRINCIPAL_KINDS.has(kind) ||
    typeof id !== "string" ||
    id.length === 0 ||
    (email !== undefined && typeof email !== "string") ||
    typeof canActFor !== "boolean"
  ) {
    throw new TypeError("Invalid security principal");
  }
  const snapshot = {
    kind: kind as PrincipalKind,
    id,
    ...(email !== undefined && { email }),
    accessRoles: snapshotAccessRoles(accessRoles),
    canActFor,
  };
  return Object.freeze(snapshot);
}

function snapshotPrincipalRef(source: PrincipalRef): PrincipalRef {
  if (!source || typeof source !== "object") throw new TypeError("Invalid principal reference");
  const record = source as unknown as Record<string, unknown>;
  const kind = record.kind;
  const id = record.id;
  if (
    typeof kind !== "string" ||
    !PRINCIPAL_KINDS.has(kind) ||
    typeof id !== "string" ||
    id.length === 0
  ) {
    throw new TypeError("Invalid principal reference");
  }
  return Object.freeze({ kind: kind as PrincipalKind, id });
}

function snapshotAccessRoles(source: unknown): Readonly<Record<string, ReadonlyArray<string>>> {
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    throw new TypeError("Invalid principal access roles");
  }
  const input = source as Record<string, unknown>;
  const output = Object.create(null) as Record<string, ReadonlyArray<string>>;
  for (const namespaceId of Object.keys(input)) {
    const roles = input[namespaceId];
    if (!Array.isArray(roles)) {
      throw new TypeError("Invalid principal access roles");
    }
    const length = roles.length;
    const roleSnapshot: string[] = [];
    for (let index = 0; index < length; index++) {
      const role = roles[index];
      if (typeof role !== "string") throw new TypeError("Invalid principal access roles");
      roleSnapshot.push(role);
    }
    output[namespaceId] = Object.freeze(roleSnapshot);
  }
  return Object.freeze(output) as Readonly<Record<string, ReadonlyArray<string>>>;
}

export function snapshotSecurityPermission(source: SecurityPermission): SecurityPermission {
  const record = source as unknown as Record<string, unknown>;
  const resource = record.resource;
  const action = record.action;
  return Object.freeze({ resource, action }) as SecurityPermission;
}

export function snapshotSecurityResource(source: SecurityResourceRef): SecurityResourceRef {
  const record = source as unknown as Record<string, unknown>;
  const kind = record.kind;
  const namespaceId = record.namespaceId;
  const id = record.id;
  const principal =
    record.principal === undefined
      ? undefined
      : snapshotPrincipalRef(record.principal as PrincipalRef);
  return Object.freeze({
    kind,
    namespaceId,
    ...(id !== undefined && { id }),
    ...(principal !== undefined && { principal }),
  }) as SecurityResourceRef;
}

function reportAuditFailure(
  logger: Pick<Logger, "error">,
  outcome: "allowed" | "denied",
  error: unknown,
): void {
  try {
    logger.error(`[zorya] security audit failed for ${outcome} request`, error);
  } catch {
    // A host logger is observability, never an authorization decision.
  }
}

function forbidden(): Response {
  return jsonError(403, "forbidden");
}
