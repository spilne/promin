import type { WallClock } from "@promin/workflow";
import { jsonError, type RouteAccessGuard } from "../router.ts";
import type { Logger } from "../server-config.ts";
import { requestSecurityContext } from "./request-principal.ts";
import type { AuthorizationRequest, SecurityAuditEvent, ZoryaSecurity } from "./security.ts";

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

    const context = requestSecurityContext(request);
    if (!context) return forbidden();

    const decision = await config.security.authorize({
      context,
      permission: access.permission,
      resource: access.target.resource,
    } as AuthorizationRequest);
    const event: SecurityAuditEvent = {
      event: "authorization",
      occurredAt: config.clock.now().toISOString(),
      context,
      namespaceId: access.target.resource.namespaceId,
      resource: access.target.resource,
      permission: access.permission,
      decision,
    };

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
