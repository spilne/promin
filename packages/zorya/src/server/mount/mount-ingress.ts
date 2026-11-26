// ---------------------------------------------------------------------------
// Inbound integrations that target the agents service: signed webhooks and
// remote-deployment self-registration.
// ---------------------------------------------------------------------------

import type {
  NamespaceRegistry,
  RunSecurityContext,
  WallClock,
  WorkflowStorage,
} from "@promin/workflow";
import { HttpError, type Router } from "../router.ts";
import type { Logger, ZoryaServerConfig } from "../server-config.ts";
import {
  requestSecurityContext,
  snapshotAuthorizationDecision,
  snapshotSecurityContext,
  snapshotSecurityPermission,
  snapshotSecurityResource,
  type AuthorizationRequest,
  type SecurityAction,
  type SecurityAuditEvent,
  type ZoryaSecurity,
} from "../security/index.ts";
import type { ZoryaAgents } from "../services/agents/index.ts";
import { protectedAccess, orgTarget } from "./route-security.ts";
import {
  heartbeatDeployment,
  listDeployments,
  registerDeployment,
  RemoteDeploymentSweeper,
  unregisterDeployment,
  type RemoteDeploymentsGatewayDeps,
} from "../routes/remote-deployments.ts";
import {
  ingestWebhook,
  WebhookDeliveryRecovery,
  WorkflowStorageWebhookDeliveryStore,
  type WebhookGatewayDeps,
} from "../routes/webhooks.ts";

/**
 * `POST /webhooks/:source/:agentId`. Returns the recovery loop that
 * finishes deliveries a crashed dispatcher left unfinished; the server
 * starts and stops it with itself.
 */
export function mountWebhooks(params: {
  readonly router: Router;
  readonly webhooks: NonNullable<ZoryaServerConfig["webhooks"]>;
  readonly agents: ZoryaAgents;
  readonly storage: WorkflowStorage;
  readonly clock: WallClock;
  /** Registry a delivery's namespace is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
  readonly logger: Logger;
  readonly security: ZoryaSecurity;
  readonly legacySecurityContext?: RunSecurityContext;
}): WebhookDeliveryRecovery {
  const { webhooks, agents } = params;
  const webhookDeps: WebhookGatewayDeps = {
    registry: agents.registry,
    // The resolve signature is structurally compatible (it returns an
    // Agent, which has invoke + withScope); the webhook deps narrow it to
    // the methods they use.
    resolve: agents.resolve as unknown as WebhookGatewayDeps["resolve"],
    sources: webhooks.sources,
    security: params.security,
    ...(params.legacySecurityContext !== undefined && {
      legacySecurityContext: params.legacySecurityContext,
    }),
    deliveries:
      webhooks.deliveries ?? new WorkflowStorageWebhookDeliveryStore({ storage: params.storage }),
    clock: params.clock,
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
  };
  params.router.route({
    method: "POST",
    path: "/webhooks/:source/:agentId",
    access: { kind: "signed-webhook" },
    handler: ingestWebhook(webhookDeps),
  });
  return new WebhookDeliveryRecovery({
    ...webhookDeps,
    ...(params.webhooks.recoveryIntervalMs !== undefined && {
      intervalMs: params.webhooks.recoveryIntervalMs,
    }),
    onError: (err) => params.logger.error("[zorya] webhook delivery recovery failed", err),
  });
}

/**
 * `/api/remote-deployments/*`. Returns the sweeper that expires lapsed
 * registrations; the server starts and stops it with itself.
 */
export function mountRemoteDeployments(params: {
  readonly router: Router;
  readonly remoteDeployments: NonNullable<ZoryaServerConfig["remoteDeployments"]>;
  readonly agents: ZoryaAgents;
  readonly clock: WallClock;
  readonly logger: Logger;
  readonly security: ZoryaSecurity;
}): RemoteDeploymentSweeper {
  const { remoteDeployments, logger } = params;
  const remoteDeps: RemoteDeploymentsGatewayDeps = {
    registry: remoteDeployments.registry,
    agents: params.agents.registry,
    canAutoPromote: async (p) => (await params.agents.canAutoPromote?.(p)) ?? true,
    ...(remoteDeployments.endpointPolicy !== undefined && {
      endpointPolicy: remoteDeployments.endpointPolicy,
    }),
  };
  const sweeper = new RemoteDeploymentSweeper({
    ...remoteDeps,
    clock: params.clock,
    ...(remoteDeployments.sweepIntervalMs !== undefined && {
      intervalMs: remoteDeployments.sweepIntervalMs,
    }),
    onError: (err) => logger.error("[zorya] remote-deployment sweep failed", err),
  });
  const deploymentSecurity = {
    deps: remoteDeps,
    security: params.security,
    clock: params.clock,
    logger,
  };
  params.router
    .route({
      method: "POST",
      path: "/api/remote-deployments/register",
      access: protectedAccess({ resource: "agent", action: "create" }),
      handler: registerDeployment(remoteDeps),
    })
    .route({
      method: "POST",
      path: "/api/remote-deployments/:deploymentId/heartbeat",
      access: protectedAccess({
        resource: "agent",
        action: "update",
        target: deploymentTarget({ ...deploymentSecurity, action: "update" }),
      }),
      handler: heartbeatDeployment(remoteDeps),
    })
    .route({
      method: "DELETE",
      path: "/api/remote-deployments/:deploymentId",
      access: protectedAccess({
        resource: "agent",
        action: "disable",
        target: deploymentTarget({ ...deploymentSecurity, action: "disable" }),
      }),
      handler: unregisterDeployment(remoteDeps),
    })
    .route({
      method: "GET",
      path: "/api/remote-deployments",
      access: protectedAccess({
        resource: "agent",
        action: "read",
        target: orgTarget({ kind: "agent" }),
      }),
      handler: listDeployments(remoteDeps),
    });
  return sweeper;
}

function deploymentTarget(params: {
  readonly deps: RemoteDeploymentsGatewayDeps;
  readonly security: ZoryaSecurity;
  readonly clock: WallClock;
  readonly logger: Logger;
  readonly action: SecurityAction<"agent">;
}) {
  return async ({
    request,
    params: route,
  }: {
    readonly request: Request;
    readonly params: Readonly<Record<string, string>>;
  }) => {
    const deploymentId = route.deploymentId;
    const deployment = deploymentId ? await params.deps.registry.get(deploymentId) : null;
    const agentIds = deployment ? [...new Set(deployment.agents)].sort() : [];
    // The route guard authorizes the first authoritative target. Resolve and
    // authorize every additional stored agent here, still before the handler.
    // Never invent a deployment id or aggregate JSON as an agent identity.
    for (const agentId of agentIds.slice(1)) {
      await authorizeDeploymentAgent({
        security: params.security,
        clock: params.clock,
        logger: params.logger,
        action: params.action,
        request,
        agentId,
      });
    }
    return {
      kind: "agent" as const,
      namespaceId: "org",
      ...(agentIds[0] !== undefined && { id: agentIds[0] }),
    };
  };
}

async function authorizeDeploymentAgent(params: {
  readonly security: ZoryaSecurity;
  readonly clock: WallClock;
  readonly logger: Logger;
  readonly action: SecurityAction<"agent">;
  readonly request: Request;
  readonly agentId: string;
}): Promise<void> {
  const requestContext = requestSecurityContext(params.request);
  if (!requestContext) return;
  const context = snapshotSecurityContext(requestContext);
  const permission = snapshotSecurityPermission({ resource: "agent", action: params.action });
  const resource = snapshotSecurityResource({
    kind: "agent",
    namespaceId: "org",
    id: params.agentId,
  });
  const decision = snapshotAuthorizationDecision(
    await params.security.authorize({ context, permission, resource } as AuthorizationRequest),
  );
  const event: SecurityAuditEvent = Object.freeze({
    event: "authorization",
    occurredAt: params.clock.now().toISOString(),
    context,
    namespaceId: resource.namespaceId,
    resource,
    permission,
    decision,
    metadata: Object.freeze({
      method: params.request.method,
      path: new URL(params.request.url).pathname,
      batch: true,
    }),
  });
  try {
    await params.security.recordAudit(event);
  } catch (error) {
    try {
      params.logger.error("[zorya] remote deployment authorization audit failed", error);
    } catch {}
    if (decision.allowed) {
      throw new HttpError({ status: 503, code: "security_audit_unavailable" });
    }
  }
  if (!decision.allowed) throw new HttpError({ status: 403, code: "forbidden" });
}
