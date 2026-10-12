// ---------------------------------------------------------------------------
// Inbound integrations that target the agents service: signed webhooks and
// remote-deployment self-registration.
// ---------------------------------------------------------------------------

import type { NamespaceRegistry, WallClock, WorkflowStorage } from "@promin/workflow";
import type { Router } from "../router.ts";
import type { Logger, ZoryaServerConfig } from "../server-config.ts";
import type { ZoryaAgents } from "../services/agents/index.ts";
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
}): WebhookDeliveryRecovery {
  const { webhooks, agents } = params;
  const webhookDeps: WebhookGatewayDeps = {
    registry: agents.registry,
    // The resolve signature is structurally compatible (it returns an
    // Agent, which has invoke + withScope); the webhook deps narrow it to
    // the methods they use.
    resolve: agents.resolve as unknown as WebhookGatewayDeps["resolve"],
    sources: webhooks.sources,
    deliveries:
      webhooks.deliveries ?? new WorkflowStorageWebhookDeliveryStore({ storage: params.storage }),
    clock: params.clock,
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
  };
  params.router.post("/webhooks/:source/:agentId", ingestWebhook(webhookDeps));
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
  params.router
    .post("/api/remote-deployments/register", registerDeployment(remoteDeps))
    .post("/api/remote-deployments/:deploymentId/heartbeat", heartbeatDeployment(remoteDeps))
    .delete("/api/remote-deployments/:deploymentId", unregisterDeployment(remoteDeps))
    .get("/api/remote-deployments", listDeployments(remoteDeps));
  return sweeper;
}
