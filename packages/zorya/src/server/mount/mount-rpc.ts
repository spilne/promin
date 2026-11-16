// ---------------------------------------------------------------------------
// Worker plane over HTTP: `/rpc/storage` (workflow storage for workers that
// drive runs), `/rpc/worker` (the worker registry), and — with a queued
// layer on the workflows chain — `/api/advertisements` and the
// `/api/worker-protocol/*` start queue. The worker WebSocket (`/ws/worker`)
// is upgraded by the listener, not routed here.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "@promin/workflow";
import type {
  WorkerRegistry,
  WorkflowAdvertisementRegistry,
  WorkflowStartQueue,
} from "@promin/workflow/distributed";
import {
  RUNNER_STORAGE_METHODS,
  createWorkerApiHandler,
  createWorkflowStorageHandler,
} from "@promin/workflow-remote";
import type { Router } from "../router.ts";
import type { StorageCaller } from "./mount-auth.ts";
import type { RunSecurityContext } from "@promin/workflow";
import {
  listAdvertisements,
  removeAdvertisements,
  upsertAdvertisements,
} from "../routes/advertisements.ts";
import {
  claimWorkflowStarts,
  completeWorkflowStart,
  heartbeatWorkflowStart,
  listWorkflowStarts,
  releaseWorkflowStart,
} from "../routes/workflow-starts.ts";

export function mountRpc(params: {
  readonly router: Router;
  readonly storage: WorkflowStorage;
  readonly workerRegistry: WorkerRegistry;
  readonly advertisements: WorkflowAdvertisementRegistry | undefined;
  /** The queued layer's start queue, when the chain has one. */
  readonly workflowStarts: WorkflowStartQueue | undefined;
  /** Who is calling `/rpc/storage` (see `AuthPlanes.storageCaller`). */
  readonly storageCaller: (req: Request) => StorageCaller | undefined;
  /** Hears claimed starts (the worker wake-ups). */
  readonly onStartsClaimed?: (starts: ReadonlyArray<{ readonly workflowId: string }>) => void;
  /** Reauthorize a claimed start before it is exposed to a worker. */
  readonly authorizeWorkerStart?: (workflowId: string) => Promise<boolean>;
  /** Reauthorize a worker immediately before it takes a run lock. */
  readonly authorizeWorkerLock?: (workflowId: string) => Promise<boolean>;
  /** Validate inherited identity on remote child creation. */
  readonly authorizeWorkerChild?: (params: {
    readonly workflowId: string;
    readonly parentWorkflowId: string;
    readonly namespace?: string;
    readonly securityContext?: RunSecurityContext;
  }) => Promise<boolean>;
}): void {
  const { router, advertisements, workflowStarts, storageCaller } = params;

  // Worker keys reach only what a runner needs; operator keys get the
  // whole contract (signal-token records, which carry bearers, included).
  const storageHandler = createWorkflowStorageHandler(params.storage, {
    allow: ({ method, req }) =>
      storageCaller(req) === "operator" || RUNNER_STORAGE_METHODS.has(method),
    authorize: async ({ method, params: rpcParams, req }) => {
      const caller = storageCaller(req);
      if (!caller) return false;
      const record = rpcParams as Record<string, unknown>;
      if (method === "tryLock" || method === "tryLockAndLoad") {
        const workflowId = record.workflowId;
        return (
          typeof workflowId === "string" &&
          (params.authorizeWorkerLock ? params.authorizeWorkerLock(workflowId) : true)
        );
      }
      if (method === "createWorkflow" && caller === "worker") {
        const workflowId = record.workflowId;
        const parentWorkflowId = record.parentWorkflowId;
        if (typeof workflowId !== "string" || typeof parentWorkflowId !== "string") return false;
        return params.authorizeWorkerChild
          ? params.authorizeWorkerChild({
              workflowId,
              parentWorkflowId,
              ...(typeof record.namespace === "string" && { namespace: record.namespace }),
              ...(record.securityContext !== undefined && {
                securityContext: record.securityContext as RunSecurityContext,
              }),
            })
          : true;
      }
      return true;
    },
  });
  router.route({
    method: "POST",
    path: "/rpc/storage",
    access: { kind: "worker" },
    handler: (req) => storageHandler(req),
  });

  // Every worker registers and heartbeats here, whatever it runs; the
  // Workers page reads the same registry.
  const workerHandler = createWorkerApiHandler({ workerRegistry: params.workerRegistry });
  router.route({
    method: "POST",
    path: "/rpc/worker",
    access: { kind: "worker" },
    handler: (req) => workerHandler(req),
  });

  if (advertisements) {
    router.route({
      method: "POST",
      path: "/api/advertisements",
      access: { kind: "worker" },
      handler: upsertAdvertisements(advertisements),
    });
    router.route({
      method: "DELETE",
      path: "/api/advertisements/:workerId",
      access: { kind: "worker" },
      handler: removeAdvertisements(advertisements),
    });
    router.route({
      method: "GET",
      path: "/api/advertisements",
      access: { kind: "worker" },
      handler: listAdvertisements(advertisements),
    });
  }

  if (workflowStarts) {
    router.route({
      method: "POST",
      path: "/api/worker-protocol/claim-starts",
      access: { kind: "worker" },
      handler: claimWorkflowStarts(
        workflowStarts,
        params.onStartsClaimed,
        params.authorizeWorkerStart
          ? (start) => params.authorizeWorkerStart!(start.workflowId)
          : undefined,
      ),
    });
    router.route({
      method: "POST",
      path: "/api/worker-protocol/heartbeat-start/:id",
      access: { kind: "worker" },
      handler: heartbeatWorkflowStart(workflowStarts),
    });
    router.route({
      method: "POST",
      path: "/api/worker-protocol/complete-start/:id",
      access: { kind: "worker" },
      handler: completeWorkflowStart(workflowStarts),
    });
    router.route({
      method: "POST",
      path: "/api/worker-protocol/release-start/:id",
      access: { kind: "worker" },
      handler: releaseWorkflowStart(workflowStarts),
    });
    router.route({
      method: "GET",
      path: "/api/worker-protocol/starts",
      access: { kind: "worker" },
      handler: listWorkflowStarts(workflowStarts),
    });
  }
}
