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
}): void {
  const { router, advertisements, workflowStarts, storageCaller } = params;

  // Worker keys reach only what a runner needs; operator keys get the
  // whole contract (signal-token records, which carry bearers, included).
  const storageHandler = createWorkflowStorageHandler(params.storage, {
    allow: ({ method, req }) =>
      storageCaller(req) === "operator" || RUNNER_STORAGE_METHODS.has(method),
  });
  router.post("/rpc/storage", (req) => storageHandler(req));

  // Every worker registers and heartbeats here, whatever it runs; the
  // Workers page reads the same registry.
  const workerHandler = createWorkerApiHandler({ workerRegistry: params.workerRegistry });
  router.post("/rpc/worker", (req) => workerHandler(req));

  if (advertisements) {
    router
      .post("/api/advertisements", upsertAdvertisements(advertisements))
      .delete("/api/advertisements/:workerId", removeAdvertisements(advertisements))
      .get("/api/advertisements", listAdvertisements(advertisements));
  }

  if (workflowStarts) {
    router
      .post(
        "/api/worker-protocol/claim-starts",
        claimWorkflowStarts(workflowStarts, params.onStartsClaimed),
      )
      .post("/api/worker-protocol/heartbeat-start/:id", heartbeatWorkflowStart(workflowStarts))
      .post("/api/worker-protocol/complete-start/:id", completeWorkflowStart(workflowStarts))
      .post("/api/worker-protocol/release-start/:id", releaseWorkflowStart(workflowStarts))
      .get("/api/worker-protocol/starts", listWorkflowStarts(workflowStarts));
  }
}
