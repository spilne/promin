// ---------------------------------------------------------------------------
// Workflow-builder API (`/api/workflow-builder/*`), mounted when the server
// is given a `ZoryaWorkflowBuilder`.
// ---------------------------------------------------------------------------

import type { Router } from "../router.ts";
import type { ZoryaWorkflowBuilder } from "../services/workflow-builder/index.ts";
import {
  createBuilderWorkflow,
  deleteBuilderWorkflow,
  getBuilderWorkflow,
  listBuilderActivities,
  listBuilderWorkflows,
  publishBuilderWorkflow,
  saveBuilderWorkflow,
  validateBuilderWorkflow,
} from "../routes/workflow-builder.ts";
import { orgTarget, protectedAccess } from "./route-security.ts";

export function mountWorkflowBuilder(params: {
  readonly router: Router;
  readonly builder: ZoryaWorkflowBuilder;
}): void {
  const deps = { builder: params.builder };
  const workflow = orgTarget({ kind: "workflow", idParam: "name" });
  params.router.route({
    method: "GET",
    path: "/api/workflow-builder/activities",
    access: protectedAccess({ resource: "workflow", action: "read" }),
    handler: listBuilderActivities(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/workflow-builder/validate",
    access: protectedAccess({ resource: "workflow", action: "create" }),
    handler: validateBuilderWorkflow(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/workflow-builder/workflows",
    access: protectedAccess({ resource: "workflow", action: "read" }),
    handler: listBuilderWorkflows(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/workflow-builder/workflows",
    access: protectedAccess({ resource: "workflow", action: "create" }),
    handler: createBuilderWorkflow(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/workflow-builder/workflows/:name",
    access: protectedAccess({ resource: "workflow", action: "read", target: workflow }),
    handler: getBuilderWorkflow(deps),
  });
  params.router.route({
    method: "PUT",
    path: "/api/workflow-builder/workflows/:name",
    access: protectedAccess({ resource: "workflow", action: "update", target: workflow }),
    handler: saveBuilderWorkflow(deps),
  });
  params.router.route({
    method: "DELETE",
    path: "/api/workflow-builder/workflows/:name",
    // Builder removal is a mutation of the workflow definition (`workflow.delete` is not a verb).
    access: protectedAccess({ resource: "workflow", action: "update", target: workflow }),
    handler: deleteBuilderWorkflow(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/workflow-builder/workflows/:name/publish",
    access: protectedAccess({ resource: "workflow", action: "update", target: workflow }),
    handler: publishBuilderWorkflow(deps),
  });
}
