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

export function mountWorkflowBuilder(params: {
  readonly router: Router;
  readonly builder: ZoryaWorkflowBuilder;
}): void {
  const deps = { builder: params.builder };
  params.router
    .get("/api/workflow-builder/activities", listBuilderActivities(deps))
    .post("/api/workflow-builder/validate", validateBuilderWorkflow(deps))
    .get("/api/workflow-builder/workflows", listBuilderWorkflows(deps))
    .post("/api/workflow-builder/workflows", createBuilderWorkflow(deps))
    .get("/api/workflow-builder/workflows/:name", getBuilderWorkflow(deps))
    .put("/api/workflow-builder/workflows/:name", saveBuilderWorkflow(deps))
    .delete("/api/workflow-builder/workflows/:name", deleteBuilderWorkflow(deps))
    .post("/api/workflow-builder/workflows/:name/publish", publishBuilderWorkflow(deps));
}
