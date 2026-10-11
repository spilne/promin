// ---------------------------------------------------------------------------
// Workflow-builder routes — the dashboard's visual workflow builder. Backed
// by `ZoryaWorkflowBuilder`; mounted only when the server is given one.
//
// Routes:
//   GET    /api/workflow-builder/activities              — the activity palette
//   POST   /api/workflow-builder/validate                — issues a save would refuse
//   GET    /api/workflow-builder/workflows               — list
//   POST   /api/workflow-builder/workflows               — create (409 when the name is taken)
//   GET    /api/workflow-builder/workflows/:name         — draft, versions, active version
//   PUT    /api/workflow-builder/workflows/:name         — save the draft (needs `revision`)
//   POST   /api/workflow-builder/workflows/:name/publish — publish the draft as the next version
//   DELETE /api/workflow-builder/workflows/:name         — delete; 409 while runs are active
//
// Published workflows run through the normal run path
// (`POST /api/workflows/:name/runs`); their versions show on the
// workflow's Versions panel like any registered version.
//
// Errors: a schema that does not validate or compile answers 400
// `invalid_workflow` with one issue per problem (zod paths under `schema`,
// graph problems — cycles, dangling dependencies, unknown activities — on
// `schema`). A save or publish naming a stale `revision` answers 409
// `revision_conflict` with the current revision: the edit is refused, never
// merged. Bodies are bounded by the server's request body limit (1 MiB by
// default) and schemas by the service's step cap.
// ---------------------------------------------------------------------------

import { z } from "zod";
import type { ActivityDescriptor } from "@promin/workflow";
import { parseBody, parseParams } from "../request-validation.ts";
import { HttpError, created, json, noContent, ok, type ApiIssue } from "../router.ts";
import {
  BuilderWorkflowConflictError,
  BuilderWorkflowInUseError,
  BuilderWorkflowNameTakenError,
  BuilderWorkflowNotFoundError,
  InvalidBuilderWorkflowError,
  type BuilderWorkflow,
  type BuilderWorkflowSummary,
  type PublishResult,
  type ZoryaWorkflowBuilder,
} from "../services/workflow-builder/index.ts";

export type {
  BuilderWorkflow,
  BuilderWorkflowSummary,
  BuilderWorkflowVersion,
  PublishResult,
} from "../services/workflow-builder/index.ts";

export interface WorkflowBuilderRoutesDeps {
  readonly builder: ZoryaWorkflowBuilder;
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

export interface BuilderActivitiesResponse {
  activities: ActivityDescriptor[];
}

export interface BuilderWorkflowsResponse {
  workflows: BuilderWorkflowSummary[];
}

export interface BuilderValidateResponse {
  issues: ApiIssue[];
}

/** 409 body of a save or publish that names a stale revision. */
export interface BuilderRevisionConflictResponse {
  error: "revision_conflict";
  message: string;
  currentRevision: number | null;
}

/** 409 body of a delete refused while runs are active. */
export interface BuilderWorkflowInUseResponse {
  error: "workflow_in_use";
  message: string;
  activeRuns: number;
}

// ---------------------------------------------------------------------------
// Request schemas
// ---------------------------------------------------------------------------

const SchemaField = z
  .unknown()
  .refine((v) => v !== undefined, { message: "Required: the WorkflowSchema JSON" });
const Revision = z.number().int().min(1);

export const BuilderWorkflowParamsSchema = z.object({ name: z.string().min(1).max(256) });

/** Body of `POST /api/workflow-builder/workflows` and `/validate`. */
export const BuilderWorkflowCreateRequestSchema = z.object({ schema: SchemaField }).strict();
export type BuilderWorkflowCreateRequest = { schema: unknown };

/** Body of `PUT /api/workflow-builder/workflows/:name`. */
export const BuilderWorkflowSaveRequestSchema = z
  .object({ schema: SchemaField, revision: Revision })
  .strict();
export type BuilderWorkflowSaveRequest = { schema: unknown; revision: number };

/** Body of `POST /api/workflow-builder/workflows/:name/publish` (optional). */
export const BuilderWorkflowPublishRequestSchema = z
  .object({ revision: Revision.optional(), promote: z.boolean().optional() })
  .strict();
export type BuilderWorkflowPublishRequest = z.infer<typeof BuilderWorkflowPublishRequestSchema>;

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

export function listBuilderActivities(deps: WorkflowBuilderRoutesDeps) {
  return async (): Promise<Response> =>
    ok({ activities: deps.builder.describeActivities() } satisfies BuilderActivitiesResponse);
}

export function validateBuilderWorkflow(deps: WorkflowBuilderRoutesDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await parseBody({ req, schema: BuilderWorkflowCreateRequestSchema });
    const issues = deps.builder.validate(body.schema).map(underSchema);
    return ok({ issues } satisfies BuilderValidateResponse);
  };
}

export function listBuilderWorkflows(deps: WorkflowBuilderRoutesDeps) {
  return async (): Promise<Response> =>
    ok({ workflows: await deps.builder.list() } satisfies BuilderWorkflowsResponse);
}

export function getBuilderWorkflow(deps: WorkflowBuilderRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { name } = parseParams({ params, schema: BuilderWorkflowParamsSchema });
    const workflow = await deps.builder.get(name);
    if (!workflow) return errorResponse(new BuilderWorkflowNotFoundError(name));
    return ok(workflow satisfies BuilderWorkflow);
  };
}

export function createBuilderWorkflow(deps: WorkflowBuilderRoutesDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await parseBody({ req, schema: BuilderWorkflowCreateRequestSchema });
    return answer(async () => created(await deps.builder.create({ schema: body.schema })));
  };
}

export function saveBuilderWorkflow(deps: WorkflowBuilderRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { name } = parseParams({ params, schema: BuilderWorkflowParamsSchema });
    const body = await parseBody({ req, schema: BuilderWorkflowSaveRequestSchema });
    return answer(async () =>
      ok(await deps.builder.saveDraft({ name, schema: body.schema, revision: body.revision })),
    );
  };
}

export function publishBuilderWorkflow(deps: WorkflowBuilderRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { name } = parseParams({ params, schema: BuilderWorkflowParamsSchema });
    const body =
      (await parseBody({ req, schema: BuilderWorkflowPublishRequestSchema.optional() })) ?? {};
    return answer(async () => {
      const result = await deps.builder.publish({
        name,
        ...(body.revision !== undefined && { revision: body.revision }),
        ...(body.promote !== undefined && { promote: body.promote }),
      });
      return json(result.created ? 201 : 200, result satisfies PublishResult);
    });
  };
}

export function deleteBuilderWorkflow(deps: WorkflowBuilderRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { name } = parseParams({ params, schema: BuilderWorkflowParamsSchema });
    return answer(async () => {
      await deps.builder.remove(name);
      return noContent();
    });
  };
}

// ---------------------------------------------------------------------------

/** Run `fn`, answering the service's errors with their HTTP status. */
async function answer(fn: () => Promise<Response>): Promise<Response> {
  try {
    return await fn();
  } catch (err) {
    return errorResponse(err);
  }
}

function errorResponse(err: unknown): Response {
  if (err instanceof BuilderWorkflowNotFoundError) {
    return json(404, { error: "builder_workflow_not_found", message: err.message });
  }
  if (err instanceof BuilderWorkflowNameTakenError) {
    const error = err.reason === "exists" ? "workflow_exists" : "workflow_name_reserved";
    return json(409, { error, message: err.message });
  }
  if (err instanceof InvalidBuilderWorkflowError) {
    const issues = err.issues.map(underSchema);
    throw new HttpError({
      status: 400,
      code: "invalid_workflow",
      message: issues.map((i) => `${i.path}: ${i.message}`).join("; "),
      issues,
    });
  }
  if (err instanceof BuilderWorkflowConflictError) {
    return json(409, {
      error: "revision_conflict",
      message: err.message,
      currentRevision: err.currentRevision,
    } satisfies BuilderRevisionConflictResponse);
  }
  if (err instanceof BuilderWorkflowInUseError) {
    return json(409, {
      error: "workflow_in_use",
      message: err.message,
      activeRuns: err.activeRuns,
    } satisfies BuilderWorkflowInUseResponse);
  }
  throw err;
}

/** Issue paths are relative to the submitted schema; the wire names its field. */
function underSchema(issue: { path: string; message: string }): ApiIssue {
  return { path: issue.path ? `schema.${issue.path}` : "schema", message: issue.message };
}
