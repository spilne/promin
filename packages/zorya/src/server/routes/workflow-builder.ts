import { validateWorkflowSchema, type WorkflowSchema } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import type { AuthoredWorkflowRecord, ZoryaWorkflowBuilder } from "../services/workflow-builder.ts";

export interface WorkflowStepCatalogResponse {
  readonly steps: ReturnType<ZoryaWorkflowBuilder["steps"]>;
}

export interface AuthoredWorkflowDto {
  readonly name: string;
  readonly version: string;
  readonly schema: WorkflowSchema;
  readonly status: AuthoredWorkflowRecord["status"];
  readonly contentHash: string;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly publishedAt?: number;
}

export interface AuthoredWorkflowsResponse {
  readonly workflows: AuthoredWorkflowDto[];
}

export interface SaveAuthoredWorkflowRequest {
  readonly schema?: unknown;
  readonly version?: unknown;
}

export interface PublishAuthoredWorkflowRequest {
  readonly version?: unknown;
  readonly promote?: unknown;
}

export function listWorkflowStepCatalog(deps: { builder: ZoryaWorkflowBuilder }) {
  return async (): Promise<Response> => json(200, { steps: deps.builder.steps() });
}

export function listAuthoredWorkflows(deps: { builder: ZoryaWorkflowBuilder }) {
  return async (): Promise<Response> =>
    json(200, { workflows: (await deps.builder.list()).map(toDto) });
}

export function getAuthoredWorkflow(deps: { builder: ZoryaWorkflowBuilder }) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const record = await deps.builder.get(params.name!, version);
    if (!record) return jsonError(404, "authored_workflow_not_found");
    return json(200, { workflow: toDto(record) });
  };
}

export function saveAuthoredWorkflow(deps: { builder: ZoryaWorkflowBuilder }) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<SaveAuthoredWorkflowRequest>(req);
    if (!body || body.schema === undefined) return jsonError(400, "schema_required");
    try {
      const schema = validateWorkflowSchema(body.schema);
      const version = typeof body.version === "string" && body.version.trim() ? body.version : "v1";
      const record = await deps.builder.save({ schema, version });
      return json(201, { workflow: toDto(record) });
    } catch (error) {
      return workflowBuilderError(error, "save_failed");
    }
  };
}

export function deleteAuthoredWorkflow(deps: { builder: ZoryaWorkflowBuilder }) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const version = new URL(req.url).searchParams.get("version");
    if (!version) return jsonError(400, "version_required");
    await deps.builder.delete(params.name!, version);
    return json(200, { ok: true });
  };
}

export function publishAuthoredWorkflow(deps: { builder: ZoryaWorkflowBuilder }) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = await readJson<PublishAuthoredWorkflowRequest>(req);
    try {
      const version =
        typeof body?.version === "string" && body.version.trim() ? body.version : undefined;
      const promote = typeof body?.promote === "boolean" ? body.promote : true;
      const record = await deps.builder.publish(params.name!, version, { promote });
      return json(200, { workflow: toDto(record) });
    } catch (error) {
      return workflowBuilderError(error, "publish_failed");
    }
  };
}

function toDto(record: AuthoredWorkflowRecord): AuthoredWorkflowDto {
  return {
    name: record.name,
    version: record.version,
    schema: record.schema,
    status: record.status,
    contentHash: record.contentHash,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
    ...(record.publishedAt !== undefined && { publishedAt: record.publishedAt }),
  };
}

function workflowBuilderError(error: unknown, fallback: string): Response {
  const message = error instanceof Error ? error.message : String(error);
  if (message === "authored_workflow_not_found") {
    return jsonError(404, "authored_workflow_not_found");
  }
  if (message.includes("Workflow compilation failed")) {
    return jsonError(400, "workflow_compilation_failed", message);
  }
  return jsonError(400, fallback, message);
}
