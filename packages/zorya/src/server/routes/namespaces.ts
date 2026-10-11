// ---------------------------------------------------------------------------
// Namespaces API — the namespace registry (`ZoryaServerConfig.namespaces`).
//
//   GET    /api/namespaces?status=active|archived  list, ordered by id
//   POST   /api/namespaces                         create (201; 409 namespace_exists)
//   GET    /api/namespaces/:id                     one namespace (404 not_found)
//   PATCH  /api/namespaces/:id                     change label, description,
//                                                  status (archive / reactivate),
//                                                  capability policy, metadata
//   DELETE /api/namespaces/:id                     remove an archived, empty
//                                                  namespace (204)
//
// A namespace is archived before it is deleted, and deleted only when no
// run or schedule still names it: archiving stops new runs and schedules
// from landing there (see `namespace-guard.ts`), so the emptiness check
// cannot race with new work.
// ---------------------------------------------------------------------------

import {
  InvalidNamespaceError,
  MAX_NAMESPACE_DESCRIPTION_LENGTH,
  MAX_NAMESPACE_DISPLAY_NAME_LENGTH,
  NAMESPACE_STATUSES,
  NamespaceExistsError,
  NamespaceNotFoundError,
  type NamespaceCapabilities,
  type NamespaceRecord,
  type NamespaceRegistry,
  type NamespaceStatus,
  type WorkflowStorage,
} from "@promin/workflow";
import type { SchedulerStorage } from "@promin/workflow/scheduler";
import { z } from "zod";
import { HttpError, json, jsonError, noContent } from "../router.ts";
import { NamespaceIdSchema } from "../api-types.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";

export interface NamespaceRoutesDeps {
  readonly registry: NamespaceRegistry;
  /** Runs, checked before a delete. */
  readonly storage: WorkflowStorage;
  /** Schedules, checked before a delete. Absent without a scheduler. */
  readonly schedulerStorage?: SchedulerStorage;
}

/** A namespace on the wire. */
export interface NamespaceDto {
  readonly id: string;
  readonly displayName: string;
  readonly description: string | null;
  readonly status: NamespaceStatus;
  readonly capabilities: NamespaceCapabilities;
  readonly metadata: Record<string, unknown>;
  /** ISO timestamp. */
  readonly createdAt: string;
  /** ISO timestamp. */
  readonly updatedAt: string;
}

export interface NamespacesResponse {
  /**
   * False when the server has no namespace registry: namespaces are then
   * free-form strings, and `namespaces` is empty (`GET /api/workflows`
   * lists the ones runs use).
   */
  readonly configured: boolean;
  readonly namespaces: NamespaceDto[];
}

const PositiveInt = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
const StringList = z.array(z.string().min(1).max(256)).max(256);

/** `NamespaceCapabilities` on the wire. Unknown sections and fields are rejected. */
export const NamespaceCapabilitiesSchema = z
  .object({
    ai: z
      .object({
        enabled: z.boolean().optional(),
        providers: StringList.optional(),
        models: StringList.optional(),
        maxTokensPerTurn: PositiveInt.optional(),
        maxTurnsPerRun: PositiveInt.optional(),
      })
      .strict()
      .optional(),
    agents: z
      .object({
        enabled: z.boolean().optional(),
        maxConcurrentThreads: PositiveInt.optional(),
        maxInvocationsPerMinute: PositiveInt.optional(),
      })
      .strict()
      .optional(),
    workflows: z
      .object({
        enabled: z.boolean().optional(),
        maxConcurrentRuns: PositiveInt.optional(),
        maxRunMs: PositiveInt.optional(),
      })
      .strict()
      .optional(),
    tools: z
      .object({
        network: z.boolean().optional(),
        filesystem: z.boolean().optional(),
        shell: z.boolean().optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

const DisplayName = z.string().max(MAX_NAMESPACE_DISPLAY_NAME_LENGTH);
const Description = z.string().max(MAX_NAMESPACE_DESCRIPTION_LENGTH).nullable();
const Metadata = z.record(z.string(), z.unknown());

/** Body of `POST /api/namespaces`. `displayName` defaults to the id. */
export const CreateNamespaceRequestSchema = z
  .object({
    id: NamespaceIdSchema,
    displayName: DisplayName.optional(),
    description: Description.optional(),
    capabilities: NamespaceCapabilitiesSchema.optional(),
    metadata: Metadata.optional(),
  })
  .strict();

/**
 * Body of `PATCH /api/namespaces/:id`: omitted fields keep their value,
 * `description: null` clears it, `capabilities` and `metadata` replace the
 * whole object, `status` archives or reactivates. The id is immutable.
 */
export const UpdateNamespaceRequestSchema = z
  .object({
    displayName: DisplayName.optional(),
    description: Description.optional(),
    status: z.enum(NAMESPACE_STATUSES).optional(),
    capabilities: NamespaceCapabilitiesSchema.optional(),
    metadata: Metadata.optional(),
  })
  .strict()
  .refine((b) => Object.values(b).some((v) => v !== undefined), {
    message: "Send at least one field",
  });

/** Query of `GET /api/namespaces`. */
export const NamespaceListQuerySchema = z.object({
  status: z.preprocess((v) => (v === "" ? undefined : v), z.enum(NAMESPACE_STATUSES).optional()),
});

const NamespaceParamsSchema = z.object({ id: NamespaceIdSchema });

export function toNamespaceDto(record: NamespaceRecord): NamespaceDto {
  return {
    id: record.id,
    displayName: record.displayName,
    description: record.description,
    status: record.status,
    capabilities: record.capabilities,
    metadata: record.metadata,
    createdAt: record.createdAt.toISOString(),
    updatedAt: record.updatedAt.toISOString(),
  };
}

/** `GET /api/namespaces` on a server without a namespace registry. */
export function listNamespacesUnconfigured() {
  return (): Response => {
    const response: NamespacesResponse = { configured: false, namespaces: [] };
    return json(200, response);
  };
}

export function listNamespaces(deps: NamespaceRoutesDeps) {
  return async (req: Request): Promise<Response> => {
    const { status } = parseQuery({ req, schema: NamespaceListQuerySchema });
    const records = await deps.registry.list(status !== undefined ? { status } : {});
    const response: NamespacesResponse = {
      configured: true,
      namespaces: records.map(toNamespaceDto),
    };
    return json(200, response);
  };
}

export function createNamespace(deps: NamespaceRoutesDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await parseBody({ req, schema: CreateNamespaceRequestSchema });
    try {
      const record = await deps.registry.create(body);
      return new Response(JSON.stringify(toNamespaceDto(record)), {
        status: 201,
        headers: {
          "content-type": "application/json",
          location: `/api/namespaces/${encodeURIComponent(record.id)}`,
        },
      });
    } catch (err) {
      if (err instanceof NamespaceExistsError) {
        return jsonError(409, "namespace_exists", err.message);
      }
      throw asRequestError(err);
    }
  };
}

export function getNamespace(deps: NamespaceRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: NamespaceParamsSchema });
    const record = await deps.registry.get(id);
    if (!record) return jsonError(404, "not_found");
    return json(200, toNamespaceDto(record));
  };
}

export function updateNamespace(deps: NamespaceRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: NamespaceParamsSchema });
    const patch = await parseBody({ req, schema: UpdateNamespaceRequestSchema });
    try {
      return json(200, toNamespaceDto(await deps.registry.update({ id, patch })));
    } catch (err) {
      if (err instanceof NamespaceNotFoundError) return jsonError(404, "not_found");
      throw asRequestError(err);
    }
  };
}

/**
 * DELETE /api/namespaces/:id — 204 once removed. 409 `namespace_active`
 * unless it was archived first; 409 `namespace_in_use` while runs or
 * schedules still name it (delete them first, or let run retention age
 * the runs out).
 */
export function deleteNamespace(deps: NamespaceRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: NamespaceParamsSchema });
    const record = await deps.registry.get(id);
    if (!record) return jsonError(404, "not_found");
    if (record.status !== "archived") {
      return jsonError(
        409,
        "namespace_active",
        `Archive namespace "${id}" before deleting it (PATCH { "status": "archived" })`,
      );
    }
    const [runs, schedules] = await Promise.all([
      deps.storage.listWorkflows({ namespace: id, limit: 1 }),
      deps.schedulerStorage?.countSchedules({ namespace: id }) ?? Promise.resolve(0),
    ]);
    if (runs.length > 0 || schedules > 0) {
      const what = [runs.length > 0 && "runs", schedules > 0 && "schedules"].filter(Boolean);
      return jsonError(
        409,
        "namespace_in_use",
        `Namespace "${id}" still has ${what.join(" and ")}; delete them first`,
      );
    }
    if (!(await deps.registry.delete(id))) return jsonError(404, "not_found");
    return noContent();
  };
}

/** A registry validation failure is the caller's fault: 400 `invalid_request`. */
function asRequestError(err: unknown): unknown {
  if (err instanceof InvalidNamespaceError) {
    return new HttpError({
      status: 400,
      code: "invalid_request",
      message: err.message,
      issues: [{ path: err.field, message: err.message }],
    });
  }
  return err;
}
