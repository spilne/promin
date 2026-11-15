// ---------------------------------------------------------------------------
// Schedules API. Without a scheduler only `GET /api/schedules` answers, with
// `configured: false`, so the UI can tell "not configured" from "empty".
// ---------------------------------------------------------------------------

import type { NamespaceRegistry, WallClock, WorkflowStorage } from "@promin/workflow";
import { json, type Router } from "../router.ts";
import type { ZoryaScheduler } from "../services/scheduler/index.ts";
import {
  createSchedule,
  deleteSchedule,
  emitScheduleNow,
  getSchedule,
  getScheduleHistory,
  getScheduleUpcoming,
  listSchedules,
  patchSchedule,
} from "../routes/schedules.ts";
import { protectedAccess } from "./route-security.ts";
import { ORG_WIDE_NAMESPACE } from "../security/index.ts";
import { assertUniqueQueryParameters } from "../request-validation.ts";

export function mountSchedules(params: {
  readonly router: Router;
  readonly scheduler: ZoryaScheduler | undefined;
  readonly storage: WorkflowStorage;
  readonly clock: WallClock;
  /** Registry a new schedule's namespace is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
}): void {
  const { router, scheduler } = params;
  if (!scheduler) {
    router.route({
      method: "GET",
      path: "/api/schedules",
      access: protectedAccess({ resource: "schedule", action: "read" }),
      handler: () => json(200, { schedules: [], total: 0, configured: false }),
    });
    return;
  }
  const sch = scheduler.storage;
  const scheduleDeps = {
    storage: sch,
    clock: params.clock,
    servesNamespace: (ns: string | undefined) => scheduler.servesNamespace(ns),
    ...(scheduler.defaultNamespace !== undefined && {
      defaultNamespace: scheduler.defaultNamespace,
    }),
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
    assignedSecurityContext: (assignment: {
      readonly scheduleId: string;
      readonly namespaceId: string;
      readonly serviceAccountId?: string;
      readonly runAsUserId?: string;
    }) => scheduler.assignedSecurityContext(assignment),
  };
  const storedTarget = async ({ params: route }: { params: Readonly<Record<string, string>> }) => {
    const config = route.id ? await sch.loadSchedule(route.id) : null;
    return {
      kind: "schedule" as const,
      namespaceId: config?.namespace ?? ORG_WIDE_NAMESPACE,
      ...(route.id !== undefined && { id: route.id }),
    };
  };
  router.route({
    method: "GET",
    path: "/api/schedules",
    access: protectedAccess({
      resource: "schedule",
      action: "read",
      target: ({ request }) => {
        const url = new URL(request.url);
        assertUniqueQueryParameters({ url, names: new Set(["namespace"]) });
        return {
          kind: "schedule",
          namespaceId: url.searchParams.get("namespace") || ORG_WIDE_NAMESPACE,
        };
      },
    }),
    handler: listSchedules(scheduleDeps),
  });
  router.route({
    method: "POST",
    path: "/api/schedules",
    access: protectedAccess({
      resource: "schedule",
      action: "create",
      target: async ({ request }) => {
        let namespaceId = scheduler.defaultNamespace ?? ORG_WIDE_NAMESPACE;
        try {
          const body = (await request.clone().json()) as Record<string, unknown>;
          if (typeof body.namespace === "string" && body.namespace.length > 0) {
            namespaceId = body.namespace;
          }
        } catch {
          // Handler validation owns the existing 4xx response.
        }
        return { kind: "schedule", namespaceId };
      },
    }),
    handler: createSchedule(scheduleDeps),
  });
  router.route({
    method: "GET",
    path: "/api/schedules/:id",
    access: protectedAccess({
      resource: "schedule",
      action: "read",
      target: storedTarget,
    }),
    handler: getSchedule(scheduleDeps),
  });
  router.route({
    method: "PATCH",
    path: "/api/schedules/:id",
    access: protectedAccess({
      resource: "schedule",
      action: "update",
      target: storedTarget,
    }),
    handler: patchSchedule(scheduleDeps),
  });
  router.route({
    method: "DELETE",
    path: "/api/schedules/:id",
    access: protectedAccess({
      resource: "schedule",
      action: "delete",
      target: storedTarget,
    }),
    handler: deleteSchedule(sch),
  });
  router.route({
    method: "GET",
    path: "/api/schedules/:id/history",
    access: protectedAccess({
      resource: "schedule",
      action: "read",
      target: storedTarget,
    }),
    handler: getScheduleHistory(sch, params.storage),
  });
  router.route({
    method: "GET",
    path: "/api/schedules/:id/upcoming",
    access: protectedAccess({
      resource: "schedule",
      action: "read",
      target: storedTarget,
    }),
    handler: getScheduleUpcoming(scheduleDeps),
  });
  router.route({
    method: "POST",
    path: "/api/schedules/:id/emit",
    access: protectedAccess({
      resource: "schedule",
      action: "update",
      target: storedTarget,
    }),
    handler: emitScheduleNow(sch, (id) => scheduler.fireOnce(id)),
  });
}
