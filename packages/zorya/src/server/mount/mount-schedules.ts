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
    router.get("/api/schedules", () => json(200, { schedules: [], total: 0, configured: false }));
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
  };
  router
    .get("/api/schedules", listSchedules(scheduleDeps))
    .post("/api/schedules", createSchedule(scheduleDeps))
    .get("/api/schedules/:id", getSchedule(scheduleDeps))
    .patch("/api/schedules/:id", patchSchedule(scheduleDeps))
    .delete("/api/schedules/:id", deleteSchedule(sch))
    .get("/api/schedules/:id/history", getScheduleHistory(sch, params.storage))
    .get("/api/schedules/:id/upcoming", getScheduleUpcoming(scheduleDeps))
    .post(
      "/api/schedules/:id/emit",
      emitScheduleNow(sch, (id) => scheduler.fireOnce(id)),
    );
}
