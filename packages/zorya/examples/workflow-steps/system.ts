import { succeed } from "@spilne/perfect-core";
import { SystemWallClock } from "@promin/workflow";
import type { ActivityLibrary } from "./types.ts";

export const systemActivities: ActivityLibrary = (deps) => {
  const clock = deps.clock ?? SystemWallClock;
  return [
    {
      id: "system.now",
      title: "Current time",
      category: "System",
      description: "Emit the host clock's time as an ISO string.",
      outputSchema: { type: "string" },
      factory: () => () => succeed(new Date(clock.currentTimeMs()).toISOString()),
    },
  ];
};
