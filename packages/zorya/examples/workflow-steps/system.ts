import { Pipeline } from "@promin/core";
import type { WorkflowStepLibrary } from "./types.ts";

export const systemSteps: WorkflowStepLibrary = (deps) => [
  {
    id: "system.now",
    title: "Current time",
    category: "System",
    description: "Emit the host clock time as an ISO string.",
    outputSchema: { type: "string" },
    activity: () => () => Pipeline.succeed((deps.now?.() ?? new Date()).toISOString()),
  },
];
