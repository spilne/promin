import { Pipeline } from "@promin/core";
import type { WorkflowStepLibrary } from "./types.ts";

export const mergeSteps: WorkflowStepLibrary = () => [
  {
    id: "transform.concat",
    title: "Join dependency outputs",
    category: "Merge",
    description: "Join dependency outputs with a configurable separator.",
    configSchema: {
      type: "object",
      properties: { separator: { type: "string", default: " " } },
    },
    outputSchema: { type: "string" },
    activity: (config) => (ctx) => {
      const separator = typeof config?.separator === "string" ? config.separator : " ";
      return Pipeline.succeed(Object.values(ctx.deps).map(String).join(separator));
    },
  },
];
