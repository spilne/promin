import { succeed } from "@spilne/perfect-core";
import type { ActivityLibrary } from "./types.ts";

export const mergeActivities: ActivityLibrary = () => [
  {
    id: "merge.join",
    title: "Join dependency outputs",
    category: "Merge",
    description: "Join the outputs of every dependency with a separator.",
    configSchema: {
      type: "object",
      properties: { separator: { type: "string", default: " " } },
    },
    outputSchema: { type: "string" },
    factory: (config) => (ctx) => {
      const separator = typeof config?.separator === "string" ? config.separator : " ";
      const values = Object.keys(ctx.deps).length > 0 ? Object.values(ctx.deps) : [ctx.prev];
      return succeed(values.map(String).join(separator));
    },
  },
];
