import { succeed } from "@spilne/perfect-core";
import type { ActivityLibrary } from "./types.ts";

// Activities with a boolean output: the builder offers them as the
// condition of a branch node.
export const predicateActivities: ActivityLibrary = () => [
  {
    id: "predicate.long",
    title: "Is long text",
    category: "Predicate",
    description: "True when the previous text is longer than a threshold.",
    configSchema: {
      type: "object",
      properties: { minLength: { type: "integer", default: 12 } },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "boolean" },
    factory: (config) => (ctx) => {
      const minLength =
        typeof config?.minLength === "number" && Number.isFinite(config.minLength)
          ? Math.max(0, Math.floor(config.minLength))
          : 12;
      return succeed(String(ctx.prev).length > minLength);
    },
  },
  {
    id: "predicate.contains",
    title: "Contains text",
    category: "Predicate",
    description: "True when the previous text contains a configured substring.",
    configSchema: {
      type: "object",
      properties: { needle: { type: "string", default: "urgent" } },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "boolean" },
    factory: (config) => (ctx) => {
      const needle = typeof config?.needle === "string" ? config.needle : "";
      return succeed(needle ? String(ctx.prev).includes(needle) : false);
    },
  },
];
