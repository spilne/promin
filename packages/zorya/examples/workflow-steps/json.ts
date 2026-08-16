import { Pipeline } from "@promin/core";
import type { WorkflowStepLibrary } from "./types.ts";

export const jsonSteps: WorkflowStepLibrary = () => [
  {
    id: "json.pick",
    title: "Pick path",
    category: "JSON",
    description: "Read a dot-separated path from the previous value.",
    configSchema: {
      type: "object",
      required: ["path"],
      properties: {
        path: {
          type: "string",
          default: "text",
          description: "Dot path such as user.name or payload.items.0.",
        },
        fallback: {
          type: "string",
          default: "",
          description: "Returned when the path is missing.",
        },
      },
    },
    outputSchema: {},
    activity: (config) => (ctx) => {
      const path = typeof config?.path === "string" ? config.path : "";
      const fallback = typeof config?.fallback === "string" ? config.fallback : undefined;
      return Pipeline.succeed(readPath(ctx.prev, path) ?? fallback);
    },
  },
  {
    id: "json.wrap",
    title: "Wrap object",
    category: "JSON",
    description: "Wrap the previous value in an object under a configured key.",
    configSchema: {
      type: "object",
      properties: {
        key: { type: "string", default: "value" },
      },
    },
    outputSchema: { type: "object" },
    activity: (config) => (ctx) => {
      const key = typeof config?.key === "string" && config.key ? config.key : "value";
      return Pipeline.succeed({ [key]: ctx.prev });
    },
  },
];

function readPath(value: unknown, path: string): unknown {
  if (!path) return value;
  return path.split(".").reduce<unknown>((current, segment) => {
    if (current === null || current === undefined) return undefined;
    if (Array.isArray(current) && /^\d+$/.test(segment)) return current[Number(segment)];
    if (typeof current === "object" && segment in current) {
      return (current as Record<string, unknown>)[segment];
    }
    return undefined;
  }, value);
}
