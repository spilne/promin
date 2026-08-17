import { Pipeline } from "@promin/core";
import type { WorkflowStepLibrary } from "./types.ts";

export const controlSteps: WorkflowStepLibrary = () => [
  {
    id: "control.if",
    title: "IF branch",
    category: "Control",
    description: "Run one of two activities based on a predicate result.",
    kind: "branch",
    defaultConfig: {
      conditionRef: "predicate.long",
      ifTrueActivityRef: "transform.uppercase",
      ifFalseActivityRef: "transform.identity",
    },
    outputSchema: {},
    activity: () => (ctx) => Pipeline.succeed(ctx.prev),
  },
  {
    id: "control.parallel",
    title: "Parallel branches",
    category: "Control",
    description: "Run multiple branch activities against the same input and collect their outputs.",
    kind: "parallel",
    outputSchema: { type: "object" },
    activity: () => (ctx) => Pipeline.succeed(ctx.prev),
  },
  {
    id: "predicate.long",
    title: "Predicate: long text",
    category: "Predicate",
    description: "Return true when the previous text is longer than a configured threshold.",
    configSchema: {
      type: "object",
      properties: {
        minLength: { type: "integer", default: 12 },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "boolean" },
    activity: (config) => (ctx) => {
      const minLength =
        typeof config?.minLength === "number" && Number.isFinite(config.minLength)
          ? Math.max(0, Math.floor(config.minLength))
          : 12;
      return Pipeline.succeed(String(ctx.prev).length > minLength);
    },
  },
  {
    id: "predicate.contains",
    title: "Predicate: contains text",
    category: "Predicate",
    description: "Return true when the previous text contains a configured substring.",
    configSchema: {
      type: "object",
      properties: {
        needle: { type: "string", default: "urgent" },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "boolean" },
    activity: (config) => (ctx) => {
      const needle = typeof config?.needle === "string" ? config.needle : "";
      return Pipeline.succeed(needle ? String(ctx.prev).includes(needle) : false);
    },
  },
];
