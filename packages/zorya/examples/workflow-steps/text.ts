import { Pipeline } from "@promin/core";
import type { WorkflowStepLibrary } from "./types.ts";

export const textSteps: WorkflowStepLibrary = () => [
  {
    id: "transform.identity",
    title: "Identity",
    category: "Text",
    description: "Pass the previous value through unchanged.",
    outputSchema: {},
    activity: () => (ctx) => Pipeline.succeed(ctx.prev),
  },
  {
    id: "transform.uppercase",
    title: "Uppercase text",
    category: "Text",
    description: "Convert the previous value to uppercase text.",
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
  },
  {
    id: "text.case",
    title: "Change case",
    category: "Text",
    description: "Convert text using an enum-backed mode.",
    configSchema: {
      type: "object",
      properties: {
        mode: {
          type: "string",
          enum: ["upper", "lower", "title"],
          default: "upper",
        },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    activity: (config) => (ctx) => {
      const text = String(ctx.prev);
      if (config?.mode === "lower") return Pipeline.succeed(text.toLowerCase());
      if (config?.mode === "title") {
        return Pipeline.succeed(text.replace(/\b\w/g, (char) => char.toUpperCase()));
      }
      return Pipeline.succeed(text.toUpperCase());
    },
  },
  {
    id: "text.template",
    title: "Template text",
    category: "Text",
    description: "Wrap the previous value with a configurable prefix and suffix.",
    configSchema: {
      type: "object",
      required: ["prefix"],
      properties: {
        prefix: {
          type: "string",
          default: "Result: ",
          description: "Text placed before the previous value.",
        },
        suffix: {
          type: "string",
          default: "",
          description: "Text placed after the previous value.",
        },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    activity: (config) => (ctx) => {
      const prefix = typeof config?.prefix === "string" ? config.prefix : "";
      const suffix = typeof config?.suffix === "string" ? config.suffix : "";
      return Pipeline.succeed(`${prefix}${String(ctx.prev)}${suffix}`);
    },
  },
  {
    id: "text.truncate",
    title: "Truncate text",
    category: "Text",
    description: "Limit text length and optionally append an ellipsis.",
    configSchema: {
      type: "object",
      properties: {
        maxLength: { type: "integer", default: 80 },
        ellipsis: { type: "boolean", default: true },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    activity: (config) => (ctx) => {
      const maxLength =
        typeof config?.maxLength === "number" && Number.isFinite(config.maxLength)
          ? Math.max(0, Math.floor(config.maxLength))
          : 80;
      const text = String(ctx.prev);
      if (text.length <= maxLength) return Pipeline.succeed(text);
      const suffix = config?.ellipsis === false ? "" : "...";
      return Pipeline.succeed(`${text.slice(0, Math.max(0, maxLength - suffix.length))}${suffix}`);
    },
  },
  {
    id: "text.replace",
    title: "Replace text",
    category: "Text",
    description: "Replace text using simple string matching.",
    configSchema: {
      type: "object",
      properties: {
        search: { type: "string", default: "foo" },
        replacement: { type: "string", default: "bar" },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    activity: (config) => (ctx) => {
      const search = typeof config?.search === "string" ? config.search : "";
      const replacement = typeof config?.replacement === "string" ? config.replacement : "";
      return Pipeline.succeed(search ? String(ctx.prev).split(search).join(replacement) : ctx.prev);
    },
  },
  {
    id: "text.length",
    title: "Text length",
    category: "Text",
    description: "Return the character length of the previous value.",
    inputSchema: { type: "string" },
    outputSchema: { type: "number" },
    activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).length),
  },
];
