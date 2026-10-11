import { succeed } from "@spilne/perfect-core";
import type { ActivityLibrary } from "./types.ts";

export const textActivities: ActivityLibrary = () => [
  {
    id: "text.identity",
    title: "Identity",
    category: "Text",
    description: "Pass the previous value through unchanged.",
    outputSchema: {},
    factory: () => (ctx) => succeed(ctx.prev),
  },
  {
    id: "text.uppercase",
    title: "Uppercase text",
    category: "Text",
    description: "Convert the previous value to uppercase text.",
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    factory: () => (ctx) => succeed(String(ctx.prev).toUpperCase()),
  },
  {
    id: "text.case",
    title: "Change case",
    category: "Text",
    description: "Convert text to upper, lower or title case.",
    configSchema: {
      type: "object",
      properties: {
        mode: { type: "string", enum: ["upper", "lower", "title"], default: "upper" },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    factory: (config) => (ctx) => {
      const text = String(ctx.prev);
      if (config?.mode === "lower") return succeed(text.toLowerCase());
      if (config?.mode === "title") return succeed(text.replace(/\b\w/g, (c) => c.toUpperCase()));
      return succeed(text.toUpperCase());
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
    factory: (config) => (ctx) => {
      const prefix = typeof config?.prefix === "string" ? config.prefix : "";
      const suffix = typeof config?.suffix === "string" ? config.suffix : "";
      return succeed(`${prefix}${String(ctx.prev)}${suffix}`);
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
    factory: (config) => (ctx) => {
      const maxLength =
        typeof config?.maxLength === "number" && Number.isFinite(config.maxLength)
          ? Math.max(0, Math.floor(config.maxLength))
          : 80;
      const text = String(ctx.prev);
      if (text.length <= maxLength) return succeed(text);
      const suffix = config?.ellipsis === false ? "" : "...";
      return succeed(`${text.slice(0, Math.max(0, maxLength - suffix.length))}${suffix}`);
    },
  },
  {
    id: "text.replace",
    title: "Replace text",
    category: "Text",
    description: "Replace every occurrence of a string.",
    configSchema: {
      type: "object",
      properties: {
        search: { type: "string", default: "foo" },
        replacement: { type: "string", default: "bar" },
      },
    },
    inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    factory: (config) => (ctx) => {
      const search = typeof config?.search === "string" ? config.search : "";
      const replacement = typeof config?.replacement === "string" ? config.replacement : "";
      return succeed(search ? String(ctx.prev).split(search).join(replacement) : ctx.prev);
    },
  },
  {
    id: "text.length",
    title: "Text length",
    category: "Text",
    description: "Return the character length of the previous value.",
    inputSchema: { type: "string" },
    outputSchema: { type: "number" },
    factory: () => (ctx) => succeed(String(ctx.prev).length),
  },
];
