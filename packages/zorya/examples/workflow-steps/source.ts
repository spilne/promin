import { succeed } from "@spilne/perfect-core";
import type { ActivityLibrary } from "./types.ts";

export const sourceActivities: ActivityLibrary = () => [
  {
    id: "source.input",
    title: "Workflow input",
    category: "Source",
    description: "Emit the workflow input, wherever the step sits in the graph.",
    outputSchema: {},
    factory: () => (ctx) => succeed(ctx.input),
  },
  {
    id: "source.literal",
    title: "Literal value",
    category: "Source",
    description: "Emit a configured value.",
    configSchema: {
      type: "object",
      properties: {
        value: {
          type: "string",
          default: "hello workflow",
          description: "Value emitted by this step.",
        },
      },
    },
    outputSchema: {},
    factory: (config) => () => succeed(config?.value ?? "hello workflow"),
  },
];
