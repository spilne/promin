import { Pipeline } from "@promin/core";
import type { WorkflowStepLibrary } from "./types.ts";

export const sourceSteps: WorkflowStepLibrary = () => [
  {
    id: "source.input",
    title: "Workflow input",
    category: "Source",
    description: "Start from the workflow input payload.",
    outputSchema: {},
    activity: () => (ctx) => Pipeline.succeed(ctx.input),
  },
  {
    id: "source.literal",
    title: "Literal value",
    category: "Source",
    description: "Emit a configured JSON-compatible value.",
    configSchema: {
      type: "object",
      properties: {
        value: {
          type: "string",
          default: "hello workflow",
          description: "Value emitted by this source step.",
        },
      },
    },
    outputSchema: {},
    activity: (config) => () => Pipeline.succeed(config?.value ?? "hello workflow"),
  },
];
