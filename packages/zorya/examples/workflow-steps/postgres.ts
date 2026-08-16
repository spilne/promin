import { Pipeline } from "@promin/core";
import type { WorkflowStepLibrary } from "./types.ts";

export const postgresSteps: WorkflowStepLibrary = (deps) => {
  if (!deps.postgres) return [];
  return [
    {
      id: "postgres.query",
      title: "Postgres query",
      category: "Postgres",
      description: "Run a configured SQL query through the host-provided Postgres dependency.",
      capabilities: ["postgres"],
      configSchema: {
        type: "object",
        required: ["sql"],
        properties: {
          sql: {
            type: "string",
            default: "select now() as now",
            description: "SQL text executed by the host-provided query client.",
          },
        },
      },
      outputSchema: { type: "array" },
      activity: (config) => () => {
        const sql = typeof config?.sql === "string" ? config.sql : "";
        return Pipeline.fromPromise(() => deps.postgres!.queryJson(sql));
      },
    },
  ];
};
