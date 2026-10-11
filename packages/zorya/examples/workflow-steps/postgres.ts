import { tryPromise } from "@spilne/perfect-core";
import type { ActivityLibrary } from "./types.ts";

// Offered only when the host passes a Postgres client: activities that need
// a dependency the host lacks never reach the palette.
export const postgresActivities: ActivityLibrary = (deps) => {
  const postgres = deps.postgres;
  if (!postgres) return [];
  return [
    {
      id: "postgres.query",
      title: "Postgres query",
      category: "Postgres",
      description: "Run a configured SQL query through the host's Postgres client.",
      capabilities: ["postgres"],
      configSchema: {
        type: "object",
        required: ["sql"],
        properties: {
          sql: {
            type: "string",
            default: "select now() as now",
            description: "SQL text the host's query client runs.",
          },
        },
      },
      outputSchema: { type: "array" },
      factory: (config) => {
        const sql = typeof config?.sql === "string" ? config.sql : "";
        return () =>
          tryPromise(
            () => postgres.queryJson(sql),
            (e) => e,
          ).orDie();
      },
    },
  ];
};
