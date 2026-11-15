import { z } from "zod";

import { conditionDepth, MAX_CONDITION_DEPTH } from "./conditions.ts";
import type { Condition, DagRecipe, NodeInputSource } from "./types.ts";

export const NODE_ID_PATTERN = /^[A-Za-z0-9_-]+$/;

const NodeInputSourceSchema: z.ZodType<NodeInputSource> = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("initial"), path: z.string() }).strict(),
  z.object({ kind: z.literal("node"), nodeId: z.string(), path: z.string() }).strict(),
  z.object({ kind: z.literal("literal"), value: z.unknown() }).strict(),
]);

const ConditionSchema: z.ZodType<Condition> = z
  .lazy(() =>
    z.discriminatedUnion("kind", [
      z
        .object({
          kind: z.literal("equals"),
          path: z.string(),
          value: z.union([z.string(), z.number(), z.boolean(), z.null()]),
        })
        .strict(),
      z
        .object({
          kind: z.literal("in"),
          path: z.string(),
          values: z.array(z.union([z.string(), z.number(), z.boolean(), z.null()])),
        })
        .strict(),
      z.object({ kind: z.literal("exists"), path: z.string() }).strict(),
      z
        .object({
          kind: z.literal("compare"),
          path: z.string(),
          op: z.enum(["gt", "gte", "lt", "lte"]),
          value: z.number(),
        })
        .strict(),
      z.object({ kind: z.literal("and"), all: z.array(ConditionSchema) }).strict(),
      z.object({ kind: z.literal("or"), any: z.array(ConditionSchema) }).strict(),
      z.object({ kind: z.literal("not"), of: ConditionSchema }).strict(),
    ]),
  )
  .superRefine((condition, ctx) => {
    if (conditionDepth(condition) > MAX_CONDITION_DEPTH) {
      ctx.addIssue({
        code: "custom",
        message: `condition nesting exceeds ${MAX_CONDITION_DEPTH}`,
      });
    }
  });

const AgentNodeSchema = z
  .object({
    id: z.string().regex(NODE_ID_PATTERN),
    kind: z.literal("agent"),
    agentId: z.string(),
    inputs: z.record(z.string(), NodeInputSourceSchema),
    onError: z.enum(["abort", "skip"]).optional(),
  })
  .strict();

const DagEdgeSchema = z
  .object({
    from: z.string(),
    to: z.string(),
    condition: ConditionSchema.optional(),
  })
  .strict();

export const DagRecipeSchema: z.ZodType<DagRecipe> = z
  .object({
    nodes: z.array(AgentNodeSchema),
    edges: z.array(DagEdgeSchema),
    entry: z.array(z.string()),
    terminals: z.array(z.string()),
    metadata: z
      .object({
        description: z.string().optional(),
        tags: z.array(z.string()).optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

function issuePath(path: ReadonlyArray<PropertyKey>): string {
  let formatted = "";
  for (const segment of path) {
    if (typeof segment === "number") formatted += `[${segment}]`;
    else formatted += formatted.length === 0 ? String(segment) : `.${String(segment)}`;
  }
  return formatted || "<root>";
}

export function recipeIssues(input: unknown): string[] {
  const parsed = DagRecipeSchema.safeParse(input);
  if (parsed.success) return [];
  return parsed.error.issues.map((issue) => `${issuePath(issue.path)}: ${issue.message}`);
}
