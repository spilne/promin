import { z } from "zod";

import { conditionDepth, MAX_CONDITION_DEPTH } from "./conditions.ts";
import { compileOutputSchema, OutputSchemaError } from "./output-schema.ts";
import type { DecisionQuestion } from "../decision/types.ts";
import type {
  Condition,
  DagNode,
  DagRecipe,
  DecisionModelRef,
  ForEach,
  NodeInputSource,
  Repeat,
  TransformOp,
} from "./types.ts";

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

const ForEachSchema: z.ZodType<ForEach> = z
  .object({
    items: NodeInputSourceSchema,
    as: z.string(),
    maxItems: z.number().int().min(1).max(100),
    concurrency: z.number().int().min(1).max(16).optional(),
    onItemError: z.enum(["null", "fail"]).optional(),
  })
  .strict();

const RepeatSchema: z.ZodType<Repeat> = z
  .object({
    until: ConditionSchema,
    maxIterations: z.number().int().min(2).max(20),
    onExhausted: z.enum(["fail", "continue"]).optional(),
  })
  .strict();

const AgentNodeSchema = z
  .object({
    id: z.string().regex(NODE_ID_PATTERN),
    kind: z.literal("agent"),
    agentId: z.string(),
    inputs: z.record(z.string(), NodeInputSourceSchema),
    output: z
      .object({ schema: z.record(z.string(), z.unknown()) })
      .strict()
      .optional(),
    forEach: ForEachSchema.optional(),
    repeat: RepeatSchema.optional(),
    join: z.enum(["all", "any"]).optional(),
    onError: z.enum(["abort", "skip"]).optional(),
  })
  .strict();

const TransformOpSchema: z.ZodType<TransformOp> = z.discriminatedUnion("op", [
  z.object({ op: z.literal("collect"), fields: z.array(z.string()) }).strict(),
  z.object({ op: z.literal("pick"), path: z.string() }).strict(),
  z.object({ op: z.literal("flatten") }).strict(),
  z.object({ op: z.literal("compact") }).strict(),
  z.object({ op: z.literal("count") }).strict(),
  z.object({ op: z.literal("dedupe"), by: z.string().optional() }).strict(),
  z.object({ op: z.literal("filter"), where: ConditionSchema }).strict(),
  z
    .object({
      op: z.literal("sort"),
      by: z.string(),
      order: z.enum(["asc", "desc"]).optional(),
    })
    .strict(),
  z.object({ op: z.literal("take"), count: z.number().int().nonnegative() }).strict(),
]);

const TransformNodeSchema = z
  .object({
    id: z.string().regex(NODE_ID_PATTERN),
    kind: z.literal("transform"),
    inputs: z.record(z.string(), NodeInputSourceSchema),
    join: z.enum(["all", "any"]).optional(),
    onError: z.enum(["abort", "skip"]).optional(),
    from: z.string().optional(),
    ops: z.array(TransformOpSchema).min(1).max(16),
  })
  .strict();

const SubDagNodeSchema = z
  .object({
    id: z.string().regex(NODE_ID_PATTERN),
    kind: z.literal("dag"),
    dagId: z.string(),
    version: z.string(),
    inputs: z.record(z.string(), NodeInputSourceSchema),
    forEach: ForEachSchema.optional(),
    repeat: RepeatSchema.optional(),
    join: z.enum(["all", "any"]).optional(),
    onError: z.enum(["abort", "skip"]).optional(),
  })
  .strict();

const DecisionModelRefSchema: z.ZodType<DecisionModelRef> = z
  .object({
    provider: z.string(),
    id: z.string(),
    credentialRef: z.string().optional(),
  })
  .strict();

const ChoiceQuestionSchema = z
  .object({
    type: z.literal("choice"),
    instructions: z.string(),
    options: z
      .array(
        z
          .object({
            name: z.string().regex(NODE_ID_PATTERN),
            description: z.string().optional(),
          })
          .strict(),
      )
      .min(2)
      .max(64),
  })
  .strict()
  .superRefine((question, ctx) => {
    const names = new Set<string>();
    for (const [index, option] of question.options.entries()) {
      if (names.has(option.name)) {
        ctx.addIssue({
          code: "custom",
          path: ["options", index, "name"],
          message: `duplicate option name "${option.name}"`,
        });
      }
      names.add(option.name);
    }
  });

const DecisionQuestionSchema: z.ZodType<DecisionQuestion> = z.discriminatedUnion("type", [
  ChoiceQuestionSchema,
  z
    .object({
      type: z.literal("score"),
      instructions: z.string(),
      levels: z.array(z.string()).min(2).max(10),
    })
    .strict(),
  z.object({ type: z.literal("yesno"), instructions: z.string() }).strict(),
]);

const DecisionQuestionsSchema = z
  .record(z.string().regex(NODE_ID_PATTERN), DecisionQuestionSchema)
  .superRefine((questions, ctx) => {
    const count = Object.keys(questions).length;
    if (count < 1 || count > 16) {
      ctx.addIssue({ code: "custom", message: "questions must contain between 1 and 16 entries" });
    }
  });

const DecideNodeSchema = z
  .object({
    id: z.string().regex(NODE_ID_PATTERN),
    kind: z.literal("decide"),
    model: DecisionModelRefSchema,
    questions: DecisionQuestionsSchema,
    inputs: z.record(z.string(), NodeInputSourceSchema),
    join: z.enum(["all", "any"]).optional(),
    onError: z.enum(["abort", "skip"]).optional(),
  })
  .strict();

const DagNodeSchema: z.ZodType<DagNode> = z.discriminatedUnion("kind", [
  AgentNodeSchema,
  TransformNodeSchema,
  SubDagNodeSchema,
  DecideNodeSchema,
]);

const DagEdgeSchema = z
  .object({
    from: z.string(),
    to: z.string(),
    condition: ConditionSchema.optional(),
  })
  .strict();

export const DagRecipeSchema: z.ZodType<DagRecipe> = z
  .object({
    nodes: z.array(DagNodeSchema),
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
  if (!parsed.success) {
    return parsed.error.issues.map((issue) => `${issuePath(issue.path)}: ${issue.message}`);
  }
  const issues: string[] = [];
  for (const [index, node] of parsed.data.nodes.entries()) {
    if (node.kind !== "agent" || node.output === undefined) continue;
    try {
      compileOutputSchema(node.output.schema);
    } catch (error) {
      if (!(error instanceof OutputSchemaError)) throw error;
      issues.push(`nodes[${index}].output.schema: ${error.message}`);
    }
  }
  return issues;
}
