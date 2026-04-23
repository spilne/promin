import { z } from "zod";
import type { AgentTool } from "./tool.ts";
import { tool } from "./tool.ts";

/**
 * Per-command definition for use with {@link multiTool}.
 *
 * The `command` discriminant field is added to the Zod schema automatically —
 * do not include it in `parameters`.
 *
 * Use the {@link command} helper to get parameter-type checking on `execute`:
 * ```ts
 * commands: {
 *   search: command({
 *     parameters: z.object({ query: z.string() }),
 *     execute: async ({ query }) => { ... }, // query: string ✓
 *   }),
 * }
 * ```
 * Plain objects work too when type inference on `execute` is not needed.
 */
// biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
export interface CommandDef<T extends z.ZodRawShape = any> {
  description?: string;
  parameters: z.ZodObject<T>;
  execute: (input: z.infer<z.ZodObject<T>>) => Promise<unknown>;
  requireApproval?: boolean;
}

/**
 * Identity helper that captures the parameter shape so TypeScript checks
 * `execute` against the inferred input type.
 */
export function command<T extends z.ZodRawShape>(def: CommandDef<T>): CommandDef<T> {
  return def;
}

// biome-ignore lint/suspicious/noExplicitAny: widened for the heterogeneous map
type AnyCommandDef = CommandDef<any>;

function buildSchema(commands: Record<string, AnyCommandDef>): z.ZodType {
  const variants = Object.entries(commands).map(([name, def]) =>
    def.parameters.extend({ command: z.literal(name) }),
  );
  if (variants.length === 0) throw new Error("multiTool: commands must not be empty");
  if (variants.length === 1) return variants[0]!;
  // Cast through unknown: each variant has a ZodLiteral "command" field at runtime
  // even though the static type (ZodRawShape) can't express the literal constraint.
  type DiscriminantOption = z.ZodObject<{ command: z.ZodLiteral<string> } & z.ZodRawShape>;
  const typed = variants as unknown as [
    DiscriminantOption,
    DiscriminantOption,
    ...DiscriminantOption[],
  ];
  return z.discriminatedUnion("command", typed);
}

function buildDescription(description: string, commands: Record<string, AnyCommandDef>): string {
  const lines = Object.entries(commands).map(([name, def]) =>
    def.description ? `  ${name}: ${def.description}` : `  ${name}`,
  );
  return `${description}\n\nCommands:\n${lines.join("\n")}`;
}

/**
 * Build a single {@link AgentTool} that dispatches to multiple sub-handlers
 * based on a `command` field.
 *
 * Instead of exposing `searchMemory` and `saveMemory` as two separate tools, you
 * can group them under one `memory` tool — useful when you want to keep the tool
 * list short or when the operations are tightly related.
 *
 * @example
 * ```ts
 * const memoryTool = multiTool({
 *   name: "memory",
 *   description: "Read and write long-term memory.",
 *   commands: {
 *     search: command({
 *       description: "Search for relevant memories",
 *       parameters: z.object({ query: z.string(), limit: z.number().optional() }),
 *       execute: async ({ query, limit }) => store.search(query, limit),
 *     }),
 *     save: command({
 *       description: "Persist a new memory",
 *       parameters: z.object({ content: z.string() }),
 *       execute: async ({ content }) => store.save({ content }),
 *     }),
 *   },
 * });
 * ```
 *
 * The LLM receives one tool whose parameter schema is a discriminated union:
 * `{ command: "search", query: string, limit?: number } | { command: "save", content: string }`.
 */
// biome-ignore lint/suspicious/noExplicitAny: dispatched and Zod-validated at runtime
export function multiTool(config: {
  name: string;
  description: string;
  commands: Record<string, AnyCommandDef>;
  /** Apply to all commands. Per-command approval is not supported because the approval
   *  gate runs before execute() is called and cannot inspect the command field. */
  requireApproval?: boolean;
}): AgentTool<any, unknown> {
  const schema = buildSchema(config.commands);
  return tool({
    name: config.name,
    description: buildDescription(config.description, config.commands),
    parameters: schema,
    requireApproval: config.requireApproval,
    execute: async (input: { command: string } & Record<string, unknown>) => {
      const { command, ...rest } = input;
      const def = config.commands[command];
      if (!def) throw new Error(`multiTool "${config.name}": unknown command "${command}"`);
      return def.execute(rest);
    },
  });
}
