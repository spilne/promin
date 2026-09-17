import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { CommandDef } from "./console-panes.ts";

/** A REPL command: metadata for display/completion + a handler. */
export interface ReplCommand extends CommandDef {
  handle(input: string): Promise<void> | void;
}

/**
 * Match input against the command list and run the first match.
 * Matching rules:
 *   - No `args`: exact match only (`input === cmd`)
 *   - Has `args`: exact match OR input starts with `cmd + " "` (for optional/required args)
 * Returns true when a command was matched and handled.
 */
export async function dispatchCommand(commands: ReplCommand[], input: string): Promise<boolean> {
  for (const cmd of commands) {
    const matched = input === cmd.cmd || (!!cmd.args && input.startsWith(`${cmd.cmd} `));
    if (matched) {
      await cmd.handle(input);
      return true;
    }
  }
  return false;
}

// ---- :tool direct-call ----

export type DirectCallResult =
  | { ok: true; title: string; lines: string[] }
  | { ok: false; error: string };

/**
 * Handles the `:toolName [json]` REPL syntax.
 * Returns null when the input is not a direct tool call.
 */
export async function executeDirectCall(
  input: string,
  registry: ToolRegistry,
): Promise<DirectCallResult | null> {
  if (!input.startsWith(":")) return null;

  const rest = input.slice(1);
  const spaceIdx = rest.indexOf(" ");
  const toolName = spaceIdx === -1 ? rest : rest.slice(0, spaceIdx);
  const jsonStr = spaceIdx === -1 ? "" : rest.slice(spaceIdx + 1).trim();

  if (!toolName) return null;

  const tools = registry.getTools();
  const toolDef = tools[toolName];
  if (!toolDef) return { ok: false, error: `Unknown tool: ${toolName}` };

  let toolInput: unknown = {};
  if (jsonStr) {
    try {
      toolInput = JSON.parse(jsonStr);
    } catch {
      return { ok: false, error: `Invalid JSON: ${jsonStr}` };
    }
  }

  try {
    const parsed = toolDef.parameters.parse(toolInput);
    // biome-ignore lint/suspicious/noExplicitAny: dynamic tool input
    const result = await toolDef.execute(parsed as any);
    const output = typeof result === "string" ? result : JSON.stringify(result, null, 2);
    return {
      ok: true,
      title: `:${toolName}`,
      lines: output.split("\n").map((l) => `  ${l}`),
    };
  } catch (err) {
    return { ok: false, error: `Tool error: ${err instanceof Error ? err.message : String(err)}` };
  }
}
