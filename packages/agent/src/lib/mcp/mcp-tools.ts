// ---------------------------------------------------------------------------
// `createMcpTools` — one MCP server's tools as `AgentTool`s.
//
// Each tool:
//   - is named `<server>__<tool>`, provider-safe (see `mcpToolName`), and
//     keeps its MCP name in `tool.mcp`;
//   - advertises the server's `inputSchema` as `jsonSchema` and validates
//     the model's input against it (see mcp-schema-validator.ts for what is
//     not validated and why), so bad input fails before it reaches the
//     server;
//   - carries the operator's trust settings: approval by default, not
//     idempotent unless marked, the server's time limit;
//   - treats what the server says as untrusted: its tool name and
//     description are capped, flattened and labelled, and every result goes
//     back to the model fenced as reference data.
//
// A server gets at most `maxTools` tools (default 128) and
// `MAX_MCP_SERVER_SCHEMA_CHARS` of schema in total; the rest are skipped and
// reported with a code, never with server-supplied text.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { fenceReferenceData, neutralizeReferenceDelimiters, oneLine } from "../reference-data.ts";
import type { AgentTool, ToolExecuteContext } from "../tool.ts";
import { normalizeToolJsonSchema } from "../tool-json-schema.ts";
import type { McpServerHandle } from "./mcp-client-pool.ts";
import type { McpInputValidators } from "./mcp-schema-validator.ts";
import { mcpToolName } from "./mcp-tool-name.ts";
import {
  DEFAULT_MCP_MAX_TOOLS,
  DEFAULT_MCP_TIMEOUT_MS,
  type McpToolDefinition,
  type McpToolResult,
} from "./types.ts";

/** Longest server-supplied tool description passed to the model. */
export const MAX_MCP_DESCRIPTION_CHARS = 1_024;
/** Largest server-supplied input schema accepted (serialized chars). */
export const MAX_MCP_SCHEMA_CHARS = 32_768;
/** Most schema (serialized chars) one server's tools may add together. */
export const MAX_MCP_SERVER_SCHEMA_CHARS = 262_144;
/** Longest tool result passed to the model; the rest is cut. */
export const MAX_MCP_RESULT_CHARS = 100_000;
/** Longest MCP tool name accepted (the MCP spec's limit). */
export const MAX_MCP_TOOL_NAME_CHARS = 128;

/** Why a server tool was left out. */
export type McpSkipCode =
  | "invalid_name"
  | "schema_too_large"
  | "schema_unusable"
  | "name_taken"
  | "too_many_tools";

/** A server tool that could not be exposed, and why. */
export interface SkippedMcpTool {
  /** The tool's name, flattened and capped for display. */
  readonly tool: string;
  readonly code: McpSkipCode;
}

export interface McpToolSet {
  /** Keyed by agent-facing tool name. */
  readonly tools: Record<string, AgentTool<Record<string, unknown>, string>>;
  /** Server tools left out. */
  readonly skipped: ReadonlyArray<SkippedMcpTool>;
}

// eslint-disable-next-line no-control-regex
const VALID_MCP_TOOL_NAME = /^[^\u0000-\u001f\u007f]+$/;

/**
 * Build `AgentTool`s for the tools `handle`'s server lists (filtered by the
 * server's `tools` allowlist, when set). Pass `definitions` to skip the
 * `tools/list` round trip.
 */
export async function createMcpTools(params: {
  readonly handle: McpServerHandle;
  readonly definitions?: ReadonlyArray<McpToolDefinition>;
  readonly signal?: AbortSignal;
}): Promise<McpToolSet> {
  const { handle } = params;
  const server = handle.server;
  const listed =
    params.definitions ??
    (await handle.listTools({
      ...(params.signal !== undefined && { signal: params.signal }),
      timeoutMs: server.timeoutMs ?? DEFAULT_MCP_TIMEOUT_MS,
    }));
  const allow = server.tools ? new Set(server.tools) : undefined;
  const maxTools = server.maxTools ?? DEFAULT_MCP_MAX_TOOLS;
  const tools: Record<string, AgentTool<Record<string, unknown>, string>> = {};
  const skipped: SkippedMcpTool[] = [];
  let count = 0;
  let schemaChars = 0;
  for (const def of listed) {
    if (allow && !allow.has(def.name)) continue;
    const display = displayToolName(def.name);
    if (count >= maxTools) {
      skipped.push({ tool: display, code: "too_many_tools" });
      continue;
    }
    const built = buildTool({ handle, def, validators: handle.validators });
    if ("code" in built) {
      skipped.push({ tool: display, code: built.code });
      continue;
    }
    if (schemaChars + built.schemaChars > MAX_MCP_SERVER_SCHEMA_CHARS) {
      skipped.push({ tool: display, code: "too_many_tools" });
      continue;
    }
    if (tools[built.tool.name] !== undefined) {
      skipped.push({ tool: display, code: "name_taken" });
      continue;
    }
    tools[built.tool.name] = built.tool;
    count += 1;
    schemaChars += built.schemaChars;
  }
  return { tools, skipped };
}

/** An MCP tool name as it may appear in text the model or an operator reads. */
export function displayToolName(name: string): string {
  const flat = oneLine(name);
  const capped =
    flat.length > MAX_MCP_TOOL_NAME_CHARS ? `${flat.slice(0, MAX_MCP_TOOL_NAME_CHARS)}…` : flat;
  return neutralizeReferenceDelimiters(capped);
}

function buildTool(params: {
  handle: McpServerHandle;
  def: McpToolDefinition;
  validators: McpInputValidators;
}):
  | { tool: AgentTool<Record<string, unknown>, string>; schemaChars: number }
  | { code: McpSkipCode } {
  const { handle, def } = params;
  const server = handle.server;
  if (def.name.length > MAX_MCP_TOOL_NAME_CHARS || !VALID_MCP_TOOL_NAME.test(def.name)) {
    return { code: "invalid_name" };
  }
  const schemaChars = JSON.stringify(def.inputSchema).length;
  if (schemaChars > MAX_MCP_SCHEMA_CHARS) return { code: "schema_too_large" };
  let jsonSchema: Record<string, unknown>;
  try {
    jsonSchema = normalizeToolJsonSchema(def.inputSchema);
  } catch {
    return { code: "schema_unusable" };
  }
  const display = displayToolName(def.name);
  const timeoutMs = server.timeoutMs ?? DEFAULT_MCP_TIMEOUT_MS;
  const tool: AgentTool<Record<string, unknown>, string> = {
    name: mcpToolName({ server: server.name, tool: def.name }),
    description: untrustedDescription({ server: server.name, display, def }),
    parameters: inputValidator({ schema: def.inputSchema, validators: params.validators }),
    jsonSchema,
    requireApproval: server.requireApproval ?? true,
    idempotent: server.idempotent ?? false,
    timeoutMs,
    mcp: { server: server.name, tool: def.name },
    execute: async (input: Record<string, unknown>, ctx?: ToolExecuteContext) => {
      const result = await handle.callTool({
        name: def.name,
        arguments: input,
        options: {
          ...(ctx?.signal !== undefined && { signal: ctx.signal }),
          timeoutMs: ctx?.timeoutMs ?? timeoutMs,
        },
      });
      const fenced = fenceResult({ server: server.name, tool: display, result });
      if (result.isError) {
        throw new Error(`MCP tool '${display}' on server '${server.name}' failed.\n${fenced}`);
      }
      return fenced;
    },
  };
  return { tool, schemaChars };
}

/**
 * Zod input schema that checks the arguments against the server's JSON
 * Schema. A schema that can't be validated safely is left to the server:
 * the arguments then only have to be an object.
 */
function inputValidator(params: {
  schema: Readonly<Record<string, unknown>>;
  validators: McpInputValidators;
}): z.ZodType<Record<string, unknown>> {
  const base = z.record(z.string(), z.unknown());
  const check = params.validators.get(params.schema);
  if (!check) return base;
  return base.superRefine((input, ctx) => {
    const result = check(input);
    if (!result.valid) {
      ctx.addIssue({
        code: "custom",
        message: neutralizeReferenceDelimiters(
          (result.errorMessage ?? "input does not match the tool's schema").slice(0, 1_000),
        ),
      });
    }
  });
}

function untrustedDescription(params: {
  server: string;
  display: string;
  def: McpToolDefinition;
}): string {
  const raw = oneLine(params.def.description ?? "");
  const capped =
    raw.length > MAX_MCP_DESCRIPTION_CHARS ? `${raw.slice(0, MAX_MCP_DESCRIPTION_CHARS)}…` : raw;
  const label = `Tool '${params.display}' from MCP server '${params.server}'.`;
  return capped.length > 0
    ? `${label} Server-provided description: ${neutralizeReferenceDelimiters(capped)}`
    : label;
}

function fenceResult(params: { server: string; tool: string; result: McpToolResult }): string {
  let text = stringifyResult(params.result);
  if (text.length > MAX_MCP_RESULT_CHARS) {
    text = `${text.slice(0, MAX_MCP_RESULT_CHARS)}\n[truncated: ${text.length} chars total]`;
  }
  return (
    fenceReferenceData({
      source: `mcp:${params.server}`,
      sections: [{ title: `${params.tool} result`, body: text }],
    }) ?? `MCP tool '${params.tool}' returned no content.`
  );
}

function stringifyResult(result: McpToolResult): string {
  const parts: string[] = [];
  for (const block of result.content) {
    switch (block.type) {
      case "text":
        parts.push(block.text);
        break;
      case "image":
        parts.push(
          `[image: ${oneLine(block.mimeType).slice(0, 100)}, ${block.data.length} bytes base64]`,
        );
        break;
      case "resource":
        parts.push(
          block.resource.text !== undefined
            ? `[resource ${block.resource.uri}]\n${block.resource.text}`
            : `[resource ${block.resource.uri}]`,
        );
        break;
    }
  }
  return parts.join("\n");
}
