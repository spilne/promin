// ---------------------------------------------------------------------------
// resolveMcpEndpointTools — turn an endpoint version's tool bindings into the
// tools it actually exposes. Each binding is looked up in its source (the
// shared tools, or the endpoint's own private tools) and dropped, with a
// reason, when it can't be served safely over MCP:
//
//   not_found          the source has no such tool
//   disabled           the operator kill-switched it (`enabled: false`)
//   requires_approval  it waits for a human approval and the endpoint does
//                      not trust the client's confirmation prompt
//   delegated          a sub-agent tool — only the agent engine can run it
//   capability_missing an elevated tool whose capability the endpoint lacks
//   invalid_name       its exposed name is not a valid MCP tool name
//   name_taken         an earlier binding already exposes that name
// ---------------------------------------------------------------------------

import { IMPLICIT_ELEVATED_CAPABILITY, type AgentTool } from "../tool.ts";
import {
  MCP_ENDPOINT_TOOL_NAME,
  type McpEndpointRecipe,
  type McpEndpointResolvedTool,
  type McpEndpointSkippedTool,
  type McpEndpointToolSource,
  type ResolvedMcpEndpointTools,
} from "./types.ts";

/** Read a tool source as a record (a `ToolRegistry` is read live). */
export function readToolSource(
  source: McpEndpointToolSource | undefined,
): Readonly<Record<string, AgentTool<any, any>>> {
  if (source === undefined) return {};
  const getTools = (source as { getTools?: unknown }).getTools;
  if (typeof getTools === "function") {
    return (source as { getTools(): Readonly<Record<string, AgentTool<any, any>>> }).getTools();
  }
  return source as Readonly<Record<string, AgentTool<any, any>>>;
}

/** Merge several sources; the first to claim a name wins (in-process before file). */
export function mergeToolSources(
  sources: ReadonlyArray<McpEndpointToolSource>,
): Readonly<Record<string, AgentTool<any, any>>> {
  const out: Record<string, AgentTool<any, any>> = {};
  for (const source of sources) {
    for (const [name, tool] of Object.entries(readToolSource(source))) {
      if (!(name in out)) out[name] = tool;
    }
  }
  return out;
}

export function resolveMcpEndpointTools(params: {
  readonly endpoint: Pick<McpEndpointRecipe, "tools" | "capabilities" | "trustClientApproval">;
  /** Shared tools, keyed by name. */
  readonly shared: Readonly<Record<string, AgentTool<any, any>>>;
  /** The endpoint's own tools, keyed by name. */
  readonly privateTools?: Readonly<Record<string, AgentTool<any, any>>>;
}): ResolvedMcpEndpointTools {
  const { endpoint, shared } = params;
  const privateTools = params.privateTools ?? {};
  const tools = new Map<string, McpEndpointResolvedTool>();
  const skipped: McpEndpointSkippedTool[] = [];

  for (const binding of endpoint.tools) {
    const isPrivate = binding.private === true;
    const skip = (code: McpEndpointSkippedTool["code"]) =>
      skipped.push({ tool: binding.tool, private: isPrivate, code });
    const source = isPrivate ? privateTools : shared;
    const tool = Object.hasOwn(source, binding.tool) ? source[binding.tool] : undefined;
    if (tool === undefined) {
      skip("not_found");
      continue;
    }
    if (tool.enabled === false) {
      skip("disabled");
      continue;
    }
    if (tool.requireApproval === true && !endpoint.trustClientApproval) {
      skip("requires_approval");
      continue;
    }
    if (tool.delegation !== undefined) {
      skip("delegated");
      continue;
    }
    if (
      tool.kind === "elevated" &&
      !endpoint.capabilities.includes(tool.requires ?? IMPLICIT_ELEVATED_CAPABILITY)
    ) {
      skip("capability_missing");
      continue;
    }
    const name = binding.name ?? binding.tool;
    if (!MCP_ENDPOINT_TOOL_NAME.test(name)) {
      skip("invalid_name");
      continue;
    }
    if (tools.has(name)) {
      skip("name_taken");
      continue;
    }
    tools.set(name, {
      name,
      description: binding.description ?? tool.description,
      tool,
      private: isPrivate,
    });
  }
  return { tools, skipped };
}
