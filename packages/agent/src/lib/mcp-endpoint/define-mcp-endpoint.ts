// ---------------------------------------------------------------------------
// mcpEndpoint — define an MCP endpoint in a file under the scanned `mcp/`
// folder. A definition may mix shared tools (by name) with tools defined
// right there, which stay private to the endpoint:
//
//   // mcp/billing.ts
//   export default mcpEndpoint({
//     title: "Billing",
//     tools: [
//       "calculate",                                   // a shared tool
//       { tool: "currentTime", name: "now" },          // shared, renamed
//       tool({ name: "lookupInvoice", ... }),          // private to "billing"
//     ],
//   });
//
// `id` defaults to the file's basename (or the folder name for
// `mcp/<id>/index.ts`). The definition is branded so the scanner can tell it
// from other exports.
// ---------------------------------------------------------------------------

import type { AgentTool } from "../tool.ts";
import {
  MCP_ENDPOINT_ID_PATTERN,
  type McpEndpointMetadata,
  type McpEndpointRecipeInput,
  type McpEndpointToolBinding,
} from "./types.ts";

const BRAND = Symbol.for("promin.mcpEndpoint");

/** A tools entry in a file definition. */
export type McpEndpointToolEntry =
  /** A shared tool by name. */
  | string
  /** A tool defined in the file: private to the endpoint. */
  | AgentTool<any, any>
  /** Either of the above, with the exposed name / description overridden. */
  | {
      readonly tool: string | AgentTool<any, any>;
      readonly name?: string;
      readonly description?: string;
    };

export interface McpEndpointDefinitionInput {
  /** Endpoint id. Default: the file basename, or the folder name for `<id>/index.ts`. */
  readonly id?: string;
  readonly title?: string;
  readonly instructions?: string;
  readonly tools: ReadonlyArray<McpEndpointToolEntry>;
  readonly capabilities?: ReadonlyArray<string>;
  readonly namespaces?: ReadonlyArray<string> | null;
  readonly trustClientApproval?: boolean;
  readonly metadata?: Partial<McpEndpointMetadata>;
}

export interface McpEndpointDefinition extends McpEndpointDefinitionInput {
  readonly [BRAND]: true;
}

export function mcpEndpoint(input: McpEndpointDefinitionInput): McpEndpointDefinition {
  return { ...input, [BRAND]: true };
}

export function isMcpEndpointDefinition(value: unknown): value is McpEndpointDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<symbol, unknown>)[BRAND] === true
  );
}

/**
 * Split a definition's tools into stored bindings and the private tools
 * they reference. Throws on a private tool name used twice, or a private
 * tool without a name.
 */
export function splitMcpEndpointTools(entries: ReadonlyArray<McpEndpointToolEntry>): {
  readonly bindings: McpEndpointToolBinding[];
  readonly privateTools: Record<string, AgentTool<any, any>>;
} {
  const bindings: McpEndpointToolBinding[] = [];
  const privateTools: Record<string, AgentTool<any, any>> = {};
  const addPrivate = (tool: AgentTool<any, any>): string => {
    if (typeof tool.name !== "string" || tool.name.length === 0) {
      throw new Error("an inline tool needs a name");
    }
    const existing = privateTools[tool.name];
    if (existing !== undefined && existing !== tool) {
      throw new Error(`two inline tools are named "${tool.name}"`);
    }
    privateTools[tool.name] = tool;
    return tool.name;
  };
  for (const entry of entries) {
    if (typeof entry === "string") {
      bindings.push({ tool: entry });
    } else if (isAgentTool(entry)) {
      bindings.push({ tool: addPrivate(entry), private: true });
    } else {
      const overrides = {
        ...(entry.name !== undefined && { name: entry.name }),
        ...(entry.description !== undefined && { description: entry.description }),
      };
      if (typeof entry.tool === "string") {
        bindings.push({ tool: entry.tool, ...overrides });
      } else {
        bindings.push({ tool: addPrivate(entry.tool), private: true, ...overrides });
      }
    }
  }
  return { bindings, privateTools };
}

/** A definition turned into what the registry stores plus the code it names. */
export interface CompiledMcpEndpoint {
  readonly id: string;
  readonly input: McpEndpointRecipeInput;
  readonly privateTools: Readonly<Record<string, AgentTool<any, any>>>;
}

/**
 * Compile a definition: split its tools into stored bindings and private
 * code, and fold in `folderTools` (a folder endpoint's `tools/`). A shared
 * name that matches a folder tool binds the folder tool (the closest one),
 * and folder tools the definition does not list are exposed too. Throws on
 * a bad id, a clash between two private tools, or a nameless inline tool.
 */
export function compileMcpEndpoint(params: {
  readonly definition: McpEndpointDefinitionInput;
  /** Id when the definition has none (the file or folder name). */
  readonly fallbackId?: string;
  readonly folderTools?: Readonly<Record<string, AgentTool<any, any>>>;
}): CompiledMcpEndpoint {
  const { definition } = params;
  const folderTools = params.folderTools ?? {};
  const id = definition.id ?? params.fallbackId;
  if (id === undefined) throw new Error("an MCP endpoint needs an id");
  if (!MCP_ENDPOINT_ID_PATTERN.test(id)) {
    throw new Error(
      `"${id}" is not a valid endpoint id (letters, digits, _ and -, starting with a letter or digit)`,
    );
  }
  const split = splitMcpEndpointTools(definition.tools);
  const privateTools: Record<string, AgentTool<any, any>> = { ...folderTools };
  for (const [name, tool] of Object.entries(split.privateTools)) {
    if (privateTools[name] !== undefined && privateTools[name] !== tool) {
      throw new Error(`inline tool "${name}" clashes with a tools/ file`);
    }
    privateTools[name] = tool;
  }
  const resolved = split.bindings.map((b) =>
    !b.private && Object.hasOwn(folderTools, b.tool) ? { ...b, private: true } : b,
  );
  const listed = new Set(resolved.filter((b) => b.private).map((b) => b.tool));
  const bindings = [
    ...resolved,
    ...Object.keys(folderTools)
      .filter((name) => !listed.has(name))
      .sort()
      .map((name) => ({ tool: name, private: true })),
  ];
  const input: McpEndpointRecipeInput = {
    tools: bindings,
    ...(definition.title !== undefined && { title: definition.title }),
    ...(definition.instructions !== undefined && { instructions: definition.instructions }),
    ...(definition.capabilities !== undefined && { capabilities: definition.capabilities }),
    ...(definition.namespaces !== undefined && { namespaces: definition.namespaces }),
    ...(definition.trustClientApproval !== undefined && {
      trustClientApproval: definition.trustClientApproval,
    }),
    ...(definition.metadata !== undefined && { metadata: definition.metadata }),
  };
  return { id, input, privateTools };
}

/** Structural check: an `AgentTool` has a name, an `execute` and `parameters`. */
export function isAgentTool(value: unknown): value is AgentTool<any, any> {
  if (typeof value !== "object" || value === null) return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.name === "string" &&
    typeof v.execute === "function" &&
    typeof v.parameters === "object" &&
    v.parameters !== null
  );
}
