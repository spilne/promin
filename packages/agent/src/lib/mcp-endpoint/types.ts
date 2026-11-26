// ---------------------------------------------------------------------------
// MCP endpoints — promin tools served over the Model Context Protocol.
//
// The other direction from `../mcp/`: there agents are MCP *clients* of
// operator-allowlisted servers; here a host publishes named, versioned
// bundles of its own tools and serves each one as an MCP server
// (`handleMcpEndpointRequest`).
//
// An endpoint version is pure data — it lives in a shared `VersionedRegistry`
// like agents and roles — so it never holds a tool's code. It names tools:
//
//   - shared tools, by their name in the host's tool sources (the in-process
//     record, a `createFileToolRegistry`): `{ tool: "calculate" }`;
//   - the endpoint's own tools, defined inline in its file or in its
//     folder's `tools/`: `{ tool: "lookupInvoice", private: true }`. The
//     scanner keeps those in a process-local `McpEndpointPrivateTools`, keyed
//     by endpoint id; every replica scans the same folder, as with `tools/`.
// ---------------------------------------------------------------------------

import type { AgentTool } from "../tool.ts";
import type {
  RegisterVersionInput,
  Versioned,
  VersionedRegistry,
} from "../versioned-registry/types.ts";

/** Endpoint ids: URL-safe, no leading `_` (reserved for literal route segments). */
export const MCP_ENDPOINT_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

/** Names a tool may be exposed under (the MCP spec's recommended tool-name charset). */
export const MCP_ENDPOINT_TOOL_NAME = /^[A-Za-z0-9_.-]{1,128}$/;

/** One tool an endpoint exposes. */
export interface McpEndpointToolBinding {
  /** The tool's name in its source (the shared tools, or the endpoint's own). */
  readonly tool: string;
  /** The endpoint's own tool (file-defined), not a shared one. Default `false`. */
  readonly private?: boolean;
  /** Name the MCP client sees. Default: `tool`. */
  readonly name?: string;
  /** Description the MCP client sees. Default: the tool's own. */
  readonly description?: string;
}

/** What an endpoint version stores. */
export interface McpEndpointRecipe {
  /** Human-readable name, sent as the server's `title`. */
  readonly title: string | null;
  /** Sent to the client in `initialize` as the server's `instructions`. */
  readonly instructions: string | null;
  readonly tools: ReadonlyArray<McpEndpointToolBinding>;
  /**
   * Capabilities that unlock elevated tools, as on a role: an elevated tool
   * is exposed only when its `requires` (default `"elevated"`) is listed.
   */
  readonly capabilities: ReadonlyArray<string>;
  /** Namespaces (tenants) that may use the endpoint. `null`: every namespace. */
  readonly namespaces: ReadonlyArray<string> | null;
  /**
   * Expose tools that require approval, trusting the MCP client's own
   * confirmation prompt instead of a zorya approval. Default `false`: such
   * tools are left out.
   */
  readonly trustClientApproval: boolean;
  readonly metadata: McpEndpointMetadata;
}

export interface McpEndpointMetadata {
  readonly description: string | null;
  readonly tags: ReadonlyArray<string>;
}

/** `register` input body: everything but `tools` defaults when omitted. */
export interface McpEndpointRecipeInput {
  readonly title?: string | null;
  readonly instructions?: string | null;
  readonly tools: ReadonlyArray<McpEndpointToolBinding>;
  readonly capabilities?: ReadonlyArray<string>;
  readonly namespaces?: ReadonlyArray<string> | null;
  readonly trustClientApproval?: boolean;
  readonly metadata?: Partial<McpEndpointMetadata>;
}

/** A stored endpoint version. `enabled` is the kill switch (disabled: 410). */
export type RegisteredMcpEndpoint = Versioned<McpEndpointRecipe>;

export type RegisterMcpEndpointInput = RegisterVersionInput<McpEndpointRecipeInput>;

export interface McpEndpointFilter {
  readonly tag?: string;
}

export type McpEndpointRegistry = VersionedRegistry<
  McpEndpointRecipe,
  McpEndpointFilter,
  McpEndpointRecipeInput
>;

/**
 * Where shared tools come from: a tool record, or anything with
 * `getTools()` (a `ToolRegistry`). Read on every resolution, so a
 * hot-reloading registry is seen live.
 */
export type McpEndpointToolSource =
  | Readonly<Record<string, AgentTool<any, any>>>
  | { getTools(): Readonly<Record<string, AgentTool<any, any>>> };

/** Why a binding was left out of an endpoint's tool list. */
export type McpEndpointSkipCode =
  | "not_found"
  | "disabled"
  | "requires_approval"
  | "delegated"
  | "capability_missing"
  | "invalid_name"
  | "name_taken";

export interface McpEndpointSkippedTool {
  readonly tool: string;
  readonly private: boolean;
  readonly code: McpEndpointSkipCode;
}

/** One tool as an endpoint exposes it. */
export interface McpEndpointResolvedTool {
  /** Name the client sees. */
  readonly name: string;
  readonly description: string;
  readonly tool: AgentTool<any, any>;
  readonly private: boolean;
}

export interface ResolvedMcpEndpointTools {
  /** Keyed by exposed name. */
  readonly tools: ReadonlyMap<string, McpEndpointResolvedTool>;
  readonly skipped: ReadonlyArray<McpEndpointSkippedTool>;
}
