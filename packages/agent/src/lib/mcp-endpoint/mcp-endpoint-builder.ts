// ---------------------------------------------------------------------------
// McpEndpoint.builder — a chainable way to write an MCP endpoint definition.
//
//   const billing = McpEndpoint.builder("billing")
//     .title("Billing")
//     .instructions("Look an invoice up before refunding it.")
//     .tool("calculate")                                 // shared, by name
//     .tool({ tool: "currentTime", name: "now" })        // shared, renamed
//     .tool(tool({ name: "lookupInvoice", ... }))        // private to "billing"
//     .namespaces(["acme"])
//     .build();
//
// `build()` returns the same `McpEndpointDefinition` as `mcpEndpoint({...})`,
// checked up front (id, exposed names, inline tool clashes), so it can be a
// file's default export under `mcp/` or be registered from host code with
// `ZoryaMcpEndpoints.define()`. The id is optional only for file exports,
// where it defaults to the file or folder name.
// ---------------------------------------------------------------------------

import {
  compileMcpEndpoint,
  mcpEndpoint,
  type McpEndpointDefinition,
  type McpEndpointToolEntry,
} from "./define-mcp-endpoint.ts";
import { MCP_ENDPOINT_TOOL_NAME } from "./types.ts";

export class McpEndpointBuilder {
  private readonly endpointId: string | undefined;
  private readonly entries: McpEndpointToolEntry[] = [];
  private titleText: string | undefined;
  private instructionsText: string | undefined;
  private descriptionText: string | undefined;
  private tagList: string[] | undefined;
  private capabilityList: string[] | undefined;
  private namespaceList: string[] | null | undefined;
  private trustApproval: boolean | undefined;

  constructor(id?: string) {
    this.endpointId = id;
  }

  /** Server title MCP clients show. */
  title(title: string): this {
    this.titleText = title;
    return this;
  }

  /** Sent to the client's model when it connects. */
  instructions(instructions: string): this {
    this.instructionsText = instructions;
    return this;
  }

  /** Dashboard description. */
  description(description: string): this {
    this.descriptionText = description;
    return this;
  }

  tags(tags: ReadonlyArray<string>): this {
    this.tagList = [...tags];
    return this;
  }

  /**
   * Expose a tool: a shared tool's name, a tool defined here (private to
   * this endpoint), or either as `{ tool, name?, description? }` to rename
   * or redescribe it for MCP clients.
   */
  tool(entry: McpEndpointToolEntry): this {
    this.entries.push(entry);
    return this;
  }

  /** `tool()` for each entry, in order. */
  tools(entries: ReadonlyArray<McpEndpointToolEntry>): this {
    this.entries.push(...entries);
    return this;
  }

  /** Capabilities that unlock elevated tools (as on a role). */
  capabilities(capabilities: ReadonlyArray<string>): this {
    this.capabilityList = [...capabilities];
    return this;
  }

  /** Tenants that may use the endpoint; `null` (default) for every namespace. */
  namespaces(namespaces: ReadonlyArray<string> | null): this {
    this.namespaceList = namespaces === null ? null : [...namespaces];
    return this;
  }

  /** Expose approval-gated tools, relying on the client's confirmation prompt. */
  trustClientApproval(trust = true): this {
    this.trustApproval = trust;
    return this;
  }

  /** The definition. Throws on a bad id, exposed name or inline tool clash. */
  build(): McpEndpointDefinition {
    const metadata = {
      ...(this.descriptionText !== undefined && { description: this.descriptionText }),
      ...(this.tagList !== undefined && { tags: this.tagList }),
    };
    const definition = mcpEndpoint({
      ...(this.endpointId !== undefined && { id: this.endpointId }),
      ...(this.titleText !== undefined && { title: this.titleText }),
      ...(this.instructionsText !== undefined && { instructions: this.instructionsText }),
      tools: [...this.entries],
      ...(this.capabilityList !== undefined && { capabilities: this.capabilityList }),
      ...(this.namespaceList !== undefined && { namespaces: this.namespaceList }),
      ...(this.trustApproval !== undefined && { trustClientApproval: this.trustApproval }),
      ...(Object.keys(metadata).length > 0 && { metadata }),
    });
    // Fail here, where the endpoint is written, not at the next scan or
    // registration. A placeholder id stands in when the file names it.
    const compiled = compileMcpEndpoint({ definition, fallbackId: "endpoint" });
    for (const binding of compiled.input.tools) {
      if (binding.name !== undefined && !MCP_ENDPOINT_TOOL_NAME.test(binding.name)) {
        throw new Error(`"${binding.name}" is not a valid MCP tool name`);
      }
    }
    return definition;
  }
}

/** Entry point for building MCP endpoint definitions: `McpEndpoint.builder(id)`. */
export const McpEndpoint = {
  builder(id?: string): McpEndpointBuilder {
    return new McpEndpointBuilder(id);
  },
} as const;
