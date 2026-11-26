// ---------------------------------------------------------------------------
// McpEndpointPrivateTools — the process-local home of tools defined inside
// an endpoint. The registry stores only their names (`{ tool, private: true }`);
// the code lives here, from two sources:
//
//   - scanned  the `mcp/` folder scan installs them on every tick (`replace`)
//   - defined  host code registers them at startup (`define`)
//
// A scan never touches defined tools, so an endpoint declared in code keeps
// its tools however the folder changes. Request handling reads by id.
// ---------------------------------------------------------------------------

import type { AgentTool } from "../tool.ts";

type ToolRecord = Readonly<Record<string, AgentTool<any, any>>>;

export class McpEndpointPrivateTools {
  private scanned = new Map<string, ToolRecord>();
  private readonly defined = new Map<string, ToolRecord>();

  /** The endpoint's own tools (empty when it has none). Code-defined win. */
  get(endpointId: string): ToolRecord {
    return this.defined.get(endpointId) ?? this.scanned.get(endpointId) ?? {};
  }

  /** Endpoint ids that have private tools. */
  ids(): string[] {
    return [...new Set([...this.defined.keys(), ...this.scanned.keys()])].sort();
  }

  /** Whether host code defined `endpointId` (as opposed to the folder scan). */
  isDefined(endpointId: string): boolean {
    return this.defined.has(endpointId);
  }

  /** Install the private tools of an endpoint declared in host code. */
  define(params: { readonly endpointId: string; readonly tools: ToolRecord }): void {
    this.defined.set(params.endpointId, params.tools);
  }

  /**
   * Install the private tools a scan found. With `complete: false` (some
   * file failed to import) endpoints the scan did not see keep their tools —
   * a file that failed to load is not a file that was removed.
   */
  replace(params: {
    readonly scanned: ReadonlyMap<string, ToolRecord>;
    readonly complete: boolean;
  }): void {
    const next = new Map(params.complete ? [] : this.scanned);
    for (const [id, tools] of params.scanned) {
      if (Object.keys(tools).length === 0) next.delete(id);
      else next.set(id, tools);
    }
    this.scanned = next;
  }
}
