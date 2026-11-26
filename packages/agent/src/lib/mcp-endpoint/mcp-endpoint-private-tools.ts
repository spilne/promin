// ---------------------------------------------------------------------------
// McpEndpointPrivateTools — the process-local home of tools defined inside
// an endpoint's file or folder. The registry stores only their names
// (`{ tool, private: true }`); the scanner fills this set with the code on
// every scan, and request handling reads it by endpoint id.
// ---------------------------------------------------------------------------

import type { AgentTool } from "../tool.ts";

export class McpEndpointPrivateTools {
  private byEndpoint = new Map<string, Readonly<Record<string, AgentTool<any, any>>>>();

  /** The endpoint's own tools (empty when it has none). */
  get(endpointId: string): Readonly<Record<string, AgentTool<any, any>>> {
    return this.byEndpoint.get(endpointId) ?? {};
  }

  /** Endpoint ids that have private tools. */
  ids(): string[] {
    return [...this.byEndpoint.keys()].sort();
  }

  /**
   * Install the private tools a scan found. With `complete: false` (some
   * file failed to import) endpoints the scan did not see keep their tools —
   * a file that failed to load is not a file that was removed.
   */
  replace(params: {
    readonly scanned: ReadonlyMap<string, Readonly<Record<string, AgentTool<any, any>>>>;
    readonly complete: boolean;
  }): void {
    const next = new Map(params.complete ? [] : this.byEndpoint);
    for (const [id, tools] of params.scanned) {
      if (Object.keys(tools).length === 0) next.delete(id);
      else next.set(id, tools);
    }
    this.byEndpoint = next;
  }
}
