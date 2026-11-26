// ---------------------------------------------------------------------------
// McpEndpointScanner — discover MCP endpoints from an `mcp/` folder.
// Sibling of `AgentScanner` / `SkillScanner`.
//
// Layout (one level deep):
//
//   mcp/billing.ts            exports one or more `mcpEndpoint({...})`; a
//                             definition without an `id` takes the basename
//   mcp/support/index.ts      exports exactly one definition; `id` defaults
//                             to the folder name
//   mcp/support/tools/**.ts   default-exported tools, private to "support"
//
// Tools defined inline in a definition, and those under its folder's
// `tools/`, are private to the endpoint: the stored version names them
// (`{ tool, private: true }`) and the scan loop installs their code in an
// `McpEndpointPrivateTools`. Other folders (no `index.*`) are skipped.
//
// `applyDiscoveredMcpEndpoints` reconciles the found set into an
// `McpEndpointRegistry` through `applyDiscovered` (unchanged files cost no
// write; opt-in `sync` removes scanner-written endpoints whose file is gone).
// ---------------------------------------------------------------------------

import { readdir, stat } from "node:fs/promises";
import { basename, extname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  isAgentTool,
  isMcpEndpointDefinition,
  splitMcpEndpointTools,
  type McpEndpointDefinition,
} from "../mcp-endpoint/define-mcp-endpoint.ts";
import type { McpEndpointPrivateTools } from "../mcp-endpoint/mcp-endpoint-private-tools.ts";
import {
  MCP_ENDPOINT_ID_PATTERN,
  type McpEndpointRecipeInput,
  type McpEndpointRegistry,
  type RegisterMcpEndpointInput,
} from "../mcp-endpoint/types.ts";
import type { AgentTool } from "../tool.ts";
import { applyDiscovered, type ApplyDiscoveredResult } from "./apply-discovered.ts";
import { startScanLoop, type ScanLoopHandle, type ScanLoopTick } from "./scan-loop.ts";

/** A discovered endpoint: its id, stored body and private tools. */
export interface McpEndpointSpec {
  readonly id: string;
  readonly input: McpEndpointRecipeInput;
  readonly privateTools: Readonly<Record<string, AgentTool<any, any>>>;
}

export interface McpEndpointScannerOptions {
  /** Module extensions to consider. Default: `.ts, .tsx, .js, .mjs`. */
  readonly extensions?: ReadonlyArray<string>;
  /**
   * Append `?v=<mtimeMs>` to each `import()` URL so an edited file loads
   * fresh. Off by default; the scan loop turns it on.
   */
  readonly cacheBust?: boolean;
}

export interface McpEndpointScanResult {
  readonly endpoints: McpEndpointSpec[];
  /** Per-id source path. */
  readonly sources: Record<string, string>;
  /** Diagnostics that did not lose an endpoint. */
  readonly warnings: string[];
  /** Files that could not be read or imported, or define endpoints badly. */
  readonly errors: string[];
}

const DEFAULT_EXTENSIONS = [".ts", ".tsx", ".js", ".mjs"];
const INDEX_BASENAME = "index";
const TOOLS_DIR = "tools";

export class McpEndpointScanner {
  private readonly extensions: ReadonlyArray<string>;
  private readonly cacheBust: boolean;

  constructor(options: McpEndpointScannerOptions = {}) {
    this.extensions = options.extensions ?? DEFAULT_EXTENSIONS;
    this.cacheBust = options.cacheBust ?? false;
  }

  static async scanFolder(
    root: string,
    options: McpEndpointScannerOptions = {},
  ): Promise<McpEndpointScanResult> {
    return new McpEndpointScanner(options).scan(root);
  }

  async scan(root: string): Promise<McpEndpointScanResult> {
    const out: McpEndpointScanResult = { endpoints: [], sources: {}, warnings: [], errors: [] };
    let entries;
    try {
      entries = await readdir(root, { withFileTypes: true });
    } catch (err) {
      out.errors.push(`failed to read ${root}: ${asMessage(err)}`);
      return out;
    }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      const full = join(root, entry.name);
      if (entry.isDirectory()) {
        if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
        await this.scanFolderEndpoint({ dir: full, name: entry.name, out });
      } else if (entry.isFile() && this.isModule(entry.name)) {
        await this.scanFileEndpoints({ absPath: full, out });
      }
    }
    return out;
  }

  private isModule(name: string): boolean {
    if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
      return false;
    }
    return this.extensions.some((ext) => name.endsWith(ext));
  }

  private async scanFileEndpoints(params: {
    absPath: string;
    out: McpEndpointScanResult;
  }): Promise<void> {
    const { absPath, out } = params;
    const mod = await this.importModule({ absPath, out });
    if (mod === undefined) return;
    const definitions = definitionsIn(mod);
    const unnamed = definitions.filter((d) => d.id === undefined);
    if (unnamed.length > 1) {
      out.errors.push(`${absPath}: several endpoints without an id; give each an id`);
      return;
    }
    const fallbackId = basename(absPath, extname(absPath));
    for (const definition of definitions) {
      this.addEndpoint({
        definition,
        id: definition.id ?? fallbackId,
        folderTools: {},
        absPath,
        out,
      });
    }
  }

  private async scanFolderEndpoint(params: {
    dir: string;
    name: string;
    out: McpEndpointScanResult;
  }): Promise<void> {
    const { dir, name, out } = params;
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      out.errors.push(`failed to read ${dir}: ${asMessage(err)}`);
      return;
    }
    const index = entries.find(
      (e) =>
        e.isFile() && this.isModule(e.name) && basename(e.name, extname(e.name)) === INDEX_BASENAME,
    );
    if (index === undefined) return;
    const absPath = join(dir, index.name);
    const mod = await this.importModule({ absPath, out });
    if (mod === undefined) return;
    const definitions = definitionsIn(mod);
    if (definitions.length !== 1) {
      out.errors.push(
        `${absPath}: a folder endpoint's index must export exactly one mcpEndpoint (found ${definitions.length})`,
      );
      return;
    }
    const folderTools: Record<string, AgentTool<any, any>> = {};
    const toolsDir = entries.find((e) => e.isDirectory() && e.name === TOOLS_DIR);
    if (toolsDir !== undefined) {
      const ok = await this.loadTools({ dir: join(dir, TOOLS_DIR), into: folderTools, out });
      if (!ok) return;
    }
    const definition = definitions[0] as McpEndpointDefinition;
    this.addEndpoint({ definition, id: definition.id ?? name, folderTools, absPath, out });
  }

  /** Load default-exported tools under `dir`; false when any file failed. */
  private async loadTools(params: {
    dir: string;
    into: Record<string, AgentTool<any, any>>;
    out: McpEndpointScanResult;
  }): Promise<boolean> {
    const { dir, into, out } = params;
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      out.errors.push(`failed to read ${dir}: ${asMessage(err)}`);
      return false;
    }
    let ok = true;
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
        ok = (await this.loadTools({ dir: full, into, out })) && ok;
        continue;
      }
      if (!entry.isFile() || !this.isModule(entry.name)) continue;
      const mod = await this.importModule({ absPath: full, out });
      if (mod === undefined) {
        ok = false;
        continue;
      }
      const tool = mod.default;
      if (!isAgentTool(tool)) {
        out.errors.push(`${full}: a tools/ file must default-export a tool`);
        ok = false;
        continue;
      }
      if (into[tool.name] !== undefined) {
        out.errors.push(`${full}: another tool in this folder is already named "${tool.name}"`);
        ok = false;
        continue;
      }
      into[tool.name] = tool;
    }
    return ok;
  }

  private addEndpoint(params: {
    definition: McpEndpointDefinition;
    id: string;
    folderTools: Record<string, AgentTool<any, any>>;
    absPath: string;
    out: McpEndpointScanResult;
  }): void {
    const { definition, id, folderTools, absPath, out } = params;
    if (!MCP_ENDPOINT_ID_PATTERN.test(id)) {
      out.errors.push(
        `${absPath}: "${id}" is not a valid endpoint id (letters, digits, _ and -, starting with a letter or digit)`,
      );
      return;
    }
    let split: ReturnType<typeof splitMcpEndpointTools>;
    try {
      split = splitMcpEndpointTools(definition.tools);
    } catch (err) {
      out.errors.push(`${absPath}: endpoint "${id}": ${asMessage(err)}`);
      return;
    }
    const privateTools: Record<string, AgentTool<any, any>> = { ...folderTools };
    for (const [name, tool] of Object.entries(split.privateTools)) {
      if (privateTools[name] !== undefined && privateTools[name] !== tool) {
        out.errors.push(
          `${absPath}: endpoint "${id}": inline tool "${name}" clashes with a tools/ file`,
        );
        return;
      }
      privateTools[name] = tool;
    }
    // A name that matches one of the folder's own tools means that tool
    // (the closest one), not a shared tool of the same name.
    const resolved = split.bindings.map((b) =>
      !b.private && Object.hasOwn(folderTools, b.tool) ? { ...b, private: true } : b,
    );
    // Folder tools are part of the endpoint even when the definition does
    // not list them: that is what putting a tool in `tools/` means.
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
    const spec: McpEndpointSpec = { id, input, privateTools };
    const existing = out.sources[id];
    if (existing !== undefined) {
      out.warnings.push(`duplicate mcp endpoint id "${id}": ${existing} vs ${absPath} — last wins`);
      const i = out.endpoints.findIndex((e) => e.id === id);
      if (i >= 0) out.endpoints[i] = spec;
    } else {
      out.endpoints.push(spec);
    }
    out.sources[id] = absPath;
  }

  private async importModule(params: {
    absPath: string;
    out: McpEndpointScanResult;
  }): Promise<Record<string, unknown> | undefined> {
    const { absPath, out } = params;
    let url = pathToFileURL(absPath).href;
    if (this.cacheBust) {
      try {
        url = `${url}?v=${(await stat(absPath)).mtimeMs}`;
      } catch {
        // Gone between the walk and the import: the import reports it.
      }
    }
    try {
      return (await import(url)) as Record<string, unknown>;
    } catch (err) {
      out.errors.push(`failed to import ${absPath}: ${asMessage(err)}`);
      return undefined;
    }
  }
}

/** Every branded definition a module exports (arrays flatten; duplicates by identity drop). */
function definitionsIn(mod: Record<string, unknown>): McpEndpointDefinition[] {
  const seen = new Set<McpEndpointDefinition>();
  for (const value of Object.values(mod)) {
    for (const candidate of Array.isArray(value) ? value : [value]) {
      if (isMcpEndpointDefinition(candidate)) seen.add(candidate);
    }
  }
  return [...seen];
}

// ---------------------------------------------------------------------------
// Reconcile into a registry.
// ---------------------------------------------------------------------------

export interface ApplyDiscoveredMcpEndpointsParams {
  readonly registry: McpEndpointRegistry;
  readonly endpoints: ReadonlyArray<McpEndpointSpec>;
  /** Remove scanner-written endpoints whose file is gone. Default `false`. */
  readonly sync?: boolean;
  /** Whether the scan read every file; `false` skips sync deletes. Default `true`. */
  readonly complete?: boolean;
}

export async function applyDiscoveredMcpEndpoints(
  params: ApplyDiscoveredMcpEndpointsParams,
): Promise<ApplyDiscoveredResult> {
  const discovered: RegisterMcpEndpointInput[] = params.endpoints.map((e) => ({
    id: e.id,
    ...e.input,
  }));
  return applyDiscovered({
    registry: params.registry,
    discovered,
    ...(params.sync !== undefined && { sync: params.sync }),
    ...(params.complete !== undefined && { complete: params.complete }),
  });
}

// ---------------------------------------------------------------------------
// Periodic scan loop.
// ---------------------------------------------------------------------------

export interface McpEndpointScanLoopOptions {
  readonly registry: McpEndpointRegistry;
  /** Receives the code of every endpoint's private tools on each scan. */
  readonly privateTools: McpEndpointPrivateTools;
  readonly root: string;
  /** Poll interval in ms. Default: `5_000`. */
  readonly intervalMs?: number;
  /** Pass through to `applyDiscoveredMcpEndpoints`. Default: `false`. */
  readonly sync?: boolean;
  readonly onTick?: (event: ScanLoopTick) => void;
  /** A failed timer tick lands here. Default: `console.error`. */
  readonly onError?: (err: unknown) => void;
  readonly clock?: WallClock;
}

export function startMcpEndpointScanLoop(
  options: McpEndpointScanLoopOptions,
): ScanLoopHandle<ScanLoopTick> {
  const clock = options.clock ?? SystemWallClock;
  const scanner = new McpEndpointScanner({ cacheBust: true });
  return startScanLoop({
    label: "mcp-endpoint-scan",
    clock,
    ...(options.intervalMs !== undefined && { intervalMs: options.intervalMs }),
    ...(options.onError !== undefined && { onError: options.onError }),
    run: async () => {
      const start = clock.currentTimeMs();
      const scan = await scanner.scan(options.root);
      const complete = scan.errors.length === 0;
      // Tools first: a version the registry makes active right after must
      // find the code its private bindings name.
      options.privateTools.replace({
        scanned: new Map(scan.endpoints.map((e) => [e.id, e.privateTools])),
        complete,
      });
      const apply = await applyDiscoveredMcpEndpoints({
        registry: options.registry,
        endpoints: scan.endpoints,
        sync: options.sync ?? false,
        complete,
      });
      const tick: ScanLoopTick = {
        added: apply.added,
        upserted: apply.upserted,
        changed: apply.changed,
        deleted: apply.deleted,
        warnings: [...scan.warnings, ...apply.warnings],
        errors: scan.errors,
        durationMs: clock.currentTimeMs() - start,
      };
      options.onTick?.(tick);
      return tick;
    },
  });
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
