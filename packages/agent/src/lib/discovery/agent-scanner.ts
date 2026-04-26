// ---------------------------------------------------------------------------
// AgentScanner — auto-discover `RegisterAgentInput` exports from a
// directory tree. Symmetric with `WorkflowScanner` / `ScheduleScanner` in
// @promin/workflow/discovery; same conventions for placement and shape.
//
// Detection is structural: an export counts as an agent recipe when it
// has a string `id` and an object `backend` with a string `type` field.
// Modules can export either a single config or an array — both flatten.
//
// `applyDiscoveredAgents` reconciles a discovered set into an
// `AgentRegistry`. Same shape as `applyDiscoveredSchedules`: upsert by
// default, opt-in `sync` mode to delete entries not in the scan.
// ---------------------------------------------------------------------------

import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { AgentRegistry, RegisterAgentInput } from "../registry/types.ts";

export interface AgentScannerOptions {
  /** File extensions to consider. Default: `.ts, .tsx, .js, .mjs`. */
  readonly extensions?: ReadonlyArray<string>;
  /** Recursion depth. Default: `10`. */
  readonly maxDepth?: number;
  /** Predicate filtering candidate files before import. */
  readonly filter?: (absPath: string) => boolean;
  /** Per-discovery callback — fires once per detected agent. */
  readonly onAgent?: (agent: RegisterAgentInput, sourcePath: string) => void;
}

export interface AgentScanResult {
  readonly agents: RegisterAgentInput[];
  /** Per-id source path — useful for duplicate diagnostics. */
  readonly sources: Record<string, string>;
  readonly warnings: string[];
}

const DEFAULT_EXTENSIONS = [".ts", ".tsx", ".js", ".mjs"];

export class AgentScanner {
  private readonly extensions: ReadonlyArray<string>;
  private readonly maxDepth: number;
  private readonly filter: (absPath: string) => boolean;
  private readonly onAgent?: AgentScannerOptions["onAgent"];

  constructor(options: AgentScannerOptions = {}) {
    this.extensions = options.extensions ?? DEFAULT_EXTENSIONS;
    this.maxDepth = options.maxDepth ?? 10;
    this.filter = options.filter ?? (() => true);
    this.onAgent = options.onAgent;
  }

  /** One-shot helper for callers that don't want to hold an instance. */
  static async scanFolder(
    root: string,
    options: AgentScannerOptions = {},
  ): Promise<AgentScanResult> {
    return new AgentScanner(options).scan(root);
  }

  async scan(root: string): Promise<AgentScanResult> {
    const agents: RegisterAgentInput[] = [];
    const sources: Record<string, string> = {};
    const warnings: string[] = [];
    await this.walk(root, 0, agents, sources, warnings);
    return { agents, sources, warnings };
  }

  private async walk(
    dir: string,
    depth: number,
    agents: RegisterAgentInput[],
    sources: Record<string, string>,
    warnings: string[],
  ): Promise<void> {
    if (depth > this.maxDepth) return;
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      warnings.push(`failed to read ${dir}: ${asMessage(err)}`);
      return;
    }

    for (const entry of entries) {
      const name = entry.name;
      const full = join(dir, name);

      if (entry.isDirectory()) {
        if (name === "node_modules" || name.startsWith(".")) continue;
        await this.walk(full, depth + 1, agents, sources, warnings);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (!this.filter(full)) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
        continue;
      }

      await this.importModule(full, agents, sources, warnings);
    }
  }

  private async importModule(
    absPath: string,
    agents: RegisterAgentInput[],
    sources: Record<string, string>,
    warnings: string[],
  ): Promise<void> {
    let mod: Record<string, unknown>;
    try {
      mod = (await import(pathToFileURL(absPath).href)) as Record<string, unknown>;
    } catch (err) {
      warnings.push(`failed to import ${absPath}: ${asMessage(err)}`);
      return;
    }

    for (const [, value] of Object.entries(mod)) {
      const candidates = Array.isArray(value) ? value : [value];
      for (const candidate of candidates) {
        if (!isAgentInput(candidate)) continue;
        const existing = sources[candidate.id];
        if (existing && existing !== absPath) {
          warnings.push(
            `duplicate agent id "${candidate.id}": ${existing} vs ${absPath} — last wins`,
          );
          const i = agents.findIndex((a) => a.id === candidate.id);
          if (i >= 0) agents[i] = candidate;
          else agents.push(candidate);
        } else {
          agents.push(candidate);
        }
        sources[candidate.id] = absPath;
        this.onAgent?.(candidate, absPath);
      }
    }
  }
}

function isAgentInput(v: unknown): v is RegisterAgentInput {
  if (v === null || typeof v !== "object") return false;
  const o = v as { id?: unknown; backend?: unknown };
  if (typeof o.id !== "string" || o.id.length === 0) return false;
  if (typeof o.backend !== "object" || o.backend === null) return false;
  const b = o.backend as { type?: unknown };
  return typeof b.type === "string" && b.type.length > 0;
}

// ---------------------------------------------------------------------------
// Reconcile a discovered set into an `AgentRegistry`.
// ---------------------------------------------------------------------------

export interface ApplyDiscoveredAgentsOptions {
  /**
   * Delete registry entries whose `id` isn't in the discovered set. Default:
   * `false` (upsert-only — operator-created entries stay). Set `true` to
   * make the filesystem authoritative.
   */
  readonly sync?: boolean;
  /**
   * Restrict the operation to entries whose discovered `id` starts with
   * this prefix. Useful for partitioning agent recipes by tenant /
   * environment without colliding ids. No filter when omitted.
   */
  readonly idPrefix?: string;
}

export interface ApplyDiscoveredAgentsResult {
  readonly upserted: string[];
  /** Subset of `upserted` that didn't exist before this call. */
  readonly added: string[];
  /** Only populated when `sync: true`. */
  readonly deleted: string[];
}

export async function applyDiscoveredAgents(
  registry: AgentRegistry,
  agents: ReadonlyArray<RegisterAgentInput>,
  options: ApplyDiscoveredAgentsOptions = {},
): Promise<ApplyDiscoveredAgentsResult> {
  const upserted: string[] = [];
  const added: string[] = [];
  const deleted: string[] = [];

  // Snapshot the existing set when we'll need it for either added-tracking or sync.
  const existing = options.sync ? await registry.list({ limit: 100_000 }) : [];
  const existingIds = new Set(existing.map((row) => row.id));

  for (const input of agents) {
    if (options.idPrefix !== undefined && !input.id.startsWith(options.idPrefix)) continue;
    const wasExisting = existingIds.has(input.id) || (await registry.get(input.id)) !== null;
    await registry.register(input);
    upserted.push(input.id);
    if (!wasExisting) added.push(input.id);
  }

  if (options.sync) {
    const discoveredIds = new Set(
      agents
        .filter((a) => options.idPrefix === undefined || a.id.startsWith(options.idPrefix))
        .map((a) => a.id),
    );
    for (const row of existing) {
      if (options.idPrefix !== undefined && !row.id.startsWith(options.idPrefix)) continue;
      if (!discoveredIds.has(row.id)) {
        await registry.unregister(row.id);
        deleted.push(row.id);
      }
    }
  }

  return { upserted, added, deleted };
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
