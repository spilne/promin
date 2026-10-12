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
// `AgentRegistry` through `applyDiscovered`: unchanged recipes are left
// alone, changed ones become a new promoted version, and opt-in `sync`
// removes scanner-written recipes whose file is gone.
// ---------------------------------------------------------------------------

import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { AgentBackend, AgentRegistry, RegisterAgentInput } from "../registry/types.ts";
import { applyDiscovered, type ApplyDiscoveredResult } from "./apply-discovered.ts";
import { startScanLoop, type ScanLoopHandle, type ScanLoopTick } from "./scan-loop.ts";

/**
 * Per-backend list of env vars that must be present at materialization
 * time. The discovery pass uses this to warn at registration time —
 * the resolver itself enforces them when called. Extend when a new
 * `AgentBackend` variant introduces auth.
 */
function backendRequiredEnv(backend: AgentBackend): ReadonlyArray<string> {
  switch (backend.type) {
    case "local":
      return backend.requiredEnv ?? [];
    case "remote":
      // Bearer-token auth lives on the recipe itself, not in env.
      return [];
  }
}

export interface AgentScannerOptions {
  /** File extensions to consider. Default: `.ts, .tsx, .js, .mjs`. */
  readonly extensions?: ReadonlyArray<string>;
  /** Recursion depth. Default: `10`. */
  readonly maxDepth?: number;
  /** Predicate filtering candidate files before import. */
  readonly filter?: (absPath: string) => boolean;
  /** Per-discovery callback — fires once per detected agent. */
  readonly onAgent?: (agent: RegisterAgentInput, sourcePath: string) => void;
  /**
   * Append `?v=<mtimeMs>` to each `import()` URL so edits to a recipe
   * file produce a fresh module instance instead of a cached one. Off
   * by default (one-shot scans don't need it). The scan loop below
   * turns this on automatically.
   */
  readonly cacheBust?: boolean;
}

export interface AgentScanResult {
  readonly agents: RegisterAgentInput[];
  /** Per-id source path — useful for duplicate diagnostics. */
  readonly sources: Record<string, string>;
  /** Diagnostics that did not lose a file (duplicate ids). */
  readonly warnings: string[];
  /** Directories or files that could not be read or imported. */
  readonly errors: string[];
}

const DEFAULT_EXTENSIONS = [".ts", ".tsx", ".js", ".mjs"];

export class AgentScanner {
  private readonly extensions: ReadonlyArray<string>;
  private readonly maxDepth: number;
  private readonly filter: (absPath: string) => boolean;
  private readonly onAgent?: AgentScannerOptions["onAgent"];
  private readonly cacheBust: boolean;

  constructor(options: AgentScannerOptions = {}) {
    this.extensions = options.extensions ?? DEFAULT_EXTENSIONS;
    this.maxDepth = options.maxDepth ?? 10;
    this.filter = options.filter ?? (() => true);
    this.onAgent = options.onAgent;
    this.cacheBust = options.cacheBust ?? false;
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
    const errors: string[] = [];
    await this.walk({ dir: root, depth: 0, agents, sources, warnings, errors });
    return { agents, sources, warnings, errors };
  }

  private async walk(params: {
    dir: string;
    depth: number;
    agents: RegisterAgentInput[];
    sources: Record<string, string>;
    warnings: string[];
    errors: string[];
  }): Promise<void> {
    const { dir, depth, agents, sources, warnings, errors } = params;
    if (depth > this.maxDepth) return;
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      errors.push(`failed to read ${dir}: ${asMessage(err)}`);
      return;
    }

    for (const entry of entries) {
      const name = entry.name;
      const full = join(dir, name);

      if (entry.isDirectory()) {
        if (name === "node_modules" || name.startsWith(".")) continue;
        await this.walk({ ...params, dir: full, depth: depth + 1 });
        continue;
      }
      if (!entry.isFile()) continue;
      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (!this.filter(full)) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
        continue;
      }

      await this.importModule({ absPath: full, agents, sources, warnings, errors });
    }
  }

  private async importModule(params: {
    absPath: string;
    agents: RegisterAgentInput[];
    sources: Record<string, string>;
    warnings: string[];
    errors: string[];
  }): Promise<void> {
    const { absPath, agents, sources, warnings, errors } = params;
    let mod: Record<string, unknown>;
    let url = pathToFileURL(absPath).href;
    if (this.cacheBust) {
      // Stat for mtime so edits → unique URL → fresh module. Falls back
      // to Date.now() if the stat fails (e.g. file just deleted between
      // walk and import); we'd rather over-import than crash the loop.
      let mtimeMs: number;
      try {
        const st = await stat(absPath);
        mtimeMs = st.mtimeMs;
      } catch {
        mtimeMs = Date.now();
      }
      url = `${url}?v=${mtimeMs}`;
    }
    try {
      mod = (await import(url)) as Record<string, unknown>;
    } catch (err) {
      errors.push(`failed to import ${absPath}: ${asMessage(err)}`);
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

export interface ApplyDiscoveredAgentsParams {
  readonly registry: AgentRegistry;
  readonly agents: ReadonlyArray<RegisterAgentInput>;
  /**
   * Remove recipes whose file is gone. Default `false`. Only versions a
   * scanner wrote are removed; operator-authored recipes always stay.
   */
  readonly sync?: boolean;
  /** Whether the scan read every file; `false` skips sync deletes. Default `true`. */
  readonly complete?: boolean;
  /** Only touch ids with this prefix (partition recipes by tenant / environment). */
  readonly idPrefix?: string;
}

export async function applyDiscoveredAgents(
  params: ApplyDiscoveredAgentsParams,
): Promise<ApplyDiscoveredResult> {
  return applyDiscovered({
    registry: params.registry,
    discovered: params.agents,
    ...(params.sync !== undefined && { sync: params.sync }),
    ...(params.complete !== undefined && { complete: params.complete }),
    ...(params.idPrefix !== undefined && { idPrefix: params.idPrefix }),
    // A recipe whose env is missing is still stored — it is just JSON and
    // works once the operator sets the vars — but the boot log says so.
    beforeRegister: (input) => {
      const missing = backendRequiredEnv(input.backend).filter((name) => !process.env[name]);
      if (missing.length > 0) {
        console.warn(
          `[agent-registry] registering "${input.id}" but required env var(s) are not set: ${missing.join(", ")}. ` +
            "The recipe will be stored; the resolver will throw until the vars are present.",
        );
      }
    },
  });
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// ---------------------------------------------------------------------------
// Periodic scan loop — auto-discovers and reconciles agents on a tick.
// Use this in dev/demo hosts to get hot-reload behaviour: drop a new
// recipe under the scan root, edit an existing one, and the registry
// catches up on the next interval. The loop runs `cacheBust: true` so
// edits to existing files actually re-import.
// ---------------------------------------------------------------------------

export interface AgentScanLoopOptions {
  readonly registry: AgentRegistry;
  readonly root: string;
  /** Poll interval in ms. Default: `5_000`. */
  readonly intervalMs?: number;
  /** Pass through to `applyDiscoveredAgents` — removes recipes whose file is gone. Default: `false`. */
  readonly sync?: boolean;
  /** Pass through to `applyDiscoveredAgents`. */
  readonly idPrefix?: string;
  /** Filter scan results before applying (e.g. drop live-only recipes when key missing). */
  readonly filterAgents?: (agents: ReadonlyArray<RegisterAgentInput>) => RegisterAgentInput[];
  /** Fired after each tick. Surfaces deltas + warnings to the host. */
  readonly onTick?: (event: ScanLoopTick) => void;
  /** A failed timer tick lands here. Default: `console.error`. */
  readonly onError?: (err: unknown) => void;
  /** Time source. Default: `SystemWallClock`. Tests pass `FakeWallClock`. */
  readonly clock?: WallClock;
  /** Forwarded to the underlying scanner. `cacheBust` defaults to `true` for the loop. */
  readonly scanner?: Omit<AgentScannerOptions, "cacheBust" | "onAgent">;
}

export function startAgentScanLoop(options: AgentScanLoopOptions): ScanLoopHandle<ScanLoopTick> {
  const scanner = new AgentScanner({ ...options.scanner, cacheBust: true });
  return startScanLoop({
    label: "agent-scan",
    ...(options.intervalMs !== undefined && { intervalMs: options.intervalMs }),
    ...(options.clock !== undefined && { clock: options.clock }),
    ...(options.onError !== undefined && { onError: options.onError }),
    run: async () => {
      const clock = options.clock ?? SystemWallClock;
      const start = clock.currentTimeMs();
      const scan = await scanner.scan(options.root);
      const filtered = options.filterAgents ? options.filterAgents(scan.agents) : scan.agents;
      const apply = await applyDiscoveredAgents({
        registry: options.registry,
        agents: filtered,
        sync: options.sync ?? false,
        complete: scan.errors.length === 0,
        ...(options.idPrefix !== undefined && { idPrefix: options.idPrefix }),
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
