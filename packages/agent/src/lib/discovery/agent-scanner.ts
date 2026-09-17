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

import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { type Clock, SystemClock } from "@promin/core";
import type { AgentBackend, AgentRegistry, RegisterAgentInput } from "../registry/types.ts";

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
    case "cursor":
      // Default mirrors `resolveCursorAgent`'s default — `CURSOR_API_KEY`
      // unless the recipe overrides.
      return backend.requiredEnv ?? ["CURSOR_API_KEY"];
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
  readonly warnings: string[];
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
    // Backend-specific required-env probe. We DON'T refuse to register a
    // recipe whose env is missing — recipe rows are JSON and stay
    // useful once the operator wires the var. We just warn so the boot
    // log makes the missing config obvious.
    const required = backendRequiredEnv(input.backend);
    if (required.length > 0) {
      const missing = required.filter((name) => !process.env[name]);
      if (missing.length > 0) {
        console.warn(
          `[agent-registry] registering "${input.id}" but required env var(s) are not set: ${missing.join(", ")}. ` +
            "The recipe will be stored; the resolver will throw until the vars are present.",
        );
      }
    }
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
  /** Pass through to `applyDiscoveredAgents` — sweeps removed files. Default: `false`. */
  readonly sync?: boolean;
  /** Pass through to `applyDiscoveredAgents`. */
  readonly idPrefix?: string;
  /** Filter scan results before applying (e.g. drop live-only recipes when key missing). */
  readonly filterAgents?: (agents: ReadonlyArray<RegisterAgentInput>) => RegisterAgentInput[];
  /** Fired after each tick. Surfaces deltas + warnings to the host. */
  readonly onTick?: (event: AgentScanLoopTick) => void;
  /** Time source. Default: `SystemClock`. Tests pass `FakeClock`. */
  readonly clock?: Clock;
  /** Forwarded to the underlying scanner. `cacheBust` defaults to `true` for the loop. */
  readonly scanner?: Omit<AgentScannerOptions, "cacheBust" | "onAgent">;
}

export interface AgentScanLoopTick {
  readonly added: string[];
  readonly upserted: string[];
  readonly deleted: string[];
  readonly warnings: string[];
  readonly durationMs: number;
}

export interface AgentScanLoopHandle {
  /** Stop polling. Idempotent. */
  stop(): void;
  /** Run one scan immediately. Resolves with the tick result. */
  tick(): Promise<AgentScanLoopTick>;
}

export function startAgentScanLoop(options: AgentScanLoopOptions): AgentScanLoopHandle {
  const clock = options.clock ?? SystemClock;
  const intervalMs = options.intervalMs ?? 5_000;
  const scanner = new AgentScanner({
    ...options.scanner,
    cacheBust: true,
  });

  let inFlight: Promise<AgentScanLoopTick> | null = null;
  let stopped = false;

  async function runOnce(): Promise<AgentScanLoopTick> {
    const start = clock.currentTimeMs();
    const scan = await scanner.scan(options.root);
    const filtered = options.filterAgents ? options.filterAgents(scan.agents) : scan.agents;
    const apply = await applyDiscoveredAgents(options.registry, filtered, {
      sync: options.sync ?? false,
      ...(options.idPrefix !== undefined ? { idPrefix: options.idPrefix } : {}),
    });
    const tick: AgentScanLoopTick = {
      added: apply.added,
      upserted: apply.upserted,
      deleted: apply.deleted,
      warnings: scan.warnings,
      durationMs: clock.currentTimeMs() - start,
    };
    options.onTick?.(tick);
    return tick;
  }

  async function tickGuarded(): Promise<AgentScanLoopTick> {
    // Coalesce overlapping ticks. If the previous scan is still in
    // flight when the timer fires (slow filesystem, many agents),
    // return that promise instead of starting a parallel scan — two
    // concurrent applyDiscoveredAgents racing for the same registry
    // row would just produce duplicate work.
    if (inFlight) return inFlight;
    const p = runOnce().finally(() => {
      inFlight = null;
    });
    inFlight = p;
    return p;
  }

  const handle = clock.setInterval(() => {
    if (stopped) return;
    void tickGuarded();
  }, intervalMs);

  return {
    stop() {
      if (stopped) return;
      stopped = true;
      handle.clear();
    },
    tick: tickGuarded,
  };
}
