// ---------------------------------------------------------------------------
// SkillScanner — auto-discover `RegisterSkillInput` exports from a directory
// tree. Sibling of `AgentScanner`; same conventions for placement, shape,
// cache-busting, and reconciliation.
//
// Detection is structural: an export counts as a skill manifest when it has
// a non-empty string `id`, `description`, `whenToUse`, and `body`. Modules
// can export either a single manifest or an array — both flatten.
//
// `applyDiscoveredSkills` reconciles a discovered set into a `SkillRegistry`:
// upsert by default, opt-in `sync` mode to delete entries not in the scan.
// ---------------------------------------------------------------------------

import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { type Clock, SystemClock } from "@promin/core";
import type { RegisterSkillInput, SkillRegistry } from "../skills/types.ts";

export interface SkillScannerOptions {
  /** File extensions to consider. Default: `.ts, .tsx, .js, .mjs`. */
  readonly extensions?: ReadonlyArray<string>;
  /** Recursion depth. Default: `10`. */
  readonly maxDepth?: number;
  /** Predicate filtering candidate files before import. */
  readonly filter?: (absPath: string) => boolean;
  /** Per-discovery callback — fires once per detected skill. */
  readonly onSkill?: (skill: RegisterSkillInput, sourcePath: string) => void;
  /**
   * Append `?v=<mtimeMs>` to each `import()` URL so edits to a skill file
   * produce a fresh module instance instead of a cached one. Off by default
   * (one-shot scans don't need it). The scan loop turns this on
   * automatically.
   */
  readonly cacheBust?: boolean;
}

export interface SkillScanResult {
  readonly skills: RegisterSkillInput[];
  /** Per-id source path — useful for duplicate diagnostics. */
  readonly sources: Record<string, string>;
  readonly warnings: string[];
}

const DEFAULT_EXTENSIONS = [".ts", ".tsx", ".js", ".mjs"];

export class SkillScanner {
  private readonly extensions: ReadonlyArray<string>;
  private readonly maxDepth: number;
  private readonly filter: (absPath: string) => boolean;
  private readonly onSkill?: SkillScannerOptions["onSkill"];
  private readonly cacheBust: boolean;

  constructor(options: SkillScannerOptions = {}) {
    this.extensions = options.extensions ?? DEFAULT_EXTENSIONS;
    this.maxDepth = options.maxDepth ?? 10;
    this.filter = options.filter ?? (() => true);
    this.onSkill = options.onSkill;
    this.cacheBust = options.cacheBust ?? false;
  }

  /** One-shot helper for callers that don't want to hold an instance. */
  static async scanFolder(
    root: string,
    options: SkillScannerOptions = {},
  ): Promise<SkillScanResult> {
    return new SkillScanner(options).scan(root);
  }

  async scan(root: string): Promise<SkillScanResult> {
    const skills: RegisterSkillInput[] = [];
    const sources: Record<string, string> = {};
    const warnings: string[] = [];
    await this.walk(root, 0, skills, sources, warnings);
    return { skills, sources, warnings };
  }

  private async walk(
    dir: string,
    depth: number,
    skills: RegisterSkillInput[],
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
        await this.walk(full, depth + 1, skills, sources, warnings);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (!this.filter(full)) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
        continue;
      }

      await this.importModule(full, skills, sources, warnings);
    }
  }

  private async importModule(
    absPath: string,
    skills: RegisterSkillInput[],
    sources: Record<string, string>,
    warnings: string[],
  ): Promise<void> {
    let mod: Record<string, unknown>;
    let url = pathToFileURL(absPath).href;
    if (this.cacheBust) {
      // Stat for mtime so edits → unique URL → fresh module. Falls back to
      // Date.now() if the stat fails (e.g. file deleted between walk and
      // import); we'd rather over-import than crash the loop.
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
        if (!isSkillInput(candidate)) continue;
        const existing = sources[candidate.id];
        if (existing && existing !== absPath) {
          warnings.push(
            `duplicate skill id "${candidate.id}": ${existing} vs ${absPath} — last wins`,
          );
          const i = skills.findIndex((s) => s.id === candidate.id);
          if (i >= 0) skills[i] = candidate;
          else skills.push(candidate);
        } else {
          skills.push(candidate);
        }
        sources[candidate.id] = absPath;
        this.onSkill?.(candidate, absPath);
      }
    }
  }
}

function isSkillInput(v: unknown): v is RegisterSkillInput {
  if (v === null || typeof v !== "object") return false;
  const o = v as {
    id?: unknown;
    description?: unknown;
    whenToUse?: unknown;
    body?: unknown;
  };
  return (
    typeof o.id === "string" &&
    o.id.length > 0 &&
    typeof o.description === "string" &&
    o.description.length > 0 &&
    typeof o.whenToUse === "string" &&
    o.whenToUse.length > 0 &&
    typeof o.body === "string" &&
    o.body.length > 0
  );
}

// ---------------------------------------------------------------------------
// Reconcile a discovered set into a `SkillRegistry`.
// ---------------------------------------------------------------------------

export interface ApplyDiscoveredSkillsOptions {
  /**
   * Delete registry entries whose `id` isn't in the discovered set. Default:
   * `false` (upsert-only — operator-created entries stay). Set `true` to
   * make the filesystem authoritative.
   */
  readonly sync?: boolean;
  /**
   * Restrict the operation to entries whose discovered `id` starts with this
   * prefix. Useful for partitioning skills by tenant / environment without
   * colliding ids. No filter when omitted.
   */
  readonly idPrefix?: string;
}

export interface ApplyDiscoveredSkillsResult {
  readonly upserted: string[];
  /** Subset of `upserted` that didn't exist before this call. */
  readonly added: string[];
  /** Only populated when `sync: true`. */
  readonly deleted: string[];
}

export async function applyDiscoveredSkills(
  registry: SkillRegistry,
  skills: ReadonlyArray<RegisterSkillInput>,
  options: ApplyDiscoveredSkillsOptions = {},
): Promise<ApplyDiscoveredSkillsResult> {
  const upserted: string[] = [];
  const added: string[] = [];
  const deleted: string[] = [];

  // Snapshot the existing set when we'll need it for added-tracking or sync.
  const existing = options.sync ? await registry.list({ limit: 100_000 }) : [];
  const existingIds = new Set(existing.map((row) => row.id));

  for (const input of skills) {
    if (options.idPrefix !== undefined && !input.id.startsWith(options.idPrefix)) continue;
    const wasExisting = existingIds.has(input.id) || (await registry.get(input.id)) !== null;
    await registry.register(input);
    upserted.push(input.id);
    if (!wasExisting) added.push(input.id);
  }

  if (options.sync) {
    const discoveredIds = new Set(
      skills
        .filter((s) => options.idPrefix === undefined || s.id.startsWith(options.idPrefix))
        .map((s) => s.id),
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
// Periodic scan loop — auto-discovers and reconciles skills on a tick.
// Drop a new skill file under the scan root, edit an existing one, and the
// registry catches up on the next interval. Runs `cacheBust: true` so edits
// to existing files actually re-import. Mirrors `startAgentScanLoop`.
// ---------------------------------------------------------------------------

export interface SkillScanLoopOptions {
  readonly registry: SkillRegistry;
  readonly root: string;
  /** Poll interval in ms. Default: `5_000`. */
  readonly intervalMs?: number;
  /** Pass through to `applyDiscoveredSkills` — sweeps removed files. Default: `false`. */
  readonly sync?: boolean;
  /** Pass through to `applyDiscoveredSkills`. */
  readonly idPrefix?: string;
  /** Filter scan results before applying. */
  readonly filterSkills?: (skills: ReadonlyArray<RegisterSkillInput>) => RegisterSkillInput[];
  /** Fired after each tick. Surfaces deltas + warnings to the host. */
  readonly onTick?: (event: SkillScanLoopTick) => void;
  /** Time source. Default: `SystemClock`. Tests pass `FakeClock`. */
  readonly clock?: Clock;
  /** Forwarded to the underlying scanner. `cacheBust` defaults to `true` for the loop. */
  readonly scanner?: Omit<SkillScannerOptions, "cacheBust" | "onSkill">;
}

export interface SkillScanLoopTick {
  readonly added: string[];
  readonly upserted: string[];
  readonly deleted: string[];
  readonly warnings: string[];
  readonly durationMs: number;
}

export interface SkillScanLoopHandle {
  /** Stop polling. Idempotent. */
  stop(): void;
  /** Run one scan immediately. Resolves with the tick result. */
  tick(): Promise<SkillScanLoopTick>;
}

export function startSkillScanLoop(options: SkillScanLoopOptions): SkillScanLoopHandle {
  const clock = options.clock ?? SystemClock;
  const intervalMs = options.intervalMs ?? 5_000;
  const scanner = new SkillScanner({
    ...options.scanner,
    cacheBust: true,
  });

  let inFlight: Promise<SkillScanLoopTick> | null = null;
  let stopped = false;

  async function runOnce(): Promise<SkillScanLoopTick> {
    const start = clock.currentTimeMs();
    const scan = await scanner.scan(options.root);
    const filtered = options.filterSkills ? options.filterSkills(scan.skills) : scan.skills;
    const apply = await applyDiscoveredSkills(options.registry, filtered, {
      sync: options.sync ?? false,
      ...(options.idPrefix !== undefined ? { idPrefix: options.idPrefix } : {}),
    });
    const tick: SkillScanLoopTick = {
      added: apply.added,
      upserted: apply.upserted,
      deleted: apply.deleted,
      warnings: scan.warnings,
      durationMs: clock.currentTimeMs() - start,
    };
    options.onTick?.(tick);
    return tick;
  }

  async function tickGuarded(): Promise<SkillScanLoopTick> {
    // Coalesce overlapping ticks — two concurrent applyDiscoveredSkills
    // racing for the same registry row would just produce duplicate work.
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
