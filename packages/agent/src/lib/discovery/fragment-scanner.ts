// ---------------------------------------------------------------------------
// FragmentScanner — auto-discover prompt fragments from a folder of `.md`
// files. Sibling of `SkillScanner` / `AgentScanner`.
//
// Each .md file is one fragment: the **key** is the file basename without
// the `.md` extension; the **content** is the entire file body. README.md
// is skipped by convention (it's prose, not a fragment).
//
// `applyDiscoveredFragments` reconciles a discovered set into a
// `FragmentRegistry`: upsert by default, opt-in `sync` mode to delete
// entries not in the scan.
// ---------------------------------------------------------------------------

import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { type Clock, SystemClock } from "@promin/core";
import type { FragmentRegistry } from "../fragments/types.ts";

export interface FragmentSpec {
  readonly key: string;
  readonly content: string;
}

export interface FragmentScannerOptions {
  /** File extensions to consider. Default: `.md`. */
  readonly extensions?: ReadonlyArray<string>;
  /** Recursion depth. Default: `10`. */
  readonly maxDepth?: number;
  /** Predicate filtering candidate files before reading. */
  readonly filter?: (absPath: string) => boolean;
  /** Per-discovery callback — fires once per detected fragment. */
  readonly onFragment?: (frag: FragmentSpec, sourcePath: string) => void;
}

export interface FragmentScanResult {
  readonly fragments: FragmentSpec[];
  /** Per-key source path — useful for duplicate diagnostics. */
  readonly sources: Record<string, string>;
  readonly warnings: string[];
}

const DEFAULT_EXTENSIONS = [".md"];

export class FragmentScanner {
  private readonly extensions: ReadonlyArray<string>;
  private readonly maxDepth: number;
  private readonly filter: (absPath: string) => boolean;
  private readonly onFragment?: FragmentScannerOptions["onFragment"];

  constructor(options: FragmentScannerOptions = {}) {
    this.extensions = options.extensions ?? DEFAULT_EXTENSIONS;
    this.maxDepth = options.maxDepth ?? 10;
    this.filter = options.filter ?? (() => true);
    this.onFragment = options.onFragment;
  }

  /** One-shot helper for callers that don't want to hold an instance. */
  static async scanFolder(
    root: string,
    options: FragmentScannerOptions = {},
  ): Promise<FragmentScanResult> {
    return new FragmentScanner(options).scan(root);
  }

  async scan(root: string): Promise<FragmentScanResult> {
    const fragments: FragmentSpec[] = [];
    const sources: Record<string, string> = {};
    const warnings: string[] = [];
    await this.walk(root, 0, fragments, sources, warnings);
    return { fragments, sources, warnings };
  }

  private async walk(
    dir: string,
    depth: number,
    fragments: FragmentSpec[],
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
        await this.walk(full, depth + 1, fragments, sources, warnings);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (!this.filter(full)) continue;
      // README.md is prose, not a fragment.
      if (name.toLowerCase() === "readme.md") continue;

      await this.loadFragment(full, name, fragments, sources, warnings);
    }
  }

  private async loadFragment(
    absPath: string,
    fileName: string,
    fragments: FragmentSpec[],
    sources: Record<string, string>,
    warnings: string[],
  ): Promise<void> {
    let text: string;
    try {
      text = await readFile(absPath, "utf8");
    } catch (err) {
      warnings.push(`failed to read ${absPath}: ${asMessage(err)}`);
      return;
    }
    const content = text.trim();
    if (content.length === 0) {
      warnings.push(`fragment file is empty: ${absPath}`);
      return;
    }
    const key = fileName.slice(0, -3); // strip ".md"
    const existing = sources[key];
    if (existing && existing !== absPath) {
      warnings.push(`duplicate fragment key "${key}": ${existing} vs ${absPath} — last wins`);
      const i = fragments.findIndex((f) => f.key === key);
      if (i >= 0) fragments[i] = { key, content };
      else fragments.push({ key, content });
    } else {
      fragments.push({ key, content });
    }
    sources[key] = absPath;
    this.onFragment?.({ key, content }, absPath);
  }
}

// ---------------------------------------------------------------------------
// Reconcile a discovered set into a `FragmentRegistry`.
// ---------------------------------------------------------------------------

export interface ApplyDiscoveredFragmentsOptions {
  /** Delete registry entries whose `key` isn't in the discovered set. */
  readonly sync?: boolean;
}

export interface ApplyDiscoveredFragmentsResult {
  /** Every fragment key that was applied this run. */
  readonly upserted: string[];
  /** Subset of `upserted` that didn't exist before this call. */
  readonly added: string[];
  /** Only populated when `sync: true`. */
  readonly deleted: string[];
}

export function applyDiscoveredFragments(
  registry: FragmentRegistry,
  fragments: ReadonlyArray<FragmentSpec>,
  options: ApplyDiscoveredFragmentsOptions = {},
): ApplyDiscoveredFragmentsResult {
  const upserted: string[] = [];
  const added: string[] = [];
  const deleted: string[] = [];

  const existingKeys = new Set(registry.list().map((f) => f.key));
  for (const frag of fragments) {
    const wasExisting = existingKeys.has(frag.key);
    registry.set(frag.key, frag.content);
    upserted.push(frag.key);
    if (!wasExisting) added.push(frag.key);
  }

  if (options.sync) {
    const discovered = new Set(fragments.map((f) => f.key));
    for (const key of existingKeys) {
      if (!discovered.has(key)) {
        registry.delete(key);
        deleted.push(key);
      }
    }
  }
  return { upserted, added, deleted };
}

// ---------------------------------------------------------------------------
// Periodic scan loop — auto-discovers and reconciles fragments on a tick.
// Mirrors startSkillScanLoop / startAgentScanLoop.
// ---------------------------------------------------------------------------

export interface FragmentScanLoopOptions {
  readonly registry: FragmentRegistry;
  readonly root: string;
  /** Poll interval in ms. Default: `5_000`. */
  readonly intervalMs?: number;
  /** Pass through to `applyDiscoveredFragments` — sweeps removed files. Default: `false`. */
  readonly sync?: boolean;
  readonly onTick?: (event: FragmentScanLoopTick) => void;
  readonly clock?: Clock;
  readonly scanner?: FragmentScannerOptions;
}

export interface FragmentScanLoopTick {
  readonly added: string[];
  readonly upserted: string[];
  readonly deleted: string[];
  readonly warnings: string[];
  readonly durationMs: number;
}

export interface FragmentScanLoopHandle {
  stop(): void;
  tick(): Promise<FragmentScanLoopTick>;
}

export function startFragmentScanLoop(options: FragmentScanLoopOptions): FragmentScanLoopHandle {
  const clock = options.clock ?? SystemClock;
  const intervalMs = options.intervalMs ?? 5_000;
  const scanner = new FragmentScanner(options.scanner ?? {});

  let inFlight: Promise<FragmentScanLoopTick> | null = null;
  let stopped = false;

  async function runOnce(): Promise<FragmentScanLoopTick> {
    const start = clock.currentTimeMs();
    const scan = await scanner.scan(options.root);
    const apply = applyDiscoveredFragments(options.registry, scan.fragments, {
      sync: options.sync ?? false,
    });
    const tick: FragmentScanLoopTick = {
      added: apply.added,
      upserted: apply.upserted,
      deleted: apply.deleted,
      warnings: scan.warnings,
      durationMs: clock.currentTimeMs() - start,
    };
    options.onTick?.(tick);
    return tick;
  }

  async function tickGuarded(): Promise<FragmentScanLoopTick> {
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

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
