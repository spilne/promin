// ---------------------------------------------------------------------------
// FragmentScanner — auto-discover prompt fragments from a folder of `.md`
// files. Sibling of `SkillScanner` / `AgentScanner`.
//
// Each .md file is one fragment: the **id** is the file basename without
// the `.md` extension; the **content** is the entire file body. README.md
// is skipped by convention (it's prose, not a fragment).
//
// `applyDiscoveredFragments` reconciles a discovered set into a
// `FragmentRegistry` through `applyDiscovered`: unchanged fragments are
// left alone, changed ones become a new promoted version, and opt-in
// `sync` removes scanner-written fragments whose file is gone — never an
// operator-authored one.
// ---------------------------------------------------------------------------

import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  FRAGMENT_VERSION_SEPARATOR,
  type FragmentRegistry,
  type RegisterFragmentInput,
} from "../fragments/types.ts";
import { applyDiscovered, type ApplyDiscoveredResult } from "./apply-discovered.ts";
import { startScanLoop, type ScanLoopHandle, type ScanLoopTick } from "./scan-loop.ts";

/** A discovered fragment: its id (file basename) and content. */
export interface FragmentSpec {
  readonly id: string;
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
  /** Per-id source path — useful for duplicate diagnostics. */
  readonly sources: Record<string, string>;
  /** Diagnostics that did not lose a fragment (duplicates). */
  readonly warnings: string[];
  /** Directories or fragment files that could not be read, or are empty or misnamed. */
  readonly errors: string[];
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
    const out: FragmentScanResult = { fragments: [], sources: {}, warnings: [], errors: [] };
    await this.walk({ dir: root, depth: 0, out });
    return out;
  }

  private async walk(params: {
    dir: string;
    depth: number;
    out: FragmentScanResult;
  }): Promise<void> {
    const { dir, depth, out } = params;
    if (depth > this.maxDepth) return;
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      out.errors.push(`failed to read ${dir}: ${asMessage(err)}`);
      return;
    }
    for (const entry of entries) {
      const name = entry.name;
      const full = join(dir, name);
      if (entry.isDirectory()) {
        if (name === "node_modules" || name.startsWith(".")) continue;
        await this.walk({ dir: full, depth: depth + 1, out });
        continue;
      }
      if (!entry.isFile()) continue;
      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (!this.filter(full)) continue;
      // README.md is prose, not a fragment.
      if (name.toLowerCase() === "readme.md") continue;

      await this.loadFragment({ absPath: full, fileName: name, out });
    }
  }

  private async loadFragment(params: {
    absPath: string;
    fileName: string;
    out: FragmentScanResult;
  }): Promise<void> {
    const { absPath, fileName, out } = params;
    const { fragments, sources, warnings } = out;
    let text: string;
    try {
      text = await readFile(absPath, "utf8");
    } catch (err) {
      out.errors.push(`failed to read ${absPath}: ${asMessage(err)}`);
      return;
    }
    const content = text.trim();
    // An empty or unusable file is a scan error, not a removed fragment:
    // sync deletes nothing on a scan with errors.
    if (content.length === 0) {
      out.errors.push(`fragment file is empty: ${absPath}`);
      return;
    }
    const id = fileName.slice(0, -3); // strip ".md"
    if (id.includes(FRAGMENT_VERSION_SEPARATOR)) {
      out.errors.push(
        `fragment file name contains "${FRAGMENT_VERSION_SEPARATOR}" (reserved for version pins): ${absPath}`,
      );
      return;
    }
    const existing = sources[id];
    if (existing && existing !== absPath) {
      warnings.push(`duplicate fragment id "${id}": ${existing} vs ${absPath} — last wins`);
      const i = fragments.findIndex((f) => f.id === id);
      if (i >= 0) fragments[i] = { id, content };
      else fragments.push({ id, content });
    } else {
      fragments.push({ id, content });
    }
    sources[id] = absPath;
    this.onFragment?.({ id, content }, absPath);
  }
}

// ---------------------------------------------------------------------------
// Reconcile a discovered set into a `FragmentRegistry`.
// ---------------------------------------------------------------------------

export interface ApplyDiscoveredFragmentsParams {
  readonly registry: FragmentRegistry;
  readonly fragments: ReadonlyArray<FragmentSpec>;
  /** Remove scanner-written fragments whose file is gone. Default `false`. */
  readonly sync?: boolean;
  /** Whether the scan read every file; `false` skips sync deletes. Default `true`. */
  readonly complete?: boolean;
}

export async function applyDiscoveredFragments(
  params: ApplyDiscoveredFragmentsParams,
): Promise<ApplyDiscoveredResult> {
  const discovered: RegisterFragmentInput[] = params.fragments.map((f) => ({
    id: f.id,
    content: f.content,
  }));
  return applyDiscovered({
    registry: params.registry,
    discovered,
    ...(params.sync !== undefined && { sync: params.sync }),
    ...(params.complete !== undefined && { complete: params.complete }),
  });
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
  /** Pass through to `applyDiscoveredFragments`. Default: `false`. */
  readonly sync?: boolean;
  readonly onTick?: (event: ScanLoopTick) => void;
  /** A failed timer tick lands here. Default: `console.error`. */
  readonly onError?: (err: unknown) => void;
  readonly clock?: WallClock;
  readonly scanner?: FragmentScannerOptions;
}

export function startFragmentScanLoop(
  options: FragmentScanLoopOptions,
): ScanLoopHandle<ScanLoopTick> {
  const clock = options.clock ?? SystemWallClock;
  const scanner = new FragmentScanner(options.scanner ?? {});
  return startScanLoop({
    label: "fragment-scan",
    clock,
    ...(options.intervalMs !== undefined && { intervalMs: options.intervalMs }),
    ...(options.onError !== undefined && { onError: options.onError }),
    run: async () => {
      const start = clock.currentTimeMs();
      const scan = await scanner.scan(options.root);
      const apply = await applyDiscoveredFragments({
        registry: options.registry,
        fragments: scan.fragments,
        sync: options.sync ?? false,
        complete: scan.errors.length === 0,
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
