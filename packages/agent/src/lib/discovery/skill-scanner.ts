// ---------------------------------------------------------------------------
// SkillScanner — auto-discover `RegisterSkillInput` exports from a directory
// tree. Sibling of `AgentScanner`; same conventions for placement, shape,
// cache-busting, and reconciliation.
//
// Detection is structural: an export counts as a skill manifest when it has
// a non-empty string `id`, `description`, `whenToUse`, and `body`. Modules
// can export either a single manifest or an array — both flatten.
//
// `applyDiscoveredSkills` reconciles a discovered set into a `SkillRegistry`
// through `applyDiscovered`: unchanged skills are left alone, changed ones
// become a new promoted version, opt-in `sync` removes scanner-written
// skills whose file is gone.
//
// Trust: a scanned file never vouches for itself. Every version the scanner
// creates lands as `needs-review` (whatever its manifest declares) — an
// operator approves each version of the text the model will read. Unchanged
// content keeps the review its version already has.
// ---------------------------------------------------------------------------

import { readFile, readdir, stat } from "node:fs/promises";
import { basename, join } from "node:path";
import { pathToFileURL } from "node:url";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { parseMarkdownSkill } from "../skills/parse-markdown-skill.ts";
import type { RegisterSkillInput, SkillRegistry } from "../skills/types.ts";
import { applyDiscovered, type ApplyDiscoveredResult } from "./apply-discovered.ts";
import { startScanLoop, type ScanLoopHandle, type ScanLoopTick } from "./scan-loop.ts";

/** File extensions a bundled skill might ship that we intentionally ignore. */
const EXECUTABLE_EXTENSIONS = [".py", ".sh", ".rb", ".js", ".ts", ".mjs", ".bin", ".pl"];

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
  /** Diagnostics that did not lose a skill (duplicates, ignored bundled files). */
  readonly warnings: string[];
  /** Directories or skill files that could not be read, parsed or imported. */
  readonly errors: string[];
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
    const out: ScanOutput = { skills: [], sources: {}, warnings: [], errors: [] };
    await this.walk({ dir: root, depth: 0, out });
    return out;
  }

  private async walk(params: { dir: string; depth: number; out: ScanOutput }): Promise<void> {
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
      if (!this.filter(full)) continue;

      // Markdown skills (SKILL.md or any *.md with frontmatter). Handled
      // before the module-extension gate so .md files are picked up even
      // though they aren't importable modules.
      if (name.endsWith(".md")) {
        await this.loadMarkdownSkill({ absPath: full, fileName: name, siblings: entries, out });
        continue;
      }

      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
        continue;
      }

      await this.importModule({ absPath: full, out });
    }
  }

  /**
   * Load a markdown skill (`SKILL.md` or a bare `*.md` with frontmatter).
   * Only the body is consumed, never bundled scripts; when a `SKILL.md`
   * ships sibling executables, a warning notes they were ignored. A
   * markdown file that is not a readable skill (empty, no frontmatter,
   * malformed) is a scan error — `README.md` excepted — so a skill caught
   * mid-edit is never taken for a removed one by sync.
   */
  private async loadMarkdownSkill(params: {
    absPath: string;
    fileName: string;
    siblings: ReadonlyArray<{ name: string; isFile(): boolean; isDirectory(): boolean }>;
    out: ScanOutput;
  }): Promise<void> {
    const { absPath, fileName, siblings, out } = params;
    let text: string;
    try {
      text = await readFile(absPath, "utf8");
    } catch (err) {
      out.errors.push(`failed to read ${absPath}: ${asMessage(err)}`);
      return;
    }
    // `SKILL.md` → id falls back to the containing directory name; a bare
    // `foo.md` → falls back to `foo`.
    const fallbackId =
      fileName.toLowerCase() === "skill.md"
        ? basename(absPath.slice(0, absPath.length - fileName.length - 1))
        : fileName.slice(0, -3);
    const parsed = parseMarkdownSkill({ text, fallbackId });
    if (parsed === null) {
      if (fileName.toLowerCase() !== "readme.md") {
        out.errors.push(`${absPath}: no frontmatter — not a readable skill`);
      }
      return;
    }
    if ("warning" in parsed) {
      out.errors.push(`${absPath}: ${parsed.warning}`);
      return;
    }
    // Instruction-only boundary: note (and ignore) any bundled executables
    // shipped beside a SKILL.md so the operator knows they weren't run.
    if (fileName.toLowerCase() === "skill.md") {
      const bundled = siblings.filter(
        (e) =>
          (e.isDirectory() && e.name === "scripts") ||
          (e.isFile() && EXECUTABLE_EXTENSIONS.some((ext) => e.name.endsWith(ext))),
      );
      if (bundled.length > 0) {
        out.warnings.push(
          `skill "${parsed.skill.id}" ships bundled files (${bundled
            .map((e) => e.name)
            .join(", ")}) — ignored; skills are instruction-only`,
        );
      }
    }
    this.recordSkill({ candidate: parsed.skill, absPath, out });
  }

  /** Add a discovered skill, warning + last-wins on a duplicate id. */
  private recordSkill(params: {
    candidate: RegisterSkillInput;
    absPath: string;
    out: ScanOutput;
  }): void {
    const { candidate, absPath } = params;
    const { skills, sources, warnings } = params.out;
    const existing = sources[candidate.id];
    if (existing && existing !== absPath) {
      warnings.push(`duplicate skill id "${candidate.id}": ${existing} vs ${absPath} — last wins`);
      const i = skills.findIndex((s) => s.id === candidate.id);
      if (i >= 0) skills[i] = candidate;
      else skills.push(candidate);
    } else {
      skills.push(candidate);
    }
    sources[candidate.id] = absPath;
    this.onSkill?.(candidate, absPath);
  }

  private async importModule(params: { absPath: string; out: ScanOutput }): Promise<void> {
    const { absPath, out } = params;
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
      out.errors.push(`failed to import ${absPath}: ${asMessage(err)}`);
      return;
    }

    for (const [, value] of Object.entries(mod)) {
      const candidates = Array.isArray(value) ? value : [value];
      for (const candidate of candidates) {
        if (!isSkillInput(candidate)) continue;
        this.recordSkill({ candidate, absPath, out });
      }
    }
  }
}

interface ScanOutput {
  skills: RegisterSkillInput[];
  sources: Record<string, string>;
  warnings: string[];
  errors: string[];
}

function isSkillInput(v: unknown): v is RegisterSkillInput {
  if (v === null || typeof v !== "object") return false;
  const o = v as { id?: unknown; description?: unknown; whenToUse?: unknown; body?: unknown };
  // whenToUse is optional (the markdown convention folds it into
  // description) — require only id + description + body.
  if (o.whenToUse !== undefined && typeof o.whenToUse !== "string") return false;
  return (
    typeof o.id === "string" &&
    o.id.length > 0 &&
    typeof o.description === "string" &&
    o.description.length > 0 &&
    typeof o.body === "string" &&
    o.body.length > 0
  );
}

// ---------------------------------------------------------------------------
// Reconcile a discovered set into a `SkillRegistry`.
// ---------------------------------------------------------------------------

export interface ApplyDiscoveredSkillsParams {
  readonly registry: SkillRegistry;
  readonly skills: ReadonlyArray<RegisterSkillInput>;
  /**
   * Remove skills whose file is gone. Default `false`. Only versions a
   * scanner wrote are removed; operator-authored skills always stay.
   */
  readonly sync?: boolean;
  /** Whether the scan read every file; `false` skips sync deletes. Default `true`. */
  readonly complete?: boolean;
  /** Only touch ids with this prefix. */
  readonly idPrefix?: string;
  /**
   * The operator vouches for every skill under the scanned root (e.g.
   * first-party skills checked into the host's own repo): scanned versions
   * register as `trusted`, including after edits. Default `false` — new and
   * edited skills land as `needs-review`. This is host config, never
   * something a skill file can set for itself.
   */
  readonly trusted?: boolean;
}

export async function applyDiscoveredSkills(
  params: ApplyDiscoveredSkillsParams,
): Promise<ApplyDiscoveredResult> {
  const { registry, trusted = false } = params;
  return applyDiscovered({
    registry,
    discovered: params.skills,
    ...(params.sync !== undefined && { sync: params.sync }),
    ...(params.complete !== undefined && { complete: params.complete }),
    ...(params.idPrefix !== undefined && { idPrefix: params.idPrefix }),
    trust: trusted ? "trusted" : "needs-review",
    // A trusted root vouches for the content already stored, too.
    onUnchanged: async (entry) => {
      if (trusted && entry.trust !== "trusted") {
        await registry.setTrust({ id: entry.id, version: entry.version, trust: "trusted" });
      }
    },
  });
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
  /** Pass through to `applyDiscoveredSkills` — removes skills whose file is gone. Default: `false`. */
  readonly sync?: boolean;
  /** Pass through to `applyDiscoveredSkills`. */
  readonly idPrefix?: string;
  /** Pass through to `applyDiscoveredSkills`. Default `false`. */
  readonly trusted?: boolean;
  /** Filter scan results before applying. */
  readonly filterSkills?: (skills: ReadonlyArray<RegisterSkillInput>) => RegisterSkillInput[];
  /** Fired after each tick. Surfaces deltas + warnings to the host. */
  readonly onTick?: (event: ScanLoopTick) => void;
  /** A failed timer tick lands here. Default: `console.error`. */
  readonly onError?: (err: unknown) => void;
  /** Time source. Default: `SystemWallClock`. Tests pass `FakeWallClock`. */
  readonly clock?: WallClock;
  /** Forwarded to the underlying scanner. `cacheBust` defaults to `true` for the loop. */
  readonly scanner?: Omit<SkillScannerOptions, "cacheBust" | "onSkill">;
}

export function startSkillScanLoop(options: SkillScanLoopOptions): ScanLoopHandle<ScanLoopTick> {
  const clock = options.clock ?? SystemWallClock;
  const scanner = new SkillScanner({ ...options.scanner, cacheBust: true });
  return startScanLoop({
    label: "skill-scan",
    clock,
    ...(options.intervalMs !== undefined && { intervalMs: options.intervalMs }),
    ...(options.onError !== undefined && { onError: options.onError }),
    run: async () => {
      const start = clock.currentTimeMs();
      const scan = await scanner.scan(options.root);
      const filtered = options.filterSkills ? options.filterSkills(scan.skills) : scan.skills;
      const apply = await applyDiscoveredSkills({
        registry: options.registry,
        skills: filtered,
        sync: options.sync ?? false,
        complete: scan.errors.length === 0,
        ...(options.idPrefix !== undefined && { idPrefix: options.idPrefix }),
        ...(options.trusted !== undefined && { trusted: options.trusted }),
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
