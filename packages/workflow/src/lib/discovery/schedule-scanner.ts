// ---------------------------------------------------------------------------
// ScheduleScanner — auto-discover DurableScheduleConfig exports from a
// directory tree. Symmetric with WorkflowScanner; same reasoning for living
// in @promin/workflow rather than @promin/zorya.
//
// Detection: an export counts as a schedule if it has a string `id` and at
// least one trigger field (`cron`, `rrule`, or `intervalMs`). Modules can
// export either a single config or an array — both flatten.
//
// `applyDiscoveredSchedules` reconciles a discovered set into a
// `SchedulerStorage`. It's exported alongside the scanner because the two
// are paired in practice: anything that scans schedules from disk almost
// always wants to push them into storage afterward.
// ---------------------------------------------------------------------------

import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { DurableScheduleConfig } from "../scheduler/types.ts";
import type { SchedulerStorage } from "../scheduler/scheduler-storage.ts";

export interface ScheduleScannerOptions {
  extensions?: ReadonlyArray<string>;
  maxDepth?: number;
  filter?: (absPath: string) => boolean;
  onSchedule?: (schedule: DurableScheduleConfig, sourcePath: string) => void;
}

export interface ScheduleScanResult {
  schedules: DurableScheduleConfig[];
  sources: Record<string, string>;
  warnings: string[];
}

const DEFAULT_EXTENSIONS = [".ts", ".tsx", ".js", ".mjs"];

export class ScheduleScanner {
  private readonly extensions: ReadonlyArray<string>;
  private readonly maxDepth: number;
  private readonly filter: (absPath: string) => boolean;
  private readonly onSchedule?: ScheduleScannerOptions["onSchedule"];

  constructor(options: ScheduleScannerOptions = {}) {
    this.extensions = options.extensions ?? DEFAULT_EXTENSIONS;
    this.maxDepth = options.maxDepth ?? 10;
    this.filter = options.filter ?? (() => true);
    this.onSchedule = options.onSchedule;
  }

  static async scanFolder(
    root: string,
    options: ScheduleScannerOptions = {},
  ): Promise<ScheduleScanResult> {
    return await new ScheduleScanner(options).scan(root);
  }

  async scan(root: string): Promise<ScheduleScanResult> {
    const schedules: DurableScheduleConfig[] = [];
    const sources: Record<string, string> = {};
    const warnings: string[] = [];
    await this.walk(root, 0, schedules, sources, warnings);
    return { schedules, sources, warnings };
  }

  private async walk(
    dir: string,
    depth: number,
    schedules: DurableScheduleConfig[],
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
        await this.walk(full, depth + 1, schedules, sources, warnings);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (!this.filter(full)) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
        continue;
      }

      await this.importModule(full, schedules, sources, warnings);
    }
  }

  private async importModule(
    absPath: string,
    schedules: DurableScheduleConfig[],
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
        if (!isScheduleConfig(candidate)) continue;
        const existing = sources[candidate.id];
        if (existing) {
          warnings.push(
            `duplicate schedule id "${candidate.id}": ${existing} vs ${absPath} — last wins`,
          );
          const i = schedules.findIndex((s) => s.id === candidate.id);
          if (i >= 0) schedules[i] = candidate;
        } else {
          schedules.push(candidate);
        }
        sources[candidate.id] = absPath;
        this.onSchedule?.(candidate, absPath);
      }
    }
  }
}

function isScheduleConfig(v: unknown): v is DurableScheduleConfig {
  if (v === null || typeof v !== "object") return false;
  const o = v as { id?: unknown; cron?: unknown; rrule?: unknown; intervalMs?: unknown };
  if (typeof o.id !== "string" || o.id.length === 0) return false;
  return (
    typeof o.cron === "string" || typeof o.rrule === "string" || typeof o.intervalMs === "number"
  );
}

// ---------------------------------------------------------------------------
// Reconcile discovered schedules into a SchedulerStorage.
// ---------------------------------------------------------------------------

export interface ApplyDiscoveredSchedulesOptions {
  /**
   * Delete storage entries whose id isn't in the discovered set. Default:
   * false (upsert-only, leaves operator-created entries alone). Set true
   * to make the filesystem authoritative.
   */
  sync?: boolean;
  /**
   * Set `nextRun = now` on every newly-added schedule so the scheduler
   * picks it up on the next poll. Useful for in-memory storage and
   * first-time installs. Default: false.
   */
  kickstart?: boolean;
  /** Restrict the operation to a single namespace. */
  namespace?: string;
}

export interface ApplyDiscoveredSchedulesResult {
  upserted: string[];
  /** Subset of `upserted` that didn't exist in storage prior to this call. */
  added: string[];
  /** Only populated when `sync: true`. */
  deleted: string[];
}

export async function applyDiscoveredSchedules(
  storage: SchedulerStorage,
  schedules: readonly DurableScheduleConfig[],
  options: ApplyDiscoveredSchedulesOptions = {},
): Promise<ApplyDiscoveredSchedulesResult> {
  const upserted: string[] = [];
  const added: string[] = [];
  const deleted: string[] = [];

  const existing =
    options.sync || options.kickstart
      ? await storage.listSchedules({ namespace: options.namespace, limit: 10_000 })
      : [];
  const existingIds = new Set(existing.map((s) => s.id));

  const now = new Date();
  for (const config of schedules) {
    if (options.namespace !== undefined && config.namespace !== options.namespace) continue;
    const wasExisting = existingIds.has(config.id);
    await storage.upsertSchedule(config);
    upserted.push(config.id);
    if (!wasExisting) {
      added.push(config.id);
      if (options.kickstart && config.enabled !== false) {
        await storage.setNextRun(config.id, now);
      }
    }
  }

  if (options.sync) {
    const discoveredIds = new Set(schedules.map((s) => s.id));
    for (const stored of existing) {
      if (!discoveredIds.has(stored.id)) {
        await storage.deleteSchedule(stored.id);
        deleted.push(stored.id);
      }
    }
  }

  return { upserted, added, deleted };
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
