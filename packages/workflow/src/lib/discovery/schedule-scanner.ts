// ---------------------------------------------------------------------------
// ScheduleScanner — auto-discover DurableScheduleConfig exports from a
// directory tree. Symmetric with WorkflowScanner; same reasoning for living
// in @promin/workflow rather than @promin/zorya.
//
// Detection: an export counts as a schedule if it has a string `id` and at
// least one trigger field (`cron`, `rrule`, or `intervalMs`). Modules can
// export either a single config or an array — both flatten. A candidate
// that fails `validateScheduleConfig` (two triggers, a bad cron, …) is
// left out with a warning.
//
// `applyDiscoveredSchedules` reconciles a discovered set into a
// `SchedulerStorage`. It's exported alongside the scanner because the two
// are paired in practice: anything that scans schedules from disk almost
// always wants to push them into storage afterward.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig } from "../scheduler/types.ts";
import type { SchedulerStorage } from "../scheduler/scheduler-storage.ts";
import { validateScheduleConfig } from "../scheduler/schedule-config.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { asMessage, scanModules } from "./scan-modules.ts";

export interface ScheduleScannerOptions {
  extensions?: ReadonlyArray<string>;
  maxDepth?: number;
  filter?: (absPath: string) => boolean;
  onSchedule?: (params: {
    readonly schedule: DurableScheduleConfig;
    readonly sourcePath: string;
  }) => void;
}

export interface ScheduleScanResult {
  schedules: DurableScheduleConfig[];
  sources: Record<string, string>;
  warnings: string[];
}

export class ScheduleScanner {
  private readonly options: ScheduleScannerOptions;

  constructor(options: ScheduleScannerOptions = {}) {
    this.options = options;
  }

  static async scanFolder(
    params: { readonly root: string } & ScheduleScannerOptions,
  ): Promise<ScheduleScanResult> {
    const { root, ...options } = params;
    return await new ScheduleScanner(options).scan(root);
  }

  async scan(root: string): Promise<ScheduleScanResult> {
    const schedules: DurableScheduleConfig[] = [];
    const sources: Record<string, string> = {};
    const warnings: string[] = [];

    await scanModules({
      root,
      extensions: this.options.extensions,
      maxDepth: this.options.maxDepth,
      filter: this.options.filter,
      warnings,
      visit: ({ exports, path }) => {
        for (const value of Object.values(exports)) {
          const candidates = Array.isArray(value) ? value : [value];
          for (const candidate of candidates) {
            if (!isScheduleConfig(candidate)) continue;
            try {
              validateScheduleConfig(candidate);
            } catch (err) {
              warnings.push(`invalid schedule "${candidate.id}" in ${path}: ${asMessage(err)}`);
              continue;
            }
            const existing = sources[candidate.id];
            if (existing) {
              warnings.push(
                `duplicate schedule id "${candidate.id}": ${existing} vs ${path} — last wins`,
              );
              const i = schedules.findIndex((s) => s.id === candidate.id);
              if (i >= 0) schedules[i] = candidate;
            } else {
              schedules.push(candidate);
            }
            sources[candidate.id] = path;
            this.options.onSchedule?.({ schedule: candidate, sourcePath: path });
          }
        }
      },
    });

    return { schedules, sources, warnings };
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

/** Page size for listing the stored schedules of the scope. */
const LIST_PAGE_SIZE = 500;

export interface ApplyDiscoveredSchedulesParams {
  readonly storage: SchedulerStorage;
  readonly schedules: readonly DurableScheduleConfig[];
  /**
   * The namespace this call manages. Discovered schedules from another
   * namespace are skipped (listed in `skipped`), and `sync` only deletes
   * stored schedules of this namespace. Default: the global scope —
   * schedules without a namespace.
   */
  readonly namespace?: string;
  /**
   * Delete stored schedules of the namespace whose id isn't in the
   * discovered set. Default: false (upsert-only, leaves operator-created
   * entries alone). Set true to make the filesystem authoritative.
   */
  readonly sync?: boolean;
  /**
   * Set `nextRun = now` on every newly-added schedule so the scheduler
   * picks it up on the next poll. Useful for in-memory storage and
   * first-time installs. Default: false.
   */
  readonly kickstart?: boolean;
  /**
   * Time source for the `kickstart` nextRun stamp. Pass the scheduler's
   * clock so the seeded time lines up with its due checks. Default:
   * `SystemWallClock`.
   */
  readonly clock?: WallClock;
}

export interface ApplyDiscoveredSchedulesResult {
  upserted: string[];
  /** Subset of `upserted` that didn't exist in storage prior to this call. */
  added: string[];
  /** Only populated when `sync: true`. */
  deleted: string[];
  /** Discovered schedules left alone because they belong to another namespace. */
  skipped: string[];
}

/**
 * Upsert discovered schedules into `storage`, scoped to one namespace.
 * Every schedule is validated first (`validateScheduleConfig`); an invalid
 * one rejects the call before anything is written.
 */
export async function applyDiscoveredSchedules(
  params: ApplyDiscoveredSchedulesParams,
): Promise<ApplyDiscoveredSchedulesResult> {
  const { storage, schedules, namespace, sync = false, kickstart = false } = params;
  const clock = params.clock ?? SystemWallClock;

  const problems: string[] = [];
  for (const config of schedules) {
    try {
      validateScheduleConfig(config);
    } catch (err) {
      problems.push(asMessage(err));
    }
  }
  if (problems.length > 0) {
    throw new Error(`applyDiscoveredSchedules: invalid schedules:\n  ${problems.join("\n  ")}`);
  }

  const inScope = (config: Pick<DurableScheduleConfig, "namespace">): boolean =>
    (config.namespace ?? undefined) === namespace;

  const existing = await listScope({ storage, namespace, inScope });
  const existingIds = new Set(existing.map((s) => s.id));

  const upserted: string[] = [];
  const added: string[] = [];
  const deleted: string[] = [];
  const skipped: string[] = [];
  const now = clock.now();

  for (const config of schedules) {
    if (!inScope(config)) {
      skipped.push(config.id);
      continue;
    }
    await storage.upsertSchedule(config);
    upserted.push(config.id);
    if (!existingIds.has(config.id)) {
      added.push(config.id);
      if (kickstart && config.enabled !== false) {
        await storage.setNextRun(config.id, now);
      }
    }
  }

  if (sync) {
    const discoveredIds = new Set(upserted);
    for (const stored of existing) {
      if (discoveredIds.has(stored.id)) continue;
      await storage.deleteSchedule(stored.id);
      deleted.push(stored.id);
    }
  }

  return { upserted, added, deleted, skipped };
}

/**
 * Every stored schedule of the scope, page by page. Backends treat an
 * absent `namespace` filter as "all namespaces", so the global scope is
 * narrowed here.
 */
async function listScope(params: {
  readonly storage: SchedulerStorage;
  readonly namespace: string | undefined;
  readonly inScope: (config: DurableScheduleConfig) => boolean;
}): Promise<DurableScheduleConfig[]> {
  const out: DurableScheduleConfig[] = [];
  for (let offset = 0; ; offset += LIST_PAGE_SIZE) {
    const page = await params.storage.listSchedules({
      namespace: params.namespace,
      limit: LIST_PAGE_SIZE,
      offset,
    });
    for (const config of page) if (params.inScope(config)) out.push(config);
    if (page.length < LIST_PAGE_SIZE) return out;
  }
}
