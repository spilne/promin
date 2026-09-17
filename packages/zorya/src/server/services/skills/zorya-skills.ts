// ---------------------------------------------------------------------------
// ZoryaSkills — owns the SkillRegistry + an optional folder scan loop.
// Sibling of ZoryaAgents. The server config expects this object when skills
// functionality is enabled; it backs the /api/skills CRUD routes, the
// /api/agents/_catalog/skills picker source, and (via its `registry`) the
// agent resolver's catalog injection.
// ---------------------------------------------------------------------------

import type { SkillRegistry } from "@promin/agent";
import { startSkillScanLoop } from "@promin/agent";

export interface ZoryaSkillsScanConfig {
  /** Filesystem root to scan for skill manifests (.ts modules or SKILL.md). */
  root: string;
  /** Poll interval in ms. Default 5000. */
  intervalMs?: number;
  /** Sweep removed files (delete from registry). Default false. */
  sync?: boolean;
  /** Fired after each scan tick. */
  onTick?: (tick: {
    added: string[];
    upserted: string[];
    deleted: string[];
    warnings: string[];
    durationMs: number;
  }) => void;
}

export interface ZoryaSkillsConfig {
  registry: SkillRegistry;
  /** Filesystem hot-reload scan loop. Omit to disable. */
  scan?: ZoryaSkillsScanConfig;
}

export class ZoryaSkills {
  readonly registry: SkillRegistry;
  private readonly scanConfig?: ZoryaSkillsScanConfig;
  private scanHandle?: { stop(): void; tick(): Promise<unknown> };
  // Ids currently backed by a file on disk (rebuilt each scan tick). The UI
  // marks these read-only so an operator edit isn't silently overwritten by
  // the next scan. Empty when no scanner is configured.
  private _fileManaged: ReadonlySet<string> = new Set();

  constructor(config: ZoryaSkillsConfig) {
    this.registry = config.registry;
    if (config.scan) this.scanConfig = config.scan;
  }

  /** Skill ids that are managed by a file on disk (vs. operator-authored). */
  fileManagedIds(): string[] {
    return [...this._fileManaged];
  }

  async start(): Promise<void> {
    if (this.scanHandle || !this.scanConfig) return;
    const userOnTick = this.scanConfig.onTick;
    this.scanHandle = startSkillScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && { intervalMs: this.scanConfig.intervalMs }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      // Rebuild the file-managed set from each tick's discovered ids
      // (`upserted` = every skill the scan applied this tick, so a
      // deleted file drops out next tick), then fan out to the user hook.
      onTick: (tick) => {
        this._fileManaged = new Set(tick.upserted);
        userOnTick?.(tick);
      },
    });
    // Populate immediately so skills (and their file-managed flags) are
    // available right after boot instead of after the first interval.
    await this.scanHandle.tick();
  }

  async stop(): Promise<void> {
    if (this.scanHandle) {
      this.scanHandle.stop();
      this.scanHandle = undefined;
    }
  }
}
