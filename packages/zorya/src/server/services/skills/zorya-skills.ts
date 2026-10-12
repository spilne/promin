// ---------------------------------------------------------------------------
// ZoryaSkills — owns the SkillRegistry + an optional folder scan loop.
// Sibling of ZoryaAgents. The server config expects this object when skills
// functionality is enabled; it backs the /api/skills CRUD routes, the
// /api/agents/_catalog/skills picker source, and (via its `registry`) the
// agent resolver's catalog injection.
// ---------------------------------------------------------------------------

import type { ScanLoopHandle, ScanLoopTick, SkillRegistry } from "@promin/agent";
import { FILE_SOURCE, listAllVersioned, startSkillScanLoop } from "@promin/agent";

export interface ZoryaSkillsScanConfig {
  /** Filesystem root to scan for skill manifests (.ts modules or SKILL.md). */
  root: string;
  /** Poll interval in ms. Default 5000. */
  intervalMs?: number;
  /** Remove scanner-written entries whose file is gone (never operator-authored ones). Default false. */
  sync?: boolean;
  /**
   * The operator vouches for every skill under `root` (first-party skills
   * in the host's own repo): they register as trusted. Default false —
   * new and edited skills need an operator's review before agents see them.
   */
  trusted?: boolean;
  /** Fired after each scan tick. */
  onTick?: (tick: ScanLoopTick) => void;
  /** A failed scan tick lands here. Default: console. */
  onError?: (err: unknown) => void;
}

export interface ZoryaSkillsConfig {
  registry: SkillRegistry;
  /** Filesystem hot-reload scan loop. Omit to disable. */
  scan?: ZoryaSkillsScanConfig;
}

export class ZoryaSkills {
  readonly registry: SkillRegistry;
  private readonly scanConfig?: ZoryaSkillsScanConfig;
  private scanHandle?: ScanLoopHandle<ScanLoopTick>;

  constructor(config: ZoryaSkillsConfig) {
    this.registry = config.registry;
    if (config.scan) this.scanConfig = config.scan;
  }

  /**
   * Ids whose active version a file scan wrote. Read from the registry, so
   * every replica agrees from boot.
   */
  async fileManagedIds(): Promise<string[]> {
    const managed = await listAllVersioned({ registry: this.registry, source: FILE_SOURCE });
    return managed.map((e) => e.id);
  }

  async start(): Promise<void> {
    if (this.scanHandle || !this.scanConfig) return;
    const userOnTick = this.scanConfig.onTick;
    this.scanHandle = startSkillScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && { intervalMs: this.scanConfig.intervalMs }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      ...(this.scanConfig.onError !== undefined && { onError: this.scanConfig.onError }),
      ...(this.scanConfig.trusted !== undefined && { trusted: this.scanConfig.trusted }),
      ...(userOnTick !== undefined && { onTick: userOnTick }),
    });
    // Populate immediately so scanned skills are available right after boot
    // instead of after the first interval.
    await this.scanHandle.tick();
  }

  async stop(): Promise<void> {
    if (this.scanHandle) {
      this.scanHandle.stop();
      this.scanHandle = undefined;
    }
  }
}
