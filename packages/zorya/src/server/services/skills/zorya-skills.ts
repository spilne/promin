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
  private scanHandle?: { stop(): void };

  constructor(config: ZoryaSkillsConfig) {
    this.registry = config.registry;
    if (config.scan) this.scanConfig = config.scan;
  }

  async start(): Promise<void> {
    if (this.scanHandle || !this.scanConfig) return;
    this.scanHandle = startSkillScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && { intervalMs: this.scanConfig.intervalMs }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      ...(this.scanConfig.onTick !== undefined && { onTick: this.scanConfig.onTick }),
    });
  }

  async stop(): Promise<void> {
    if (this.scanHandle) {
      this.scanHandle.stop();
      this.scanHandle = undefined;
    }
  }
}
