// ---------------------------------------------------------------------------
// ZoryaFragments — owns the FragmentRegistry + an optional folder scan loop.
// Sibling of ZoryaSkills / ZoryaAgents. Backs the /api/fragments CRUD
// routes, the agent editor's layered-prompt picker (via the existing
// /_catalog/fragments), and the resolver's `fragments` dep.
//
// Scanned and operator-authored fragments share the one durable registry
// (every replica reads the same rows); scanned versions carry
// `source: "file"`, and scan sync never removes an operator's fragment.
// ---------------------------------------------------------------------------

import type { FragmentRegistry, ScanLoopHandle, ScanLoopTick } from "@promin/agent";
import { FILE_SOURCE, listAllVersioned, startFragmentScanLoop } from "@promin/agent";

export interface ZoryaFragmentsScanConfig {
  /** Filesystem root to scan for fragment files (one .md per fragment). */
  root: string;
  /** Poll interval in ms. Default 5000. */
  intervalMs?: number;
  /** Remove scanner-written fragments whose file is gone. Default false. */
  sync?: boolean;
  /** Fired after each scan tick. */
  onTick?: (tick: ScanLoopTick) => void;
  /** A failed scan tick lands here. Default: console. */
  onError?: (err: unknown) => void;
}

export interface ZoryaFragmentsConfig {
  registry: FragmentRegistry;
  /** Filesystem hot-reload scan loop. Omit to disable. */
  scan?: ZoryaFragmentsScanConfig;
}

export class ZoryaFragments {
  readonly registry: FragmentRegistry;
  private readonly scanConfig?: ZoryaFragmentsScanConfig;
  private scanHandle?: ScanLoopHandle<ScanLoopTick>;

  constructor(config: ZoryaFragmentsConfig) {
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
    this.scanHandle = startFragmentScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && { intervalMs: this.scanConfig.intervalMs }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      ...(this.scanConfig.onError !== undefined && { onError: this.scanConfig.onError }),
      ...(userOnTick !== undefined && { onTick: userOnTick }),
    });
    // Immediate tick so fragments + flags are available at boot, not after
    // the first interval. Mirrors ZoryaSkills.start().
    await this.scanHandle.tick();
  }

  async stop(): Promise<void> {
    if (this.scanHandle) {
      this.scanHandle.stop();
      this.scanHandle = undefined;
    }
  }
}
