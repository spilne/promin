// ---------------------------------------------------------------------------
// ZoryaFragments — owns the FragmentRegistry + an optional folder scan loop.
// Sibling of ZoryaSkills / ZoryaAgents. Backs the /api/fragments CRUD
// routes, the agent editor's layered-prompt picker (via the existing
// /_catalog/fragments), and the resolver's `fragments` dep.
// ---------------------------------------------------------------------------

import type { FragmentRegistry, FragmentStore } from "@promin/agent";
import { startFragmentScanLoop } from "@promin/agent";

export interface ZoryaFragmentsScanConfig {
  /** Filesystem root to scan for fragment files (one .md per fragment). */
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

export interface ZoryaFragmentsConfig {
  registry: FragmentRegistry;
  /**
   * Durable persistence for operator-authored fragments. When set, the
   * service loads everything from the store into the registry at boot
   * (before the scan loop), and `setFragment` / `deleteFragment` write
   * through to it. File-scanned fragments are NEVER persisted here —
   * the .md file is their source of truth.
   */
  store?: FragmentStore;
  /** Filesystem hot-reload scan loop. Omit to disable. */
  scan?: ZoryaFragmentsScanConfig;
}

export class ZoryaFragments {
  readonly registry: FragmentRegistry;
  readonly store?: FragmentStore;
  private readonly scanConfig?: ZoryaFragmentsScanConfig;
  private scanHandle?: { stop(): void; tick(): Promise<unknown> };
  // Fragment keys currently backed by a file on disk — rebuilt each tick.
  // The manager UI uses this to mark file-managed fragments read-only so
  // an in-place edit isn't silently overwritten by the next scan. Mirrors
  // ZoryaSkills.fileManagedIds().
  private _fileManaged: ReadonlySet<string> = new Set();
  private hydrated = false;

  constructor(config: ZoryaFragmentsConfig) {
    this.registry = config.registry;
    if (config.store) this.store = config.store;
    if (config.scan) this.scanConfig = config.scan;
  }

  /** Fragment keys currently managed by a file on disk. */
  fileManagedIds(): string[] {
    return [...this._fileManaged];
  }

  /**
   * Operator write: update the in-memory registry AND persist to the
   * durable store (if any). Used by the /api/fragments CRUD routes.
   * Scanned fragments do NOT come through this path.
   */
  async setFragment(key: string, content: string): Promise<void> {
    this.registry.set(key, content);
    await this.store?.set(key, content);
  }

  async deleteFragment(key: string): Promise<void> {
    this.registry.delete(key);
    await this.store?.delete(key);
  }

  async start(): Promise<void> {
    // Seed the registry from durable storage BEFORE the scan loop runs, so
    // operator-authored fragments are present at boot. Idempotent across
    // restarts via the in-memory cache flag.
    if (!this.hydrated && this.store) {
      const all = await this.store.loadAll();
      for (const f of all) this.registry.set(f.key, f.content);
      this.hydrated = true;
    }
    if (this.scanHandle || !this.scanConfig) return;
    const userOnTick = this.scanConfig.onTick;
    this.scanHandle = startFragmentScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && { intervalMs: this.scanConfig.intervalMs }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      onTick: (tick) => {
        this._fileManaged = new Set(tick.upserted);
        userOnTick?.(tick);
      },
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
