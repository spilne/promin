import type { WorkflowStorage } from "@promin/workflow";
import type { Logger, WorkflowRetentionConfig } from "../server.ts";

const DEFAULT_INTERVAL_MS = 60 * 60 * 1000;
const DEFAULT_BATCH_SIZE = 500;

/** Runs bounded terminal-run retention sweeps for a server's workflow store. */
export class WorkflowRetentionCleaner {
  private timer?: ReturnType<typeof setInterval>;
  private running = false;

  constructor(
    private readonly storage: WorkflowStorage,
    private readonly config: WorkflowRetentionConfig,
    private readonly logger: Logger,
  ) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(
      () => void this.sweep(),
      this.config.intervalMs ?? DEFAULT_INTERVAL_MS,
    );
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  private async sweep(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const deleted = await this.storage.purgeCompleted({
        olderThanMs: this.config.maxAgeDays * 24 * 60 * 60 * 1000,
        limit: this.config.batchSize ?? DEFAULT_BATCH_SIZE,
      });
      if (deleted > 0) {
        this.logger.log(`[zorya] retention: purged ${deleted} completed workflow runs`);
      }
    } catch (error) {
      this.logger.warn("[zorya] retention sweep failed:", error);
    } finally {
      this.running = false;
    }
  }
}

export function validateWorkflowRetentionConfig(config: WorkflowRetentionConfig): void {
  if (!Number.isFinite(config.maxAgeDays) || config.maxAgeDays <= 0) {
    throw new Error("ZoryaServer retention.maxAgeDays must be a positive finite number");
  }
  if (
    config.intervalMs !== undefined &&
    (!Number.isFinite(config.intervalMs) || config.intervalMs <= 0)
  ) {
    throw new Error("ZoryaServer retention.intervalMs must be a positive finite number");
  }
  if (
    config.batchSize !== undefined &&
    (!Number.isInteger(config.batchSize) || config.batchSize <= 0)
  ) {
    throw new Error("ZoryaServer retention.batchSize must be a positive integer");
  }
}
