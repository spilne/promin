// ---------------------------------------------------------------------------
// InMemoryToolAuditLogger — in-process ToolAuditLogger.
//
// The default impl for tests and single-process deployments. Keeps every
// recorded entry in memory; swap a durable backend in for multi-process /
// compliance use.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { ToolAuditEntry, ToolAuditLogger, ToolAuditRecord } from "./types.ts";

export interface InMemoryToolAuditLoggerConfig {
  /** Time source. Default: `SystemWallClock`. Tests pass a `FakeWallClock`. */
  readonly clock?: WallClock;
}

export class InMemoryToolAuditLogger implements ToolAuditLogger {
  private readonly clock: WallClock;
  private readonly entries: ToolAuditRecord[] = [];

  constructor(config: InMemoryToolAuditLoggerConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async record(entry: ToolAuditEntry): Promise<void> {
    this.entries.push({ ...entry, timestamp: this.clock.currentTimeMs() });
  }

  /** All recorded entries, oldest first. Inspection / test affordance. */
  list(): ReadonlyArray<ToolAuditRecord> {
    return this.entries.slice();
  }
}
