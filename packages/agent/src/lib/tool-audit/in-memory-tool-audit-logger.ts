// ---------------------------------------------------------------------------
// InMemoryToolAuditLogger — in-process ToolAuditLogger.
//
// The default impl for tests and single-process deployments. Keeps every
// recorded entry in memory; swap a durable backend in for multi-process /
// compliance use.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import type { ToolAuditEntry, ToolAuditLogger, ToolAuditRecord } from "./types.ts";

export interface InMemoryToolAuditLoggerConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemoryToolAuditLogger implements ToolAuditLogger {
  private readonly clock: Clock;
  private readonly entries: ToolAuditRecord[] = [];

  constructor(config: InMemoryToolAuditLoggerConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  async record(entry: ToolAuditEntry): Promise<void> {
    this.entries.push({ ...entry, timestamp: this.clock.currentTimeMs() });
  }

  /** All recorded entries, oldest first. Inspection / test affordance. */
  list(): ReadonlyArray<ToolAuditRecord> {
    return this.entries.slice();
  }
}
