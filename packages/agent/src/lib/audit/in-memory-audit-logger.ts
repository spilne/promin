// ---------------------------------------------------------------------------
// InMemoryAuditLogger — in-process AuditLogger.
//
// The default impl for tests and single-process deployments. Keeps every
// recorded entry in memory; swap a durable backend in for multi-process /
// compliance use.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import type { AuditEntry, AuditLogger, AuditRecord } from "./types.ts";

export interface InMemoryAuditLoggerConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemoryAuditLogger implements AuditLogger {
  private readonly clock: Clock;
  private readonly entries: AuditRecord[] = [];

  constructor(config: InMemoryAuditLoggerConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  async record(entry: AuditEntry): Promise<void> {
    this.entries.push({ ...entry, timestamp: this.clock.currentTimeMs() });
  }

  /** All recorded entries, oldest first. Inspection / test affordance. */
  list(): ReadonlyArray<AuditRecord> {
    return this.entries.slice();
  }
}
