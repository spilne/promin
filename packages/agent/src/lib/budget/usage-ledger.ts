// ---------------------------------------------------------------------------
// UsageLedger — durable record of what each turn spent, for thread and
// namespace budgets.
//
// One row per turn, keyed by the turn's workflow id: a turn records its
// spend (delegated sub-agents included) when it commits or fails. A
// turn's spend only grows, so a record of a turn already recorded keeps
// the larger value per field: a replay or retry records nothing new, and
// a turn that failed and was then run on records what it added. Thread
// totals sum a thread's rows over all time; namespace windows sum a
// namespace's rows recorded within the last `windowMs`.
//
// Retention: rows are kept. Thread totals need every row of the thread;
// a host that wants to bound the table deletes rows older than its
// largest namespace window for threads it no longer budgets (or folds
// them into an aggregate of its own).
//
// Implementations: `InMemoryUsageLedger` (here), `SqliteUsageLedger`,
// `PostgresUsageLedger` — all pass `usageLedgerTestSuite`. Windows are
// measured on the store's clock: the injected `WallClock` for in-memory
// and SQLite, the database's `NOW()` for Postgres, which stamps rows too.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import { addUsage, EMPTY_USAGE, type BudgetUsage } from "./budget.ts";

/** The larger value of each field. */
export function maxUsage(a: BudgetUsage, b: BudgetUsage): BudgetUsage {
  return {
    inputTokens: Math.max(a.inputTokens, b.inputTokens),
    outputTokens: Math.max(a.outputTokens, b.outputTokens),
    cacheReadTokens: Math.max(a.cacheReadTokens, b.cacheReadTokens),
    cacheWriteTokens: Math.max(a.cacheWriteTokens, b.cacheWriteTokens),
    costUsd: Math.max(a.costUsd, b.costUsd),
    llmCalls: Math.max(a.llmCalls, b.llmCalls),
    toolCalls: Math.max(a.toolCalls, b.toolCalls),
  };
}

/** What one turn spent. */
export interface UsageEntry {
  /** The turn's workflow id: the row's idempotency key. */
  readonly turnId: string;
  readonly namespaceId: string;
  readonly threadId?: string;
  readonly agentId?: string;
  readonly usage: BudgetUsage;
}

export interface UsageTotalQuery {
  readonly namespaceId: string;
  /** Only this thread's turns. */
  readonly threadId?: string;
  /** Only turns recorded within the last `windowMs` ms. Unset: all time. */
  readonly windowMs?: number;
}

export interface UsageLedger {
  /**
   * Record a turn's spend. Atomic per `(namespaceId, turnId)`: concurrent
   * records of different turns all count; a record of a turn already
   * recorded keeps the larger value of each field (a turn's spend only
   * grows), so recording the same spend again changes nothing. Resolves
   * `true` when this call created the turn's row.
   */
  record(entry: UsageEntry): Promise<boolean>;
  /** Sum of the matching turns' spend. */
  total(query: UsageTotalQuery): Promise<BudgetUsage>;
}

export interface InMemoryUsageLedgerConfig {
  /** Time source for row stamps and windows. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface Row {
  readonly entry: UsageEntry;
  readonly recordedAt: number;
}

/** Process-local `UsageLedger`, for tests and single-process hosts. */
export class InMemoryUsageLedger implements UsageLedger {
  private readonly clock: WallClock;
  private readonly rows = new Map<string, Row>();

  constructor(config: InMemoryUsageLedgerConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async record(entry: UsageEntry): Promise<boolean> {
    const key = JSON.stringify([entry.namespaceId, entry.turnId]);
    const existing = this.rows.get(key);
    if (existing) {
      this.rows.set(key, {
        ...existing,
        entry: { ...existing.entry, usage: maxUsage(existing.entry.usage, entry.usage) },
      });
      return false;
    }
    this.rows.set(key, { entry, recordedAt: this.clock.currentTimeMs() });
    return true;
  }

  async total(query: UsageTotalQuery): Promise<BudgetUsage> {
    const since =
      query.windowMs === undefined ? undefined : this.clock.currentTimeMs() - query.windowMs;
    let total = EMPTY_USAGE;
    for (const { entry, recordedAt } of this.rows.values()) {
      if (entry.namespaceId !== query.namespaceId) continue;
      if (query.threadId !== undefined && entry.threadId !== query.threadId) continue;
      if (since !== undefined && recordedAt <= since) continue;
      total = addUsage(total, entry.usage);
    }
    return total;
  }
}
