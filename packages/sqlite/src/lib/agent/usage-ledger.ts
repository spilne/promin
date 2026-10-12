// ---------------------------------------------------------------------------
// SqliteUsageLedger — durable `UsageLedger` for embedded / single-process
// deployments: one row per turn, keyed by (namespace, turn workflow id).
//
// `record` is one transaction around an upsert that keeps the larger value
// of each field, so a retried record of a turn changes nothing and
// concurrent records never lose a row. Rows are
// stamped with the injected `WallClock` (default `SystemWallClock`), which
// also bounds windows: SQLite is in-process, so the app clock is the
// database clock.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { BudgetUsage, UsageEntry, UsageLedger, UsageTotalQuery } from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteUsageLedgerConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `agent_usage_ledger`). */
  readonly table?: string;
  /** Time source for row stamps and windows. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface TotalRow {
  input_tokens: number | null;
  output_tokens: number | null;
  cache_read_tokens: number | null;
  cache_write_tokens: number | null;
  cost_usd: number | null;
  llm_calls: number | null;
  tool_calls: number | null;
}

export class SqliteUsageLedger implements UsageLedger {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: WallClock;

  constructor(config: SqliteUsageLedgerConfig) {
    this.db = config.db;
    this.table = config.table ?? "agent_usage_ledger";
    this.clock = config.clock ?? SystemWallClock;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        namespace_id       TEXT NOT NULL,
        turn_id            TEXT NOT NULL,
        thread_id          TEXT,
        agent_id           TEXT,
        input_tokens       INTEGER NOT NULL,
        output_tokens      INTEGER NOT NULL,
        cache_read_tokens  INTEGER NOT NULL,
        cache_write_tokens INTEGER NOT NULL,
        cost_usd           REAL NOT NULL,
        llm_calls          INTEGER NOT NULL,
        tool_calls         INTEGER NOT NULL,
        recorded_at        INTEGER NOT NULL,
        PRIMARY KEY (namespace_id, turn_id)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_window_idx ON ${this.table} (namespace_id, recorded_at)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_thread_idx ON ${this.table} (namespace_id, thread_id)`,
    );
  }

  async record(entry: UsageEntry): Promise<boolean> {
    const u = entry.usage;
    const t = this.table;
    return this.db.transaction(() => {
      const existed =
        this.db
          .query(`SELECT 1 AS one FROM ${t} WHERE namespace_id = ? AND turn_id = ?`)
          .get(entry.namespaceId, entry.turnId) !== null;
      this.db
        .query(
          `INSERT INTO ${t}
             (namespace_id, turn_id, thread_id, agent_id, input_tokens, output_tokens,
              cache_read_tokens, cache_write_tokens, cost_usd, llm_calls, tool_calls, recorded_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (namespace_id, turn_id) DO UPDATE SET
             input_tokens = MAX(input_tokens, excluded.input_tokens),
             output_tokens = MAX(output_tokens, excluded.output_tokens),
             cache_read_tokens = MAX(cache_read_tokens, excluded.cache_read_tokens),
             cache_write_tokens = MAX(cache_write_tokens, excluded.cache_write_tokens),
             cost_usd = MAX(cost_usd, excluded.cost_usd),
             llm_calls = MAX(llm_calls, excluded.llm_calls),
             tool_calls = MAX(tool_calls, excluded.tool_calls)`,
        )
        .run(
          entry.namespaceId,
          entry.turnId,
          entry.threadId ?? null,
          entry.agentId ?? null,
          u.inputTokens,
          u.outputTokens,
          u.cacheReadTokens,
          u.cacheWriteTokens,
          u.costUsd,
          u.llmCalls,
          u.toolCalls,
          this.clock.currentTimeMs(),
        );
      return !existed;
    })();
  }

  async total(query: UsageTotalQuery): Promise<BudgetUsage> {
    const where = ["namespace_id = ?"];
    const params: Array<string | number> = [query.namespaceId];
    if (query.threadId !== undefined) {
      where.push("thread_id = ?");
      params.push(query.threadId);
    }
    if (query.windowMs !== undefined) {
      where.push("recorded_at > ?");
      params.push(this.clock.currentTimeMs() - query.windowMs);
    }
    const row = this.db
      .query<TotalRow>(
        `SELECT SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens,
                SUM(cache_read_tokens) AS cache_read_tokens,
                SUM(cache_write_tokens) AS cache_write_tokens, SUM(cost_usd) AS cost_usd,
                SUM(llm_calls) AS llm_calls, SUM(tool_calls) AS tool_calls
           FROM ${this.table} WHERE ${where.join(" AND ")}`,
      )
      .get(...params);
    return {
      inputTokens: row?.input_tokens ?? 0,
      outputTokens: row?.output_tokens ?? 0,
      cacheReadTokens: row?.cache_read_tokens ?? 0,
      cacheWriteTokens: row?.cache_write_tokens ?? 0,
      costUsd: row?.cost_usd ?? 0,
      llmCalls: row?.llm_calls ?? 0,
      toolCalls: row?.tool_calls ?? 0,
    };
  }
}
