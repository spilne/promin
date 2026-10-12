// ---------------------------------------------------------------------------
// PostgresUsageLedger — durable `UsageLedger` shared by every replica: one
// row per turn in `agent_usage_ledger`, keyed by (namespace, turn workflow
// id).
//
// `record` is one `INSERT … ON CONFLICT DO UPDATE` keeping the larger value
// of each field (`GREATEST`): a retried record of a turn changes nothing and
// concurrent records of different turns all land, with no read-modify-write
// to race. Rows are stamped by the database
// (`recorded_at DEFAULT NOW()`) and windows compare against `NOW()` too, so
// every replica agrees on a window whatever its own clock says.
// ---------------------------------------------------------------------------

import { and, eq, sql } from "drizzle-orm";
import type { BudgetUsage, UsageEntry, UsageLedger, UsageTotalQuery } from "@promin/agent";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { agentUsageLedger } from "../schema.ts";

export interface PostgresUsageLedgerConfig {
  readonly db: DrizzleDb;
}

export class PostgresUsageLedger implements UsageLedger {
  private readonly db: DrizzleDb;

  constructor(config: PostgresUsageLedgerConfig) {
    this.db = config.db;
  }

  async record(entry: UsageEntry): Promise<boolean> {
    const u = entry.usage;
    const inserted = await this.db
      .insert(agentUsageLedger)
      .values({
        namespaceId: entry.namespaceId,
        turnId: entry.turnId,
        threadId: entry.threadId ?? null,
        agentId: entry.agentId ?? null,
        inputTokens: u.inputTokens,
        outputTokens: u.outputTokens,
        cacheReadTokens: u.cacheReadTokens,
        cacheWriteTokens: u.cacheWriteTokens,
        costUsd: u.costUsd,
        llmCalls: u.llmCalls,
        toolCalls: u.toolCalls,
      })
      .onConflictDoUpdate({
        target: [agentUsageLedger.namespaceId, agentUsageLedger.turnId],
        set: {
          inputTokens: sql`GREATEST(${agentUsageLedger.inputTokens}, excluded.input_tokens)`,
          outputTokens: sql`GREATEST(${agentUsageLedger.outputTokens}, excluded.output_tokens)`,
          cacheReadTokens: sql`GREATEST(${agentUsageLedger.cacheReadTokens}, excluded.cache_read_tokens)`,
          cacheWriteTokens: sql`GREATEST(${agentUsageLedger.cacheWriteTokens}, excluded.cache_write_tokens)`,
          costUsd: sql`GREATEST(${agentUsageLedger.costUsd}, excluded.cost_usd)`,
          llmCalls: sql`GREATEST(${agentUsageLedger.llmCalls}, excluded.llm_calls)`,
          toolCalls: sql`GREATEST(${agentUsageLedger.toolCalls}, excluded.tool_calls)`,
        },
      })
      // `xmax = 0` only on a freshly inserted row.
      .returning({ inserted: sql<boolean>`(xmax = 0)` });
    return inserted[0]?.inserted === true;
  }

  async total(query: UsageTotalQuery): Promise<BudgetUsage> {
    const conditions = [eq(agentUsageLedger.namespaceId, query.namespaceId)];
    if (query.threadId !== undefined) {
      conditions.push(eq(agentUsageLedger.threadId, query.threadId));
    }
    if (query.windowMs !== undefined) {
      conditions.push(
        sql`${agentUsageLedger.recordedAt} > NOW() - (${query.windowMs} * INTERVAL '1 millisecond')`,
      );
    }
    const [row] = await this.db
      .select({
        inputTokens: sql<string | null>`SUM(${agentUsageLedger.inputTokens})`,
        outputTokens: sql<string | null>`SUM(${agentUsageLedger.outputTokens})`,
        cacheReadTokens: sql<string | null>`SUM(${agentUsageLedger.cacheReadTokens})`,
        cacheWriteTokens: sql<string | null>`SUM(${agentUsageLedger.cacheWriteTokens})`,
        costUsd: sql<string | null>`SUM(${agentUsageLedger.costUsd})`,
        llmCalls: sql<string | null>`SUM(${agentUsageLedger.llmCalls})`,
        toolCalls: sql<string | null>`SUM(${agentUsageLedger.toolCalls})`,
      })
      .from(agentUsageLedger)
      .where(and(...conditions));
    // SUM over bigint / integer comes back as numeric (a string); NULL when no rows.
    const num = (v: string | number | null | undefined): number => (v == null ? 0 : Number(v));
    return {
      inputTokens: num(row?.inputTokens),
      outputTokens: num(row?.outputTokens),
      cacheReadTokens: num(row?.cacheReadTokens),
      cacheWriteTokens: num(row?.cacheWriteTokens),
      costUsd: num(row?.costUsd),
      llmCalls: num(row?.llmCalls),
      toolCalls: num(row?.toolCalls),
    };
  }
}
