// ---------------------------------------------------------------------------
// InMemorySignalTokens — the public-bearer signal-token table.
// ---------------------------------------------------------------------------

import type {
  CreateSignalTokenParams,
  MarkSignalTokenCompletedParams,
  MarkSignalTokenCompletedResult,
  SignalTokenRecord,
} from "../storage/signal-store.ts";
import type { WallClock } from "../../shared/wall-clock.ts";

interface MutableSignalToken {
  tokenId: string;
  workflowId: string;
  signalName: string;
  bearer: string;
  tags: string[];
  idempotencyKey: string | null;
  expiresAt: Date;
  completedAt: Date | null;
  completedValue: unknown;
  createdAt: Date;
}

/** A detached copy of a token row: callers can't change the table through it. */
function snapshot(t: MutableSignalToken): SignalTokenRecord {
  return {
    tokenId: t.tokenId,
    workflowId: t.workflowId,
    signalName: t.signalName,
    bearer: t.bearer,
    tags: [...t.tags],
    idempotencyKey: t.idempotencyKey,
    expiresAt: new Date(t.expiresAt.getTime()),
    completedAt: t.completedAt ? new Date(t.completedAt.getTime()) : null,
    completedValue: t.completedValue,
    createdAt: new Date(t.createdAt.getTime()),
  };
}

export class InMemorySignalTokens {
  /** Tokens keyed by tokenId. */
  private readonly tokens = new Map<string, MutableSignalToken>();

  constructor(private readonly clock: WallClock) {}

  create(params: CreateSignalTokenParams): { record: SignalTokenRecord; isCached: boolean } {
    if (params.idempotencyKey) {
      for (const t of this.tokens.values()) {
        if (t.workflowId === params.workflowId && t.idempotencyKey === params.idempotencyKey) {
          return { record: snapshot(t), isCached: true };
        }
      }
    }
    const record: MutableSignalToken = {
      tokenId: params.tokenId,
      workflowId: params.workflowId,
      signalName: params.signalName,
      bearer: params.bearer,
      tags: [...params.tags],
      idempotencyKey: params.idempotencyKey ?? null,
      expiresAt: params.expiresAt,
      completedAt: null,
      completedValue: null,
      createdAt: this.clock.now(),
    };
    this.tokens.set(params.tokenId, record);
    return { record: snapshot(record), isCached: false };
  }

  findById(tokenId: string): SignalTokenRecord | null {
    const t = this.tokens.get(tokenId);
    return t ? snapshot(t) : null;
  }

  markCompleted(params: MarkSignalTokenCompletedParams): MarkSignalTokenCompletedResult {
    const t = this.tokens.get(params.tokenId);
    if (!t) {
      throw new Error(`signal token ${params.tokenId} not found`);
    }
    if (t.completedAt !== null) {
      return { outcome: "already_completed", record: snapshot(t) };
    }
    t.completedAt = params.now;
    t.completedValue = params.value;
    return { outcome: "delivered", record: snapshot(t) };
  }

  /** Every token of one workflow, newest first. */
  listForWorkflow(workflowId: string): SignalTokenRecord[] {
    const out: MutableSignalToken[] = [];
    for (const t of this.tokens.values()) {
      if (t.workflowId === workflowId) out.push(t);
    }
    out.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
    return out.map(snapshot);
  }

  /** Drop every token of one workflow (purge). */
  deleteWorkflow(workflowId: string): void {
    for (const [tokenId, token] of this.tokens) {
      if (token.workflowId === workflowId) this.tokens.delete(tokenId);
    }
  }

  clear(): void {
    this.tokens.clear();
  }
}
