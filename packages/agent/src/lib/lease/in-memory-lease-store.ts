// ---------------------------------------------------------------------------
// InMemoryLeaseStore — single-process LeaseStore for tests and dev. Same
// contract as PgLeaseStore (which see in @promin/postgres); both pass
// `leaseStoreTestSuite`.
//
// Concurrency: per-call `acquire` / `extend` / `release` are synchronous
// JS — there's only one event-loop turn between read and write — so no
// explicit lock is needed within a single process.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import type {
  AcquireResult,
  ExtendResult,
  LeaseStore,
  ThreadLease,
  ThreadLeaseKey,
} from "./types.ts";

export interface InMemoryLeaseStoreConfig {
  /** Optional clock override for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class InMemoryLeaseStore implements LeaseStore {
  private readonly clock: () => number;
  /** Keyed by `${namespaceId}|${threadId}`. */
  private readonly byKey = new Map<string, ThreadLease>();
  /** leaseId → key, for fast extend/release lookup. */
  private readonly leaseIdToKey = new Map<string, string>();

  constructor(config: InMemoryLeaseStoreConfig = {}) {
    this.clock = config.now ?? (() => Date.now());
  }

  async acquire(params: {
    key: ThreadLeaseKey;
    ownerId: string;
    ttlMs: number;
  }): Promise<AcquireResult> {
    const k = encodeKey(params.key);
    const now = this.clock();
    const existing = this.byKey.get(k);
    if (existing && existing.expiresAt > now) {
      return { acquired: false, currentLease: existing };
    }
    if (existing) {
      // Stealing an expired lease — invalidate the prior lease id so
      // the dead owner's `release()` becomes a no-op.
      this.leaseIdToKey.delete(existing.leaseId);
    }
    const lease: ThreadLease = {
      key: { namespaceId: params.key.namespaceId, threadId: params.key.threadId },
      ownerId: params.ownerId,
      leaseId: randomUUID(),
      acquiredAt: now,
      expiresAt: now + params.ttlMs,
    };
    this.byKey.set(k, lease);
    this.leaseIdToKey.set(lease.leaseId, k);
    return { acquired: true, lease };
  }

  async extend(params: { leaseId: string; additionalMs: number }): Promise<ExtendResult> {
    const k = this.leaseIdToKey.get(params.leaseId);
    if (!k) {
      return { extended: false, currentLease: null };
    }
    const current = this.byKey.get(k);
    if (!current || current.leaseId !== params.leaseId) {
      this.leaseIdToKey.delete(params.leaseId);
      return { extended: false, currentLease: current ?? null };
    }
    const now = this.clock();
    if (current.expiresAt <= now) {
      // Already expired (and possibly stealable) — caller must re-acquire.
      return { extended: false, currentLease: current };
    }
    const updated: ThreadLease = { ...current, expiresAt: current.expiresAt + params.additionalMs };
    this.byKey.set(k, updated);
    return { extended: true, lease: updated };
  }

  async release(params: { leaseId: string }): Promise<void> {
    const k = this.leaseIdToKey.get(params.leaseId);
    if (!k) return;
    const current = this.byKey.get(k);
    if (current && current.leaseId === params.leaseId) {
      this.byKey.delete(k);
    }
    this.leaseIdToKey.delete(params.leaseId);
  }

  async get(key: ThreadLeaseKey): Promise<ThreadLease | null> {
    return this.byKey.get(encodeKey(key)) ?? null;
  }
}

function encodeKey(k: ThreadLeaseKey): string {
  return `${k.namespaceId}|${k.threadId}`;
}
