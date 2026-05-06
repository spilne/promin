// ---------------------------------------------------------------------------
// AgentTurnGate — high-level API on top of LeaseStore. Wraps
// acquire / extend / release into a single `run({ policy, ... })` call.
//
// Two policies:
//
//   strict  — try-acquire once. Held → throw `TurnInProgress`. Caller
//             surfaces 409 with the current owner. Matches OpenAI
//             Assistants API semantics. Implemented in Phase 1.
//
//   queued  — wait for the lease to free, then run. Tracked separately
//             (xywa Phase 3); throws `NotImplementedError` today. Two
//             possible impls (held HTTP connection with backoff, or
//             async dispatch through pg-step-queue) — pick when the
//             ticket is picked up.
//
// Failures inside the gated callback are propagated unchanged AFTER
// release; the lease is always released, even on error, so a crashed
// turn doesn't leave a stale lease past its TTL window. (Worker-process
// crashes are handled by lease expiry on the store side.)
// ---------------------------------------------------------------------------

import type { LeaseStore, ThreadLease, ThreadLeaseKey } from "./types.ts";

export type AgentTurnPolicy = "strict" | "queued";

/**
 * Default lease TTL when caller doesn't specify. 5 minutes is a
 * compromise: long enough to cover most agent turns (LLM call +
 * tools + memory writes), short enough to recover quickly from a
 * crashed worker. Long-running turns should call `lease.extend()`
 * before they hit this window.
 */
export const DEFAULT_TURN_LEASE_TTL_MS = 5 * 60 * 1000;

export class TurnInProgressError extends Error {
  readonly _tag = "TurnInProgress";
  readonly currentLease: ThreadLease;
  constructor(currentLease: ThreadLease) {
    super(
      `Thread ${currentLease.key.namespaceId}/${currentLease.key.threadId} ` +
        `is already being processed by ${currentLease.ownerId} ` +
        `(lease expires at ${new Date(currentLease.expiresAt).toISOString()}).`,
    );
    this.currentLease = currentLease;
    this.name = "TurnInProgressError";
  }
}

export class QueuedPolicyNotImplementedError extends Error {
  readonly _tag = "QueuedPolicyNotImplemented";
  constructor() {
    super(
      "AgentTurnGate policy 'queued' is not yet implemented. " +
        "Tracked as xywa Phase 3 — see promin-872n. Use policy 'strict' " +
        "or fall back to single-replica deployment for now.",
    );
    this.name = "QueuedPolicyNotImplementedError";
  }
}

export interface AgentTurnGateConfig {
  readonly leaseStore: LeaseStore;
  /** Default TTL when caller doesn't specify. Default: 5 minutes. */
  readonly defaultTtlMs?: number;
}

export interface RunParams<T> {
  readonly key: ThreadLeaseKey;
  readonly ownerId: string;
  /** Per-call TTL override (rare — most callers use the gate default). */
  readonly ttlMs?: number;
  readonly policy: AgentTurnPolicy;
  /**
   * The actual turn body. Receives a `lease` handle so long-running
   * bodies can call `lease.extend()` to push out the TTL.
   */
  readonly run: (lease: AgentTurnLease) => Promise<T>;
}

/**
 * Lease handle handed to the gated callback. Mirrors `ThreadLease` plus
 * a bound `extend()` shortcut so callers don't have to re-pass leaseId.
 */
export interface AgentTurnLease extends ThreadLease {
  /** Push out expiresAt by `additionalMs`. Returns the new lease. */
  extend(additionalMs: number): Promise<ThreadLease>;
}

export class AgentTurnGate {
  private readonly leaseStore: LeaseStore;
  private readonly defaultTtlMs: number;

  constructor(config: AgentTurnGateConfig) {
    this.leaseStore = config.leaseStore;
    this.defaultTtlMs = config.defaultTtlMs ?? DEFAULT_TURN_LEASE_TTL_MS;
  }

  async run<T>(params: RunParams<T>): Promise<T> {
    if (params.policy === "queued") {
      throw new QueuedPolicyNotImplementedError();
    }
    return this.runStrict(params);
  }

  private async runStrict<T>(params: RunParams<T>): Promise<T> {
    const ttlMs = params.ttlMs ?? this.defaultTtlMs;
    const result = await this.leaseStore.acquire({
      key: params.key,
      ownerId: params.ownerId,
      ttlMs,
    });
    if (!result.acquired) {
      throw new TurnInProgressError(result.currentLease);
    }
    const lease = result.lease;
    const handle: AgentTurnLease = {
      ...lease,
      extend: async (additionalMs) => {
        const r = await this.leaseStore.extend({ leaseId: lease.leaseId, additionalMs });
        if (!r.extended) {
          throw new Error(
            `Failed to extend lease for ${lease.key.namespaceId}/${lease.key.threadId} ` +
              `— either expired or stolen. Caller should treat the turn as aborted.`,
          );
        }
        return r.lease;
      },
    };
    try {
      return await params.run(handle);
    } finally {
      // Best-effort release; if it fails (network, etc.), the lease will
      // expire on its own at expiresAt. We don't surface release errors.
      await this.leaseStore.release({ leaseId: lease.leaseId }).catch(() => {});
    }
  }
}
