// ---------------------------------------------------------------------------
// SignalStore and SignalTokenStore — signal delivery and the public-bearer
// tokens that authorize it.
// ---------------------------------------------------------------------------

import type { SignalState } from "../workflow-state.ts";

/** Params of `deliverSignal`. */
export interface DeliverSignalParams {
  readonly workflowId: string;
  readonly signalName: string;
  readonly payload: unknown;
}

/** Delivered signals of a run. */
export interface SignalStore {
  /**
   * Deliver a signal to a workflow. Signals are scoped to the current run
   * and keyed by name: a second delivery under the same name replaces the
   * first (last delivery wins), and `startFreshRun` drops every delivered
   * signal so a new run never sees the previous run's deliveries.
   *
   * A delivery is a value, not a message: reading it never consumes it.
   * It stays visible until a later delivery under the same name replaces
   * it or a fresh run drops it — so a delivery that lands before its
   * waiter suspends still satisfies the wait, and a wait that runs again
   * (after `resetSteps`) sees the latest delivery. Child-ended wakeups
   * (`workflow.child-ended:<id>#<run>`) rely on this: the child may end
   * before its parent has parked. A workflow that needs one wake per
   * message uses a distinct signal name per message, or a stream.
   *
   * Unfenced: external senders hold no lock.
   */
  deliverSignal(params: DeliverSignalParams): Promise<void>;

  /**
   * Load the signals delivered to the current run — at most one per name,
   * the latest delivery. Reading does not consume them.
   */
  loadSignals(workflowId: string): Promise<SignalState[]>;
}

/**
 * A single row from the public-bearer signal-token table. Issued via
 * `createSignalToken`, consumed via the public completion endpoint, and
 * surfaced in the dashboard via `listSignalTokensForWorkflow`.
 *
 * `bearer` is the plaintext credential — short-lived, single-use, bounded
 * by `expiresAt`. Compared with constant-time equality at completion time.
 */
export interface SignalTokenRecord {
  readonly tokenId: string;
  readonly workflowId: string;
  readonly signalName: string;
  readonly bearer: string;
  readonly tags: ReadonlyArray<string>;
  readonly idempotencyKey: string | null;
  readonly expiresAt: Date;
  readonly completedAt: Date | null;
  readonly completedValue: unknown;
  readonly createdAt: Date;
}

/** Params of `createSignalToken`. */
export interface CreateSignalTokenParams {
  readonly tokenId: string;
  readonly workflowId: string;
  readonly signalName: string;
  readonly bearer: string;
  readonly tags: ReadonlyArray<string>;
  readonly idempotencyKey?: string | null;
  readonly expiresAt: Date;
}

/** Params of `completeSignalToken`. */
export interface CompleteSignalTokenParams {
  readonly tokenId: string;
  /** Recorded on the token and delivered as the signal's payload. */
  readonly value: unknown;
  /** Recorded as the token's `completedAt`. */
  readonly now: Date;
}

/** Answer of `completeSignalToken`. */
export type CompleteSignalTokenResult =
  | { readonly outcome: "delivered"; readonly record: SignalTokenRecord }
  | { readonly outcome: "already_completed"; readonly record: SignalTokenRecord };

/**
 * Signal tokens — public-bearer authorization for `deliverSignal`.
 *
 * A signal token grants one-shot delivery rights to an external completer
 * (no Zorya auth) for a specific (workflowId, signalName). The completion
 * route validates the bearer, then `completeSignalToken` consumes the token
 * and delivers its signal in one write, so the workflow resumes through the
 * existing signal path. Tokens don't change suspend semantics — they're an
 * authz sidecar, not a new primitive. Tokens are workflow-scoped: they
 * survive `startFreshRun`.
 */
export interface SignalTokenStore {
  /**
   * Insert a signal token or return the existing row when an idempotency
   * key matches. `isCached: true` indicates the caller hit a dedup —
   * the original `(tokenId, bearer)` pair is reused so retries see the
   * same credentials.
   */
  createSignalToken(
    params: CreateSignalTokenParams,
  ): Promise<{ readonly record: SignalTokenRecord; readonly isCached: boolean }>;

  /** Lookup by token id — used by the public completion endpoint. */
  findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null>;

  /**
   * Consume a pending token and deliver its signal, as one atomic write:
   * the token is marked completed (`completedAt` + `completedValue`) and
   * `value` is delivered to the token's `(workflowId, signalName)` exactly
   * as `deliverSignal` would. Either both land or neither does, so a
   * failed call leaves the token pending and completable again.
   *
   * Of concurrent completions of one token exactly one answers
   * `delivered`; the others answer `already_completed` with the winning
   * record and deliver nothing. A token completed earlier answers
   * `already_completed` too. Throws for an unknown token id.
   */
  completeSignalToken(params: CompleteSignalTokenParams): Promise<CompleteSignalTokenResult>;

  /**
   * List every token issued for one workflow — drives
   * `runs.retrieve(workflowId).signalTokens[]` in the dashboard.
   * Ordered by `createdAt DESC`.
   */
  listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>>;
}
