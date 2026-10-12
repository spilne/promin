// ---------------------------------------------------------------------------
// RateLimitedConsolidator — decorator that caps `distillThread` cost.
//
// Distillation is one or more LLM calls per fire. With autoDistill on, a
// busy resource can rack up dozens of distill calls in a short window. The
// rate limiter caps them per (namespace, resource) and, optionally, per
// namespace, throwing `ConsolidatorRateLimitError` when a cap is hit.
//
// The count lives in the memory store (`MemoryStore.reserveConsolidation`):
// counting and recording a run is one atomic step, so concurrent distills
// — on one process or many sharing a store — never overshoot the cap. A
// run that fails gives its slot back; a call that would be a no-op (the
// thread is already distilled and `force` is off) takes no slot.
//
// Wraps ALL distill paths (auto-trigger + manual gateway + scripts) by
// design — capping only auto leaves a hole.
// ---------------------------------------------------------------------------

import {
  sweepResourceThreads,
  type Consolidator,
  type DistillResourceOptions,
  type DistillThreadOptions,
} from "./consolidator.ts";
import {
  DISTILL_EPISODE_KIND,
  type EpisodicRecord,
  type MemoryStore,
  type ScopedKey,
  type ThreadKey,
} from "./types.ts";

export interface ConsolidatorRateLimitConfig {
  /**
   * Time window in milliseconds. Distill calls within the trailing
   * `windowMs` count toward `maxDistillsPerWindow`. Common: 3_600_000
   * (1 hour), 86_400_000 (1 day).
   */
  readonly windowMs: number;
  /**
   * Maximum distill calls allowed per (namespace, resource) within the
   * window. Once reached, further distill calls throw
   * `ConsolidatorRateLimitError` until older ones age out.
   */
  readonly maxDistillsPerWindow: number;
  /**
   * Optional second cap, applied across ALL resources in a namespace
   * within the same window. The per-resource cap protects per-user
   * cost; this protects per-tenant cost — a namespace with hundreds of
   * resources, each just under its own cap, can still rack up unbounded
   * distill spend. When set, BOTH caps apply; whichever trips first
   * throws (`scope.kind` tells the caller which).
   */
  readonly maxDistillsPerNamespacePerWindow?: number;
}

/**
 * Default distill rate limit of a `LocalAgent` with memory: 30 distills per
 * user and 1 000 per namespace per hour.
 */
export const DEFAULT_DISTILL_RATE_LIMIT: ConsolidatorRateLimitConfig = {
  windowMs: 3_600_000,
  maxDistillsPerWindow: 30,
  maxDistillsPerNamespacePerWindow: 1_000,
};

/**
 * Which cap a `ConsolidatorRateLimitError` tripped — lets a gateway
 * distinguish per-user (`resource`) from per-tenant (`namespace`)
 * throttling in the 429 body.
 */
export type ConsolidatorRateLimitScope =
  | { readonly kind: "resource"; readonly namespaceId: string; readonly resourceId: string }
  | { readonly kind: "namespace"; readonly namespaceId: string };

export class ConsolidatorRateLimitError extends Error {
  readonly kind = "distill" as const;
  readonly scope: ConsolidatorRateLimitScope;
  readonly windowMs: number;
  readonly max: number;
  readonly seen: number;
  /** Time until the oldest counted distill leaves the window, in ms. */
  readonly retryAfterMs: number;

  constructor(args: {
    scope: ConsolidatorRateLimitScope;
    windowMs: number;
    max: number;
    seen: number;
    retryAfterMs: number;
  }) {
    const where =
      args.scope.kind === "resource"
        ? `${args.scope.namespaceId}/${args.scope.resourceId}`
        : `namespace ${args.scope.namespaceId}`;
    super(
      `Consolidator distill rate limit exceeded for ${where}: ` +
        `${args.seen}/${args.max} distills in the past ${args.windowMs}ms`,
    );
    this.name = "ConsolidatorRateLimitError";
    this.scope = args.scope;
    this.windowMs = args.windowMs;
    this.max = args.max;
    this.seen = args.seen;
    this.retryAfterMs = args.retryAfterMs;
  }

  /** Seconds to wait before retrying. Suitable for the `Retry-After` HTTP header. */
  retryAfterSeconds(): number {
    return Math.max(1, Math.ceil(this.retryAfterMs / 1000));
  }
}

export class RateLimitedConsolidator implements Consolidator {
  private readonly inner: Consolidator;
  private readonly memory: MemoryStore;
  private readonly limits: ConsolidatorRateLimitConfig;

  constructor(params: {
    readonly inner: Consolidator;
    /** Store holding the distill counts — the one the agents share. */
    readonly memory: MemoryStore;
    readonly limits: ConsolidatorRateLimitConfig;
  }) {
    this.inner = params.inner;
    this.memory = params.memory;
    this.limits = params.limits;
  }

  compactThread(...args: Parameters<Consolidator["compactThread"]>) {
    return this.inner.compactThread(...args);
  }

  /**
   * The resource sweep, each thread distilled through this limiter (so
   * every LLM distill counts). The sweep stops at the first rate-limited
   * thread and returns what it distilled.
   */
  distillResource(key: ScopedKey, opts: DistillResourceOptions = {}): Promise<EpisodicRecord[]> {
    return sweepResourceThreads({
      store: this.memory,
      key,
      opts,
      distill: (thread, distillOpts) => this.distillThread(thread, distillOpts),
      stopOn: (err) => err instanceof ConsolidatorRateLimitError,
    });
  }

  async distillThread(key: ThreadKey, opts?: DistillThreadOptions): Promise<EpisodicRecord> {
    if (!key.resourceId) {
      // Distillation requires a resourceId in the underlying impl
      // anyway; defer to inner so the same error surfaces.
      return this.inner.distillThread(key, opts);
    }
    const resourceKey = { namespaceId: key.namespaceId, resourceId: key.resourceId };
    if (!opts?.force) {
      // Already distilled: the inner call returns the episode without an
      // LLM call, so it costs no slot.
      const existing = await this.memory.listResourceEpisodes(resourceKey, {
        kind: DISTILL_EPISODE_KIND,
        sourceThreadId: key.threadId,
        limit: 1,
      });
      if (existing.length > 0) return this.inner.distillThread(key, opts);
    }

    const reservation = await this.memory.reserveConsolidation({
      ...resourceKey,
      kind: DISTILL_EPISODE_KIND,
      windowMs: this.limits.windowMs,
      maxPerResource: this.limits.maxDistillsPerWindow,
      ...(this.limits.maxDistillsPerNamespacePerWindow !== undefined && {
        maxPerNamespace: this.limits.maxDistillsPerNamespacePerWindow,
      }),
    });
    if (!reservation.granted) {
      throw new ConsolidatorRateLimitError({
        scope:
          reservation.scope === "resource"
            ? { kind: "resource", ...resourceKey }
            : { kind: "namespace", namespaceId: key.namespaceId },
        windowMs: this.limits.windowMs,
        max: reservation.max,
        seen: reservation.seen,
        retryAfterMs: reservation.retryAfterMs,
      });
    }
    try {
      return await this.inner.distillThread(key, opts);
    } catch (err) {
      await this.memory.releaseConsolidation({
        namespaceId: key.namespaceId,
        reservationId: reservation.reservationId,
      });
      throw err;
    }
  }
}
