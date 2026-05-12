// ---------------------------------------------------------------------------
// RateLimitedConsolidator — decorator that caps `distillThread` cost.
//
// Distillation is one LLM call per fire. With autoDistill on, a busy
// resource can rack up dozens of distill calls in a short window —
// every 6 messages (default messageThreshold) per active thread. The
// rate limiter caps that at the (namespace, resource) layer, throwing
// `ConsolidatorRateLimitError` when the cap is hit.
//
// Source of truth: existing `resource_episodes` rows. We count distill
// episodes (`metadata.kind === "distill"`) with `createdAt` in the
// trailing window — no new storage. Works uniformly across the
// in-memory / sqlite / postgres backends because all three implement
// `listResourceEpisodes`.
//
// Wraps ALL distill paths (auto-trigger + manual gateway + scripts) by
// design — capping only auto leaves a hole.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import type { Consolidator, DistillThreadOptions } from "./consolidator.ts";
import type { EpisodicRecord, MemoryStore, ThreadKey } from "./types.ts";

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
   * `ConsolidatorRateLimitError` until older episodes age out.
   */
  readonly maxDistillsPerWindow: number;
  /** Time source. Default: `SystemClock`. Tests pass `FakeClock`. */
  readonly clock?: Clock;
}

export class ConsolidatorRateLimitError extends Error {
  readonly kind = "distill" as const;
  readonly scope: { readonly namespaceId: string; readonly resourceId: string };
  readonly windowMs: number;
  readonly max: number;
  readonly seen: number;
  /**
   * `createdAt` of the oldest distill episode currently inside the
   * window. The earliest moment another distill could succeed is
   * `oldestInWindowAt + windowMs` (when this episode ages out and
   * `seen` drops to `max - 1`). Gateways turn this into Retry-After.
   */
  readonly oldestInWindowAt: number;

  constructor(args: {
    scope: { namespaceId: string; resourceId: string };
    windowMs: number;
    max: number;
    seen: number;
    oldestInWindowAt: number;
  }) {
    super(
      `Consolidator distill rate limit exceeded for ${args.scope.namespaceId}/${args.scope.resourceId}: ` +
        `${args.seen}/${args.max} distills in the past ${args.windowMs}ms`,
    );
    this.name = "ConsolidatorRateLimitError";
    this.scope = args.scope;
    this.windowMs = args.windowMs;
    this.max = args.max;
    this.seen = args.seen;
    this.oldestInWindowAt = args.oldestInWindowAt;
  }

  /** Seconds to wait before retrying. Suitable for the `Retry-After` HTTP header. */
  retryAfterSeconds(now: number): number {
    return Math.max(1, Math.ceil((this.oldestInWindowAt + this.windowMs - now) / 1000));
  }
}

export class RateLimitedConsolidator implements Consolidator {
  private readonly clock: Clock;

  constructor(
    private readonly inner: Consolidator,
    private readonly memory: MemoryStore,
    private readonly config: ConsolidatorRateLimitConfig,
  ) {
    this.clock = config.clock ?? SystemClock;
  }

  compactThread(...args: Parameters<Consolidator["compactThread"]>) {
    return this.inner.compactThread(...args);
  }

  distillResource(...args: Parameters<Consolidator["distillResource"]>) {
    return this.inner.distillResource(...args);
  }

  async distillThread(key: ThreadKey, opts?: DistillThreadOptions): Promise<EpisodicRecord> {
    if (!key.resourceId) {
      // Distillation requires a resourceId in the underlying impl
      // anyway; defer to inner so the same error surfaces.
      return this.inner.distillThread(key, opts);
    }
    const scope = { namespaceId: key.namespaceId, resourceId: key.resourceId };
    const since = this.clock.currentTimeMs() - this.config.windowMs;
    const recent = await this.memory.listResourceEpisodes(scope, { order: "createdDesc" });
    let seen = 0;
    let oldestInWindowAt = Number.POSITIVE_INFINITY;
    for (const ep of recent) {
      if (ep.createdAt < since) break; // ordered desc — older entries are older
      if ((ep.metadata as { kind?: unknown } | null)?.kind === "distill") {
        seen += 1;
        if (ep.createdAt < oldestInWindowAt) oldestInWindowAt = ep.createdAt;
      }
    }
    if (seen >= this.config.maxDistillsPerWindow) {
      throw new ConsolidatorRateLimitError({
        scope,
        windowMs: this.config.windowMs,
        max: this.config.maxDistillsPerWindow,
        seen,
        oldestInWindowAt,
      });
    }
    return this.inner.distillThread(key, opts);
  }
}
