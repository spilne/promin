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
  /**
   * Optional second cap, applied across ALL resources in a namespace
   * within the same window. The per-resource cap protects per-user
   * cost; this protects per-tenant cost — a namespace with hundreds of
   * resources, each just under its own cap, can still rack up unbounded
   * distill spend. When set, BOTH caps apply; whichever trips first
   * throws (`scope.kind` tells the caller which).
   */
  readonly maxDistillsPerNamespacePerWindow?: number;
  /** Time source. Default: `SystemClock`. Tests pass `FakeClock`. */
  readonly clock?: Clock;
}

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
  /**
   * `createdAt` of the oldest distill episode currently inside the
   * window. The earliest moment another distill could succeed is
   * `oldestInWindowAt + windowMs` (when this episode ages out and
   * `seen` drops to `max - 1`). Gateways turn this into Retry-After.
   */
  readonly oldestInWindowAt: number;

  constructor(args: {
    scope: ConsolidatorRateLimitScope;
    windowMs: number;
    max: number;
    seen: number;
    oldestInWindowAt: number;
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
    const resourceId = key.resourceId;
    const since = this.clock.currentTimeMs() - this.config.windowMs;

    // Per-(namespace, resource) cap — protects per-user cost.
    const resourceEpisodes = await this.memory.listResourceEpisodes(
      { namespaceId: key.namespaceId, resourceId },
      { order: "createdDesc" },
    );
    const perResource = countDistillsInWindow(resourceEpisodes, since);
    if (perResource.seen >= this.config.maxDistillsPerWindow) {
      throw new ConsolidatorRateLimitError({
        scope: { kind: "resource", namespaceId: key.namespaceId, resourceId },
        windowMs: this.config.windowMs,
        max: this.config.maxDistillsPerWindow,
        seen: perResource.seen,
        oldestInWindowAt: perResource.oldestInWindowAt,
      });
    }

    // Optional per-namespace cap — protects per-tenant cost.
    const nsMax = this.config.maxDistillsPerNamespacePerWindow;
    if (nsMax !== undefined) {
      const nsEpisodes = await this.memory.listResourceEpisodesForNamespace(key.namespaceId, {
        order: "createdDesc",
      });
      const perNamespace = countDistillsInWindow(nsEpisodes, since);
      if (perNamespace.seen >= nsMax) {
        throw new ConsolidatorRateLimitError({
          scope: { kind: "namespace", namespaceId: key.namespaceId },
          windowMs: this.config.windowMs,
          max: nsMax,
          seen: perNamespace.seen,
          oldestInWindowAt: perNamespace.oldestInWindowAt,
        });
      }
    }

    return this.inner.distillThread(key, opts);
  }
}

/**
 * Count distill-kind episodes whose `createdAt` falls inside the
 * trailing window. `episodes` must be ordered `createdDesc` — the scan
 * stops at the first entry older than `since`.
 */
function countDistillsInWindow(
  episodes: ReadonlyArray<EpisodicRecord>,
  since: number,
): { seen: number; oldestInWindowAt: number } {
  let seen = 0;
  let oldestInWindowAt = Number.POSITIVE_INFINITY;
  for (const ep of episodes) {
    if (ep.createdAt < since) break; // ordered desc — older entries are older
    if ((ep.metadata as { kind?: unknown } | null)?.kind === "distill") {
      seen += 1;
      if (ep.createdAt < oldestInWindowAt) oldestInWindowAt = ep.createdAt;
    }
  }
  return { seen, oldestInWindowAt };
}
