import { Effect, Ref, Data } from "effect";
import { Pipeline, type TaggedError } from "./pipeline.ts";

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

export class RateLimitExceeded extends Data.TaggedError("RateLimitExceeded")<{
  readonly retryAfterMs: number;
}> {}

// ---------------------------------------------------------------------------
// Strategy types
// ---------------------------------------------------------------------------

type Strategy = "sliding-window" | "fixed-window" | "token-bucket";

type SlidingWindowState = {
  readonly _tag: "sliding-window";
  readonly timestamps: readonly number[];
};
type FixedWindowState = {
  readonly _tag: "fixed-window";
  readonly windowStart: number;
  readonly count: number;
};
type TokenBucketState = {
  readonly _tag: "token-bucket";
  readonly tokens: number;
  readonly lastRefill: number;
};
type RateLimiterState = SlidingWindowState | FixedWindowState | TokenBucketState;

type MutableSlidingWindow = { _tag: "sliding-window"; timestamps: number[] };
type MutableFixedWindow = { _tag: "fixed-window"; windowStart: number; count: number };
type MutableTokenBucket = { _tag: "token-bucket"; tokens: number; lastRefill: number };
type MutableRateLimiterState = MutableSlidingWindow | MutableFixedWindow | MutableTokenBucket;

// ---------------------------------------------------------------------------
// State factories
// ---------------------------------------------------------------------------

function createInitialState(strategy: Strategy, limit: number): RateLimiterState {
  switch (strategy) {
    case "sliding-window":
      return { _tag: "sliding-window", timestamps: [] };
    case "fixed-window":
      return { _tag: "fixed-window", windowStart: Date.now(), count: 0 };
    case "token-bucket":
      return { _tag: "token-bucket", tokens: limit, lastRefill: Date.now() };
  }
}

function createMutableState(strategy: Strategy, limit: number): MutableRateLimiterState {
  switch (strategy) {
    case "sliding-window":
      return { _tag: "sliding-window", timestamps: [] };
    case "fixed-window":
      return { _tag: "fixed-window", windowStart: Date.now(), count: 0 };
    case "token-bucket":
      return { _tag: "token-bucket", tokens: limit, lastRefill: Date.now() };
  }
}

// ---------------------------------------------------------------------------
// PipelineRateLimiter
// ---------------------------------------------------------------------------

/** Pluggable rate limiter interface — test against this, implement with any backend. */
export interface RateLimiter {
  acquireAsync(resource?: string): Promise<void>;
  tryAcquireAsync(resource?: string): Promise<boolean>;
  withLimitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T>;
  remainingAsync(resource?: string): Promise<number>;
}

export class PipelineRateLimiter implements RateLimiter {
  private readonly asyncStateMap = new Map<string, MutableRateLimiterState>();

  private constructor(
    private readonly limit: number,
    private readonly windowMs: number,
    private readonly strategy: Strategy,
    private readonly state: Ref.Ref<RateLimiterState>,
  ) {}

  private getAsyncState(resource?: string): MutableRateLimiterState {
    const key = resource ?? "";
    let s = this.asyncStateMap.get(key);
    if (!s) {
      s = createMutableState(this.strategy, this.limit);
      this.asyncStateMap.set(key, s);
    }
    return s;
  }

  static make(params: {
    limit: number;
    windowMs: number;
    strategy?: Strategy;
  }): PipelineRateLimiter {
    const strategy = params.strategy ?? "sliding-window";
    const initialState = createInitialState(strategy, params.limit);
    return new PipelineRateLimiter(
      params.limit,
      params.windowMs,
      strategy,
      Effect.runSync(Ref.make(initialState)),
    );
  }

  // ---------------------------------------------------------------------------
  // Effect API
  // ---------------------------------------------------------------------------

  get acquire(): Effect.Effect<void, RateLimitExceeded> {
    return Effect.flatMap(
      Effect.sync(() => Date.now()),
      (now) =>
        Effect.flatMap(
          Ref.modify(this.state, (s) => this.tryAcquireState(s, now)),
          (result) =>
            result._tag === "ok"
              ? Effect.void
              : Effect.fail(new RateLimitExceeded({ retryAfterMs: result.retryAfterMs })),
        ),
    );
  }

  get tryAcquire(): Effect.Effect<boolean> {
    return Effect.flatMap(
      Effect.sync(() => Date.now()),
      (now) =>
        Ref.modify(this.state, (s): [boolean, RateLimiterState] => {
          const [result, next] = this.tryAcquireState(s, now);
          if (result._tag === "ok") return [true, next];
          return [false, s];
        }),
    );
  }

  withLimit<T, E extends TaggedError>(
    effect: Effect.Effect<T, E>,
  ): Effect.Effect<T, E | RateLimitExceeded> {
    return Effect.andThen(this.acquire, effect);
  }

  withLimitPipeline<T, E extends TaggedError>(
    pipeline: Pipeline<T, E>,
  ): Pipeline<T, E | RateLimitExceeded> {
    return Pipeline.from(this.withLimit(pipeline.effect));
  }

  get remaining(): Effect.Effect<number> {
    return Effect.flatMap(
      Effect.sync(() => Date.now()),
      (now) => Effect.map(Ref.get(this.state), (s) => this.computeRemaining(s, now)),
    );
  }

  get resetAt(): Effect.Effect<number> {
    return Effect.flatMap(
      Effect.sync(() => Date.now()),
      (now) => Effect.map(Ref.get(this.state), (s) => this.computeResetAt(s, now)),
    );
  }

  // ---------------------------------------------------------------------------
  // Promise API
  // ---------------------------------------------------------------------------

  async acquireAsync(resource?: string): Promise<void> {
    const now = Date.now();
    const result = this.tryAcquireMutable(now, false, resource);
    if (result._tag === "rejected") {
      throw new RateLimitExceeded({ retryAfterMs: result.retryAfterMs });
    }
  }

  async tryAcquireAsync(resource?: string): Promise<boolean> {
    const now = Date.now();
    const result = this.tryAcquireMutable(now, true, resource);
    return result._tag === "ok";
  }

  async withLimitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T> {
    await this.acquireAsync(resource);
    return fn();
  }

  async remainingAsync(resource?: string): Promise<number> {
    return this.computeRemainingMutable(Date.now(), resource);
  }

  async resetAtAsync(): Promise<number> {
    return this.computeResetAtMutable(Date.now());
  }

  // ---------------------------------------------------------------------------
  // Strategy logic (immutable — for Effect/Ref)
  // ---------------------------------------------------------------------------

  private tryAcquireState(
    s: RateLimiterState,
    now: number,
  ): [{ _tag: "ok" } | { _tag: "rejected"; retryAfterMs: number }, RateLimiterState] {
    switch (s._tag) {
      case "sliding-window": {
        const cutoff = now - this.windowMs;
        const active = s.timestamps.filter((t) => t > cutoff);
        if (active.length < this.limit) {
          return [{ _tag: "ok" }, { _tag: "sliding-window", timestamps: [...active, now] }];
        }
        const oldest = active[0]!;
        return [
          { _tag: "rejected", retryAfterMs: oldest + this.windowMs - now },
          { _tag: "sliding-window", timestamps: active },
        ];
      }

      case "fixed-window": {
        const windowEnd = s.windowStart + this.windowMs;
        if (now >= windowEnd) {
          return [{ _tag: "ok" }, { _tag: "fixed-window", windowStart: now, count: 1 }];
        }
        if (s.count < this.limit) {
          return [
            { _tag: "ok" },
            { _tag: "fixed-window", windowStart: s.windowStart, count: s.count + 1 },
          ];
        }
        return [{ _tag: "rejected", retryAfterMs: windowEnd - now }, s];
      }

      case "token-bucket": {
        const elapsed = now - s.lastRefill;
        const refillRate = this.limit / this.windowMs;
        const newTokens = Math.min(this.limit, s.tokens + elapsed * refillRate);
        if (newTokens >= 1) {
          return [{ _tag: "ok" }, { _tag: "token-bucket", tokens: newTokens - 1, lastRefill: now }];
        }
        const retryAfterMs = (1 - newTokens) / refillRate;
        return [
          { _tag: "rejected", retryAfterMs },
          { _tag: "token-bucket", tokens: newTokens, lastRefill: now },
        ];
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Strategy logic (mutable — for Promise API)
  // ---------------------------------------------------------------------------

  private tryAcquireMutable(
    now: number,
    dryRun = false,
    resource?: string,
  ): { _tag: "ok" } | { _tag: "rejected"; retryAfterMs: number } {
    const s = this.getAsyncState(resource);

    switch (s._tag) {
      case "sliding-window": {
        const cutoff = now - this.windowMs;
        s.timestamps = s.timestamps.filter((t) => t > cutoff);
        if (s.timestamps.length < this.limit) {
          if (!dryRun) s.timestamps.push(now);
          return { _tag: "ok" };
        }
        const oldest = s.timestamps[0]!;
        return { _tag: "rejected", retryAfterMs: oldest + this.windowMs - now };
      }

      case "fixed-window": {
        const windowEnd = s.windowStart + this.windowMs;
        if (now >= windowEnd) {
          if (!dryRun) {
            s.windowStart = now;
            s.count = 1;
          }
          return { _tag: "ok" };
        }
        if (s.count < this.limit) {
          if (!dryRun) s.count++;
          return { _tag: "ok" };
        }
        return { _tag: "rejected", retryAfterMs: windowEnd - now };
      }

      case "token-bucket": {
        const elapsed = now - s.lastRefill;
        const refillRate = this.limit / this.windowMs;
        s.tokens = Math.min(this.limit, s.tokens + elapsed * refillRate);
        s.lastRefill = now;
        if (s.tokens >= 1) {
          if (!dryRun) s.tokens--;
          return { _tag: "ok" };
        }
        const retryAfterMs = (1 - s.tokens) / refillRate;
        return { _tag: "rejected", retryAfterMs };
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Remaining / resetAt helpers
  // ---------------------------------------------------------------------------

  private computeRemaining(s: RateLimiterState, now: number): number {
    switch (s._tag) {
      case "sliding-window": {
        const active = s.timestamps.filter((t) => t > now - this.windowMs);
        return Math.max(0, this.limit - active.length);
      }
      case "fixed-window": {
        if (now >= s.windowStart + this.windowMs) return this.limit;
        return Math.max(0, this.limit - s.count);
      }
      case "token-bucket": {
        const elapsed = now - s.lastRefill;
        const refillRate = this.limit / this.windowMs;
        return Math.min(this.limit, Math.floor(s.tokens + elapsed * refillRate));
      }
    }
  }

  private computeResetAt(s: RateLimiterState, now: number): number {
    switch (s._tag) {
      case "sliding-window": {
        const active = s.timestamps.filter((t) => t > now - this.windowMs);
        if (active.length === 0) return now;
        return active[0]! + this.windowMs;
      }
      case "fixed-window":
        return s.windowStart + this.windowMs;
      case "token-bucket": {
        const elapsed = now - s.lastRefill;
        const refillRate = this.limit / this.windowMs;
        const current = Math.min(this.limit, s.tokens + elapsed * refillRate);
        if (current >= this.limit) return now;
        return now + (this.limit - current) / refillRate;
      }
    }
  }

  private computeRemainingMutable(now: number, resource?: string): number {
    return this.computeRemaining(this.getAsyncState(resource), now);
  }

  private computeResetAtMutable(now: number, resource?: string): number {
    return this.computeResetAt(this.getAsyncState(resource), now);
  }
}
