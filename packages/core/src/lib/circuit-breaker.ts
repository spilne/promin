import { Effect } from "effect";
import { CircuitOpenError } from "./pipeline-error.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type CircuitState =
  | { status: "closed"; failures: number }
  | { status: "open"; openedAt: number }
  | { status: "half-open" };

export interface CircuitBreakerConfig {
  /** Number of consecutive failures before the circuit opens. */
  readonly failureThreshold: number;
  /** Time in ms to wait before transitioning from open to half-open. */
  readonly resetTimeoutMs: number;
  /** Which errors count as failures. Defaults to all errors. */
  readonly isFailure?: (error: any) => boolean;
}

// ---------------------------------------------------------------------------
// CircuitBreaker
// ---------------------------------------------------------------------------

/**
 * Circuit breaker — fail fast when a downstream dependency is broken.
 *
 * States: closed → open (after N failures) → half-open (after cooldown) → closed (on success).
 *
 * @example
 * ```ts
 * const breaker = new CircuitBreaker({
 *   failureThreshold: 5,
 *   resetTimeoutMs: 30_000,
 * });
 *
 * pipeline.withCircuitBreaker(breaker).runPromise();
 * ```
 */
export class CircuitBreaker {
  private state: CircuitState = { status: "closed", failures: 0 };

  constructor(private readonly config: CircuitBreakerConfig) {}

  /** Get the current circuit state. */
  get currentState(): "closed" | "open" | "half-open" {
    if (this.state.status === "open") {
      const elapsed = Date.now() - this.state.openedAt;
      if (elapsed >= this.config.resetTimeoutMs) return "half-open";
    }
    return this.state.status;
  }

  /** Reset the circuit breaker to closed state. */
  reset(): void {
    this.state = { status: "closed", failures: 0 };
  }

  /** Wrap an Effect with circuit breaker logic. */
  protect<T, E>(effect: Effect.Effect<T, E>): Effect.Effect<T, E | CircuitOpenError> {
    return Effect.suspend((): Effect.Effect<T, E | CircuitOpenError> => {
      // Check if open and possibly transition to half-open
      if (this.state.status === "open") {
        const elapsed = Date.now() - this.state.openedAt;
        if (elapsed < this.config.resetTimeoutMs) {
          return Effect.fail(
            new CircuitOpenError({
              message: `Circuit open, resets in ${this.config.resetTimeoutMs - elapsed}ms`,
            }),
          );
        }
        this.state = { status: "half-open" };
      }

      return effect.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            // Success → close circuit
            this.state = { status: "closed", failures: 0 };
          }),
        ),
        Effect.tapError((error) =>
          Effect.sync(() => {
            const shouldCount = this.config.isFailure?.(error) ?? true;
            if (!shouldCount) return;

            if (this.state.status === "half-open") {
              this.state = { status: "open", openedAt: Date.now() };
            } else if (this.state.status === "closed") {
              const failures = this.state.failures + 1;
              if (failures >= this.config.failureThreshold) {
                this.state = { status: "open", openedAt: Date.now() };
              } else {
                this.state = { status: "closed", failures };
              }
            }
          }),
        ),
      );
    });
  }
}
