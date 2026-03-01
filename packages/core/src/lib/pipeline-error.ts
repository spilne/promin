import { Data } from "effect";

/** Pipeline exceeded its timeout budget. */
export class TimeoutError extends Data.TaggedError("TimeoutError")<{
  readonly timeoutMs: number;
  readonly message: string;
}> {}

/** Polling exhausted all attempts or the time budget. */
export class PollTimeoutError extends Data.TaggedError("PollTimeoutError")<{
  readonly attempts: number;
  readonly lastResult: unknown;
  readonly message: string;
}> {}

/** Circuit breaker is open — downstream is unavailable. */
export class CircuitOpenError extends Data.TaggedError("CircuitOpenError")<{
  readonly message: string;
}> {}
