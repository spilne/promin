// ---------------------------------------------------------------------------
// Shared HTTP plumbing for the LLM adapters: the typed errors they throw,
// a per-request abort scope (caller signal + request timeout) and a guarded
// tool-arguments JSON parse.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";

/** Default per-request timeout for every adapter: 10 minutes. */
export const DEFAULT_LLM_REQUEST_TIMEOUT_MS = 600_000;

/** Longest response-body excerpt kept on an {@link LLMHttpError}. */
const BODY_EXCERPT_CHARS = 2_000;

/**
 * A provider answered with an error: a non-2xx HTTP status, or an error
 * event inside a streamed 200 response (mapped to the status the provider
 * would have sent for it, e.g. Anthropic `overloaded_error` → 529).
 *
 * `classifyLLMError` reads `status` and `retryAfterMs` off this error, so
 * retry and failover decisions never depend on message wording.
 */
export class LLMHttpError extends Error {
  /** HTTP status (or the status a streamed error event maps to). */
  readonly status: number;
  /** Server-requested wait before retrying, from `retry-after-ms` / `Retry-After`. */
  readonly retryAfterMs: number | undefined;
  /** Adapter that made the call, e.g. `"anthropic"`, `"openai"`. */
  readonly provider: string;
  /** Leading excerpt of the response body, for diagnostics. */
  readonly body: string;

  constructor(args: { status: number; provider: string; body: string; retryAfterMs?: number }) {
    const body =
      args.body.length > BODY_EXCERPT_CHARS
        ? `${args.body.slice(0, BODY_EXCERPT_CHARS)}…`
        : args.body;
    super(`${args.provider} API error ${args.status}: ${body}`);
    this.name = "LLMHttpError";
    this.status = args.status;
    this.retryAfterMs = args.retryAfterMs;
    this.provider = args.provider;
    this.body = body;
  }
}

/** A request exceeded the adapter's `timeoutMs`. Retryable. */
export class LLMTimeoutError extends Error {
  readonly provider: string;
  readonly timeoutMs: number;

  constructor(args: { provider: string; timeoutMs: number }) {
    super(`${args.provider} request timed out after ${args.timeoutMs}ms`);
    this.name = "LLMTimeoutError";
    this.provider = args.provider;
    this.timeoutMs = args.timeoutMs;
  }
}

/**
 * A streamed response ended before the provider's end-of-message marker:
 * the connection dropped mid-answer. Retryable.
 */
export class LLMStreamInterruptedError extends Error {
  readonly provider: string;

  constructor(args: { provider: string; detail: string }) {
    super(`${args.provider} stream interrupted: ${args.detail}`);
    this.name = "LLMStreamInterruptedError";
    this.provider = args.provider;
  }
}

/**
 * Parse the wait a server asks for: `retry-after-ms` (milliseconds, sent
 * by OpenAI and Anthropic) first, then `Retry-After` as delay-seconds or
 * an HTTP date.
 */
export function parseRetryAfterMs(args: { headers: Headers; nowMs: number }): number | undefined {
  const ms = args.headers.get("retry-after-ms");
  if (ms !== null) {
    const n = Number(ms);
    if (ms.trim() !== "" && Number.isFinite(n) && n >= 0) return n;
  }
  const value = args.headers.get("retry-after");
  if (value === null || value.trim() === "") return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return seconds >= 0 ? seconds * 1000 : undefined;
  const at = Date.parse(value);
  if (!Number.isFinite(at)) return undefined;
  return Math.max(0, at - args.nowMs);
}

/**
 * Build the {@link LLMHttpError} for a non-2xx response, reading a body
 * excerpt and the Retry-After headers.
 */
export async function httpErrorFromResponse(args: {
  provider: string;
  response: Response;
  clock: WallClock;
}): Promise<LLMHttpError> {
  let body = "";
  try {
    body = await args.response.text();
  } catch {
    // Body unreadable (connection dropped) — the status alone still classifies.
  }
  const retryAfterMs = parseRetryAfterMs({
    headers: args.response.headers,
    nowMs: args.clock.currentTimeMs(),
  });
  return new LLMHttpError({
    status: args.response.status,
    provider: args.provider,
    body,
    ...(retryAfterMs !== undefined && { retryAfterMs }),
  });
}

/** The `fetch` shape the adapters call — `globalThis.fetch` or a test fake. */
export type LLMFetch = (url: string, init: RequestInit) => Promise<Response>;

/** Transport settings every HTTP adapter accepts. */
export interface LLMHttpOptions {
  /**
   * Per-request timeout in ms, covering the whole call (for streams,
   * through the last event). Default: {@link DEFAULT_LLM_REQUEST_TIMEOUT_MS}.
   */
  timeoutMs?: number;
  /** Time source for the request timeout and Retry-After dates. Default: `SystemWallClock`. */
  clock?: WallClock;
  /** `fetch` implementation. Default: `globalThis.fetch`. */
  fetch?: LLMFetch;
}

/** Abort scope for one adapter request. */
export interface LLMRequestScope {
  /** Aborts when the caller's signal aborts or the timeout fires. */
  readonly signal: AbortSignal;
  /** Replace an error caused by the timeout firing with an {@link LLMTimeoutError}. */
  mapError(err: unknown): unknown;
  /** Clear the timer and detach from the caller's signal. */
  dispose(): void;
}

/**
 * Start a request scope: one signal that aborts on the caller's `signal`
 * or after `timeoutMs`, whichever comes first. Always `dispose()` it.
 */
export function startLLMRequest(args: {
  provider: string;
  signal?: AbortSignal;
  timeoutMs: number;
  clock: WallClock;
}): LLMRequestScope {
  const controller = new AbortController();
  let timedOut = false;

  const onCallerAbort = () => controller.abort(args.signal?.reason);
  if (args.signal?.aborted) onCallerAbort();
  else args.signal?.addEventListener("abort", onCallerAbort, { once: true });

  const timer = args.clock.setTimeout(() => {
    timedOut = true;
    controller.abort(new LLMTimeoutError({ provider: args.provider, timeoutMs: args.timeoutMs }));
  }, args.timeoutMs);
  timer.unref?.();

  return {
    signal: controller.signal,
    mapError: (err) =>
      timedOut && !(err instanceof LLMTimeoutError)
        ? new LLMTimeoutError({ provider: args.provider, timeoutMs: args.timeoutMs })
        : err,
    dispose: () => {
      timer.clear();
      args.signal?.removeEventListener("abort", onCallerAbort);
    },
  };
}

/** Resolve the transport settings shared by every HTTP adapter. */
export function resolveHttpOptions(options: LLMHttpOptions): {
  timeoutMs: number;
  clock: WallClock;
  fetch: LLMFetch;
} {
  return {
    timeoutMs: options.timeoutMs ?? DEFAULT_LLM_REQUEST_TIMEOUT_MS,
    clock: options.clock ?? SystemWallClock,
    fetch: options.fetch ?? ((input, init) => globalThis.fetch(input, init)),
  };
}

/**
 * Parse a tool call's JSON arguments without throwing. On malformed or
 * truncated JSON returns `{}` as the input plus an `inputError` the agent
 * reports back to the model as the tool result, so the model can retry
 * the call instead of the turn crashing.
 */
export function parseToolArguments(args: { json: string; truncated?: boolean }): {
  input: unknown;
  inputError?: string;
} {
  if (args.json.trim() === "") return { input: {} };
  try {
    return { input: JSON.parse(args.json) as unknown };
  } catch (err) {
    const excerpt = args.json.length > 200 ? `${args.json.slice(0, 200)}…` : args.json;
    const reason = args.truncated
      ? "the response hit the output token limit before the arguments were complete"
      : err instanceof Error
        ? err.message
        : String(err);
    return {
      input: {},
      inputError: `tool arguments are not valid JSON (${reason}). Received: ${excerpt}`,
    };
  }
}
