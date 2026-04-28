// ---------------------------------------------------------------------------
// Classify LLM errors so the resilient wrapper can pick the right
// recovery strategy. Heuristic — different providers surface the same
// underlying conditions in slightly different shapes (Anthropic 429
// with `type: rate_limit_error`, OpenAI 429 with `error.code:
// rate_limit_exceeded`, raw `fetch` connection errors, etc.).
//
// Categories
// ----------
//   transient       — retry the same provider after backoff. Network
//                     errors, 5xx, "overloaded" responses.
//   rate_limit      — quota / RPM exhausted. Retry-with-backoff; if a
//                     fallback provider is configured, switch to it.
//   context_overflow — the prompt is too long for this model. Retrying
//                     the same provider will fail again — only a
//                     fallback (typically a model with a bigger window)
//                     can help.
//   permanent       — auth, malformed request, content policy. Don't
//                     retry; propagate to the caller.
//
// We optimise for false-positive permanent rather than transient: a
// missed transient just costs the user one extra failure, but a
// missed permanent burns retry budget on a request that will never
// succeed. So heuristics here lean conservative on what counts as
// recoverable.
// ---------------------------------------------------------------------------

export type LLMErrorClass = "transient" | "rate_limit" | "context_overflow" | "permanent";

export function classifyLLMError(err: unknown): LLMErrorClass {
  if (err === null || err === undefined) return "permanent";

  // Native fetch / undici / AbortError surfaces — these are typically
  // network-level and almost always recoverable.
  if (err instanceof TypeError) {
    const m = err.message.toLowerCase();
    if (
      m.includes("fetch failed") ||
      m.includes("network") ||
      m.includes("connection") ||
      m.includes("socket") ||
      m.includes("econnreset") ||
      m.includes("econnrefused")
    ) {
      return "transient";
    }
  }

  // Skip aborts — those are intentional cancellations, not failures
  // the caller wants retried.
  if (err instanceof Error && err.name === "AbortError") return "permanent";

  // Provider-shaped error. We look at message + any status code embedded
  // either as a property or in the message itself.
  const msg = err instanceof Error ? err.message : String(err);
  const lower = msg.toLowerCase();

  const status = extractStatusCode(err, lower);

  // Rate limit signals
  if (
    status === 429 ||
    lower.includes("rate limit") ||
    lower.includes("rate_limit") ||
    lower.includes("quota") ||
    lower.includes("rpm exceeded") ||
    lower.includes("too many requests")
  ) {
    return "rate_limit";
  }

  // Context-length signals — Anthropic + OpenAI both phrase this slightly
  // differently. Match conservatively.
  if (
    lower.includes("context length") ||
    lower.includes("context_length") ||
    lower.includes("maximum context") ||
    lower.includes("prompt is too long") ||
    lower.includes("too many tokens") ||
    lower.includes("input is too long")
  ) {
    return "context_overflow";
  }

  // Service-level transient signals
  if (
    (status !== undefined && status >= 500 && status < 600) ||
    lower.includes("overloaded") ||
    lower.includes("temporarily unavailable") ||
    lower.includes("service unavailable") ||
    lower.includes("upstream") ||
    lower.includes("gateway timeout") ||
    lower.includes("remoteprotocolerror") || // ai_coach's specific case
    lower.includes("server disconnected")
  ) {
    return "transient";
  }

  return "permanent";
}

/**
 * Best-effort status-code extraction. Providers attach the status in
 * different places — this looks at the obvious property names first,
 * then falls back to a regex over the message.
 */
function extractStatusCode(err: unknown, lowerMsg: string): number | undefined {
  if (err && typeof err === "object") {
    const obj = err as Record<string, unknown>;
    for (const key of ["status", "statusCode", "code"]) {
      const v = obj[key];
      if (typeof v === "number" && v >= 100 && v < 600) return v;
    }
  }
  // Common phrasings — the message often embeds the status directly.
  // Match the standalone 3-digit code; avoid matching every number.
  const m =
    lowerMsg.match(/\bhttp\s*(\d{3})\b/i) ?? lowerMsg.match(/\bstatus\s*[:=]?\s*(\d{3})\b/i);
  if (m) {
    const n = Number.parseInt(m[1]!, 10);
    if (n >= 100 && n < 600) return n;
  }
  return undefined;
}
