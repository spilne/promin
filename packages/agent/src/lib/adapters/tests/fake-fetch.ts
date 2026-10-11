// Recorded-response fakes for the LLM adapter tests — no network.

import type { LLMFetch } from "../llm-http.ts";

export interface RecordedRequest {
  url: string;
  body: Record<string, unknown>;
}

/** A `fetch` that answers every call with `respond()` and records the requests. */
export function fakeFetch(
  respond: (signal: AbortSignal | undefined) => Response | Promise<Response>,
): {
  fetch: LLMFetch;
  requests: RecordedRequest[];
} {
  const requests: RecordedRequest[] = [];
  return {
    requests,
    fetch: async (url, init) => {
      requests.push({ url, body: JSON.parse(String(init.body)) as Record<string, unknown> });
      return respond(init.signal ?? undefined);
    },
  };
}

export function jsonResponse(
  body: unknown,
  init: { status?: number; headers?: Record<string, string> } = {},
): Response {
  return new Response(JSON.stringify(body), {
    status: init.status ?? 200,
    headers: { "content-type": "application/json", ...init.headers },
  });
}

/** An SSE response carrying `events` as `data:` lines, then optionally `[DONE]`. */
export function sseResponse(events: unknown[], opts: { done?: boolean } = {}): Response {
  const text =
    events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("") +
    (opts.done ? "data: [DONE]\n\n" : "");
  return new Response(text, { status: 200, headers: { "content-type": "text/event-stream" } });
}

/** A `fetch` that never answers; it rejects with the signal's reason once aborted. */
export function hangingResponse(signal: AbortSignal | undefined): Promise<Response> {
  return new Promise((_, reject) => {
    signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
}

/** Yield to the event loop until `cond()` holds, bounded by iterations. */
export async function waitFor(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !cond(); i++) await new Promise((r) => setTimeout(r, 0));
  if (!cond()) throw new Error("waitFor: condition never held");
}

export async function collect<T>(iter: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of iter) out.push(x);
  return out;
}
